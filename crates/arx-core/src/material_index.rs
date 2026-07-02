//! Persistent Tantivy full-text index over cached paper material.
//!
//! The index lives at `<cache_root>/search-index/` and is derived data: it
//! can always be rebuilt from the paper cache (`arx index`), so corruption
//! or version mismatches are handled by wiping the directory and starting
//! over rather than by migration.
//!
//! Process topology: arxd is the only writer (fetch-time incremental updates
//! and full rebuilds), while arx-mcp and arx-cli open the index read-only per
//! query. Tantivy enforces a single `IndexWriter` via a lock file that fails
//! fast instead of waiting, so writer acquisition retries briefly to absorb
//! races around arxd startup.

use crate::paths;
use anyhow::{Context, Result, bail};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tantivy::{
    Index, IndexWriter, TantivyDocument, TantivyError, Term,
    collector::TopDocs,
    directory::MmapDirectory,
    query::{BooleanQuery, Occur, Query, TermQuery},
    schema::{Field, IndexRecordOption, STORED, STRING, Schema, TEXT, Value},
    snippet::SnippetGenerator,
};

/// Bump to force existing indexes to be discarded and rebuilt (e.g. after a
/// schema change or a tantivy upgrade that breaks the on-disk format).
const MATERIAL_INDEX_VERSION: &str = "2";
const VERSION_MARKER_FILE: &str = "arx-index-version";
const WRITER_MEMORY_BYTES: usize = 50_000_000;
const WRITER_LOCK_TIMEOUT: Duration = Duration::from_secs(15);
const WRITER_LOCK_RETRY_DELAY: Duration = Duration::from_millis(100);
const SNIPPET_MAX_CHARS: usize = 400;

/// Tokenize a query the same way tantivy's default tokenizer treats indexed
/// text: split on non-alphanumeric characters and lowercase. This also
/// handles TeX markup — `\section{Deep Learning}` yields `section`, `deep`,
/// `learning`, and `arXiv:2101.00001` yields `arxiv`, `2101`, `00001`.
/// Single-character tokens are dropped because inline math makes them
/// ubiquitous noise.
pub fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|token| token.chars().count() >= 2)
        .map(|token| token.to_lowercase())
        .collect()
}

/// Content category of a material chunk, used for query-time scope filtering.
/// Values: `title`, `metadata`, `body`, `bibliography`.
pub type ChunkCategory = String;

/// Category constants — use these when constructing MaterialChunk values.
pub mod category {
    pub const TITLE: &str = "title";
    pub const METADATA: &str = "metadata";
    pub const BODY: &str = "body";
    pub const BIBLIOGRAPHY: &str = "bibliography";
}

/// One indexable unit of paper material: a metadata field, a citation
/// record, or a paragraph of TeX/source text.
#[derive(Debug, Clone)]
pub struct MaterialChunk {
    pub source: String,
    /// Content category for scope-based filtering (title/metadata/body/bibliography).
    pub category: ChunkCategory,
    pub field: Option<String>,
    pub path: Option<String>,
    pub line_start: Option<usize>,
    pub line_end: Option<usize>,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CorpusSearchResult {
    pub arxiv_id: String,
    pub source: String,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub line_start: Option<usize>,
    #[serde(default)]
    pub line_end: Option<usize>,
    pub snippet: String,
    #[schemars(description = "BM25 relevance score; results are sorted best-first.")]
    pub score: f64,
}

#[derive(Debug, Clone, Copy)]
struct MaterialFields {
    text: Field,
    arxiv_id: Field,
    source: Field,
    category: Field,
    field: Field,
    path: Field,
    line_start: Field,
    line_end: Field,
}

fn material_schema() -> (Schema, MaterialFields) {
    let mut builder = Schema::builder();
    let fields = MaterialFields {
        text: builder.add_text_field("text", TEXT | STORED),
        arxiv_id: builder.add_text_field("arxiv_id", STRING | STORED),
        source: builder.add_text_field("source", STORED),
        // Raw filterable field for scope-based query filtering (title/metadata/body/bibliography).
        category: builder.add_text_field("category", STRING | STORED),
        field: builder.add_text_field("field", STORED),
        path: builder.add_text_field("path", STORED),
        line_start: builder.add_u64_field("line_start", STORED),
        line_end: builder.add_u64_field("line_end", STORED),
    };
    (builder.build(), fields)
}

#[derive(Clone)]
pub struct MaterialIndex {
    index: Index,
    fields: MaterialFields,
}

impl MaterialIndex {
    pub fn open_or_create(cache_root: impl AsRef<Path>) -> Result<Self> {
        let dir = paths::search_index_dir(cache_root);
        match Self::try_open(&dir) {
            Ok(index) => Ok(index),
            Err(_) => {
                // The index is derived data; on version mismatch or
                // corruption, discard and start empty. `arx index` rebuilds.
                if dir.exists() {
                    fs::remove_dir_all(&dir).with_context(|| {
                        format!("clearing stale search index {}", dir.display())
                    })?;
                }
                Self::try_open(&dir)
            }
        }
    }

    fn try_open(dir: &PathBuf) -> Result<Self> {
        fs::create_dir_all(dir)
            .with_context(|| format!("creating search index directory {}", dir.display()))?;
        let marker = dir.join(VERSION_MARKER_FILE);
        if marker.exists() {
            let version = fs::read_to_string(&marker)
                .with_context(|| format!("reading {}", marker.display()))?;
            if version.trim() != MATERIAL_INDEX_VERSION {
                bail!(
                    "search index version {} does not match expected {MATERIAL_INDEX_VERSION}",
                    version.trim()
                );
            }
        } else {
            fs::write(&marker, MATERIAL_INDEX_VERSION)
                .with_context(|| format!("writing {}", marker.display()))?;
        }
        let (schema, fields) = material_schema();
        let directory = MmapDirectory::open(dir)
            .with_context(|| format!("opening search index directory {}", dir.display()))?;
        let index = Index::open_or_create(directory, schema)
            .with_context(|| format!("opening search index {}", dir.display()))?;
        Ok(Self { index, fields })
    }

    /// Replace all indexed chunks for one paper (delete-by-id then insert,
    /// committed atomically from the reader's perspective).
    pub fn replace_paper(&self, arxiv_id: &str, chunks: &[MaterialChunk]) -> Result<usize> {
        let mut writer = self.writer()?;
        writer.delete_term(Term::from_field_text(self.fields.arxiv_id, arxiv_id));
        for chunk in chunks {
            writer
                .add_document(self.document(arxiv_id, chunk))
                .context("adding material chunk to search index")?;
        }
        writer
            .commit()
            .context("committing material index update")?;
        Ok(chunks.len())
    }

    /// Rebuild the whole index from scratch. Stale papers disappear because
    /// only what is passed in survives.
    pub fn rebuild(&self, papers: &[(String, Vec<MaterialChunk>)]) -> Result<usize> {
        let mut writer = self.writer()?;
        writer
            .delete_all_documents()
            .context("clearing search index for rebuild")?;
        let mut total = 0;
        for (arxiv_id, chunks) in papers {
            for chunk in chunks {
                writer
                    .add_document(self.document(arxiv_id, chunk))
                    .context("adding material chunk to search index")?;
            }
            total += chunks.len();
        }
        writer.commit().context("committing search index rebuild")?;
        Ok(total)
    }

    /// BM25-ranked search, best first. `query_tokens` are OR-combined so
    /// multi-term queries rank by relevance without requiring every term;
    /// `arxiv_id` optionally restricts results to one paper; `categories`
    /// optionally restricts results to chunks with matching category values.
    pub fn search(
        &self,
        query_tokens: &[String],
        arxiv_id: Option<&str>,
        categories: Option<&[&str]>,
        limit: usize,
    ) -> Result<Vec<CorpusSearchResult>> {
        if query_tokens.is_empty() {
            return Ok(Vec::new());
        }
        let reader = self.index.reader().context("opening search index reader")?;
        let searcher = reader.searcher();

        let token_clauses: Vec<(Occur, Box<dyn Query>)> = query_tokens
            .iter()
            .map(|token| {
                (
                    Occur::Should,
                    Box::new(TermQuery::new(
                        Term::from_field_text(self.fields.text, token),
                        IndexRecordOption::WithFreqsAndPositions,
                    )) as Box<dyn Query>,
                )
            })
            .collect();
        let token_query = BooleanQuery::new(token_clauses);

        // Build filter clauses (Must): arxiv_id and/or category scope.
        let mut must_clauses: Vec<(Occur, Box<dyn Query>)> =
            vec![(Occur::Must, Box::new(token_query) as Box<dyn Query>)];
        if let Some(arxiv_id) = arxiv_id {
            must_clauses.push((
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(self.fields.arxiv_id, arxiv_id),
                    IndexRecordOption::Basic,
                )),
            ));
        }
        if let Some(cats) = categories {
            let category_clauses: Vec<(Occur, Box<dyn Query>)> = cats
                .iter()
                .map(|cat| {
                    (
                        Occur::Should,
                        Box::new(TermQuery::new(
                            Term::from_field_text(self.fields.category, cat),
                            IndexRecordOption::Basic,
                        )) as Box<dyn Query>,
                    )
                })
                .collect();
            must_clauses.push((Occur::Must, Box::new(BooleanQuery::new(category_clauses))));
        }
        let query: Box<dyn Query> = if must_clauses.len() == 1 {
            // No filters: unwrap the single Must token query directly.
            // BooleanQuery with a single Must clause still works, but using
            // the inner query directly preserves scoring behavior.
            let (_, q) = must_clauses.remove(0);
            q
        } else {
            Box::new(BooleanQuery::new(must_clauses))
        };

        let top_docs = searcher
            .search(&query, &TopDocs::with_limit(limit).order_by_score())
            .context("searching material index")?;
        let mut snippets = SnippetGenerator::create(&searcher, &*query, self.fields.text)
            .context("creating snippet generator")?;
        snippets.set_max_num_chars(SNIPPET_MAX_CHARS);

        top_docs
            .into_iter()
            .map(|(score, address)| {
                let document: TantivyDocument = searcher
                    .doc(address)
                    .context("loading search index document")?;
                Ok(self.to_result(&document, &snippets, score))
            })
            .collect()
    }

    pub fn chunk_count(&self) -> Result<u64> {
        let reader = self.index.reader().context("opening search index reader")?;
        Ok(reader.searcher().num_docs())
    }

    /// Number of indexed chunks for one paper; 0 means the paper has never
    /// been indexed (or was pruned) and a self-heal reindex is warranted.
    pub fn paper_chunk_count(&self, arxiv_id: &str) -> Result<u64> {
        let reader = self.index.reader().context("opening search index reader")?;
        let query = TermQuery::new(
            Term::from_field_text(self.fields.arxiv_id, arxiv_id),
            IndexRecordOption::Basic,
        );
        let count = reader
            .searcher()
            .search(&query, &tantivy::collector::Count)
            .context("counting indexed chunks for paper")?;
        Ok(count as u64)
    }

    fn document(&self, arxiv_id: &str, chunk: &MaterialChunk) -> TantivyDocument {
        let mut document = TantivyDocument::new();
        document.add_text(self.fields.text, &chunk.text);
        document.add_text(self.fields.arxiv_id, arxiv_id);
        document.add_text(self.fields.source, &chunk.source);
        document.add_text(self.fields.category, &chunk.category);
        if let Some(field) = &chunk.field {
            document.add_text(self.fields.field, field);
        }
        if let Some(path) = &chunk.path {
            document.add_text(self.fields.path, path);
        }
        if let Some(line) = chunk.line_start {
            document.add_u64(self.fields.line_start, line as u64);
        }
        if let Some(line) = chunk.line_end {
            document.add_u64(self.fields.line_end, line as u64);
        }
        document
    }

    fn to_result(
        &self,
        document: &TantivyDocument,
        snippets: &SnippetGenerator,
        score: f32,
    ) -> CorpusSearchResult {
        let text_field = |field: Field| {
            document
                .get_first(field)
                .and_then(|value| value.as_str())
                .map(str::to_string)
        };
        let line_field = |field: Field| {
            document
                .get_first(field)
                .and_then(|value| value.as_u64())
                .map(|line| line as usize)
        };
        let text = text_field(self.fields.text).unwrap_or_default();
        let fragment = snippets.snippet_from_doc(document).fragment().to_string();
        let snippet = if fragment.trim().is_empty() {
            truncate_chars(&text, SNIPPET_MAX_CHARS)
        } else {
            clean_ws(&fragment)
        };
        let (line_start, line_end) = snippet_lines(
            line_field(self.fields.line_start),
            line_field(self.fields.line_end),
            &text,
            &fragment,
        );
        CorpusSearchResult {
            arxiv_id: text_field(self.fields.arxiv_id).unwrap_or_default(),
            source: text_field(self.fields.source).unwrap_or_default(),
            category: text_field(self.fields.category),
            field: text_field(self.fields.field),
            path: text_field(self.fields.path),
            line_start,
            line_end,
            snippet,
            score: round_score(score as f64),
        }
    }

    /// Tantivy's writer lock fails fast (no SQLite-style busy timeout), so
    /// retry briefly; arxd is the designed single writer and contention only
    /// occurs around process races.
    fn writer(&self) -> Result<IndexWriter> {
        let start = Instant::now();
        loop {
            match self.index.writer(WRITER_MEMORY_BYTES) {
                Ok(writer) => return Ok(writer),
                Err(TantivyError::LockFailure(..)) if start.elapsed() < WRITER_LOCK_TIMEOUT => {
                    std::thread::sleep(WRITER_LOCK_RETRY_DELAY);
                }
                Err(error) => {
                    return Err(error).context("acquiring search index writer lock");
                }
            }
        }
    }
}

/// Narrow a chunk's line range to the lines the snippet fragment actually
/// covers, so agents can jump straight to the match in the file. The
/// fragment is a contiguous slice of the stored chunk text, so its position
/// locates it; chunks can span many lines (e.g. a long .bbl block), making
/// the full chunk range too coarse for "go read the file" follow-ups.
fn snippet_lines(
    chunk_start: Option<usize>,
    chunk_end: Option<usize>,
    text: &str,
    fragment: &str,
) -> (Option<usize>, Option<usize>) {
    let Some(chunk_start) = chunk_start else {
        return (chunk_start, chunk_end);
    };
    if fragment.is_empty() {
        return (Some(chunk_start), chunk_end);
    }
    let Some(offset) = text.find(fragment) else {
        return (Some(chunk_start), chunk_end);
    };
    let first = chunk_start + text[..offset].matches('\n').count();
    let last = first + fragment.matches('\n').count();
    (Some(first), Some(last))
}

fn clean_ws(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let cleaned = clean_ws(value);
    if cleaned.chars().count() <= max_chars {
        cleaned
    } else {
        let truncated: String = cleaned.chars().take(max_chars).collect();
        format!("{truncated}…")
    }
}

fn round_score(score: f64) -> f64 {
    (score * 1e4).round() / 1e4
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn chunk(text: &str) -> MaterialChunk {
        MaterialChunk {
            source: "source".to_string(),
            category: category::BODY.to_string(),
            field: None,
            path: Some("main.tex".to_string()),
            line_start: Some(1),
            line_end: Some(2),
            text: text.to_string(),
        }
    }

    #[test]
    fn tokenize_handles_tex_markup() {
        assert_eq!(
            tokenize(r"\section{Deep Learning} $x_i$ arXiv:2101.00001"),
            vec!["section", "deep", "learning", "arxiv", "2101", "00001"]
        );
    }

    #[test]
    fn tokenize_lowercases_and_drops_single_characters() {
        assert_eq!(
            tokenize("A Calibration THEOREM"),
            vec!["calibration", "theorem"]
        );
    }

    #[test]
    fn replace_paper_is_idempotent_and_searchable() -> Result<()> {
        let temp = tempdir()?;
        let index = MaterialIndex::open_or_create(temp.path())?;
        index.replace_paper("2401.11111", &[chunk("stochastic gradient descent")])?;
        index.replace_paper("2401.11111", &[chunk("stochastic gradient descent")])?;
        assert_eq!(index.chunk_count()?, 1);

        let results = index.search(&["stochastic".to_string()], None, None, 10)?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].arxiv_id, "2401.11111");
        assert_eq!(results[0].line_start, Some(1));
        assert!(results[0].score > 0.0);
        Ok(())
    }

    #[test]
    fn search_filters_by_arxiv_id() -> Result<()> {
        let temp = tempdir()?;
        let index = MaterialIndex::open_or_create(temp.path())?;
        index.replace_paper("2401.11111", &[chunk("attention transformers")])?;
        index.replace_paper("2401.22222", &[chunk("attention mechanisms everywhere")])?;

        let all = index.search(&["attention".to_string()], None, None, 10)?;
        assert_eq!(all.len(), 2);
        let filtered = index.search(&["attention".to_string()], Some("2401.22222"), None, 10)?;
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].arxiv_id, "2401.22222");
        Ok(())
    }

    #[test]
    fn search_narrows_line_range_to_the_matched_fragment() -> Result<()> {
        let temp = tempdir()?;
        let index = MaterialIndex::open_or_create(temp.path())?;
        // A 30-line chunk long enough (> 400 snippet chars) that the
        // fragment cannot cover the whole text; "zebra" sits on line 25
        // of the file (chunk starts at line 1).
        let lines: Vec<String> = (1..=30)
            .map(|line| {
                if line == 25 {
                    "the zebra stripes theorem appears here".to_string()
                } else {
                    format!("filler prose line {line} with padding words")
                }
            })
            .collect();
        index.replace_paper(
            "2401.11111",
            &[MaterialChunk {
                source: "source".to_string(),
                category: category::BODY.to_string(),
                field: None,
                path: Some("main.tex".to_string()),
                line_start: Some(1),
                line_end: Some(30),
                text: lines.join("\n"),
            }],
        )?;

        let results = index.search(&["zebra".to_string()], None, None, 10)?;
        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert!(result.snippet.contains("zebra"));
        let start = result.line_start.unwrap();
        let end = result.line_end.unwrap();
        assert!((start..=end).contains(&25), "range {start}-{end}");
        assert!(end - start < 29, "range {start}-{end} was not narrowed");
        Ok(())
    }

    #[test]
    fn rebuild_drops_papers_not_passed_in() -> Result<()> {
        let temp = tempdir()?;
        let index = MaterialIndex::open_or_create(temp.path())?;
        index.replace_paper("2401.11111", &[chunk("calibration")])?;
        index.rebuild(&[("2401.22222".to_string(), vec![chunk("transformers")])])?;
        assert_eq!(index.chunk_count()?, 1);
        assert!(
            index
                .search(&["calibration".to_string()], None, None, 10)?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn version_mismatch_wipes_and_recreates_index() -> Result<()> {
        let temp = tempdir()?;
        let index = MaterialIndex::open_or_create(temp.path())?;
        index.replace_paper("2401.11111", &[chunk("calibration")])?;
        drop(index);

        fs::write(
            paths::search_index_dir(temp.path()).join(VERSION_MARKER_FILE),
            "0",
        )?;
        let reopened = MaterialIndex::open_or_create(temp.path())?;
        assert_eq!(reopened.chunk_count()?, 0);
        Ok(())
    }

    fn chunk_with_category(text: &str, cat: &str) -> MaterialChunk {
        MaterialChunk {
            source: "source".to_string(),
            category: cat.to_string(),
            field: None,
            path: Some("main.tex".to_string()),
            line_start: Some(1),
            line_end: Some(1),
            text: text.to_string(),
        }
    }

    #[test]
    fn scope_default_excludes_bibliography_chunks() -> Result<()> {
        let temp = tempdir()?;
        let index = MaterialIndex::open_or_create(temp.path())?;
        index.replace_paper(
            "2401.11111",
            &[
                chunk_with_category("attention transformer body content", category::BODY),
                chunk_with_category("attention transformer bib entry", category::BIBLIOGRAPHY),
                chunk_with_category("attention transformer title", category::TITLE),
                chunk_with_category("attention transformer metadata", category::METADATA),
            ],
        )?;

        // Default scope: title + metadata + body — no bibliography.
        let default_cats: &[&str] = &[category::TITLE, category::METADATA, category::BODY];
        let results = index.search(&["attention".to_string()], None, Some(default_cats), 10)?;
        assert_eq!(
            results.len(),
            3,
            "default scope should exclude bibliography"
        );
        assert!(
            results
                .iter()
                .all(|r| r.category.as_deref() != Some(category::BIBLIOGRAPHY)),
            "bibliography chunk must not appear in default scope"
        );
        Ok(())
    }

    #[test]
    fn scope_bibliography_returns_only_bibliography_chunks() -> Result<()> {
        let temp = tempdir()?;
        let index = MaterialIndex::open_or_create(temp.path())?;
        index.replace_paper(
            "2401.11111",
            &[
                chunk_with_category("deep learning body paragraph", category::BODY),
                chunk_with_category("deep learning bib reference entry", category::BIBLIOGRAPHY),
            ],
        )?;

        let bib_cats: &[&str] = &[category::BIBLIOGRAPHY];
        let results = index.search(&["deep".to_string()], None, Some(bib_cats), 10)?;
        assert_eq!(
            results.len(),
            1,
            "bibliography scope should return only bibliography chunks"
        );
        assert_eq!(results[0].category.as_deref(), Some(category::BIBLIOGRAPHY));
        Ok(())
    }

    #[test]
    fn scope_all_returns_bibliography_and_body() -> Result<()> {
        let temp = tempdir()?;
        let index = MaterialIndex::open_or_create(temp.path())?;
        index.replace_paper(
            "2401.11111",
            &[
                chunk_with_category("gradient descent body", category::BODY),
                chunk_with_category("gradient descent bib entry", category::BIBLIOGRAPHY),
            ],
        )?;

        // "all" scope passes no category filter (None).
        let results = index.search(&["gradient".to_string()], None, None, 10)?;
        assert_eq!(results.len(), 2, "all scope should return every category");
        Ok(())
    }

    #[test]
    fn scope_titles_returns_only_title_chunks() -> Result<()> {
        let temp = tempdir()?;
        let index = MaterialIndex::open_or_create(temp.path())?;
        index.replace_paper(
            "2401.11111",
            &[
                chunk_with_category(
                    "Stochastic gradient descent convergence title",
                    category::TITLE,
                ),
                chunk_with_category("stochastic gradient descent body paragraph", category::BODY),
            ],
        )?;

        let title_cats: &[&str] = &[category::TITLE];
        let results = index.search(&["stochastic".to_string()], None, Some(title_cats), 10)?;
        assert_eq!(
            results.len(),
            1,
            "titles scope should return only title chunks"
        );
        assert_eq!(results[0].category.as_deref(), Some(category::TITLE));
        Ok(())
    }
}
