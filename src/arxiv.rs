use crate::{paths, rate_limit::RateLimiter};
use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufWriter, Cursor, Read, Write},
    path::{Path, PathBuf},
    sync::LazyLock,
};
use walkdir::WalkDir;

static ARXIV_ID_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^(?:\d{4}\.\d{4,5}|[a-z][a-z0-9-]*(?:\.[a-z]{2})?/\d{7})(?:v\d+)?$")
        .expect("valid arXiv id regex")
});

static PREFIXED_CITATION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?ix)(?:arxiv\s*:\s*|arxiv\.org/(?:abs|pdf|e-print)/)(?P<id>(?:\d{4}\.\d{4,5}|[a-z][a-z0-9-]*(?:\.[a-z]{2})?/\d{7})(?:v\d+)?)",
    )
    .expect("valid prefixed citation regex")
});

static BIBTEX_EPRINT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?ix)eprint\s*=\s*[\{\"]?\s*(?P<id>(?:\d{4}\.\d{4,5}|[a-z][a-z0-9-]*(?:\.[a-z]{2})?/\d{7})(?:v\d+)?)\s*[\}\"]?"#,
    )
    .expect("valid BibTeX eprint regex")
});

#[derive(Debug, Clone)]
pub struct ArxivFetcher {
    cache_root: PathBuf,
    client: reqwest::Client,
    rate_limiter: RateLimiter,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FetchPaperRequest {
    #[schemars(
        description = "arXiv identifier, with or without a version suffix, e.g. 2401.12345v2 or hep-th/9901001"
    )]
    pub arxiv_id: String,
    #[serde(default)]
    #[schemars(description = "Download/cache the PDF. Defaults to true.")]
    pub include_pdf: Option<bool>,
    #[serde(default)]
    #[schemars(
        description = "Download/cache the TeX/source bundle and derive citations.jsonl. Defaults to true."
    )]
    pub include_source: Option<bool>,
    #[serde(default)]
    #[schemars(description = "Ignore existing cached files and fetch them again.")]
    pub refresh: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LocatePaperRequest {
    #[schemars(description = "arXiv identifier, with or without a version suffix")]
    pub arxiv_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FetchPaperResponse {
    pub arxiv_id: String,
    pub cache_dir: String,
    pub metadata_path: String,
    pub pdf_path: Option<String>,
    pub source_archive_path: Option<String>,
    pub source_extracted_dir: Option<String>,
    pub citations_jsonl_path: Option<String>,
    pub title: Option<String>,
    pub authors: Vec<String>,
    pub citation_count: usize,
    pub cache_hit: bool,
    pub network_requests: usize,
    pub rate_limit_delay_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LocatePaperResponse {
    pub arxiv_id: String,
    pub cache_dir: String,
    pub exists: bool,
    pub metadata_path: Option<String>,
    pub pdf_path: Option<String>,
    pub source_archive_path: Option<String>,
    pub source_extracted_dir: Option<String>,
    pub citations_jsonl_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperMetadata {
    pub arxiv_id: String,
    pub abs_url: Option<String>,
    pub pdf_url: Option<String>,
    pub title: Option<String>,
    pub authors: Vec<String>,
    pub summary: Option<String>,
    pub published: Option<String>,
    pub updated: Option<String>,
    pub categories: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceManifest {
    source_archive_path: String,
    source_extracted_dir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CitationRecord {
    citing_arxiv_id: String,
    cited_arxiv_id: String,
    source_file: String,
    line: usize,
    context: String,
}

#[derive(Debug, Clone, Copy)]
pub struct FetchOptions {
    pub include_pdf: bool,
    pub include_source: bool,
    pub refresh: bool,
}

impl From<&FetchPaperRequest> for FetchOptions {
    fn from(request: &FetchPaperRequest) -> Self {
        Self {
            include_pdf: request.include_pdf.unwrap_or(true),
            include_source: request.include_source.unwrap_or(true),
            refresh: request.refresh.unwrap_or(false),
        }
    }
}

impl ArxivFetcher {
    pub fn new(cache_root: PathBuf) -> Result<Self> {
        fs::create_dir_all(&cache_root)
            .with_context(|| format!("creating cache root {}", cache_root.display()))?;
        let user_agent = std::env::var("ARX_USER_AGENT")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| {
                format!("arx/{} (local MCP arXiv cache)", env!("CARGO_PKG_VERSION"))
            });
        let client = reqwest::Client::builder()
            .user_agent(user_agent)
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .context("building HTTP client")?;
        Ok(Self {
            rate_limiter: RateLimiter::new(&cache_root),
            cache_root,
            client,
        })
    }

    pub fn cache_root(&self) -> &Path {
        &self.cache_root
    }

    pub async fn fetch(&self, request: FetchPaperRequest) -> Result<FetchPaperResponse> {
        let arxiv_id = normalize_arxiv_id(&request.arxiv_id)?;
        let options = FetchOptions::from(&request);
        let paths = PaperPaths::new(&self.cache_root, &arxiv_id);
        fs::create_dir_all(&paths.cache_dir)
            .with_context(|| format!("creating paper cache {}", paths.cache_dir.display()))?;

        if !options.refresh && paths.is_complete(options) {
            return self.cached_response(&arxiv_id, &paths);
        }

        let mut network_requests = 0;
        let metadata = if options.refresh || !paths.metadata_path.exists() {
            network_requests += 1;
            let metadata = self.fetch_metadata(&arxiv_id).await?;
            write_json_pretty(&paths.metadata_path, &metadata)?;
            metadata
        } else {
            read_json(&paths.metadata_path)?
        };

        if options.include_pdf && (options.refresh || !paths.pdf_path.exists()) {
            network_requests += 1;
            let pdf = self.download_bytes(pdf_url(&arxiv_id)).await?;
            write_bytes_atomic(&paths.pdf_path, &pdf)?;
        }

        if options.include_source && (options.refresh || !paths.source_manifest_path.exists()) {
            network_requests += 1;
            let bytes = self.download_bytes(source_url(&arxiv_id)).await?;
            let manifest = materialize_source(&paths, &bytes)?;
            write_json_pretty(&paths.source_manifest_path, &manifest)?;
            let citation_count = extract_citations(&arxiv_id, &paths, &manifest)?;
            return Ok(FetchPaperResponse {
                arxiv_id,
                cache_dir: display_path(&paths.cache_dir),
                metadata_path: display_path(&paths.metadata_path),
                pdf_path: options.include_pdf.then(|| display_path(&paths.pdf_path)),
                source_archive_path: Some(manifest.source_archive_path),
                source_extracted_dir: manifest.source_extracted_dir,
                citations_jsonl_path: Some(display_path(&paths.citations_path)),
                title: metadata.title,
                authors: metadata.authors,
                citation_count,
                cache_hit: false,
                network_requests,
                rate_limit_delay_seconds: crate::rate_limit::ARXIV_DELAY.as_secs(),
            });
        }

        let citation_count = count_jsonl_records(&paths.citations_path).unwrap_or(0);
        let manifest = read_manifest_if_present(&paths)?;
        Ok(FetchPaperResponse {
            arxiv_id,
            cache_dir: display_path(&paths.cache_dir),
            metadata_path: display_path(&paths.metadata_path),
            pdf_path: options.include_pdf.then(|| display_path(&paths.pdf_path)),
            source_archive_path: manifest
                .as_ref()
                .map(|manifest| manifest.source_archive_path.clone()),
            source_extracted_dir: manifest.and_then(|manifest| manifest.source_extracted_dir),
            citations_jsonl_path: options
                .include_source
                .then(|| display_path(&paths.citations_path)),
            title: metadata.title,
            authors: metadata.authors,
            citation_count,
            cache_hit: false,
            network_requests,
            rate_limit_delay_seconds: crate::rate_limit::ARXIV_DELAY.as_secs(),
        })
    }

    pub fn locate(&self, request: LocatePaperRequest) -> Result<LocatePaperResponse> {
        let arxiv_id = normalize_arxiv_id(&request.arxiv_id)?;
        let paths = PaperPaths::new(&self.cache_root, &arxiv_id);
        let manifest = read_manifest_if_present(&paths)?;
        Ok(LocatePaperResponse {
            arxiv_id,
            cache_dir: display_path(&paths.cache_dir),
            exists: paths.cache_dir.exists(),
            metadata_path: paths
                .metadata_path
                .exists()
                .then(|| display_path(&paths.metadata_path)),
            pdf_path: paths
                .pdf_path
                .exists()
                .then(|| display_path(&paths.pdf_path)),
            source_archive_path: manifest
                .as_ref()
                .map(|manifest| manifest.source_archive_path.clone()),
            source_extracted_dir: manifest.and_then(|manifest| manifest.source_extracted_dir),
            citations_jsonl_path: paths
                .citations_path
                .exists()
                .then(|| display_path(&paths.citations_path)),
        })
    }

    fn cached_response(&self, arxiv_id: &str, paths: &PaperPaths) -> Result<FetchPaperResponse> {
        let metadata: PaperMetadata = read_json(&paths.metadata_path)?;
        let manifest = read_manifest_if_present(paths)?;
        Ok(FetchPaperResponse {
            arxiv_id: arxiv_id.to_string(),
            cache_dir: display_path(&paths.cache_dir),
            metadata_path: display_path(&paths.metadata_path),
            pdf_path: paths
                .pdf_path
                .exists()
                .then(|| display_path(&paths.pdf_path)),
            source_archive_path: manifest
                .as_ref()
                .map(|manifest| manifest.source_archive_path.clone()),
            source_extracted_dir: manifest.and_then(|manifest| manifest.source_extracted_dir),
            citations_jsonl_path: paths
                .citations_path
                .exists()
                .then(|| display_path(&paths.citations_path)),
            title: metadata.title,
            authors: metadata.authors,
            citation_count: count_jsonl_records(&paths.citations_path).unwrap_or(0),
            cache_hit: true,
            network_requests: 0,
            rate_limit_delay_seconds: crate::rate_limit::ARXIV_DELAY.as_secs(),
        })
    }

    async fn fetch_metadata(&self, arxiv_id: &str) -> Result<PaperMetadata> {
        let text = self
            .rate_limiter
            .run(|| async {
                let response = self
                    .client
                    .get("https://export.arxiv.org/api/query")
                    .query(&[("id_list", arxiv_id), ("max_results", "1")])
                    .send()
                    .await
                    .context("requesting arXiv metadata")?
                    .error_for_status()
                    .context("arXiv metadata returned an error status")?;
                response.text().await.context("reading arXiv metadata")
            })
            .await?;
        parse_metadata(arxiv_id, &text)
    }

    async fn download_bytes(&self, url: String) -> Result<Vec<u8>> {
        self.rate_limiter
            .run(|| async {
                let response = self
                    .client
                    .get(&url)
                    .send()
                    .await
                    .with_context(|| format!("requesting {url}"))?
                    .error_for_status()
                    .with_context(|| format!("{url} returned an error status"))?;
                Ok(response.bytes().await?.to_vec())
            })
            .await
    }
}

#[derive(Debug, Clone)]
struct PaperPaths {
    cache_dir: PathBuf,
    metadata_path: PathBuf,
    pdf_path: PathBuf,
    source_dir: PathBuf,
    source_extracted_dir: PathBuf,
    source_manifest_path: PathBuf,
    citations_path: PathBuf,
}

impl PaperPaths {
    fn new(cache_root: impl Into<PathBuf>, arxiv_id: &str) -> Self {
        let cache_dir = paths::paper_cache_dir(cache_root, arxiv_id);
        let source_dir = cache_dir.join("source");
        Self {
            metadata_path: cache_dir.join("metadata.json"),
            pdf_path: cache_dir.join("paper.pdf"),
            source_extracted_dir: source_dir.join("extracted"),
            source_manifest_path: source_dir.join("manifest.json"),
            citations_path: cache_dir.join("citations.jsonl"),
            source_dir,
            cache_dir,
        }
    }

    fn is_complete(&self, options: FetchOptions) -> bool {
        self.metadata_path.exists()
            && (!options.include_pdf || self.pdf_path.exists())
            && (!options.include_source
                || (self.source_manifest_path.exists() && self.citations_path.exists()))
    }
}

pub fn normalize_arxiv_id(input: &str) -> Result<String> {
    let mut value = input.trim().trim_matches(|ch: char| ch == '<' || ch == '>');
    let lower = value.to_ascii_lowercase();
    if lower.starts_with("arxiv:") {
        value = value[6..].trim();
    } else if let Some(index) = lower.find("arxiv.org/") {
        value = &value[index + "arxiv.org/".len()..];
        let lower_value = value.to_ascii_lowercase();
        for prefix in ["abs/", "pdf/", "e-print/"] {
            if lower_value.starts_with(prefix) {
                value = &value[prefix.len()..];
                break;
            }
        }
    }

    let value = value
        .split(['?', '#'])
        .next()
        .unwrap_or(value)
        .trim()
        .trim_end_matches(".pdf")
        .trim_end_matches('/');

    if ARXIV_ID_RE.is_match(value) {
        Ok(value.to_string())
    } else {
        bail!("invalid arXiv id: {input}")
    }
}

fn base_arxiv_id(arxiv_id: &str) -> String {
    let version_re = Regex::new(r"(?i)v\d+$").expect("valid version regex");
    version_re.replace(arxiv_id, "").to_string()
}

fn pdf_url(arxiv_id: &str) -> String {
    format!("https://export.arxiv.org/pdf/{arxiv_id}")
}

fn source_url(arxiv_id: &str) -> String {
    format!("https://export.arxiv.org/e-print/{arxiv_id}")
}

fn parse_metadata(requested_id: &str, atom: &str) -> Result<PaperMetadata> {
    let doc = roxmltree::Document::parse(atom).context("parsing arXiv Atom metadata")?;
    let entry = doc
        .descendants()
        .find(|node| node.has_tag_name("entry"))
        .context("arXiv metadata did not contain an entry")?;

    let entry_id = child_text(entry, "id");
    let arxiv_id = entry_id
        .as_deref()
        .and_then(|url| normalize_arxiv_id(url).ok())
        .unwrap_or_else(|| requested_id.to_string());
    let title = child_text(entry, "title").map(clean_ws);
    let summary = child_text(entry, "summary").map(clean_ws);
    let published = child_text(entry, "published");
    let updated = child_text(entry, "updated");
    let authors = entry
        .children()
        .filter(|node| node.has_tag_name("author"))
        .filter_map(|author| child_text(author, "name"))
        .map(clean_ws)
        .collect();
    let categories = entry
        .children()
        .filter(|node| node.has_tag_name("category"))
        .filter_map(|category| category.attribute("term").map(str::to_string))
        .collect();
    let pdf_url = entry
        .children()
        .filter(|node| node.has_tag_name("link"))
        .find(|link| {
            link.attribute("title") == Some("pdf")
                || link.attribute("type") == Some("application/pdf")
        })
        .and_then(|link| link.attribute("href"))
        .map(str::to_string);

    Ok(PaperMetadata {
        arxiv_id,
        abs_url: entry_id,
        pdf_url,
        title,
        authors,
        summary,
        published,
        updated,
        categories,
    })
}

fn child_text(node: roxmltree::Node<'_, '_>, name: &str) -> Option<String> {
    node.children()
        .find(|child| child.has_tag_name(name))
        .and_then(|child| child.text())
        .map(str::to_string)
}

fn clean_ws(value: String) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn materialize_source(paths: &PaperPaths, bytes: &[u8]) -> Result<SourceManifest> {
    if paths.source_dir.exists() {
        fs::remove_dir_all(&paths.source_dir)
            .with_context(|| format!("clearing source directory {}", paths.source_dir.display()))?;
    }
    fs::create_dir_all(&paths.source_dir)
        .with_context(|| format!("creating source directory {}", paths.source_dir.display()))?;

    let (archive_path, extracted_dir) = if is_gzip(bytes) {
        let archive_path = paths.source_dir.join("e-print.tar.gz");
        write_bytes_atomic(&archive_path, bytes)?;
        let decompressed = gzip_decode(bytes)?;
        if unpack_tar(&decompressed, &paths.source_extracted_dir).is_ok() {
            (archive_path, Some(paths.source_extracted_dir.clone()))
        } else {
            let source_path = paths.source_dir.join("source.tex");
            write_bytes_atomic(&source_path, &decompressed)?;
            (archive_path, None)
        }
    } else if unpack_tar(bytes, &paths.source_extracted_dir).is_ok() {
        let archive_path = paths.source_dir.join("e-print.tar");
        write_bytes_atomic(&archive_path, bytes)?;
        (archive_path, Some(paths.source_extracted_dir.clone()))
    } else {
        let source_path = if std::str::from_utf8(bytes).is_ok() {
            paths.source_dir.join("source.tex")
        } else {
            paths.source_dir.join("e-print")
        };
        write_bytes_atomic(&source_path, bytes)?;
        (source_path, None)
    };

    Ok(SourceManifest {
        source_archive_path: display_path(&archive_path),
        source_extracted_dir: extracted_dir.as_ref().map(display_path),
    })
}

fn is_gzip(bytes: &[u8]) -> bool {
    bytes.starts_with(&[0x1f, 0x8b])
}

fn gzip_decode(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(Cursor::new(bytes));
    let mut decoded = Vec::new();
    decoder
        .read_to_end(&mut decoded)
        .context("decompressing arXiv source")?;
    Ok(decoded)
}

fn unpack_tar(bytes: &[u8], destination: &Path) -> Result<()> {
    if destination.exists() {
        fs::remove_dir_all(destination)
            .with_context(|| format!("clearing extraction directory {}", destination.display()))?;
    }
    fs::create_dir_all(destination)
        .with_context(|| format!("creating extraction directory {}", destination.display()))?;
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    archive
        .unpack(destination)
        .with_context(|| format!("unpacking source archive into {}", destination.display()))?;
    Ok(())
}

fn extract_citations(
    citing_arxiv_id: &str,
    paths: &PaperPaths,
    manifest: &SourceManifest,
) -> Result<usize> {
    let mut records: BTreeMap<String, CitationRecord> = BTreeMap::new();
    let own_base = base_arxiv_id(citing_arxiv_id);

    let mut roots = Vec::new();
    if let Some(extracted) = &manifest.source_extracted_dir {
        roots.push(PathBuf::from(extracted));
    }
    roots.push(paths.source_dir.clone());

    for root in roots {
        if !root.exists() {
            continue;
        }
        for entry in WalkDir::new(&root).follow_links(false) {
            let entry =
                entry.with_context(|| format!("walking source directory {}", root.display()))?;
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path();
            if path == paths.source_manifest_path || path == paths.citations_path {
                continue;
            }
            let Ok(bytes) = fs::read(path) else {
                continue;
            };
            if bytes.len() > 20 * 1024 * 1024 {
                continue;
            }
            let Ok(text) = std::str::from_utf8(&bytes) else {
                continue;
            };
            collect_prefixed_citations(citing_arxiv_id, &own_base, path, text, &mut records)?;
            collect_bibtex_eprint_citations(citing_arxiv_id, &own_base, path, text, &mut records)?;
        }
    }

    if let Some(parent) = paths.citations_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating citation directory {}", parent.display()))?;
    }
    let mut writer = BufWriter::new(
        File::create(&paths.citations_path)
            .with_context(|| format!("creating {}", paths.citations_path.display()))?,
    );
    for record in records.values() {
        serde_json::to_writer(&mut writer, record).context("serializing citation record")?;
        writer
            .write_all(b"\n")
            .context("writing citation newline")?;
    }
    writer.flush().context("flushing citation JSONL")?;
    Ok(records.len())
}

fn collect_prefixed_citations(
    citing_arxiv_id: &str,
    own_base: &str,
    path: &Path,
    text: &str,
    records: &mut BTreeMap<String, CitationRecord>,
) -> Result<()> {
    for captures in PREFIXED_CITATION_RE.captures_iter(text) {
        if let Some(id) = captures.name("id") {
            add_citation(
                citing_arxiv_id,
                own_base,
                path,
                text,
                id.start(),
                id.as_str(),
                records,
            )?;
        }
    }
    Ok(())
}

fn collect_bibtex_eprint_citations(
    citing_arxiv_id: &str,
    own_base: &str,
    path: &Path,
    text: &str,
    records: &mut BTreeMap<String, CitationRecord>,
) -> Result<()> {
    for captures in BIBTEX_EPRINT_RE.captures_iter(text) {
        if let Some(id) = captures.name("id") {
            let start = id.start();
            let window_start = start.saturating_sub(512);
            let window_end = (id.end() + 512).min(text.len());
            if text[window_start..window_end]
                .to_ascii_lowercase()
                .contains("arxiv")
            {
                add_citation(
                    citing_arxiv_id,
                    own_base,
                    path,
                    text,
                    start,
                    id.as_str(),
                    records,
                )?;
            }
        }
    }
    Ok(())
}

fn add_citation(
    citing_arxiv_id: &str,
    own_base: &str,
    path: &Path,
    text: &str,
    start: usize,
    raw_id: &str,
    records: &mut BTreeMap<String, CitationRecord>,
) -> Result<()> {
    let cited_arxiv_id = normalize_arxiv_id(raw_id)?;
    let cited_base = base_arxiv_id(&cited_arxiv_id);
    if cited_base.eq_ignore_ascii_case(own_base) {
        return Ok(());
    }
    records.entry(cited_base).or_insert_with(|| CitationRecord {
        citing_arxiv_id: citing_arxiv_id.to_string(),
        cited_arxiv_id,
        source_file: display_path(path),
        line: line_number(text, start),
        context: context_line(text, start),
    });
    Ok(())
}

fn line_number(text: &str, start: usize) -> usize {
    text[..start].bytes().filter(|byte| *byte == b'\n').count() + 1
}

fn context_line(text: &str, start: usize) -> String {
    let line_start = text[..start]
        .rfind('\n')
        .map(|index| index + 1)
        .unwrap_or(0);
    let line_end = text[start..]
        .find('\n')
        .map(|offset| start + offset)
        .unwrap_or(text.len());
    clean_ws(text[line_start..line_end].to_string())
}

fn read_manifest_if_present(paths: &PaperPaths) -> Result<Option<SourceManifest>> {
    if paths.source_manifest_path.exists() {
        Ok(Some(read_json(&paths.source_manifest_path)?))
    } else {
        Ok(None)
    }
}

fn count_jsonl_records(path: &Path) -> Result<usize> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(text.lines().filter(|line| !line.trim().is_empty()).count())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing JSON {}", path.display()))
}

fn write_json_pretty<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value).context("serializing JSON")?;
    write_bytes_atomic(path, &bytes)
}

fn write_bytes_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    let tmp_path = path.with_extension("tmp");
    fs::write(&tmp_path, bytes).with_context(|| format!("writing {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("renaming {} to {}", tmp_path.display(), path.display()))?;
    Ok(())
}

fn display_path(path: impl AsRef<Path>) -> String {
    path.as_ref().display().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::{collections::BTreeSet, fs, io::Write};
    use tempfile::tempdir;

    #[test]
    fn normalize_arxiv_id_accepts_raw_urls_old_style_and_rejects_invalid() {
        let valid_cases = [
            ("2401.12345", "2401.12345"),
            (" arXiv:2401.12345v2 ", "2401.12345v2"),
            (
                "https://arxiv.org/abs/2401.12345v2#references",
                "2401.12345v2",
            ),
            (
                "https://arxiv.org/pdf/hep-th/9901001.pdf?download=1",
                "hep-th/9901001",
            ),
            ("hep-th/9901001", "hep-th/9901001"),
        ];

        for (input, expected) in valid_cases {
            assert_eq!(normalize_arxiv_id(input).unwrap(), expected, "{input}");
        }

        for input in [
            "",
            "not an arxiv id",
            "2401.123",
            "https://example.com/2401.12345",
        ] {
            assert!(
                normalize_arxiv_id(input).is_err(),
                "{input} should be rejected"
            );
        }
    }

    #[test]
    fn locate_maps_normalized_id_to_safe_cache_path_without_fetching() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;

        let located = fetcher.locate(LocatePaperRequest {
            arxiv_id: "https://arxiv.org/pdf/hep-th/9901001.pdf?download=1".to_string(),
        })?;

        assert_eq!(located.arxiv_id, "hep-th/9901001");
        assert_eq!(
            PathBuf::from(&located.cache_dir),
            temp.path().join("papers").join("hep-th_9901001")
        );
        assert!(!located.exists);
        assert!(located.metadata_path.is_none());
        assert!(located.pdf_path.is_none());
        assert!(located.citations_jsonl_path.is_none());
        Ok(())
    }

    #[test]
    fn extract_citations_writes_unique_non_self_jsonl_from_source_bundle() -> Result<()> {
        let temp = tempdir()?;
        let citing_id = "2401.12345v2";
        let paths = PaperPaths::new(temp.path(), citing_id);
        let manifest = materialize_source(&paths, &synthetic_source_bundle()?)?;

        let count = extract_citations(citing_id, &paths, &manifest)?;

        assert_eq!(count, 3);
        let jsonl = fs::read_to_string(&paths.citations_path)?;
        let records = jsonl
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<serde_json::Result<Vec<_>>>()?;
        assert_eq!(records.len(), 3);

        let cited_ids = records
            .iter()
            .map(|record| record["cited_arxiv_id"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            cited_ids,
            BTreeSet::from(["2101.00001v2", "2203.12345", "hep-th/9901001"])
        );
        assert!(records.iter().all(|record| {
            !record["cited_arxiv_id"]
                .as_str()
                .unwrap()
                .starts_with("2401.12345")
        }));
        assert_eq!(
            records
                .iter()
                .filter(|record| record["cited_arxiv_id"]
                    .as_str()
                    .unwrap()
                    .starts_with("2101.00001"))
                .count(),
            1
        );

        let bib_record = records
            .iter()
            .find(|record| record["cited_arxiv_id"] == "2203.12345")
            .expect("BibTeX eprint citation should be present");
        assert!(
            bib_record["source_file"]
                .as_str()
                .unwrap()
                .ends_with("refs.bib")
        );
        assert!(bib_record["context"].as_str().unwrap().contains("eprint"));

        for record in records {
            assert_eq!(record["citing_arxiv_id"], citing_id);
            assert!(record["line"].as_u64().unwrap() > 0);
        }
        Ok(())
    }

    fn synthetic_source_bundle() -> Result<Vec<u8>> {
        let tex = r#"
See arXiv:2101.00001v2 and later https://arxiv.org/abs/2101.00001v3.
Do not count our own earlier version arXiv:2401.12345v1.
Old-style citations still appear at https://arxiv.org/pdf/hep-th/9901001.
"#;
        let bib = r#"
@article{with_arxiv_eprint,
  archivePrefix = {arXiv},
  eprint = {2203.12345},
}

@article{self_citation,
  archivePrefix = {arXiv},
  eprint = {2401.12345},
}
"#;
        let mut bytes = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut bytes);
            append_tar_file(&mut builder, "main.tex", tex)?;
            append_tar_file(&mut builder, "refs.bib", bib)?;
            builder.finish()?;
        }
        Ok(bytes)
    }

    fn append_tar_file<W: Write>(
        builder: &mut tar::Builder<W>,
        path: &str,
        contents: &str,
    ) -> Result<()> {
        let mut header = tar::Header::new_gnu();
        header.set_size(contents.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append_data(&mut header, path, contents.as_bytes())?;
        Ok(())
    }
}
