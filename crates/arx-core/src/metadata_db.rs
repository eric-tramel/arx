use crate::{arxiv::PaperMetadata, paths};
use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct MetadataDatabase {
    path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IndexReport {
    pub database_path: String,
    pub scanned_metadata_files: usize,
    pub indexed_papers: usize,
    pub removed_papers: usize,
    #[serde(default)]
    pub indexed_material_chunks: usize,
}

impl MetadataDatabase {
    pub fn new(cache_root: impl AsRef<Path>) -> Self {
        Self {
            path: paths::metadata_db_path(cache_root),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn index_cache(&self, cache_root: impl AsRef<Path>) -> Result<IndexReport> {
        let cache_root = cache_root.as_ref();
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction()
            .context("opening metadata index transaction")?;
        let mut seen = BTreeSet::new();
        let mut scanned_metadata_files = 0;
        let mut indexed_papers = 0;
        let indexed_at_unix_ms = unix_ms()?;

        for metadata_path in metadata_files(cache_root) {
            scanned_metadata_files += 1;
            let metadata = read_metadata(&metadata_path)?;
            let cache_dir = metadata_path.parent().with_context(|| {
                format!(
                    "locating paper cache directory for {}",
                    metadata_path.display()
                )
            })?;
            let safe_id = cache_dir
                .file_name()
                .and_then(|value| value.to_str())
                .map(str::to_string)
                .unwrap_or_else(|| paths::safe_arxiv_id(&metadata.arxiv_id));
            upsert_metadata(
                &transaction,
                &metadata,
                &safe_id,
                cache_dir,
                &metadata_path,
                indexed_at_unix_ms,
            )?;
            seen.insert(metadata.arxiv_id);
            indexed_papers += 1;
        }

        let existing_ids = {
            let mut statement = transaction
                .prepare("SELECT arxiv_id FROM papers")
                .context("preparing stale metadata lookup")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .context("querying indexed metadata ids")?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };

        let mut removed_papers = 0;
        for arxiv_id in existing_ids {
            if !seen.contains(&arxiv_id) {
                removed_papers += transaction
                    .execute("DELETE FROM papers WHERE arxiv_id = ?1", params![&arxiv_id])
                    .with_context(|| format!("removing stale metadata index entry {arxiv_id}"))?;
            }
        }

        transaction.commit().context("committing metadata index")?;
        Ok(IndexReport {
            database_path: self.path.display().to_string(),
            scanned_metadata_files,
            indexed_papers,
            removed_papers,
            indexed_material_chunks: 0,
        })
    }

    pub fn upsert_paper(
        &self,
        cache_root: impl AsRef<Path>,
        metadata: &PaperMetadata,
    ) -> Result<()> {
        let cache_root = cache_root.as_ref();
        let cache_dir = paths::paper_cache_dir(cache_root, &metadata.arxiv_id);
        let metadata_path = cache_dir.join("metadata.json");
        let safe_id = cache_dir
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::to_string)
            .unwrap_or_else(|| paths::safe_arxiv_id(&metadata.arxiv_id));
        let connection = self.connect()?;
        upsert_metadata(
            &connection,
            metadata,
            &safe_id,
            &cache_dir,
            &metadata_path,
            unix_ms()?,
        )
    }

    fn connect(&self) -> Result<Connection> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("creating metadata database directory {}", parent.display())
            })?;
        }
        let connection = Connection::open(&self.path)
            .with_context(|| format!("opening metadata database {}", self.path.display()))?;
        connection
            .busy_timeout(Duration::from_secs(30))
            .context("configuring metadata database busy timeout")?;
        migrate(&connection)?;
        Ok(connection)
    }
}

fn migrate(connection: &Connection) -> Result<()> {
    connection
        .execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS papers (
                arxiv_id TEXT PRIMARY KEY NOT NULL,
                safe_id TEXT NOT NULL,
                cache_dir TEXT NOT NULL,
                metadata_path TEXT NOT NULL,
                abs_url TEXT,
                pdf_url TEXT,
                title TEXT,
                summary TEXT,
                authors_json TEXT NOT NULL,
                published TEXT,
                updated TEXT,
                categories_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                indexed_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS papers_published_idx ON papers(published);
            CREATE INDEX IF NOT EXISTS papers_updated_idx ON papers(updated);
            CREATE INDEX IF NOT EXISTS papers_title_idx ON papers(title);
            -- Full-text material search moved to the Tantivy index under
            -- <cache_root>/search-index/; drop the FTS5 table left behind
            -- by earlier versions.
            DROP TABLE IF EXISTS material_fts;
            "#,
        )
        .context("migrating metadata database")?;
    Ok(())
}

pub(crate) fn metadata_files(cache_root: &Path) -> Vec<PathBuf> {
    let papers_dir = cache_root.join("papers");
    if !papers_dir.exists() {
        return Vec::new();
    }

    WalkDir::new(papers_dir)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file() && entry.file_name() == "metadata.json")
        .map(|entry| entry.into_path())
        .collect()
}

fn read_metadata(path: &Path) -> Result<PaperMetadata> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing metadata JSON {}", path.display()))
}

fn upsert_metadata(
    connection: &Connection,
    metadata: &PaperMetadata,
    safe_id: &str,
    cache_dir: &Path,
    metadata_path: &Path,
    indexed_at_unix_ms: u64,
) -> Result<()> {
    let authors_json =
        serde_json::to_string(&metadata.authors).context("serializing metadata authors")?;
    let categories_json =
        serde_json::to_string(&metadata.categories).context("serializing metadata categories")?;
    let metadata_json = serde_json::to_string(metadata).context("serializing paper metadata")?;
    let cache_dir = cache_dir.display().to_string();
    let metadata_path = metadata_path.display().to_string();
    connection
        .execute(
            r#"
            INSERT INTO papers (
                arxiv_id, safe_id, cache_dir, metadata_path, abs_url, pdf_url, title, summary,
                authors_json, published, updated, categories_json, metadata_json, indexed_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            ON CONFLICT(arxiv_id) DO UPDATE SET
                safe_id = excluded.safe_id,
                cache_dir = excluded.cache_dir,
                metadata_path = excluded.metadata_path,
                abs_url = excluded.abs_url,
                pdf_url = excluded.pdf_url,
                title = excluded.title,
                summary = excluded.summary,
                authors_json = excluded.authors_json,
                published = excluded.published,
                updated = excluded.updated,
                categories_json = excluded.categories_json,
                metadata_json = excluded.metadata_json,
                indexed_at_unix_ms = excluded.indexed_at_unix_ms
            "#,
            params![
                &metadata.arxiv_id,
                safe_id,
                cache_dir,
                metadata_path,
                metadata.abs_url.as_deref(),
                metadata.pdf_url.as_deref(),
                metadata.title.as_deref(),
                metadata.summary.as_deref(),
                authors_json,
                metadata.published.as_deref(),
                metadata.updated.as_deref(),
                categories_json,
                metadata_json,
                indexed_at_unix_ms,
            ],
        )
        .with_context(|| format!("indexing metadata for {}", metadata.arxiv_id))?;
    Ok(())
}

fn unix_ms() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?
        .as_millis() as u64)
}
