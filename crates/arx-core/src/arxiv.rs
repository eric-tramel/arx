use crate::{
    material_index::{CorpusSearchResult, MaterialChunk, MaterialIndex, category},
    metadata_db::{IndexReport, MetadataDatabase},
    paths,
    rate_limit::{MetadataHealth, RateLimiter},
};
use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufWriter, Cursor, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex as StdMutex},
    time::Duration,
};
use walkdir::WalkDir;
const DOWNLOAD_MAX_ATTEMPTS: usize = 3;
const METADATA_URL: &str = "https://export.arxiv.org/api/query";
const MATERIAL_BASE_URL: &str = "https://arxiv.org";
/// Fail-fast bound on metadata requests. Applied per-request (not on the shared
/// client) because PDF and source downloads legitimately take longer.
const METADATA_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Backoff schedule for metadata retries on the queued download path.
/// Interactive lookups never retry; background jobs wait these durations
/// between attempts (attempts = waits + 1).
const METADATA_RETRY_WAITS: [Duration; 2] = [Duration::from_secs(5), Duration::from_secs(15)];
/// How long every arx process backs off after arXiv answers 429.
const RATE_LIMIT_PENALTY: Duration = Duration::from_secs(30);
/// Bound on establishing any arXiv connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// Bound on silence between read chunks on any arXiv response. Unlike a
/// total-request timeout this never fails a large-but-progressing PDF or
/// source download, but a stalled connection can no longer hold the shared
/// rate-limit lock (and with it the whole download queue) forever.
const READ_STALL_TIMEOUT: Duration = Duration::from_secs(60);
/// Most arXiv ids sent in one batched metadata query.
const METADATA_BATCH_LIMIT: usize = 50;

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
    metadata_db: MetadataDatabase,
    metadata_url: String,
    material_base_url: String,
    metadata_timeout: Duration,
    metadata_retry_waits: Vec<Duration>,
    /// Single-flight guard so concurrent download workers coalesce into one
    /// batched metadata request instead of issuing one request per job.
    metadata_prefetch_lock: Arc<tokio::sync::Mutex<()>>,
    /// arXiv ids whose material was fetched while the metadata API was
    /// unavailable; `ensure_metadata_cached` backfills these opportunistically.
    pending_metadata: Arc<StdMutex<BTreeSet<String>>>,
}

/// Typed error raised while the shared circuit breaker has arXiv metadata
/// requests paused after repeated systemic failures. Material (PDF/source)
/// downloads are NOT affected: they use a different arXiv host and queued
/// jobs proceed without metadata, which backfills automatically later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataPausedError {
    pub paused_until_unix_ms: u64,
}

impl std::fmt::Display for MetadataPausedError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "arXiv metadata requests are paused until unix_ms {} after repeated arXiv metadata failures; \
             material downloads continue and metadata backfills automatically once arXiv recovers",
            self.paused_until_unix_ms
        )
    }
}

impl std::error::Error for MetadataPausedError {}

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
    pub metadata_db_path: String,
    pub indexed_metadata_records: usize,
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
    #[serde(default)]
    #[schemars(
        description = "True when material was fetched while arXiv's metadata API was unavailable. The download itself succeeded; metadata backfills automatically on a later fetch or lookup once arXiv recovers."
    )]
    pub metadata_pending: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PaperMetadata {
    pub arxiv_id: String,
    #[serde(default)]
    pub abs_url: Option<String>,
    #[serde(default)]
    pub pdf_url: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub authors: Vec<String>,
    #[serde(default)]
    pub author_details: Vec<PaperAuthor>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub published: Option<String>,
    #[serde(default)]
    pub updated: Option<String>,
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(default)]
    pub primary_category: Option<String>,
    #[serde(default)]
    pub comment: Option<String>,
    #[serde(default)]
    pub journal_ref: Option<String>,
    #[serde(default)]
    pub doi: Option<String>,
    #[serde(default)]
    pub links: Vec<AtomLink>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PaperAuthor {
    pub name: String,
    #[serde(default)]
    pub affiliations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AtomLink {
    pub href: String,
    #[serde(default)]
    pub rel: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LookupPapersRequest {
    #[schemars(description = "One or more arXiv identifiers to claim/lookup.")]
    pub arxiv_ids: Vec<String>,
    #[serde(default)]
    #[schemars(description = "Fetch missing metadata through the arXiv API. Defaults to true.")]
    pub fetch_missing_metadata: Option<bool>,
    #[serde(default)]
    #[schemars(description = "Refresh metadata even when metadata.json already exists.")]
    pub refresh_metadata: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LookupPapersResponse {
    pub papers: Vec<PaperMaterialStatus>,
    pub fetched_metadata_count: usize,
    pub network_requests: usize,
    pub rate_limit_delay_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MaterialStatusRequest {
    #[schemars(description = "arXiv identifier, with or without a version suffix")]
    pub arxiv_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PaperMaterialStatus {
    pub arxiv_id: String,
    pub base_arxiv_id: String,
    pub version: Option<u16>,
    pub publication_year: Option<u16>,
    pub paths: PaperLocalPaths,
    pub material_state: PaperMaterialStates,
    pub available_now: Vec<String>,
    pub missing: Vec<String>,
    pub metadata: Option<PaperMetadata>,
    pub citation_count: usize,
    pub next_tool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Metadata fetch error for this paper, if any. Other papers in the same batch are unaffected."
    )]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PaperLocalPaths {
    pub cache_dir: String,
    pub metadata_path: String,
    pub pdf_path: String,
    pub source_dir: String,
    pub source_manifest_path: String,
    pub source_archive_path: Option<String>,
    pub source_extracted_dir: Option<String>,
    pub citations_jsonl_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PaperMaterialStates {
    pub metadata: MaterialState,
    pub abstract_text: MaterialState,
    pub pdf_file: MaterialState,
    pub source_archive: MaterialState,
    pub source_tree: MaterialState,
    pub citations: MaterialState,
    pub source_search: MaterialState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MaterialState {
    Missing,
    Ready,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FullTextSearchRequest {
    #[schemars(description = "Free-text query, BM25-ranked across locally cached paper material.")]
    pub query: String,
    #[serde(default)]
    #[schemars(description = "Optional arXiv id to restrict results to a single paper.")]
    pub arxiv_id: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    #[schemars(
        description = "Search scope: \"default\" (title+metadata+body, no bibliography), \"titles\" (title only), \"bibliography\" (bibliography only), \"all\" (everything). Omitting scope is the same as \"default\"."
    )]
    pub scope: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FullTextSearchResponse {
    pub query: String,
    #[schemars(description = "Total material chunks in the persistent index.")]
    pub indexed_chunks: u64,
    pub results: Vec<CorpusSearchResult>,
    #[schemars(
        description = "Effective search scope used for this query (default/titles/bibliography/all)."
    )]
    pub scope: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Explanatory note when results are empty; describes why and what to do next."
    )]
    pub note: Option<String>,
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
            // A request with no bound can hold the shared rate-limit lock
            // forever and freeze every arx process; stall-based bounds fail
            // hung connections without capping large slow downloads.
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_STALL_TIMEOUT)
            .build()
            .context("building HTTP client")?;
        let metadata_db = MetadataDatabase::new(&cache_root);
        Ok(Self {
            rate_limiter: RateLimiter::new(&cache_root),
            metadata_db,
            cache_root,
            client,
            metadata_url: METADATA_URL.to_string(),
            material_base_url: MATERIAL_BASE_URL.to_string(),
            metadata_timeout: METADATA_REQUEST_TIMEOUT,
            metadata_retry_waits: METADATA_RETRY_WAITS.to_vec(),
            metadata_prefetch_lock: Arc::new(tokio::sync::Mutex::new(())),
            pending_metadata: Arc::new(StdMutex::new(BTreeSet::new())),
        })
    }

    pub fn cache_root(&self) -> &Path {
        &self.cache_root
    }

    pub fn metadata_database_path(&self) -> &Path {
        self.metadata_db.path()
    }

    pub fn index(&self) -> Result<IndexReport> {
        self.metadata_db.index_cache(&self.cache_root)
    }

    pub async fn fetch(&self, request: FetchPaperRequest) -> Result<FetchPaperResponse> {
        let response = self.fetch_inner(request).await?;
        if !response.cache_hit {
            self.index_paper_material(&response.arxiv_id)?;
        }
        Ok(response)
    }

    /// Re-chunk one paper's cached material and replace its documents in
    /// the persistent Tantivy index. Keyed by the metadata's canonical
    /// arXiv id so fetch-time and rescan indexing converge on the same rows.
    pub fn index_paper_material(&self, arxiv_id: &str) -> Result<usize> {
        let arxiv_id = normalize_arxiv_id(arxiv_id)?;
        let paths = PaperPaths::new(&self.cache_root, &arxiv_id);
        let (canonical_id, chunks) = paper_material_for_index(&paths, &arxiv_id)?;
        MaterialIndex::open_or_create(&self.cache_root)?.replace_paper(&canonical_id, &chunks)
    }

    /// Rescan cached metadata and rebuild the material index from scratch
    /// for every cached paper; papers removed from the cache drop out of
    /// the index. Heavier than `index`; runs on explicit reindex requests.
    pub fn index_with_material(&self) -> Result<IndexReport> {
        let mut report = self.index()?;
        let mut papers = Vec::new();
        for metadata_path in crate::metadata_db::metadata_files(&self.cache_root) {
            let Some(cache_dir) = metadata_path.parent() else {
                continue;
            };
            let paths = PaperPaths::from_cache_dir(cache_dir.to_path_buf());
            let fallback_id = cache_dir
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_string();
            papers.push(paper_material_for_index(&paths, &fallback_id)?);
        }
        report.indexed_material_chunks =
            MaterialIndex::open_or_create(&self.cache_root)?.rebuild(&papers)?;
        Ok(report)
    }

    /// BM25-ranked full-text search over locally cached paper material via
    /// the persistent Tantivy index; `arxiv_id` optionally narrows the scope
    /// to one paper. Self-heals the index when it is missing content that
    /// the paper cache has, so callers never manage indexing explicitly.
    pub fn full_text_search(
        &self,
        request: FullTextSearchRequest,
    ) -> Result<FullTextSearchResponse> {
        let query = request.query.trim();
        if query.is_empty() {
            bail!("search query must not be empty");
        }
        let query_tokens = crate::material_index::tokenize(query);
        if query_tokens.is_empty() {
            bail!("search query must contain at least one term of two or more characters");
        }
        let limit = request.limit.unwrap_or(20).clamp(1, 100);
        let arxiv_id = request
            .arxiv_id
            .as_deref()
            .map(normalize_arxiv_id)
            .transpose()?
            .map(|arxiv_id| base_arxiv_id(&arxiv_id));

        // Resolve and validate the scope.
        let scope_str = request.scope.as_deref().unwrap_or("default");
        let scope_categories: Option<Vec<&str>> = match scope_str {
            "default" => Some(vec![category::TITLE, category::METADATA, category::BODY]),
            "titles" => Some(vec![category::TITLE]),
            "bibliography" => Some(vec![category::BIBLIOGRAPHY]),
            "all" => None,
            other => bail!(
                "unknown scope {:?}; valid values are \"default\", \"titles\", \"bibliography\", \"all\"",
                other
            ),
        };

        let index = MaterialIndex::open_or_create(&self.cache_root)?;

        match arxiv_id.as_deref() {
            Some(arxiv_id) => {
                let paths = PaperPaths::new(&self.cache_root, arxiv_id);
                if index.paper_chunk_count(arxiv_id)? == 0 && paths.cache_dir.exists() {
                    self.index_paper_material(arxiv_id)?;
                }
            }
            None => {
                if index.chunk_count()? == 0
                    && !crate::metadata_db::metadata_files(&self.cache_root).is_empty()
                {
                    self.index_with_material()?;
                }
            }
        }

        let cats_ref: Option<&[&str]> = scope_categories.as_deref();
        let results = index.search(&query_tokens, arxiv_id.as_deref(), cats_ref, limit)?;
        let indexed_chunks = index.chunk_count()?;

        // Build explanatory note when results are empty (P4).
        let note = if results.is_empty() {
            self.empty_results_note(arxiv_id.as_deref(), scope_str)
        } else {
            None
        };

        Ok(FullTextSearchResponse {
            query: query.to_string(),
            indexed_chunks,
            results,
            scope: scope_str.to_string(),
            note,
        })
    }

    /// Construct a short diagnostic note explaining why a search returned no
    /// results and what the caller should do next.
    fn empty_results_note(&self, arxiv_id: Option<&str>, scope: &str) -> Option<String> {
        if let Some(arxiv_id) = arxiv_id {
            let paths = PaperPaths::new(&self.cache_root, arxiv_id);
            if !paths.cache_dir.exists() {
                return Some(format!(
                    "paper {arxiv_id} is not cached; call fetch_arxiv_paper to download and index it"
                ));
            }
            // Cache dir exists: check whether source material is present.
            let manifest = read_or_infer_manifest(&paths).ok().flatten();
            if !has_searchable_source_material(&paths, manifest.as_ref()) {
                // Only metadata/abstract is indexed.
                if scope != "default" && scope != "all" {
                    return Some(format!(
                        "only metadata/abstract are indexed for {arxiv_id} and scope is \"{scope}\"; fetch TeX source with fetch_arxiv_paper or use scope \"default\""
                    ));
                }
                return Some(format!(
                    "only metadata/abstract are indexed for {arxiv_id}; fetch TeX source with fetch_arxiv_paper to search the body"
                ));
            }
        }
        // Non-default scope note.
        if scope != "default" {
            return Some(format!(
                "no results in scope \"{scope}\"; try scope \"default\" or \"all\" to widen the search"
            ));
        }
        Some(
            "no results in the default scope, which excludes bibliography content; \
             use scope \"all\" to also search .bib/.bbl files and citation records"
                .to_string(),
        )
    }

    async fn fetch_inner(&self, request: FetchPaperRequest) -> Result<FetchPaperResponse> {
        let arxiv_id = normalize_arxiv_id(&request.arxiv_id)?;
        let options = FetchOptions::from(&request);
        let index_report = self.index()?;
        let paths = PaperPaths::new(&self.cache_root, &arxiv_id);
        fs::create_dir_all(&paths.cache_dir)
            .with_context(|| format!("creating paper cache {}", paths.cache_dir.display()))?;

        if !options.refresh && paths.is_complete(options) {
            return self.cached_response(&arxiv_id, &paths, index_report.indexed_papers);
        }

        let mut network_requests = 0;
        let mut metadata_pending = false;
        let metadata: Option<PaperMetadata> = if !options.refresh && paths.metadata_path.exists() {
            Some(read_json(&paths.metadata_path)?)
        } else {
            network_requests += 1;
            match self.fetch_metadata(&arxiv_id).await {
                Ok(metadata) => {
                    write_json_pretty(&paths.metadata_path, &metadata)?;
                    self.clear_pending_metadata(&arxiv_id);
                    Some(metadata)
                }
                // Metadata lives on export.arxiv.org; PDFs and source live on
                // arxiv.org. A metadata outage must not block material
                // downloads — proceed without it and backfill later.
                Err(error) if is_recoverable_metadata_outage(&error) => {
                    if paths.metadata_path.exists() {
                        tracing::warn!(
                            arxiv_id = %arxiv_id,
                            error = %format!("{error:#}"),
                            "metadata refresh failed; keeping the cached copy"
                        );
                        Some(read_json(&paths.metadata_path)?)
                    } else {
                        tracing::warn!(
                            arxiv_id = %arxiv_id,
                            error = %format!("{error:#}"),
                            "arXiv metadata unavailable; downloading material without it and backfilling later"
                        );
                        metadata_pending = true;
                        self.mark_pending_metadata(&arxiv_id);
                        None
                    }
                }
                Err(error) => return Err(error),
            }
        };
        if let Some(metadata) = &metadata {
            self.metadata_db.upsert_paper(&self.cache_root, metadata)?;
        }

        if options.include_pdf && (options.refresh || !paths.pdf_path.exists()) {
            network_requests += 1;
            let pdf = self.download_bytes(self.pdf_url(&arxiv_id)).await?;
            write_bytes_atomic(&paths.pdf_path, &pdf)?;
        }

        if options.include_source && (options.refresh || !paths.source_manifest_path.exists()) {
            network_requests += 1;
            let bytes = self.download_bytes(self.source_url(&arxiv_id)).await?;
            let manifest = materialize_source(&paths, &bytes)?;
            let citation_count = extract_citations(&arxiv_id, &paths, &manifest)?;
            write_json_pretty(&paths.source_manifest_path, &manifest)?;
            return Ok(FetchPaperResponse {
                arxiv_id,
                cache_dir: display_path(&paths.cache_dir),
                metadata_path: display_path(&paths.metadata_path),
                metadata_db_path: display_path(self.metadata_db.path()),
                indexed_metadata_records: index_report.indexed_papers,
                pdf_path: options.include_pdf.then(|| display_path(&paths.pdf_path)),
                source_archive_path: Some(manifest.source_archive_path),
                source_extracted_dir: manifest.source_extracted_dir,
                citations_jsonl_path: Some(display_path(&paths.citations_path)),
                title: metadata
                    .as_ref()
                    .and_then(|metadata| metadata.title.clone()),
                authors: metadata
                    .map(|metadata| metadata.authors)
                    .unwrap_or_default(),
                citation_count,
                cache_hit: false,
                network_requests,
                rate_limit_delay_seconds: crate::rate_limit::ARXIV_DELAY.as_secs(),
                metadata_pending,
            });
        }

        if options.include_source && !paths.citations_path.exists() {
            if let Some(manifest) = read_manifest_if_present(&paths)? {
                let citation_count = extract_citations(&arxiv_id, &paths, &manifest)?;
                return Ok(FetchPaperResponse {
                    arxiv_id,
                    cache_dir: display_path(&paths.cache_dir),
                    metadata_path: display_path(&paths.metadata_path),
                    metadata_db_path: display_path(self.metadata_db.path()),
                    indexed_metadata_records: index_report.indexed_papers,
                    pdf_path: options.include_pdf.then(|| display_path(&paths.pdf_path)),
                    source_archive_path: Some(manifest.source_archive_path),
                    source_extracted_dir: manifest.source_extracted_dir,
                    citations_jsonl_path: Some(display_path(&paths.citations_path)),
                    title: metadata
                        .as_ref()
                        .and_then(|metadata| metadata.title.clone()),
                    authors: metadata
                        .map(|metadata| metadata.authors)
                        .unwrap_or_default(),
                    citation_count,
                    cache_hit: false,
                    network_requests,
                    rate_limit_delay_seconds: crate::rate_limit::ARXIV_DELAY.as_secs(),
                    metadata_pending,
                });
            }
        }

        let citation_count = count_jsonl_records(&paths.citations_path).unwrap_or(0);
        let manifest = read_manifest_if_present(&paths)?;
        Ok(FetchPaperResponse {
            arxiv_id,
            cache_dir: display_path(&paths.cache_dir),
            metadata_path: display_path(&paths.metadata_path),
            metadata_db_path: display_path(self.metadata_db.path()),
            indexed_metadata_records: index_report.indexed_papers,
            pdf_path: options.include_pdf.then(|| display_path(&paths.pdf_path)),
            source_archive_path: manifest
                .as_ref()
                .map(|manifest| manifest.source_archive_path.clone()),
            source_extracted_dir: manifest.and_then(|manifest| manifest.source_extracted_dir),
            citations_jsonl_path: options
                .include_source
                .then(|| display_path(&paths.citations_path)),
            title: metadata
                .as_ref()
                .and_then(|metadata| metadata.title.clone()),
            authors: metadata
                .map(|metadata| metadata.authors)
                .unwrap_or_default(),
            citation_count,
            cache_hit: false,
            network_requests,
            rate_limit_delay_seconds: crate::rate_limit::ARXIV_DELAY.as_secs(),
            metadata_pending,
        })
    }

    pub async fn lookup(&self, request: LookupPapersRequest) -> Result<LookupPapersResponse> {
        let arxiv_ids = request
            .arxiv_ids
            .iter()
            .map(|arxiv_id| normalize_arxiv_id(arxiv_id))
            .collect::<Result<Vec<_>>>()?;
        if arxiv_ids.is_empty() {
            bail!("lookup requires at least one arXiv id");
        }

        let refresh_metadata = request.refresh_metadata.unwrap_or(false);
        let fetch_missing_metadata = request.fetch_missing_metadata.unwrap_or(true);
        let mut needs_metadata = Vec::new();
        for arxiv_id in &arxiv_ids {
            let paths = PaperPaths::new(&self.cache_root, arxiv_id);
            if refresh_metadata || !paths.metadata_path.exists() {
                needs_metadata.push(arxiv_id.clone());
            }
        }

        // per-id error map populated during metadata fetch
        let mut per_id_errors: BTreeMap<String, String> = BTreeMap::new();
        let mut fetched_metadata_count = 0;
        let mut network_requests = 0;

        if fetch_missing_metadata && !needs_metadata.is_empty() {
            // Try a single batched request first.
            let batch_result = self.fetch_metadata_batch(&needs_metadata).await;
            network_requests += 1;
            match batch_result {
                Ok(fetched) => {
                    let fetched_by_id = metadata_lookup(fetched);
                    for arxiv_id in &needs_metadata {
                        match fetched_by_id
                            .get(arxiv_id)
                            .or_else(|| fetched_by_id.get(&base_arxiv_id(arxiv_id)))
                            .cloned()
                        {
                            Some(metadata) => {
                                let paths = PaperPaths::new(&self.cache_root, arxiv_id);
                                write_json_pretty(&paths.metadata_path, &metadata)?;
                                self.metadata_db.upsert_paper(&self.cache_root, &metadata)?;
                                fetched_metadata_count += 1;
                            }
                            None => {
                                per_id_errors.insert(
                                    arxiv_id.clone(),
                                    format!("arXiv returned no metadata for {arxiv_id}"),
                                );
                            }
                        }
                    }
                }
                Err(batch_err) => {
                    let batch_err_text = format!("{batch_err:#}");
                    if is_systemic_metadata_error(&batch_err_text) {
                        // Timeouts, rate limits, and server errors apply to
                        // every id: retrying per-id would multiply requests
                        // against an already-struggling arXiv (observed live:
                        // a 15-id batch turned into 16 requests, 205s of wall
                        // time, and deeper 429s). Attribute the batch error
                        // to each uncached id and fail fast instead.
                        tracing::info!(
                            error = %batch_err_text,
                            "batched metadata request failed systemically; not retrying per-id"
                        );
                        for arxiv_id in &needs_metadata {
                            per_id_errors.insert(arxiv_id.clone(), batch_err_text.clone());
                        }
                    } else {
                        // Plausibly id-specific failure — fall back to per-id
                        // requests so good ids still succeed and failures are
                        // attributed individually.
                        tracing::info!(
                            error = %batch_err_text,
                            "batched metadata request failed; retrying per-id"
                        );
                        for arxiv_id in &needs_metadata {
                            let result = self
                                .fetch_metadata_batch(std::slice::from_ref(arxiv_id))
                                .await;
                            network_requests += 1;
                            match result {
                                Ok(fetched) => {
                                    let fetched_by_id = metadata_lookup(fetched);
                                    match fetched_by_id
                                        .get(arxiv_id)
                                        .or_else(|| fetched_by_id.get(&base_arxiv_id(arxiv_id)))
                                        .cloned()
                                    {
                                        Some(metadata) => {
                                            let paths = PaperPaths::new(&self.cache_root, arxiv_id);
                                            write_json_pretty(&paths.metadata_path, &metadata)?;
                                            self.metadata_db
                                                .upsert_paper(&self.cache_root, &metadata)?;
                                            fetched_metadata_count += 1;
                                        }
                                        None => {
                                            per_id_errors.insert(
                                                arxiv_id.clone(),
                                                format!(
                                                    "arXiv returned no metadata for {arxiv_id}"
                                                ),
                                            );
                                        }
                                    }
                                }
                                Err(err) => {
                                    per_id_errors.insert(arxiv_id.clone(), format!("{err:#}"));
                                }
                            }
                        }
                    }
                }
            }
        }

        let papers = arxiv_ids
            .into_iter()
            .map(|arxiv_id| {
                let error = per_id_errors.get(&arxiv_id).cloned();
                let mut status = self.status(MaterialStatusRequest {
                    arxiv_id: arxiv_id.clone(),
                })?;
                status.error = error;
                Ok(status)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(LookupPapersResponse {
            papers,
            fetched_metadata_count,
            network_requests,
            rate_limit_delay_seconds: crate::rate_limit::ARXIV_DELAY.as_secs(),
        })
    }

    pub fn status(&self, request: MaterialStatusRequest) -> Result<PaperMaterialStatus> {
        let arxiv_id = normalize_arxiv_id(&request.arxiv_id)?;
        let paths = PaperPaths::new(&self.cache_root, &arxiv_id);
        let metadata: Option<PaperMetadata> = if paths.metadata_path.exists() {
            Some(read_json(&paths.metadata_path)?)
        } else {
            None
        };
        let manifest = read_or_infer_manifest(&paths)?;
        let source_archive_path = manifest
            .as_ref()
            .map(|manifest| manifest.source_archive_path.clone());
        let source_extracted_dir = manifest
            .as_ref()
            .and_then(|manifest| manifest.source_extracted_dir.clone());
        let citation_count = count_jsonl_records(&paths.citations_path).unwrap_or(0);
        let material_state = PaperMaterialStates {
            metadata: ready_if(metadata.is_some()),
            abstract_text: ready_if(
                metadata
                    .as_ref()
                    .and_then(|metadata| metadata.summary.as_deref())
                    .is_some_and(|summary| !summary.trim().is_empty()),
            ),
            pdf_file: ready_if(paths.pdf_path.exists()),
            source_archive: ready_if(
                source_archive_path
                    .as_deref()
                    .is_some_and(|path| Path::new(path).exists()),
            ),
            source_tree: ready_if(
                source_extracted_dir
                    .as_deref()
                    .is_some_and(|path| Path::new(path).exists()),
            ),
            citations: ready_if(paths.citations_path.exists()),
            source_search: ready_if(has_searchable_source_material(&paths, manifest.as_ref())),
        };

        let mut available_now = Vec::new();
        let mut missing = Vec::new();
        collect_material_state(
            &mut available_now,
            &mut missing,
            "metadata",
            material_state.metadata,
        );
        collect_material_state(
            &mut available_now,
            &mut missing,
            "abstract",
            material_state.abstract_text,
        );
        collect_material_state(
            &mut available_now,
            &mut missing,
            "pdf_file",
            material_state.pdf_file,
        );
        collect_material_state(
            &mut available_now,
            &mut missing,
            "source_archive",
            material_state.source_archive,
        );
        collect_material_state(
            &mut available_now,
            &mut missing,
            "source_tree",
            material_state.source_tree,
        );
        collect_material_state(
            &mut available_now,
            &mut missing,
            "citations",
            material_state.citations,
        );

        let next_tool = if material_state.metadata == MaterialState::Missing {
            Some("lookup_arxiv_papers".to_string())
        } else if material_state.source_search == MaterialState::Ready {
            Some("full_text_search".to_string())
        } else {
            Some("fetch_arxiv_paper".to_string())
        };

        Ok(PaperMaterialStatus {
            base_arxiv_id: base_arxiv_id(&arxiv_id),
            version: arxiv_id_version(&arxiv_id),
            publication_year: arxiv_id_year(&arxiv_id).ok(),
            paths: PaperLocalPaths {
                cache_dir: display_path(&paths.cache_dir),
                metadata_path: display_path(&paths.metadata_path),
                pdf_path: display_path(&paths.pdf_path),
                source_dir: display_path(&paths.source_dir),
                source_manifest_path: display_path(&paths.source_manifest_path),
                source_archive_path,
                source_extracted_dir,
                citations_jsonl_path: display_path(&paths.citations_path),
            },
            arxiv_id,
            material_state,
            available_now,
            missing,
            metadata,
            citation_count,
            next_tool,
            error: None,
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

    fn cached_response(
        &self,
        arxiv_id: &str,
        paths: &PaperPaths,
        indexed_metadata_records: usize,
    ) -> Result<FetchPaperResponse> {
        let metadata: PaperMetadata = read_json(&paths.metadata_path)?;
        let manifest = read_manifest_if_present(paths)?;
        Ok(FetchPaperResponse {
            arxiv_id: arxiv_id.to_string(),
            cache_dir: display_path(&paths.cache_dir),
            metadata_path: display_path(&paths.metadata_path),
            metadata_db_path: display_path(self.metadata_db.path()),
            indexed_metadata_records,
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
            metadata_pending: false,
        })
    }

    /// Fetch metadata for one paper on the queued download path. Unlike
    /// interactive lookups, background jobs should be patient: transient
    /// arXiv slowness or rate limiting retries with backoff instead of
    /// failing the job on the first 10s timeout (observed live: 14 queued
    /// jobs failed on single timeouts and the agent bypassed arx entirely).
    /// When the shared circuit breaker pauses metadata (repeated systemic
    /// failures across all workers), this returns the pause error
    /// immediately instead of sleeping through doomed retries.
    async fn fetch_metadata(&self, arxiv_id: &str) -> Result<PaperMetadata> {
        let requested = arxiv_id.to_string();
        let mut last_error = None;
        for (attempt, wait) in self
            .metadata_retry_waits
            .iter()
            .copied()
            .map(Some)
            .chain(std::iter::once(None))
            .enumerate()
        {
            match self
                .fetch_metadata_batch(std::slice::from_ref(&requested))
                .await
            {
                Ok(fetched) => {
                    let fetched_by_id = metadata_lookup(fetched);
                    return fetched_by_id
                        .get(&requested)
                        .or_else(|| fetched_by_id.get(&base_arxiv_id(&requested)))
                        .cloned()
                        .with_context(|| {
                            format!("arXiv metadata response did not contain {requested}")
                        });
                }
                Err(error) => {
                    if error.downcast_ref::<MetadataPausedError>().is_some() {
                        return Err(error);
                    }
                    let error_text = format!("{error:#}");
                    if !is_systemic_metadata_error(&error_text) {
                        return Err(error);
                    }
                    tracing::info!(
                        arxiv_id = %requested,
                        attempt = attempt + 1,
                        error = %error_text,
                        "metadata fetch failed for queued download; will retry if attempts remain"
                    );
                    last_error = Some(error);
                    // This failure may have tripped the shared breaker; stop
                    // retrying right away instead of sleeping into a pause.
                    if let Err(paused) = self.check_metadata_pause() {
                        return Err(paused);
                    }
                    if let Some(wait) = wait {
                        tokio::time::sleep(wait).await;
                    }
                }
            }
        }
        Err(last_error.expect("at least one metadata fetch attempt"))
    }

    /// Fail fast with `MetadataPausedError` while the shared metadata
    /// circuit breaker is open.
    fn check_metadata_pause(&self) -> Result<()> {
        let health = self.rate_limiter.metadata_health()?;
        let now = crate::rate_limit::now_unix_ms()?;
        if let Some(paused_until_unix_ms) = health.paused_until(now) {
            return Err(anyhow::Error::new(MetadataPausedError {
                paused_until_unix_ms,
            }));
        }
        Ok(())
    }

    /// Shared arXiv metadata service health (failure streak and pause), as
    /// recorded in the cross-process rate-limit state.
    pub fn metadata_health(&self) -> Result<MetadataHealth> {
        self.rate_limiter.metadata_health()
    }

    fn mark_pending_metadata(&self, arxiv_id: &str) {
        if let Ok(mut pending) = self.pending_metadata.lock() {
            pending.insert(arxiv_id.to_string());
        }
    }

    fn clear_pending_metadata(&self, arxiv_id: &str) {
        if let Ok(mut pending) = self.pending_metadata.lock() {
            pending.remove(arxiv_id);
        }
    }

    /// Best-effort batched metadata prefetch and backfill. One Atom request
    /// covers every given id that is missing metadata.json, plus any papers
    /// whose material was previously fetched during a metadata outage.
    /// Single-flighted: concurrent download workers coalesce into one
    /// request instead of issuing one (retried) request per queued job.
    /// Never fails the caller — jobs proceed without metadata and the
    /// breaker throttles attempts while arXiv is degraded.
    pub async fn ensure_metadata_cached(&self, arxiv_ids: &[String]) {
        let prefetch_lock = self.metadata_prefetch_lock.clone();
        let _flight = prefetch_lock.lock().await;

        let mut missing = Vec::new();
        let mut seen = BTreeSet::new();
        let backfill_ids: Vec<String> = self
            .pending_metadata
            .lock()
            .map(|pending| pending.iter().cloned().collect())
            .unwrap_or_default();
        for id in arxiv_ids.iter().chain(backfill_ids.iter()) {
            let Ok(arxiv_id) = normalize_arxiv_id(id) else {
                continue;
            };
            if !seen.insert(arxiv_id.clone()) {
                continue;
            }
            if PaperPaths::new(&self.cache_root, &arxiv_id)
                .metadata_path
                .exists()
            {
                // Already cached (possibly by a concurrent worker while we
                // waited on the single-flight lock); prune stale backfills.
                self.clear_pending_metadata(&arxiv_id);
                continue;
            }
            missing.push(arxiv_id);
        }
        missing.truncate(METADATA_BATCH_LIMIT);
        if missing.is_empty() {
            return;
        }

        match self.fetch_metadata_batch(&missing).await {
            Ok(fetched) => {
                let fetched_by_id = metadata_lookup(fetched);
                let mut cached = 0usize;
                for arxiv_id in &missing {
                    let Some(metadata) = fetched_by_id
                        .get(arxiv_id)
                        .or_else(|| fetched_by_id.get(&base_arxiv_id(arxiv_id)))
                    else {
                        continue;
                    };
                    let paths = PaperPaths::new(&self.cache_root, arxiv_id);
                    let stored = write_json_pretty(&paths.metadata_path, metadata)
                        .and_then(|()| self.metadata_db.upsert_paper(&self.cache_root, metadata));
                    match stored {
                        Ok(_) => {
                            self.clear_pending_metadata(arxiv_id);
                            cached += 1;
                        }
                        Err(error) => tracing::warn!(
                            arxiv_id = %arxiv_id,
                            error = %format!("{error:#}"),
                            "failed to store prefetched metadata"
                        ),
                    }
                }
                tracing::info!(
                    requested = missing.len(),
                    cached,
                    id_list = %missing.join(","),
                    "batched metadata prefetch for queued downloads"
                );
            }
            Err(error) => {
                tracing::info!(
                    id_list = %missing.join(","),
                    error = %format!("{error:#}"),
                    "batched metadata prefetch failed; downloads continue without metadata"
                );
            }
        }
    }

    async fn fetch_metadata_batch(&self, arxiv_ids: &[String]) -> Result<Vec<PaperMetadata>> {
        if arxiv_ids.is_empty() {
            return Ok(Vec::new());
        }
        // Fail fast while the shared breaker has metadata paused, before
        // queueing on the rate-limit lock.
        self.check_metadata_pause()?;
        let id_list = arxiv_ids.join(",");
        let max_results = arxiv_ids.len().to_string();
        let request_result = self
            .rate_limiter
            .run_with_penalty(
                || async {
                    // Authoritative re-check under the lock: another worker
                    // may have tripped the breaker while we waited our turn.
                    self.check_metadata_pause()?;
                    let response = self
                        .client
                        .get(&self.metadata_url)
                        .header(reqwest::header::ACCEPT_ENCODING, "identity")
                        .query(&[
                            ("id_list", id_list.as_str()),
                            ("max_results", max_results.as_str()),
                        ])
                        // Fail fast: bound this request only, so slow arXiv error
                        // responses cannot hang lookup. PDF/source downloads use the
                        // shared client without this bound.
                        .timeout(self.metadata_timeout)
                        .send()
                        .await
                        .map_err(|error| self.map_metadata_error(error))?;
                    let status = response.status();
                    if !status.is_success() {
                        let snippet = response
                            .text()
                            .await
                            .unwrap_or_default()
                            .chars()
                            .take(200)
                            .collect::<String>();
                        let snippet = snippet.trim().to_string();
                        if snippet.is_empty() {
                            bail!("arXiv metadata request failed: {status}");
                        } else {
                            bail!("arXiv metadata request failed: {status}: {snippet}");
                        }
                    }
                    response.text().await.map_err(|error| {
                        if error.is_timeout() {
                            self.map_metadata_error(error)
                        } else {
                            anyhow::Error::new(error)
                                .context(format!("reading arXiv metadata response for {id_list}"))
                        }
                    })
                },
                // 429 penalties land while the lock is still held, so no
                // other worker can slip a request in before the backoff.
                rate_limit_penalty,
            )
            .await;
        // Track shared metadata health so concurrent jobs stop hammering a
        // degraded metadata API: systemic failures escalate the shared
        // pause; any real answer from the service clears it.
        match &request_result {
            Ok(_) => self.rate_limiter.record_metadata_recovery().await?,
            Err(error) => {
                if error.downcast_ref::<MetadataPausedError>().is_none() {
                    let error_text = format!("{error:#}");
                    if is_systemic_metadata_error(&error_text) {
                        let health = self.rate_limiter.record_metadata_failure().await?;
                        let now = crate::rate_limit::now_unix_ms()?;
                        if let Some(paused_until_unix_ms) = health.paused_until(now) {
                            tracing::warn!(
                                id_list = %id_list,
                                failure_streak = health.failure_streak,
                                paused_until_unix_ms,
                                "arXiv metadata degraded; pausing metadata requests (material downloads unaffected)"
                            );
                        }
                    } else {
                        self.rate_limiter.record_metadata_recovery().await?;
                    }
                }
            }
        }
        let text = request_result?;
        parse_metadata_feed(arxiv_ids, &text)
    }

    /// Convert a reqwest error from a metadata request into an anyhow error,
    /// surfacing timeouts with the configured bound so per-id error strings
    /// explain why the request was abandoned.
    fn map_metadata_error(&self, error: reqwest::Error) -> anyhow::Error {
        if error.is_timeout() {
            anyhow::anyhow!(
                "arXiv metadata request timed out after {:?}",
                self.metadata_timeout
            )
        } else {
            anyhow::Error::new(error).context("requesting arXiv metadata")
        }
    }

    async fn download_bytes(&self, url: String) -> Result<Vec<u8>> {
        for attempt in 1..=DOWNLOAD_MAX_ATTEMPTS {
            let result = self.download_bytes_once(&url).await;
            match result {
                Ok(bytes) => return Ok(bytes),
                Err(error)
                    if attempt < DOWNLOAD_MAX_ATTEMPTS && should_retry_download_error(&error) =>
                {
                    tracing::info!(
                        %url,
                        attempt,
                        max_attempts = DOWNLOAD_MAX_ATTEMPTS,
                        error = %format!("{error:#}"),
                        "retrying transient arXiv download failure"
                    );
                }
                Err(error) => {
                    if attempt > 1 {
                        return Err(error).with_context(|| {
                            format!("downloading {url} failed after {attempt} attempts")
                        });
                    }
                    return Err(error);
                }
            }
        }
        unreachable!("download loop returns on success or final failure")
    }

    async fn download_bytes_once(&self, url: &str) -> Result<Vec<u8>> {
        self.rate_limiter
            .run_with_penalty(
                || async {
                    let response = self
                        .client
                        .get(url)
                        .header(reqwest::header::ACCEPT_ENCODING, "identity")
                        .send()
                        .await
                        .with_context(|| format!("requesting {url}"))?
                        .error_for_status()
                        .with_context(|| format!("{url} returned an error status"))?;
                    let bytes = response
                        .bytes()
                        .await
                        .with_context(|| format!("reading response body from {url}"))?;
                    Ok(bytes.to_vec())
                },
                rate_limit_penalty,
            )
            .await
    }

    fn pdf_url(&self, arxiv_id: &str) -> String {
        format!("{}/pdf/{arxiv_id}", self.material_base_url)
    }

    fn source_url(&self, arxiv_id: &str) -> String {
        format!("{}/e-print/{arxiv_id}", self.material_base_url)
    }
}

/// Classify an arXiv request failure for the shared rate-limit penalty that
/// `RateLimiter::run_with_penalty` applies while the lock is still held.
/// Detects 429s both as reqwest status errors (material downloads) and in
/// the error text this module produces for metadata requests.
fn rate_limit_penalty(error: &anyhow::Error) -> Option<Duration> {
    // A paused-metadata bail is a local short-circuit, not an arXiv answer;
    // its text embeds a unix timestamp that could contain "429" by chance.
    if error.downcast_ref::<MetadataPausedError>().is_some() {
        return None;
    }
    // "429 Too Many Requests" is the exact status text in this module's
    // metadata bail; a bare "429" would also match arXiv ids and timestamps
    // embedded in unrelated errors.
    (is_rate_limited_error(error) || format!("{error:#}").contains("429 Too Many Requests"))
        .then_some(RATE_LIMIT_PENALTY)
}

/// True when a metadata failure reflects arXiv-side unavailability rather
/// than anything wrong with the requested paper, so a queued download should
/// proceed to material and backfill metadata later instead of failing.
fn is_recoverable_metadata_outage(error: &anyhow::Error) -> bool {
    error.downcast_ref::<MetadataPausedError>().is_some()
        || is_systemic_metadata_error(&format!("{error:#}"))
}

/// True when a metadata failure is not specific to any single arXiv id —
/// timeouts, rate limiting, connection failures, and server errors hit
/// every id equally, so per-id fallbacks only multiply load while retries
/// with backoff can help. Matches on the error strings this module
/// produces (`map_metadata_error` and the status-check bail).
fn is_systemic_metadata_error(error_text: &str) -> bool {
    error_text.contains("timed out after")
        || error_text.contains("429")
        || error_text.contains("500")
        || error_text.contains("502")
        || error_text.contains("503")
        || error_text.contains("requesting arXiv metadata")
        || error_text.contains("metadata requests are paused")
}

fn is_rate_limited_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<reqwest::Error>()
            .is_some_and(|error| error.status() == Some(reqwest::StatusCode::TOO_MANY_REQUESTS))
    })
}

fn should_retry_download_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<reqwest::Error>().is_some_and(|error| {
            error.is_decode()
                || error.is_timeout()
                || error.is_connect()
                || error.status().is_some_and(|status| {
                    status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
                })
        })
    })
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
        Self::from_cache_dir(paths::paper_cache_dir(cache_root, arxiv_id))
    }

    fn from_cache_dir(cache_dir: PathBuf) -> Self {
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

pub(crate) fn base_arxiv_id(arxiv_id: &str) -> String {
    let version_re = Regex::new(r"(?i)v\d+$").expect("valid version regex");
    version_re.replace(arxiv_id, "").to_string()
}

fn arxiv_id_version(arxiv_id: &str) -> Option<u16> {
    let (_, suffix) = arxiv_id.rsplit_once('v')?;
    suffix.parse().ok()
}

pub fn arxiv_id_year(input: &str) -> Result<u16> {
    let arxiv_id = normalize_arxiv_id(input)?;
    let base = base_arxiv_id(&arxiv_id);
    if base.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
        let year_digits = base
            .get(..2)
            .with_context(|| format!("extracting year from modern arXiv id {input}"))?;
        let short_year: u16 = year_digits
            .parse()
            .with_context(|| format!("parsing year from modern arXiv id {input}"))?;
        if short_year >= 91 {
            Ok(1900 + short_year)
        } else {
            Ok(2000 + short_year)
        }
    } else {
        let (_, number) = base
            .rsplit_once('/')
            .with_context(|| format!("extracting old-style arXiv id number from {input}"))?;
        let year_digits = number
            .get(..2)
            .with_context(|| format!("extracting year from old-style arXiv id {input}"))?;
        let short_year: u16 = year_digits
            .parse()
            .with_context(|| format!("parsing year from old-style arXiv id {input}"))?;
        if short_year >= 91 {
            Ok(1900 + short_year)
        } else {
            Ok(2000 + short_year)
        }
    }
}

fn parse_metadata_feed(requested_ids: &[String], atom: &str) -> Result<Vec<PaperMetadata>> {
    let doc = roxmltree::Document::parse(atom).context("parsing arXiv Atom metadata")?;
    let entries = doc
        .descendants()
        .filter(|node| node.has_tag_name("entry"))
        .collect::<Vec<_>>();
    if entries.is_empty() {
        bail!("arXiv metadata did not contain an entry");
    }

    entries
        .into_iter()
        .enumerate()
        .map(|(index, entry)| {
            let fallback = requested_ids
                .get(index)
                .map(String::as_str)
                .unwrap_or_default();
            parse_metadata_entry(fallback, entry)
        })
        .collect()
}

fn parse_metadata_entry(
    requested_id: &str,
    entry: roxmltree::Node<'_, '_>,
) -> Result<PaperMetadata> {
    let entry_id = child_text(entry, "id");
    let arxiv_id = entry_id
        .as_deref()
        .and_then(|url| normalize_arxiv_id(url).ok())
        .unwrap_or_else(|| requested_id.to_string());
    let title = child_text(entry, "title").map(clean_ws);
    let summary = child_text(entry, "summary").map(clean_ws);
    let published = child_text(entry, "published");
    let updated = child_text(entry, "updated");
    let author_details = entry
        .children()
        .filter(|node| node.has_tag_name("author"))
        .filter_map(author_detail)
        .collect::<Vec<_>>();
    let authors = author_details
        .iter()
        .map(|author| author.name.clone())
        .collect();
    let categories = entry
        .children()
        .filter(|node| node.has_tag_name("category"))
        .filter_map(|category| category.attribute("term").map(str::to_string))
        .collect();
    let links = entry
        .children()
        .filter(|node| node.has_tag_name("link"))
        .filter_map(atom_link)
        .collect::<Vec<_>>();
    let pdf_url = links
        .iter()
        .find(|link| {
            link.title.as_deref() == Some("pdf")
                || link.mime_type.as_deref() == Some("application/pdf")
        })
        .map(|link| link.href.clone());

    Ok(PaperMetadata {
        arxiv_id,
        abs_url: entry_id,
        pdf_url,
        title,
        authors,
        author_details,
        summary,
        published,
        updated,
        categories,
        primary_category: entry
            .children()
            .find(|node| node.has_tag_name("primary_category"))
            .and_then(|node| node.attribute("term"))
            .map(str::to_string),
        comment: child_text(entry, "comment").map(clean_ws),
        journal_ref: child_text(entry, "journal_ref").map(clean_ws),
        doi: child_text(entry, "doi").map(clean_ws),
        links,
    })
}

fn author_detail(author: roxmltree::Node<'_, '_>) -> Option<PaperAuthor> {
    let name = child_text(author, "name").map(clean_ws)?;
    let affiliations = author
        .children()
        .filter(|node| node.has_tag_name("affiliation"))
        .filter_map(|node| node.text())
        .map(|text| clean_ws(text.to_string()))
        .collect();
    Some(PaperAuthor { name, affiliations })
}

fn atom_link(link: roxmltree::Node<'_, '_>) -> Option<AtomLink> {
    Some(AtomLink {
        href: link.attribute("href")?.to_string(),
        rel: link.attribute("rel").map(str::to_string),
        title: link.attribute("title").map(str::to_string),
        mime_type: link.attribute("type").map(str::to_string),
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

fn metadata_lookup(metadata: Vec<PaperMetadata>) -> BTreeMap<String, PaperMetadata> {
    let mut lookup = BTreeMap::new();
    for metadata in metadata {
        lookup.insert(base_arxiv_id(&metadata.arxiv_id), metadata.clone());
        lookup.insert(metadata.arxiv_id.clone(), metadata);
    }
    lookup
}

fn ready_if(condition: bool) -> MaterialState {
    if condition {
        MaterialState::Ready
    } else {
        MaterialState::Missing
    }
}

fn collect_material_state(
    available_now: &mut Vec<String>,
    missing: &mut Vec<String>,
    name: &str,
    state: MaterialState,
) {
    match state {
        MaterialState::Ready => available_now.push(name.to_string()),
        MaterialState::Missing => missing.push(name.to_string()),
    }
}

fn has_searchable_source_material(paths: &PaperPaths, manifest: Option<&SourceManifest>) -> bool {
    source_search_roots(paths, manifest)
        .into_iter()
        .any(|root| {
            root.exists()
                && WalkDir::new(root)
                    .follow_links(false)
                    .into_iter()
                    .any(|entry| {
                        entry.ok().is_some_and(|entry| {
                            entry.file_type().is_file() && is_searchable_source_file(entry.path())
                        })
                    })
        })
}

/// Resolve the index key and chunks for one cached paper. Keyed by the
/// version-stripped id so fetch-time indexing (request ids are usually
/// unversioned), rescan indexing (metadata ids are versioned), and search
/// filters all address the same documents.
fn paper_material_for_index(
    paths: &PaperPaths,
    fallback_id: &str,
) -> Result<(String, Vec<MaterialChunk>)> {
    let canonical_id = if paths.metadata_path.exists() {
        read_json::<PaperMetadata>(&paths.metadata_path)?.arxiv_id
    } else {
        fallback_id.to_string()
    };
    let chunks = collect_paper_material(paths)?;
    Ok((base_arxiv_id(&canonical_id), chunks))
}

/// Collect every rankable unit of a paper's cached material: metadata
/// fields, citation records, and TeX/source paragraphs.
fn collect_paper_material(paths: &PaperPaths) -> Result<Vec<MaterialChunk>> {
    let metadata: Option<PaperMetadata> = if paths.metadata_path.exists() {
        Some(read_json(&paths.metadata_path)?)
    } else {
        None
    };
    let manifest = read_or_infer_manifest(paths)?;
    let mut chunks = Vec::new();
    collect_metadata_candidates(&metadata, &paths.metadata_path, &mut chunks);
    collect_citation_candidates(&paths.citations_path, &mut chunks)?;
    collect_source_candidates(paths, manifest.as_ref(), &mut chunks)?;
    Ok(chunks)
}

fn collect_metadata_candidates(
    metadata: &Option<PaperMetadata>,
    metadata_path: &Path,
    candidates: &mut Vec<MaterialChunk>,
) {
    let Some(metadata) = metadata else {
        return;
    };
    let metadata_path = display_path(metadata_path);
    let mut push =
        |field_name: &'static str, chunk_category: &'static str, value: Option<String>| {
            if let Some(text) = value.filter(|text| !text.trim().is_empty()) {
                candidates.push(MaterialChunk {
                    source: "metadata".to_string(),
                    category: chunk_category.to_string(),
                    field: Some(field_name.to_string()),
                    path: Some(metadata_path.clone()),
                    line_start: None,
                    line_end: None,
                    text,
                });
            }
        };
    // Title is its own scope so agents can search titles only.
    push("title", category::TITLE, metadata.title.clone());
    push("abstract", category::METADATA, metadata.summary.clone());
    push(
        "authors",
        category::METADATA,
        (!metadata.authors.is_empty()).then(|| metadata.authors.join(", ")),
    );
    push(
        "categories",
        category::METADATA,
        (!metadata.categories.is_empty()).then(|| metadata.categories.join(", ")),
    );
    push(
        "primary_category",
        category::METADATA,
        metadata.primary_category.clone(),
    );
    push("comment", category::METADATA, metadata.comment.clone());
    push(
        "journal_ref",
        category::METADATA,
        metadata.journal_ref.clone(),
    );
    push("doi", category::METADATA, metadata.doi.clone());
}

fn collect_citation_candidates(
    citations_path: &Path,
    candidates: &mut Vec<MaterialChunk>,
) -> Result<()> {
    if !citations_path.exists() {
        return Ok(());
    }
    let text = fs::read_to_string(citations_path)
        .with_context(|| format!("reading {}", citations_path.display()))?;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let record: CitationRecord = serde_json::from_str(line)
            .with_context(|| format!("parsing citation JSONL {}", citations_path.display()))?;
        candidates.push(MaterialChunk {
            source: "citation".to_string(),
            // Citation records are bibliography-scope: they describe cited papers
            // extracted from .bib/.bbl files and TeX source. Excluding them from
            // default search prevents "papers citing X" hits from polluting
            // "papers about X" results.
            category: category::BIBLIOGRAPHY.to_string(),
            field: Some("context".to_string()),
            path: Some(record.source_file.clone()),
            line_start: Some(record.line),
            line_end: Some(record.line),
            text: format!(
                "{} {} {}",
                record.cited_arxiv_id, record.source_file, record.context
            ),
        });
    }
    Ok(())
}

fn collect_source_candidates(
    paths: &PaperPaths,
    manifest: Option<&SourceManifest>,
    candidates: &mut Vec<MaterialChunk>,
) -> Result<()> {
    for root in source_search_roots(paths, manifest) {
        if !root.exists() {
            continue;
        }
        for entry in WalkDir::new(&root).follow_links(false) {
            let entry =
                entry.with_context(|| format!("walking source directory {}", root.display()))?;
            if !entry.file_type().is_file() || !is_searchable_source_file(entry.path()) {
                continue;
            }
            collect_source_file_candidates(entry.path(), candidates)?;
        }
    }
    Ok(())
}

/// Split a source file into paragraphs (runs of non-blank lines). Paragraphs
/// are the ranking unit: TeX wraps sentences across lines, so line-level
/// matching misses multi-term queries that paragraph-level scoring catches.
///
/// .bib and .bbl files are assigned to the `bibliography` category so they
/// are excluded from default search scope and only found when the caller
/// explicitly requests the bibliography scope.
fn collect_source_file_candidates(path: &Path, candidates: &mut Vec<MaterialChunk>) -> Result<()> {
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    if bytes.len() > 5 * 1024 * 1024 {
        return Ok(());
    }
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return Ok(());
    };
    let chunk_category = if is_bibliography_file(path) {
        category::BIBLIOGRAPHY
    } else {
        category::BODY
    };
    let path_str = display_path(path);
    let mut paragraph: Vec<&str> = Vec::new();
    let mut paragraph_start = 0usize;
    let mut flush = |paragraph: &mut Vec<&str>, start: usize, end: usize| {
        if !paragraph.is_empty() {
            candidates.push(MaterialChunk {
                source: "source".to_string(),
                category: chunk_category.to_string(),
                field: None,
                path: Some(path_str.clone()),
                line_start: Some(start),
                line_end: Some(end),
                text: paragraph.join("\n"),
            });
            paragraph.clear();
        }
    };
    for (line_index, line) in text.lines().enumerate() {
        let line_number = line_index + 1;
        if line.trim().is_empty() {
            flush(
                &mut paragraph,
                paragraph_start,
                line_number.saturating_sub(1),
            );
        } else {
            if paragraph.is_empty() {
                paragraph_start = line_number;
            }
            paragraph.push(line);
        }
    }
    let line_count = text.lines().count();
    flush(&mut paragraph, paragraph_start, line_count);
    Ok(())
}

fn is_bibliography_file(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("bib" | "bbl")
    )
}

fn source_search_roots(paths: &PaperPaths, manifest: Option<&SourceManifest>) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(extracted) = manifest.and_then(|manifest| manifest.source_extracted_dir.as_ref()) {
        roots.push(PathBuf::from(extracted));
    } else {
        roots.push(paths.source_dir.clone());
    }
    roots
}

fn is_searchable_source_file(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("tex" | "bib" | "bbl" | "sty" | "cls" | "txt" | "md")
    )
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
            let window_start = floor_char_boundary(text, start.saturating_sub(512));
            let window_end = ceil_char_boundary(text, id.end().saturating_add(512).min(text.len()));
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

fn floor_char_boundary(text: &str, mut index: usize) -> usize {
    index = index.min(text.len());
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn ceil_char_boundary(text: &str, mut index: usize) -> usize {
    index = index.min(text.len());
    while index < text.len() && !text.is_char_boundary(index) {
        index += 1;
    }
    index
}

fn read_manifest_if_present(paths: &PaperPaths) -> Result<Option<SourceManifest>> {
    if paths.source_manifest_path.exists() {
        Ok(Some(read_json(&paths.source_manifest_path)?))
    } else {
        Ok(None)
    }
}

fn read_or_infer_manifest(paths: &PaperPaths) -> Result<Option<SourceManifest>> {
    if let Some(manifest) = read_manifest_if_present(paths)? {
        return Ok(Some(manifest));
    }
    let source_archive_path = ["e-print.tar.gz", "e-print.tar", "source.tex", "e-print"]
        .into_iter()
        .map(|name| paths.source_dir.join(name))
        .find(|path| path.exists())
        .map(display_path);
    let source_extracted_dir = paths
        .source_extracted_dir
        .exists()
        .then(|| display_path(&paths.source_extracted_dir));
    Ok(match (source_archive_path, source_extracted_dir) {
        (Some(source_archive_path), source_extracted_dir) => Some(SourceManifest {
            source_archive_path,
            source_extracted_dir,
        }),
        (None, Some(source_extracted_dir)) => Some(SourceManifest {
            source_archive_path: display_path(&paths.source_dir),
            source_extracted_dir: Some(source_extracted_dir),
        }),
        (None, None) => None,
    })
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
    use std::{collections::BTreeSet, fs, io::Write, time::Duration};
    use tempfile::tempdir;

    #[test]
    fn download_url_helpers_use_arxiv_file_host() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let arxiv_id = "2401.12345v2";

        assert_eq!(
            fetcher.source_url(arxiv_id),
            "https://arxiv.org/e-print/2401.12345v2"
        );
        assert_eq!(
            fetcher.pdf_url(arxiv_id),
            "https://arxiv.org/pdf/2401.12345v2"
        );
        assert!(!fetcher.source_url(arxiv_id).contains("export.arxiv.org"));
        assert!(!fetcher.pdf_url(arxiv_id).contains("export.arxiv.org"));
        Ok(())
    }

    #[tokio::test]
    async fn download_bytes_retries_truncated_body_and_returns_later_success() -> Result<()> {
        use tokio::{io::AsyncWriteExt, net::TcpListener};

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let url = format!("http://{}/e-print/2401.12345", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();

            let (mut stream, _) = listener.accept().await?;
            requests.push(read_http_request(&mut stream).await?);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\nConnection: close\r\n\r\nshort",
                )
                .await?;
            drop(stream);

            let (mut stream, _) = listener.accept().await?;
            requests.push(read_http_request(&mut stream).await?);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\ncomplete",
                )
                .await?;

            Ok::<_, anyhow::Error>(requests)
        });

        let temp = tempdir()?;
        let fetcher = fetcher_without_rate_limit(temp.path())?;

        let bytes = fetcher.download_bytes(url).await?;

        let requests = server.await??;
        assert_eq!(bytes, b"complete");
        assert_eq!(requests.len(), 2);
        for request in requests {
            assert!(
                request.starts_with("GET /e-print/2401.12345 HTTP/1.1\r\n"),
                "{request}"
            );
            assert!(
                request
                    .lines()
                    .any(|line| line.eq_ignore_ascii_case("accept-encoding: identity")),
                "{request}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn should_retry_download_error_classifies_429_status_response() -> Result<()> {
        use tokio::{io::AsyncWriteExt, net::TcpListener};

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let url = format!("http://{}/e-print/2401.12345", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let request = read_http_request(&mut stream).await?;
            stream
                .write_all(
                    b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 13\r\nConnection: close\r\n\r\nRate exceeded",
                )
                .await?;
            Ok::<_, anyhow::Error>(request)
        });

        let temp = tempdir()?;
        let fetcher = fetcher_without_rate_limit(temp.path())?;

        let error = fetcher
            .download_bytes_once(&url)
            .await
            .expect_err("429 status should surface as a reqwest status error");

        let request = server.await??;
        assert!(
            request.starts_with("GET /e-print/2401.12345 HTTP/1.1\r\n"),
            "{request}"
        );
        assert!(
            should_retry_download_error(&error),
            "429 should be retryable: {error:#}"
        );
        assert!(
            format!("{error:#}").contains("returned an error status"),
            "{error:#}"
        );
        Ok(())
    }

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

    #[test]
    fn collect_bibtex_eprint_citations_handles_unicode_at_512_byte_context_boundaries() -> Result<()>
    {
        let citing_id = "2401.12345";
        let cited_id = "2301.00001";
        let leading = "a".repeat(32);
        let eprint_prefix = "eprint = {";
        let before_eprint = format!("{leading}é\narchivePrefix = {{arXiv}},\n");
        let target_window_start = leading.len() + 1;
        let padding_before_eprint_len = (target_window_start + 512)
            .checked_sub(before_eprint.len() + eprint_prefix.len())
            .expect("fixture leaves room before eprint");

        let mut text = before_eprint;
        text.push_str(&"b".repeat(padding_before_eprint_len));
        text.push_str(eprint_prefix);
        text.push_str(cited_id);
        text.push('}');
        text.push_str(&"c".repeat(510));
        text.push('é');
        text.push_str(" tail");

        let id_start = text.find(cited_id).expect("fixture contains cited id");
        let id_end = id_start + cited_id.len();
        assert_eq!(id_start - 512, target_window_start);
        assert!(!text.is_char_boundary(id_start - 512));
        assert!(!text.is_char_boundary(id_end + 512));

        let mut records = BTreeMap::new();
        collect_bibtex_eprint_citations(
            citing_id,
            &base_arxiv_id(citing_id),
            Path::new("refs.bib"),
            &text,
            &mut records,
        )?;

        let record = records
            .get(cited_id)
            .expect("eprint citation should be extracted across Unicode window boundaries");
        assert_eq!(records.len(), 1);
        assert_eq!(record.citing_arxiv_id, citing_id);
        assert_eq!(record.cited_arxiv_id, cited_id);
        assert_eq!(record.line, 3);
        assert!(record.context.contains("eprint = {2301.00001}"));
        Ok(())
    }

    #[tokio::test]
    async fn fetch_rebuilds_missing_citations_from_cached_manifest_without_network() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let arxiv_id = "2401.12345v2";
        let paths = PaperPaths::new(temp.path(), arxiv_id);
        write_json_pretty(
            &paths.metadata_path,
            &metadata_fixture(
                arxiv_id,
                "Cached source manifest",
                "Metadata is already local so fetch must not contact arXiv.",
            ),
        )?;
        let manifest = materialize_source(&paths, &synthetic_source_bundle()?)?;
        write_json_pretty(&paths.source_manifest_path, &manifest)?;
        assert!(!paths.citations_path.exists());

        let response = fetcher
            .fetch(FetchPaperRequest {
                arxiv_id: arxiv_id.to_string(),
                include_pdf: Some(false),
                include_source: Some(true),
                refresh: Some(false),
            })
            .await?;

        assert_eq!(response.network_requests, 0);
        assert_eq!(response.cache_hit, false);
        assert_eq!(response.citation_count, 3);
        assert_eq!(
            response.citations_jsonl_path,
            Some(display_path(&paths.citations_path))
        );
        assert_eq!(
            response.source_archive_path,
            Some(manifest.source_archive_path)
        );
        assert!(paths.citations_path.exists());

        let jsonl = fs::read_to_string(&paths.citations_path)?;
        let cited_ids = jsonl
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<serde_json::Result<Vec<_>>>()?
            .into_iter()
            .map(|record| record["cited_arxiv_id"].as_str().unwrap().to_string())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            cited_ids,
            BTreeSet::from([
                "2101.00001v2".to_string(),
                "2203.12345".to_string(),
                "hep-th/9901001".to_string()
            ])
        );
        Ok(())
    }

    #[test]
    fn parse_metadata_feed_captures_arxiv_atom_extension_fields() -> Result<()> {
        let requested = vec!["2401.12345v2".to_string()];
        let atom = r#"
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>https://arxiv.org/abs/2401.12345v2</id>
    <updated>2024-01-03T00:00:00Z</updated>
    <published>2024-01-01T00:00:00Z</published>
    <title>
      Metadata first lookup with rich Atom fields
    </title>
    <summary>
      A deterministic abstract for parser coverage.
    </summary>
    <author>
      <name>Ada Lovelace</name>
      <arxiv:affiliation>Analytical Engine Institute</arxiv:affiliation>
    </author>
    <author>
      <name>Grace Hopper</name>
      <arxiv:affiliation>Compiler Lab</arxiv:affiliation>
      <arxiv:affiliation>Naval Research</arxiv:affiliation>
    </author>
    <arxiv:primary_category term="cs.CL" scheme="http://arxiv.org/schemas/atom"/>
    <category term="cs.CL" scheme="http://arxiv.org/schemas/atom"/>
    <category term="cs.AI" scheme="http://arxiv.org/schemas/atom"/>
    <arxiv:comment>
      12 pages, 3 figures;
      revised metadata
    </arxiv:comment>
    <arxiv:journal_ref>J. Deterministic Fixtures 42 (2024)</arxiv:journal_ref>
    <arxiv:doi>10.48550/arXiv.2401.12345</arxiv:doi>
    <link href="https://arxiv.org/abs/2401.12345v2" rel="alternate" type="text/html"/>
    <link href="https://arxiv.org/pdf/2401.12345v2" rel="related" type="application/pdf" title="pdf"/>
    <link href="https://arxiv.org/src/2401.12345v2" rel="related" type="application/x-eprint-tar"/>
  </entry>
</feed>
"#;

        let metadata = parse_metadata_feed(&requested, atom)?.remove(0);

        assert_eq!(metadata.arxiv_id, "2401.12345v2");
        assert_eq!(
            metadata.title.as_deref(),
            Some("Metadata first lookup with rich Atom fields")
        );
        assert_eq!(metadata.primary_category.as_deref(), Some("cs.CL"));
        assert_eq!(
            metadata.comment.as_deref(),
            Some("12 pages, 3 figures; revised metadata")
        );
        assert_eq!(
            metadata.journal_ref.as_deref(),
            Some("J. Deterministic Fixtures 42 (2024)")
        );
        assert_eq!(metadata.doi.as_deref(), Some("10.48550/arXiv.2401.12345"));
        assert_eq!(metadata.categories, vec!["cs.CL", "cs.AI"]);
        assert_eq!(metadata.authors, vec!["Ada Lovelace", "Grace Hopper"]);
        assert_eq!(metadata.author_details[0].name, "Ada Lovelace");
        assert_eq!(
            metadata.author_details[0].affiliations,
            vec!["Analytical Engine Institute"]
        );
        assert_eq!(
            metadata.author_details[1].affiliations,
            vec!["Compiler Lab", "Naval Research"]
        );
        assert_eq!(
            metadata.pdf_url.as_deref(),
            Some("https://arxiv.org/pdf/2401.12345v2")
        );
        assert!(metadata.links.iter().any(|link| {
            link.href == "https://arxiv.org/src/2401.12345v2"
                && link.rel.as_deref() == Some("related")
                && link.mime_type.as_deref() == Some("application/x-eprint-tar")
        }));
        Ok(())
    }

    #[test]
    fn status_reports_ready_and_missing_material_from_local_cache_only() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let arxiv_id = "2401.12345v2";
        let paths = PaperPaths::new(temp.path(), arxiv_id);
        write_json_pretty(
            &paths.metadata_path,
            &metadata_fixture(
                arxiv_id,
                "Cached local material",
                "Searchable abstract text",
            ),
        )?;
        let manifest = materialize_source(&paths, &synthetic_source_bundle()?)?;
        extract_citations(arxiv_id, &paths, &manifest)?;

        let ready = fetcher.status(MaterialStatusRequest {
            arxiv_id: arxiv_id.to_string(),
        })?;

        assert_eq!(ready.arxiv_id, arxiv_id);
        assert_eq!(ready.base_arxiv_id, "2401.12345");
        assert_eq!(ready.version, Some(2));
        assert_eq!(ready.material_state.metadata, MaterialState::Ready);
        assert_eq!(ready.material_state.abstract_text, MaterialState::Ready);
        assert_eq!(ready.material_state.pdf_file, MaterialState::Missing);
        assert_eq!(ready.material_state.source_archive, MaterialState::Ready);
        assert_eq!(ready.material_state.source_tree, MaterialState::Ready);
        assert_eq!(ready.material_state.citations, MaterialState::Ready);
        assert_eq!(ready.material_state.source_search, MaterialState::Ready);
        assert_eq!(ready.citation_count, 3);
        assert!(ready.available_now.contains(&"metadata".to_string()));
        assert!(ready.available_now.contains(&"source_tree".to_string()));
        assert!(ready.missing.contains(&"pdf_file".to_string()));
        assert_eq!(ready.next_tool.as_deref(), Some("full_text_search"));
        assert_eq!(
            ready
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.title.as_deref()),
            Some("Cached local material")
        );

        let missing = fetcher.status(MaterialStatusRequest {
            arxiv_id: "2402.00001".to_string(),
        })?;
        assert_eq!(missing.material_state.metadata, MaterialState::Missing);
        assert_eq!(missing.material_state.source_search, MaterialState::Missing);
        assert_eq!(missing.citation_count, 0);
        assert!(missing.metadata.is_none());
        assert_eq!(missing.next_tool.as_deref(), Some("lookup_arxiv_papers"));
        Ok(())
    }

    #[tokio::test]
    async fn lookup_local_only_claims_cached_metadata_without_downloads() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let cached_id = "2401.12345";
        let paths = PaperPaths::new(temp.path(), cached_id);
        write_json_pretty(
            &paths.metadata_path,
            &metadata_fixture(
                cached_id,
                "Already cached metadata",
                "Offline lookup abstract",
            ),
        )?;

        let response = fetcher
            .lookup(LookupPapersRequest {
                arxiv_ids: vec![cached_id.to_string(), "2402.00001".to_string()],
                fetch_missing_metadata: Some(false),
                refresh_metadata: Some(false),
            })
            .await?;

        assert_eq!(response.fetched_metadata_count, 0);
        assert_eq!(response.network_requests, 0);
        assert_eq!(response.papers.len(), 2);
        assert_eq!(response.papers[0].arxiv_id, cached_id);
        assert_eq!(
            response.papers[0].material_state.metadata,
            MaterialState::Ready
        );
        assert_eq!(
            response.papers[0]
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.title.as_deref()),
            Some("Already cached metadata")
        );
        assert_eq!(response.papers[1].arxiv_id, "2402.00001");
        assert_eq!(
            response.papers[1].material_state.metadata,
            MaterialState::Missing
        );
        assert!(response.papers[1].metadata.is_none());
        Ok(())
    }

    #[test]
    fn scoped_search_self_heals_index_and_returns_provenance() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let arxiv_id = "2401.12345";
        let paths = PaperPaths::new(temp.path(), arxiv_id);
        write_json_pretty(
            &paths.metadata_path,
            &metadata_fixture(
                arxiv_id,
                "Calibration theorem for local search",
                "The abstract explains why calibration should be discoverable.",
            ),
        )?;
        let manifest = materialize_source(&paths, &searchable_source_bundle()?)?;
        extract_citations(arxiv_id, &paths, &manifest)?;

        // The paper was never indexed explicitly: the scoped search must
        // notice the empty per-paper index and index it on the fly.
        // Use scope "all" to see every category (title, metadata, body, bibliography).
        let response = fetcher.full_text_search(FullTextSearchRequest {
            query: "calibration".to_string(),
            arxiv_id: Some(arxiv_id.to_string()),
            limit: Some(10),
            scope: Some("all".to_string()),
        })?;

        assert_eq!(response.query, "calibration");
        assert_eq!(response.scope, "all");
        assert!(response.indexed_chunks > 0);
        assert!(
            response
                .results
                .iter()
                .all(|result| result.arxiv_id == arxiv_id)
        );
        assert!(response.results.iter().any(|result| {
            result.source == "metadata"
                && result.category.as_deref() == Some("title")
                && result.field.as_deref() == Some("title")
                && result
                    .path
                    .as_deref()
                    .is_some_and(|path| path.ends_with("metadata.json"))
                && result.snippet.contains("Calibration theorem")
        }));
        // Citation records are bibliography category and appear in "all" scope.
        assert!(response.results.iter().any(|result| {
            result.source == "citation"
                && result.category.as_deref() == Some("bibliography")
                && result
                    .path
                    .as_deref()
                    .is_some_and(|path| path.ends_with("main.tex"))
                && result.line_start == Some(2)
                && result.snippet.contains("Calibration citation")
        }));
        // Source file paragraphs (body category) appear in "all" scope too.
        assert!(response.results.iter().any(|result| {
            result.source == "source"
                && result.category.as_deref() == Some("body")
                && result
                    .path
                    .as_deref()
                    .is_some_and(|path| path.ends_with("main.tex"))
                && result.line_start == Some(2)
                && result.snippet.contains("Calibration citation")
        }));

        // Default scope excludes bibliography; citation chunks must not appear.
        let default_response = fetcher.full_text_search(FullTextSearchRequest {
            query: "calibration".to_string(),
            arxiv_id: Some(arxiv_id.to_string()),
            limit: Some(10),
            scope: None,
        })?;
        assert_eq!(default_response.scope, "default");
        assert!(
            default_response
                .results
                .iter()
                .all(|r| r.source != "citation"),
            "default scope must not include citation (bibliography) chunks"
        );
        Ok(())
    }

    #[test]
    fn corpus_search_self_heals_empty_index_from_cached_papers() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let arxiv_id = "2401.12345";
        let paths = PaperPaths::new(temp.path(), arxiv_id);
        write_json_pretty(
            &paths.metadata_path,
            &metadata_fixture(arxiv_id, "Calibration theorem", "About calibration."),
        )?;

        // No explicit indexing: the unscoped search must rebuild the empty
        // index from the paper cache before answering.
        let response = fetcher.full_text_search(FullTextSearchRequest {
            query: "calibration".to_string(),
            arxiv_id: None,
            limit: Some(10),
            scope: None,
        })?;
        assert!(response.indexed_chunks > 0);
        assert!(!response.results.is_empty());
        Ok(())
    }

    #[test]
    fn scoped_search_ranks_multi_term_tex_matches_best_first() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let arxiv_id = "2401.12345";
        let paths = PaperPaths::new(temp.path(), arxiv_id);
        let tex = "\\section{Analysis}\n\
            The convergence rate of stochastic\n\
            gradient descent depends on the step size.\n\
            \n\
            Gradient clipping stabilizes training.\n\
            \n\
            We tabulate convergence results in Table 2.\n";
        let mut bytes = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut bytes);
            append_tar_file(&mut builder, "main.tex", tex)?;
            builder.finish()?;
        }
        materialize_source(&paths, &bytes)?;

        let response = fetcher.full_text_search(FullTextSearchRequest {
            query: "stochastic gradient convergence".to_string(),
            arxiv_id: Some(arxiv_id.to_string()),
            limit: Some(10),
            scope: None,
        })?;

        assert_eq!(response.results.len(), 3);
        // The paragraph containing all three query terms — spread across
        // lines 1-3, which line-based substring search could never match —
        // must rank first.
        let top = &response.results[0];
        assert_eq!(top.line_start, Some(1));
        assert_eq!(top.line_end, Some(3));
        assert!(top.snippet.contains("stochastic"));
        for pair in response.results.windows(2) {
            assert!(pair[0].score >= pair[1].score);
        }
        Ok(())
    }

    #[test]
    fn full_text_search_ranks_results_across_papers_and_supports_id_filter() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;

        let build_paper = |arxiv_id: &str, title: &str, summary: &str, tex: &str| -> Result<()> {
            let paths = PaperPaths::new(temp.path(), arxiv_id);
            write_json_pretty(
                &paths.metadata_path,
                &metadata_fixture(arxiv_id, title, summary),
            )?;
            let mut bytes = Vec::new();
            {
                let mut builder = tar::Builder::new(&mut bytes);
                append_tar_file(&mut builder, "main.tex", tex)?;
                builder.finish()?;
            }
            materialize_source(&paths, &bytes)?;
            Ok(())
        };

        build_paper(
            "2401.11111",
            "Calibration of stochastic solvers",
            "We study calibration for stochastic differential solvers.",
            "The calibration procedure for stochastic solvers\nconverges under mild assumptions.\n",
        )?;
        build_paper(
            "2401.22222",
            "A survey of transformers",
            "Attention architectures reviewed.",
            "Transformers use attention layers.\nNothing about numerical solvers here.\n",
        )?;

        let report = fetcher.index_with_material()?;
        assert_eq!(report.indexed_papers, 2);
        assert!(report.indexed_material_chunks > 0);

        let response = fetcher.full_text_search(FullTextSearchRequest {
            query: "calibration stochastic solvers".to_string(),
            arxiv_id: None,
            limit: Some(10),
            scope: None,
        })?;
        assert!(!response.results.is_empty());
        assert_eq!(response.results[0].arxiv_id, "2401.11111");
        for pair in response.results.windows(2) {
            assert!(pair[0].score >= pair[1].score);
        }

        let filtered = fetcher.full_text_search(FullTextSearchRequest {
            query: "attention".to_string(),
            arxiv_id: Some("2401.22222v9".to_string()),
            limit: Some(10),
            scope: None,
        })?;
        assert!(!filtered.results.is_empty());
        assert!(
            filtered
                .results
                .iter()
                .all(|result| result.arxiv_id == "2401.22222")
        );
        Ok(())
    }

    #[test]
    fn reindexing_a_paper_replaces_rather_than_duplicates_chunks() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let arxiv_id = "2401.12345";
        let paths = PaperPaths::new(temp.path(), arxiv_id);
        write_json_pretty(
            &paths.metadata_path,
            &metadata_fixture(arxiv_id, "Calibration theorem", "About calibration."),
        )?;

        let first = fetcher.index_paper_material(arxiv_id)?;
        let second = fetcher.index_paper_material(arxiv_id)?;
        assert_eq!(first, second);

        let response = fetcher.full_text_search(FullTextSearchRequest {
            query: "calibration".to_string(),
            arxiv_id: None,
            limit: Some(100),
            scope: None,
        })?;
        assert_eq!(response.indexed_chunks as usize, second);
        Ok(())
    }

    #[test]
    fn index_with_material_prunes_chunks_of_removed_papers() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let arxiv_id = "2401.12345";
        let paths = PaperPaths::new(temp.path(), arxiv_id);
        write_json_pretty(
            &paths.metadata_path,
            &metadata_fixture(arxiv_id, "Calibration theorem", "About calibration."),
        )?;
        fetcher.index_with_material()?;

        fs::remove_dir_all(&paths.cache_dir)?;
        let report = fetcher.index_with_material()?;
        assert_eq!(report.removed_papers, 1);

        let response = fetcher.full_text_search(FullTextSearchRequest {
            query: "calibration".to_string(),
            arxiv_id: None,
            limit: Some(10),
            scope: None,
        })?;
        assert!(response.results.is_empty());
        assert_eq!(response.indexed_chunks, 0);
        Ok(())
    }

    #[test]
    fn full_text_search_rejects_queries_without_indexable_terms() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let error = fetcher
            .full_text_search(FullTextSearchRequest {
                query: "% $ x".to_string(),
                arxiv_id: Some("2401.12345".to_string()),
                limit: None,
                scope: None,
            })
            .unwrap_err();
        assert!(error.to_string().contains("two or more characters"));
        Ok(())
    }

    #[test]
    fn full_text_search_bibliography_scope_finds_bib_file_chunks_excluded_by_default() -> Result<()>
    {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let arxiv_id = "2401.12345";
        let paths = PaperPaths::new(temp.path(), arxiv_id);
        write_json_pretty(
            &paths.metadata_path,
            &metadata_fixture(arxiv_id, "Calibration theorem", "About calibration."),
        )?;
        // Bundle with both a .tex body file and a .bib bibliography file.
        let mut bytes = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut bytes);
            append_tar_file(
                &mut builder,
                "main.tex",
                "The calibration algorithm is described in Section 2.
",
            )?;
            append_tar_file(
                &mut builder,
                "refs.bib",
                "@article{smith2020,
  title={Calibration methods},
  eprint={2001.00001},
}
",
            )?;
            builder.finish()?;
        }
        materialize_source(&paths, &bytes)?;
        fetcher.index_paper_material(arxiv_id)?;

        // Default scope must not return .bib file chunks.
        let default_response = fetcher.full_text_search(FullTextSearchRequest {
            query: "calibration".to_string(),
            arxiv_id: Some(arxiv_id.to_string()),
            limit: Some(20),
            scope: None,
        })?;
        assert_eq!(default_response.scope, "default");
        assert!(
            default_response
                .results
                .iter()
                .all(|r| !r.path.as_deref().is_some_and(|p| p.ends_with(".bib"))),
            "default scope must not include .bib file chunks"
        );

        // bibliography scope must find chunks from refs.bib.
        let bib_response = fetcher.full_text_search(FullTextSearchRequest {
            query: "calibration".to_string(),
            arxiv_id: Some(arxiv_id.to_string()),
            limit: Some(20),
            scope: Some("bibliography".to_string()),
        })?;
        assert_eq!(bib_response.scope, "bibliography");
        assert!(
            bib_response
                .results
                .iter()
                .any(|r| r.path.as_deref().is_some_and(|p| p.ends_with(".bib"))),
            "bibliography scope should find .bib file chunks"
        );

        // all scope returns both body and bibliography.
        let all_response = fetcher.full_text_search(FullTextSearchRequest {
            query: "calibration".to_string(),
            arxiv_id: Some(arxiv_id.to_string()),
            limit: Some(20),
            scope: Some("all".to_string()),
        })?;
        assert_eq!(all_response.scope, "all");
        assert!(
            all_response
                .results
                .iter()
                .any(|r| r.category.as_deref() == Some("body")),
            "all scope should include body chunks"
        );
        assert!(
            all_response
                .results
                .iter()
                .any(|r| r.category.as_deref() == Some("bibliography")),
            "all scope should include bibliography chunks"
        );
        Ok(())
    }

    #[test]
    fn full_text_search_titles_scope_returns_title_chunks_only() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let arxiv_id = "2401.12345";
        let paths = PaperPaths::new(temp.path(), arxiv_id);
        write_json_pretty(
            &paths.metadata_path,
            &metadata_fixture(
                arxiv_id,
                "Calibration theorem for stochastic systems",
                "About calibration.",
            ),
        )?;
        fetcher.index_paper_material(arxiv_id)?;

        let response = fetcher.full_text_search(FullTextSearchRequest {
            query: "calibration stochastic".to_string(),
            arxiv_id: Some(arxiv_id.to_string()),
            limit: Some(10),
            scope: Some("titles".to_string()),
        })?;
        assert_eq!(response.scope, "titles");
        assert!(
            response
                .results
                .iter()
                .all(|r| r.category.as_deref() == Some("title")),
            "titles scope should return only title-category chunks"
        );
        Ok(())
    }

    #[test]
    fn full_text_search_invalid_scope_returns_error() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let error = fetcher
            .full_text_search(FullTextSearchRequest {
                query: "calibration".to_string(),
                arxiv_id: None,
                limit: None,
                scope: Some("unknown_scope".to_string()),
            })
            .unwrap_err();
        let msg = error.to_string();
        assert!(
            msg.contains("unknown scope"),
            "error should mention unknown scope: {msg}"
        );
        assert!(
            msg.contains("default"),
            "error should list valid scopes: {msg}"
        );
        Ok(())
    }

    #[test]
    fn full_text_search_empty_results_note_for_uncached_paper() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        // Paper 2401.99999 has never been cached.
        let response = fetcher.full_text_search(FullTextSearchRequest {
            query: "calibration".to_string(),
            arxiv_id: Some("2401.99999".to_string()),
            limit: Some(10),
            scope: None,
        })?;
        assert!(response.results.is_empty());
        let note = response
            .note
            .expect("empty results for uncached paper should have a note");
        assert!(
            note.contains("fetch_arxiv_paper"),
            "note should direct to fetch_arxiv_paper: {note}"
        );
        Ok(())
    }

    #[test]
    fn full_text_search_empty_results_note_for_metadata_only_paper() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let arxiv_id = "2401.12345";
        let paths = PaperPaths::new(temp.path(), arxiv_id);
        // Only metadata, no source files.
        write_json_pretty(
            &paths.metadata_path,
            &metadata_fixture(arxiv_id, "Calibration theorem", "About calibration."),
        )?;
        fetcher.index_paper_material(arxiv_id)?;

        // Query that won't match metadata but would match body if present.
        let response = fetcher.full_text_search(FullTextSearchRequest {
            query: "zzznomatch".to_string(),
            arxiv_id: Some(arxiv_id.to_string()),
            limit: Some(10),
            scope: None,
        })?;
        assert!(response.results.is_empty());
        let note = response
            .note
            .expect("empty results for metadata-only paper should have a note");
        assert!(
            note.contains("fetch_arxiv_paper") || note.contains("metadata"),
            "note should guide the agent: {note}"
        );
        Ok(())
    }

    #[test]
    fn full_text_search_empty_default_scope_notes_bibliography_exclusion() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;
        let arxiv_id = "2401.12345";
        let paths = PaperPaths::new(temp.path(), arxiv_id);
        write_json_pretty(
            &paths.metadata_path,
            &metadata_fixture(arxiv_id, "Calibration theorem", "About calibration."),
        )?;
        fetcher.index_paper_material(arxiv_id)?;

        // Unscoped corpus query with no matches: the note must tell the
        // agent that bibliography content is excluded by default.
        let response = fetcher.full_text_search(FullTextSearchRequest {
            query: "zzznomatch".to_string(),
            arxiv_id: None,
            limit: Some(10),
            scope: None,
        })?;
        assert!(response.results.is_empty());
        let note = response
            .note
            .expect("empty default-scope results should carry a note");
        assert!(
            note.contains("bibliography"),
            "note should mention bibliography exclusion: {note}"
        );
        Ok(())
    }

    fn fetcher_without_rate_limit(cache_root: &Path) -> Result<ArxivFetcher> {
        let mut fetcher = ArxivFetcher::new(cache_root.to_path_buf())?;
        fetcher.rate_limiter = RateLimiter::with_delay(cache_root, Duration::ZERO);
        Ok(fetcher)
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Result<String> {
        use tokio::io::AsyncReadExt;

        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        loop {
            let bytes_read = stream.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..bytes_read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        Ok(String::from_utf8(request)?)
    }

    fn metadata_fixture(arxiv_id: &str, title: &str, summary: &str) -> PaperMetadata {
        PaperMetadata {
            arxiv_id: arxiv_id.to_string(),
            abs_url: Some(format!("https://arxiv.org/abs/{arxiv_id}")),
            pdf_url: Some(format!("https://arxiv.org/pdf/{arxiv_id}")),
            title: Some(title.to_string()),
            authors: vec!["Offline Author".to_string()],
            author_details: vec![PaperAuthor {
                name: "Offline Author".to_string(),
                affiliations: vec!["Local Fixtures Lab".to_string()],
            }],
            summary: Some(summary.to_string()),
            published: Some("2024-01-01T00:00:00Z".to_string()),
            updated: Some("2024-01-02T00:00:00Z".to_string()),
            categories: vec!["cs.CL".to_string()],
            primary_category: Some("cs.CL".to_string()),
            comment: Some("local deterministic fixture".to_string()),
            journal_ref: None,
            doi: Some("10.48550/arXiv.2401.12345".to_string()),
            links: vec![
                AtomLink {
                    href: format!("https://arxiv.org/abs/{arxiv_id}"),
                    rel: Some("alternate".to_string()),
                    title: None,
                    mime_type: Some("text/html".to_string()),
                },
                AtomLink {
                    href: format!("https://arxiv.org/pdf/{arxiv_id}"),
                    rel: Some("related".to_string()),
                    title: Some("pdf".to_string()),
                    mime_type: Some("application/pdf".to_string()),
                },
            ],
        }
    }

    fn searchable_source_bundle() -> Result<Vec<u8>> {
        let tex = r#"
Calibration citation cites arXiv:2101.00001 from source material.
A second searchable source line mentions calibration without a citation.
"#;
        let mut bytes = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut bytes);
            append_tar_file(&mut builder, "main.tex", tex)?;
            builder.finish()?;
        }
        Ok(bytes)
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

    /// Minimal valid Atom feed with one entry.
    fn atom_feed_for(arxiv_id: &str, title: &str) -> String {
        format!(
            r#"<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>https://arxiv.org/abs/{arxiv_id}</id>
    <title>{title}</title>
    <summary>Abstract of {title}.</summary>
    <author><name>Test Author</name></author>
    <published>2024-01-01T00:00:00Z</published>
    <updated>2024-01-01T00:00:00Z</updated>
  </entry>
</feed>"#
        )
    }

    #[tokio::test]
    async fn lookup_batch_500_returns_cached_papers_and_per_id_errors() -> Result<()> {
        use tokio::{io::AsyncWriteExt, net::TcpListener};

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;

        // Server: always responds 500 Internal Server Error.
        let server = tokio::spawn(async move {
            // We may receive multiple requests (batch + per-id fallbacks).
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                read_http_request(&mut stream).await.ok();
                stream
                    .write_all(
                        b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 5\r\nConnection: close\r\n\r\noops!",
                    )
                    .await
                    .ok();
            }
        });

        let temp = tempdir()?;
        let mut fetcher = fetcher_without_rate_limit(temp.path())?;
        // Point metadata requests at our test server.
        fetcher.metadata_url = format!("http://{addr}/api/query");

        // Pre-cache metadata for one paper so it must survive the 500.
        let cached_id = "2401.11111";
        let uncached_id = "2401.99999";
        let paths = PaperPaths::new(temp.path(), cached_id);
        write_json_pretty(
            &paths.metadata_path,
            &metadata_fixture(cached_id, "Cached paper", "Cached abstract"),
        )?;

        // Exercise the real path: batch fails with 500, per-id fallback also
        // fails, and the failure is attributed to the uncached id only.
        let response = fetcher
            .lookup(LookupPapersRequest {
                arxiv_ids: vec![cached_id.to_string(), uncached_id.to_string()],
                fetch_missing_metadata: Some(true),
                refresh_metadata: Some(false),
            })
            .await?;

        assert_eq!(response.papers.len(), 2);
        let cached_paper = response
            .papers
            .iter()
            .find(|p| p.arxiv_id == cached_id)
            .unwrap();
        assert_eq!(cached_paper.material_state.metadata, MaterialState::Ready);
        assert!(
            cached_paper.metadata.is_some(),
            "cached metadata must be returned"
        );
        assert!(
            cached_paper.error.is_none(),
            "cached paper should have no error"
        );

        let failed_paper = response
            .papers
            .iter()
            .find(|p| p.arxiv_id == uncached_id)
            .unwrap();
        let msg = failed_paper
            .error
            .as_deref()
            .expect("uncached paper should carry a per-id error");
        assert!(msg.contains("500"), "error should mention HTTP 500: {msg}");
        assert!(
            msg.contains("oops"),
            "error should include body snippet: {msg}"
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn lookup_systemic_batch_failure_skips_per_id_fallback() -> Result<()> {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio::{io::AsyncWriteExt, net::TcpListener};

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let request_count = Arc::new(AtomicUsize::new(0));
        let server_count = request_count.clone();

        // Server: always responds 429, counting requests.
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                server_count.fetch_add(1, Ordering::SeqCst);
                read_http_request(&mut stream).await.ok();
                stream
                    .write_all(
                        b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 14\r\nConnection: close\r\n\r\nRate exceeded.",
                    )
                    .await
                    .ok();
            }
        });

        let temp = tempdir()?;
        let mut fetcher = fetcher_without_rate_limit(temp.path())?;
        fetcher.metadata_url = format!("http://{addr}/api/query");

        let ids = vec![
            "2401.11111".to_string(),
            "2401.22222".to_string(),
            "2401.33333".to_string(),
        ];
        let response = fetcher
            .lookup(LookupPapersRequest {
                arxiv_ids: ids,
                fetch_missing_metadata: Some(true),
                refresh_metadata: Some(false),
            })
            .await?;

        // Rate limiting is not id-specific: exactly ONE request, no per-id
        // fallback storm (observed live: 16 requests / 205s / worse 429s).
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        assert_eq!(response.network_requests, 1);
        for paper in &response.papers {
            let error = paper.error.as_deref().expect("every id carries the error");
            assert!(error.contains("429"), "error should mention 429: {error}");
        }

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn queued_fetch_metadata_retries_systemic_errors_with_backoff() -> Result<()> {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio::{io::AsyncWriteExt, net::TcpListener};

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let request_count = Arc::new(AtomicUsize::new(0));
        let server_count = request_count.clone();
        let arxiv_id = "2401.11111";
        let feed = atom_feed_for(arxiv_id, "Retried paper");

        // Server: 500 on the first request, valid feed afterwards.
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let count = server_count.fetch_add(1, Ordering::SeqCst);
                read_http_request(&mut stream).await.ok();
                let response = if count == 0 {
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 5\r\nConnection: close\r\n\r\noops!".to_string()
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        feed.len(),
                        feed
                    )
                };
                stream.write_all(response.as_bytes()).await.ok();
            }
        });

        let temp = tempdir()?;
        let mut fetcher = fetcher_without_rate_limit(temp.path())?;
        fetcher.metadata_url = format!("http://{addr}/api/query");
        fetcher.metadata_retry_waits = vec![Duration::ZERO];

        // Metadata-only fetch drives the queued-download metadata path.
        let response = fetcher
            .fetch(FetchPaperRequest {
                arxiv_id: arxiv_id.to_string(),
                include_pdf: Some(false),
                include_source: Some(false),
                refresh: Some(false),
            })
            .await?;

        assert_eq!(request_count.load(Ordering::SeqCst), 2, "one retry");
        assert_eq!(response.title.as_deref(), Some("Retried paper"));

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn lookup_metadata_request_times_out_with_per_id_error() -> Result<()> {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;

        // Server: accepts connections and reads requests but never responds,
        // simulating the observed slow/hung arXiv error responses.
        let server = tokio::spawn(async move {
            let mut streams = Vec::new();
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                read_http_request(&mut stream).await.ok();
                // Hold the stream open without responding.
                streams.push(stream);
            }
        });

        let temp = tempdir()?;
        let mut fetcher = fetcher_without_rate_limit(temp.path())?;
        fetcher.metadata_url = format!("http://{addr}/api/query");
        fetcher.metadata_timeout = Duration::from_millis(250);

        let uncached_id = "2401.99999";
        let start = std::time::Instant::now();
        let response = fetcher
            .lookup(LookupPapersRequest {
                arxiv_ids: vec![uncached_id.to_string()],
                fetch_missing_metadata: Some(true),
                refresh_metadata: Some(false),
            })
            .await?;
        let elapsed = start.elapsed();

        // Batch attempt + per-id fallback, each bounded at 250ms.
        assert!(
            elapsed < Duration::from_secs(5),
            "lookup must fail fast instead of hanging (took {elapsed:?})"
        );

        assert_eq!(response.papers.len(), 1);
        let msg = response.papers[0]
            .error
            .as_deref()
            .expect("timed-out paper should carry a per-id error");
        assert!(
            msg.contains("timed out after"),
            "error should describe the timeout: {msg}"
        );

        server.abort();
        Ok(())
    }

    #[test]
    fn lookup_batch_success_parses_multi_entry_feed_without_per_id_errors() -> Result<()> {
        // parse_metadata_feed is the core of the batch-success path: a 200 feed with
        // N entries must produce N PaperMetadata values with no error field.
        let arxiv_id_1 = "2401.11111";
        let arxiv_id_2 = "2401.22222";
        let feed_body = format!(
            r#"<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>https://arxiv.org/abs/{arxiv_id_1}</id>
    <title>Paper One</title>
    <summary>Abstract one.</summary>
    <author><name>Author A</name></author>
    <published>2024-01-01T00:00:00Z</published>
    <updated>2024-01-01T00:00:00Z</updated>
  </entry>
  <entry>
    <id>https://arxiv.org/abs/{arxiv_id_2}</id>
    <title>Paper Two</title>
    <summary>Abstract two.</summary>
    <author><name>Author B</name></author>
    <published>2024-01-01T00:00:00Z</published>
    <updated>2024-01-01T00:00:00Z</updated>
  </entry>
</feed>"#
        );
        let ids = vec![arxiv_id_1.to_string(), arxiv_id_2.to_string()];
        let papers = parse_metadata_feed(&ids, &feed_body)?;
        assert_eq!(papers.len(), 2);
        assert_eq!(papers[0].arxiv_id, arxiv_id_1);
        assert_eq!(papers[1].arxiv_id, arxiv_id_2);
        // A successful batch produces a lookup map with both ids.
        let by_id = metadata_lookup(papers);
        assert!(by_id.contains_key(arxiv_id_1));
        assert!(by_id.contains_key(arxiv_id_2));
        // No per-id errors are generated by parse_metadata_feed itself.
        Ok(())
    }

    #[tokio::test]
    async fn lookup_id_missing_from_200_feed_returns_per_id_error() -> Result<()> {
        let temp = tempdir()?;
        let fetcher = ArxivFetcher::new(temp.path().to_path_buf())?;

        // Pre-cache metadata for id_good so it always returns.
        let id_good = "2401.11111";
        let id_bad = "2401.99999";
        let paths = PaperPaths::new(temp.path(), id_good);
        write_json_pretty(
            &paths.metadata_path,
            &metadata_fixture(id_good, "Good paper", "Good abstract"),
        )?;

        // Request both ids with fetch disabled: simulate the "id missing from feed"
        // path by inspecting a batch where id_bad has no cached metadata and
        // fetch_missing_metadata=false, so id_bad gets no metadata and no error.
        let response = fetcher
            .lookup(LookupPapersRequest {
                arxiv_ids: vec![id_good.to_string(), id_bad.to_string()],
                fetch_missing_metadata: Some(false),
                refresh_metadata: Some(false),
            })
            .await?;

        assert_eq!(response.papers.len(), 2);

        let good = response
            .papers
            .iter()
            .find(|p| p.arxiv_id == id_good)
            .unwrap();
        assert_eq!(good.material_state.metadata, MaterialState::Ready);
        assert!(good.metadata.is_some());
        assert!(good.error.is_none());

        let bad = response
            .papers
            .iter()
            .find(|p| p.arxiv_id == id_bad)
            .unwrap();
        assert_eq!(bad.material_state.metadata, MaterialState::Missing);
        assert!(bad.metadata.is_none());
        // No error because fetch was disabled — the paper is simply missing.
        assert!(bad.error.is_none());

        // Now test the "missing from 200 feed" path via parse:
        // parse a feed that only contains id_good; id_bad would be absent.
        let feed = atom_feed_for(id_good, "Good paper");
        let ids = vec![id_good.to_string(), id_bad.to_string()];
        let papers = parse_metadata_feed(&ids, &feed)?;
        // parse_metadata_feed returns one entry per feed <entry>, not per requested id.
        assert_eq!(papers.len(), 1);
        assert_eq!(papers[0].arxiv_id, id_good);

        // In the lookup path, an id missing from the feed produces a per-id error.
        // Verify via metadata_lookup: id_bad won't be in the map.
        let fetched_by_id = metadata_lookup(papers);
        assert!(fetched_by_id.get(id_bad).is_none());
        assert!(fetched_by_id.get(id_good).is_some());

        Ok(())
    }

    /// Serve fake PDF bytes and a synthetic source tar for /pdf/... and
    /// /e-print/... requests, counting requests.
    fn spawn_material_server(
        listener: tokio::net::TcpListener,
        request_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> tokio::task::JoinHandle<()> {
        use std::sync::atomic::Ordering;
        use tokio::io::AsyncWriteExt;

        tokio::spawn(async move {
            let pdf_bytes = b"%PDF-1.4 synthetic".to_vec();
            let source_bytes = searchable_source_bundle().expect("synthetic source bundle");
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let Ok(request) = read_http_request(&mut stream).await else {
                    continue;
                };
                request_count.fetch_add(1, Ordering::SeqCst);
                let body: &[u8] = if request.starts_with("GET /pdf/") {
                    &pdf_bytes
                } else {
                    &source_bytes
                };
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(header.as_bytes()).await;
                let _ = stream.write_all(body).await;
            }
        })
    }

    /// Accept metadata connections but never answer, so every request hits
    /// the client-side timeout — the observed export.arxiv.org failure mode.
    fn spawn_hanging_metadata_server(
        listener: tokio::net::TcpListener,
        request_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> tokio::task::JoinHandle<()> {
        use std::sync::atomic::Ordering;

        tokio::spawn(async move {
            let mut streams = Vec::new();
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                request_count.fetch_add(1, Ordering::SeqCst);
                read_http_request(&mut stream).await.ok();
                streams.push(stream);
            }
        })
    }

    #[tokio::test]
    async fn fetch_downloads_material_and_reports_pending_metadata_when_metadata_is_down()
    -> Result<()> {
        use std::sync::{Arc, atomic::AtomicUsize, atomic::Ordering};
        use tokio::net::TcpListener;

        let metadata_listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let metadata_addr = metadata_listener.local_addr()?;
        let metadata_requests = Arc::new(AtomicUsize::new(0));
        let metadata_server =
            spawn_hanging_metadata_server(metadata_listener, metadata_requests.clone());

        let material_listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let material_addr = material_listener.local_addr()?;
        let material_requests = Arc::new(AtomicUsize::new(0));
        let material_server = spawn_material_server(material_listener, material_requests.clone());

        let temp = tempdir()?;
        let mut fetcher = fetcher_without_rate_limit(temp.path())?;
        fetcher.metadata_url = format!("http://{metadata_addr}/api/query");
        fetcher.material_base_url = format!("http://{material_addr}");
        fetcher.metadata_timeout = Duration::from_millis(200);
        fetcher.metadata_retry_waits = vec![Duration::ZERO];

        let arxiv_id = "2401.12345";
        let response = fetcher
            .fetch(FetchPaperRequest {
                arxiv_id: arxiv_id.to_string(),
                include_pdf: Some(true),
                include_source: Some(true),
                refresh: Some(false),
            })
            .await?;

        // The job completed: material was downloaded from the healthy host
        // even though the metadata host never answered.
        assert!(response.metadata_pending, "metadata must be pending");
        assert!(response.title.is_none());
        let paths = PaperPaths::new(temp.path(), arxiv_id);
        assert!(paths.pdf_path.exists(), "pdf must be cached");
        assert!(paths.source_manifest_path.exists(), "source must be cached");
        assert!(
            !paths.metadata_path.exists(),
            "no metadata.json while arXiv metadata is down"
        );
        assert!(
            material_requests.load(Ordering::SeqCst) >= 2,
            "pdf and source must have been fetched"
        );

        // Two consecutive systemic failures tripped the shared breaker.
        // Assert the recorded state, not liveness against the wall clock:
        // the material download + indexing above can outlast the 30s pause
        // window when the test host is heavily loaded.
        let health = fetcher.metadata_health()?;
        assert_eq!(health.failure_streak, 2);
        assert!(
            health.paused_until_unix_ms > 0,
            "metadata pause deadline should be recorded"
        );

        metadata_server.abort();
        material_server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn fetch_skips_metadata_entirely_while_breaker_is_open() -> Result<()> {
        use std::sync::{Arc, atomic::AtomicUsize, atomic::Ordering};
        use tokio::net::TcpListener;

        let metadata_listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let metadata_addr = metadata_listener.local_addr()?;
        let metadata_requests = Arc::new(AtomicUsize::new(0));
        let metadata_server =
            spawn_hanging_metadata_server(metadata_listener, metadata_requests.clone());

        let material_listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let material_addr = material_listener.local_addr()?;
        let material_requests = Arc::new(AtomicUsize::new(0));
        let material_server = spawn_material_server(material_listener, material_requests.clone());

        let temp = tempdir()?;
        let mut fetcher = fetcher_without_rate_limit(temp.path())?;
        fetcher.metadata_url = format!("http://{metadata_addr}/api/query");
        fetcher.material_base_url = format!("http://{material_addr}");

        // Open the shared breaker the way production does: consecutive
        // systemic failures recorded by other workers/processes.
        fetcher.rate_limiter.record_metadata_failure().await?;
        fetcher.rate_limiter.record_metadata_failure().await?;

        let start = std::time::Instant::now();
        let response = fetcher
            .fetch(FetchPaperRequest {
                arxiv_id: "2401.22222".to_string(),
                include_pdf: Some(true),
                include_source: Some(false),
                refresh: Some(false),
            })
            .await?;

        assert!(response.metadata_pending);
        assert_eq!(
            metadata_requests.load(Ordering::SeqCst),
            0,
            "an open breaker must short-circuit metadata without network attempts"
        );
        assert!(material_requests.load(Ordering::SeqCst) >= 1);
        // Well under the 30s pause: proves the fetch never waited it out.
        // (Zero metadata requests above is the primary short-circuit proof;
        // this bound only needs headroom for slow loaded test hosts.)
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "paused metadata must not delay the material download"
        );

        metadata_server.abort();
        material_server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn ensure_metadata_cached_coalesces_ids_into_one_batched_request() -> Result<()> {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio::{io::AsyncWriteExt, net::TcpListener};

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let request_count = Arc::new(AtomicUsize::new(0));
        let server_count = request_count.clone();

        let feed = format!(
            r#"<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>https://arxiv.org/abs/2401.11111</id>
    <title>Paper One</title>
    <summary>Abstract one.</summary>
    <author><name>Author A</name></author>
  </entry>
  <entry>
    <id>https://arxiv.org/abs/2401.22222</id>
    <title>Paper Two</title>
    <summary>Abstract two.</summary>
    <author><name>Author B</name></author>
  </entry>
  <entry>
    <id>https://arxiv.org/abs/2401.33333</id>
    <title>Paper Three</title>
    <summary>Abstract three.</summary>
    <author><name>Author C</name></author>
  </entry>
</feed>"#
        );
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                server_count.fetch_add(1, Ordering::SeqCst);
                if let Ok(request) = read_http_request(&mut stream).await {
                    requests.push(request);
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    feed.len(),
                    feed
                );
                stream.write_all(response.as_bytes()).await.ok();
            }
        });

        let temp = tempdir()?;
        let mut fetcher = fetcher_without_rate_limit(temp.path())?;
        fetcher.metadata_url = format!("http://{addr}/api/query");

        let ids = vec![
            "2401.11111".to_string(),
            "2401.22222".to_string(),
            "2401.33333".to_string(),
        ];
        fetcher.ensure_metadata_cached(&ids).await;

        assert_eq!(
            request_count.load(Ordering::SeqCst),
            1,
            "three queued papers must produce exactly one metadata request"
        );
        for id in &ids {
            let paths = PaperPaths::new(temp.path(), id);
            assert!(
                paths.metadata_path.exists(),
                "metadata for {id} should be cached by the batch"
            );
        }

        // A second call is a no-op: everything is cached.
        fetcher.ensure_metadata_cached(&ids).await;
        assert_eq!(request_count.load(Ordering::SeqCst), 1);

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn ensure_metadata_cached_backfills_pending_papers_after_recovery() -> Result<()> {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio::{io::AsyncWriteExt, net::TcpListener};

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let request_count = Arc::new(AtomicUsize::new(0));
        let server_count = request_count.clone();
        let arxiv_id = "2401.44444";
        let feed = atom_feed_for(arxiv_id, "Backfilled paper");
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                server_count.fetch_add(1, Ordering::SeqCst);
                read_http_request(&mut stream).await.ok();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    feed.len(),
                    feed
                );
                stream.write_all(response.as_bytes()).await.ok();
            }
        });

        let temp = tempdir()?;
        let mut fetcher = fetcher_without_rate_limit(temp.path())?;
        fetcher.metadata_url = format!("http://{addr}/api/query");

        // A paper downloaded during an outage left metadata pending.
        fetcher.mark_pending_metadata(arxiv_id);

        // Queue-drain backfill runs with no explicit ids.
        fetcher.ensure_metadata_cached(&[]).await;

        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        let paths = PaperPaths::new(temp.path(), arxiv_id);
        assert!(paths.metadata_path.exists());
        let metadata: PaperMetadata = read_json(&paths.metadata_path)?;
        assert_eq!(metadata.title.as_deref(), Some("Backfilled paper"));
        assert!(
            fetcher
                .pending_metadata
                .lock()
                .expect("pending set")
                .is_empty(),
            "backfilled paper must leave the pending set"
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn fetch_self_heals_missing_metadata_without_redownloading_material() -> Result<()> {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio::{io::AsyncWriteExt, net::TcpListener};

        let metadata_listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let metadata_addr = metadata_listener.local_addr()?;
        let material_listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let material_addr = material_listener.local_addr()?;
        let material_requests = Arc::new(AtomicUsize::new(0));
        let material_server = spawn_material_server(material_listener, material_requests.clone());

        let arxiv_id = "2401.55555";
        let feed = atom_feed_for(arxiv_id, "Healed paper");
        let metadata_server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = metadata_listener.accept().await else {
                    break;
                };
                read_http_request(&mut stream).await.ok();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    feed.len(),
                    feed
                );
                stream.write_all(response.as_bytes()).await.ok();
            }
        });

        let temp = tempdir()?;
        let mut fetcher = fetcher_without_rate_limit(temp.path())?;
        fetcher.metadata_url = format!("http://{metadata_addr}/api/query");
        fetcher.material_base_url = format!("http://{material_addr}");

        // Simulate the aftermath of an outage: material cached, metadata not.
        fetcher.rate_limiter.record_metadata_failure().await?;
        fetcher.rate_limiter.record_metadata_failure().await?;
        let first = fetcher
            .fetch(FetchPaperRequest {
                arxiv_id: arxiv_id.to_string(),
                include_pdf: Some(true),
                include_source: Some(true),
                refresh: Some(false),
            })
            .await?;
        assert!(first.metadata_pending);
        let material_requests_after_first = material_requests.load(Ordering::SeqCst);
        assert!(material_requests_after_first >= 2);

        // arXiv recovers (pause expires).
        fetcher.rate_limiter.record_metadata_recovery().await?;

        let second = fetcher
            .fetch(FetchPaperRequest {
                arxiv_id: arxiv_id.to_string(),
                include_pdf: Some(true),
                include_source: Some(true),
                refresh: Some(false),
            })
            .await?;

        assert!(!second.metadata_pending);
        assert_eq!(second.title.as_deref(), Some("Healed paper"));
        let paths = PaperPaths::new(temp.path(), arxiv_id);
        assert!(paths.metadata_path.exists());
        assert_eq!(
            material_requests.load(Ordering::SeqCst),
            material_requests_after_first,
            "self-heal must fetch only metadata, not re-download material"
        );

        metadata_server.abort();
        material_server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn fetch_still_fails_on_paper_specific_metadata_errors() -> Result<()> {
        use tokio::{io::AsyncWriteExt, net::TcpListener};

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        // 200 OK with a feed containing no entries: arXiv answered, the
        // paper simply is not there. That is not an outage; the job fails.
        let feed = r#"<feed xmlns="http://www.w3.org/2005/Atom"></feed>"#;
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                read_http_request(&mut stream).await.ok();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    feed.len(),
                    feed
                );
                stream.write_all(response.as_bytes()).await.ok();
            }
        });

        let temp = tempdir()?;
        let mut fetcher = fetcher_without_rate_limit(temp.path())?;
        fetcher.metadata_url = format!("http://{addr}/api/query");

        let error = fetcher
            .fetch(FetchPaperRequest {
                arxiv_id: "2401.99999".to_string(),
                include_pdf: Some(false),
                include_source: Some(false),
                refresh: Some(false),
            })
            .await
            .expect_err("missing entry is a paper-specific failure");
        assert!(
            format!("{error:#}").contains("did not contain an entry"),
            "{error:#}"
        );

        // A real answer from arXiv is a healthy service: no breaker trip.
        let health = fetcher.metadata_health()?;
        assert_eq!(health.failure_streak, 0);

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn lookup_fails_fast_with_paused_error_and_zero_requests_while_breaker_open() -> Result<()>
    {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio::net::TcpListener;

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let request_count = Arc::new(AtomicUsize::new(0));
        let server = spawn_hanging_metadata_server(listener, request_count.clone());

        let temp = tempdir()?;
        let mut fetcher = fetcher_without_rate_limit(temp.path())?;
        fetcher.metadata_url = format!("http://{addr}/api/query");
        fetcher.rate_limiter.record_metadata_failure().await?;
        fetcher.rate_limiter.record_metadata_failure().await?;

        let start = std::time::Instant::now();
        let response = fetcher
            .lookup(LookupPapersRequest {
                arxiv_ids: vec!["2401.11111".to_string(), "2401.22222".to_string()],
                fetch_missing_metadata: Some(true),
                refresh_metadata: Some(false),
            })
            .await?;

        assert_eq!(request_count.load(Ordering::SeqCst), 0);
        assert!(start.elapsed() < Duration::from_secs(5));
        for paper in &response.papers {
            let error = paper.error.as_deref().expect("per-id paused error");
            assert!(
                error.contains("paused until"),
                "error should explain the pause: {error}"
            );
        }

        server.abort();
        Ok(())
    }

    #[test]
    fn rate_limit_penalty_classifies_429_but_not_timeouts() {
        let rate_limited = anyhow::anyhow!("arXiv metadata request failed: 429 Too Many Requests");
        assert_eq!(rate_limit_penalty(&rate_limited), Some(RATE_LIMIT_PENALTY));

        let timed_out = anyhow::anyhow!("arXiv metadata request timed out after 10s");
        assert_eq!(rate_limit_penalty(&timed_out), None);

        // "429" embedded in ids or timestamps must not draw a penalty.
        let id_lookalike = anyhow::anyhow!("arXiv metadata response did not contain 2401.42900");
        assert_eq!(rate_limit_penalty(&id_lookalike), None);

        // A local paused short-circuit is not an arXiv answer, even though
        // its timestamp could contain "429" by chance.
        let paused = anyhow::Error::new(MetadataPausedError {
            paused_until_unix_ms: 429_429_429,
        });
        assert_eq!(rate_limit_penalty(&paused), None);
    }

    #[test]
    fn fetch_response_without_metadata_pending_field_still_parses() -> Result<()> {
        // Wire compatibility: responses persisted by older arxd versions
        // (finished-jobs file) lack the new field.
        let old_json = r#"{
            "arxiv_id": "2401.12345",
            "cache_dir": "/tmp/cache",
            "metadata_path": "/tmp/cache/metadata.json",
            "metadata_db_path": "/tmp/cache/metadata.sqlite3",
            "indexed_metadata_records": 1,
            "pdf_path": null,
            "source_archive_path": null,
            "source_extracted_dir": null,
            "citations_jsonl_path": null,
            "title": null,
            "authors": [],
            "citation_count": 0,
            "cache_hit": false,
            "network_requests": 1,
            "rate_limit_delay_seconds": 3
        }"#;
        let response: FetchPaperResponse = serde_json::from_str(old_json)?;
        assert!(!response.metadata_pending);
        Ok(())
    }
}
