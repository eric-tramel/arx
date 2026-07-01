use arx_core::{
    arxiv::{
        ArxivFetcher, FetchPaperRequest, LocatePaperRequest, LocatePaperResponse,
        LookupPapersRequest, LookupPapersResponse, MaterialStatusRequest, PaperMaterialStatus,
        SearchCorpusRequest, SearchCorpusResponse, SearchMaterialRequest, SearchMaterialResponse,
    },
    daemon::{
        ArxdClient, DownloadQueueStatusRequest, DownloadQueueStatusResponse, QueuedFetchResponse,
    },
    metadata_db::IndexReport,
};
use rmcp::{
    Json, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{Implementation, ProtocolVersion, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct ArxMcpServer {
    fetcher: Arc<ArxivFetcher>,
    daemon_client: ArxdClient,
    tool_router: ToolRouter<Self>,
}

impl ArxMcpServer {
    pub fn new(fetcher: ArxivFetcher) -> Self {
        let daemon_client = ArxdClient::new(fetcher.cache_root().to_path_buf());
        Self {
            fetcher: Arc::new(fetcher),
            daemon_client,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router(router = tool_router)]
impl ArxMcpServer {
    #[tool(
        name = "lookup_arxiv_papers",
        description = "Metadata-first arXiv lookup. Returns local material readiness, cached metadata/abstract immediately when present, and fetches only missing metadata through the arXiv API by default. It never downloads PDF or source material."
    )]
    pub async fn lookup_arxiv_papers(
        &self,
        Parameters(request): Parameters<LookupPapersRequest>,
    ) -> Result<Json<LookupPapersResponse>, String> {
        self.fetcher
            .lookup(request)
            .await
            .map(Json)
            .map_err(|error| error.to_string())
    }

    #[tool(
        name = "get_arxiv_material_status",
        description = "Return local-only material readiness for an arXiv paper: metadata, abstract, PDF file, source archive/tree, citations, and source search availability. This never contacts arXiv."
    )]
    pub fn get_arxiv_material_status(
        &self,
        Parameters(request): Parameters<MaterialStatusRequest>,
    ) -> Result<Json<PaperMaterialStatus>, String> {
        self.fetcher
            .status(request)
            .map(Json)
            .map_err(|error| error.to_string())
    }

    #[tool(
        name = "search_arxiv_material",
        description = "Search only local cached arXiv material for a paper. It runs BM25-ranked free-text search over cached metadata/abstract, citations JSONL, and extracted TeX/source paragraphs, returning best-first snippets with scores, source paths, and line ranges. This never contacts arXiv."
    )]
    pub fn search_arxiv_material(
        &self,
        Parameters(request): Parameters<SearchMaterialRequest>,
    ) -> Result<Json<SearchMaterialResponse>, String> {
        self.fetcher
            .search_material(request)
            .map(Json)
            .map_err(|error| error.to_string())
    }

    #[tool(
        name = "fetch_arxiv_paper",
        description = "Queue an arXiv paper download with arxd and return immediately with a download job id. The arxd backend indexes cached metadata first, then downloads metadata/PDF/source as requested and stores metadata in the cache SQLite database."
    )]
    pub async fn fetch_arxiv_paper(
        &self,
        Parameters(request): Parameters<FetchPaperRequest>,
    ) -> Result<Json<QueuedFetchResponse>, String> {
        self.daemon_client
            .enqueue_fetch(request)
            .await
            .map(Json)
            .map_err(|error| error.to_string())
    }

    #[tool(
        name = "prepare_arxiv_material",
        description = "Queue PDF and/or source acquisition through arxd and return immediately with a download job id. Use lookup_arxiv_papers first for metadata/abstract; use this only when local PDF/source/search material is missing and needed."
    )]
    pub async fn prepare_arxiv_material(
        &self,
        Parameters(request): Parameters<FetchPaperRequest>,
    ) -> Result<Json<QueuedFetchResponse>, String> {
        self.daemon_client
            .enqueue_fetch(request)
            .await
            .map(Json)
            .map_err(|error| error.to_string())
    }

    #[tool(
        name = "get_arxiv_download_queue_status",
        description = "Ask arxd for queued, in-progress, completed, and failed arXiv download jobs with rough seconds-remaining estimates. Pass a job_id to inspect one download."
    )]
    pub async fn get_arxiv_download_queue_status(
        &self,
        Parameters(request): Parameters<DownloadQueueStatusRequest>,
    ) -> Result<Json<DownloadQueueStatusResponse>, String> {
        self.daemon_client
            .queue_status(request)
            .await
            .map(Json)
            .map_err(|error| error.to_string())
    }

    #[tool(
        name = "search_arxiv_corpus",
        description = "BM25-ranked free-text search across ALL locally cached arXiv papers at once, using the persistent FTS5 index over metadata, citations, and TeX/source paragraphs. Returns best-first snippets with arXiv ids, source paths, and line ranges. Optionally restrict to one paper with arxiv_id. This never contacts arXiv; run index_cached_arxiv_metadata first if results seem incomplete."
    )]
    pub fn search_arxiv_corpus(
        &self,
        Parameters(request): Parameters<SearchCorpusRequest>,
    ) -> Result<Json<SearchCorpusResponse>, String> {
        self.fetcher
            .search_corpus(request)
            .map(Json)
            .map_err(|error| error.to_string())
    }

    #[tool(
        name = "index_cached_arxiv_metadata",
        description = "Ask arxd to scan cached arXiv metadata JSON files under the local XDG cache, upsert them into the cache SQLite metadata database, and rebuild the full-text material index used by search_arxiv_corpus. This never contacts arXiv."
    )]
    pub async fn index_cached_arxiv_metadata(&self) -> Result<Json<IndexReport>, String> {
        self.daemon_client
            .index()
            .await
            .map(Json)
            .map_err(|error| error.to_string())
    }

    #[tool(
        name = "locate_cached_arxiv_paper",
        description = "Return local XDG cache paths for an arXiv paper if it has already been fetched. This never contacts arXiv."
    )]
    pub async fn locate_cached_arxiv_paper(
        &self,
        Parameters(request): Parameters<LocatePaperRequest>,
    ) -> Result<Json<LocatePaperResponse>, String> {
        self.fetcher
            .locate(request)
            .map(Json)
            .map_err(|error| error.to_string())
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for ArxMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "arx MCP is a metadata-first frontend for local arXiv grounding. Start with lookup_arxiv_papers: it returns a stable paper object, cached metadata/abstract, local material readiness, and only fetches missing metadata by default. Use get_arxiv_material_status for local-only readiness checks, search_arxiv_material for BM25-ranked snippets within one paper, and search_arxiv_corpus for BM25-ranked search across every locally cached paper. Use prepare_arxiv_material or fetch_arxiv_paper only when PDF/source acquisition is explicitly needed; they queue arxd work and return a job id immediately. Use get_arxiv_download_queue_status to inspect queued, in-progress, completed, and failed jobs. Use index_cached_arxiv_metadata to rescan local metadata without network access. arxd enforces the cross-process arXiv request delay and shuts down after its queue is idle."
                    .to_string(),
            ),
        }
    }
}
