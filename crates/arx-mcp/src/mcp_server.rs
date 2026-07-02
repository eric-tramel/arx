use arx_core::{
    arxiv::{
        ArxivFetcher, FetchPaperRequest, FullTextSearchRequest, FullTextSearchResponse,
        LookupPapersRequest, LookupPapersResponse,
    },
    daemon::{
        ArxdClient, DownloadQueueStatusRequest, DownloadQueueStatusResponse, QueuedFetchResponse,
    },
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
        description = "Metadata-first arXiv lookup. Returns local material readiness, cache file paths, and cached metadata/abstract immediately when present, fetching only missing metadata through the arXiv API by default. It never downloads PDF or source material."
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
        name = "full_text_search",
        description = "BM25-ranked full-text search over locally cached arXiv material. Scope controls which content is searched: scope=default (or omit) searches title+metadata+body and EXCLUDES bibliography; scope=titles searches title only; scope=bibliography searches citation records and .bib/.bbl files only; scope=all searches everything. Searches every cached paper by default; pass arxiv_id to restrict to one paper. Returns best-first snippets with scores, arXiv ids, source paths, and line ranges. Empty results include a note field explaining why and what to do next. The index maintains itself; this never contacts arXiv."
    )]
    pub fn full_text_search(
        &self,
        Parameters(request): Parameters<FullTextSearchRequest>,
    ) -> Result<Json<FullTextSearchResponse>, String> {
        self.fetcher
            .full_text_search(request)
            .map(Json)
            .map_err(|error| error.to_string())
    }

    #[tool(
        name = "fetch_arxiv_paper",
        description = "Queue an arXiv paper download (metadata, PDF, and/or TeX source) through arxd and return immediately with a download job id. Fetched material is cached locally and indexed for full_text_search automatically. Use only when lookup_arxiv_papers shows needed material is missing."
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
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for ArxMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "arx MCP grounds agents in locally cached arXiv papers. Start with lookup_arxiv_papers: it returns metadata, abstract, local material readiness, and cache paths, fetching only missing metadata. Use full_text_search for BM25-ranked snippets across all cached papers, or scoped to one paper with arxiv_id; the search index maintains itself. Use fetch_arxiv_paper only when needed PDF/source material is missing; it queues arxd work and returns a job id immediately, and get_arxiv_download_queue_status tracks the job. arxd enforces the cross-process arXiv request delay and shuts down after its queue is idle."
                    .to_string(),
            ),
        }
    }
}
