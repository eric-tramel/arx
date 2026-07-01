use arx_core::arxiv::{
    ArxivFetcher, FetchPaperRequest, FetchPaperResponse, LocatePaperRequest, LocatePaperResponse,
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
    tool_router: ToolRouter<Self>,
}

impl ArxMcpServer {
    pub fn new(fetcher: ArxivFetcher) -> Self {
        Self {
            fetcher: Arc::new(fetcher),
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router(router = tool_router)]
impl ArxMcpServer {
    #[tool(
        name = "fetch_arxiv_paper",
        description = "Download an arXiv paper into the local XDG cache and return paths to metadata, PDF, source, and citations.jsonl. Cached hits do not contact arXiv."
    )]
    pub async fn fetch_arxiv_paper(
        &self,
        Parameters(request): Parameters<FetchPaperRequest>,
    ) -> Result<Json<FetchPaperResponse>, String> {
        self.fetcher
            .fetch(request)
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
                "Use fetch_arxiv_paper to cache arXiv metadata, PDF, source/TeX, and citations.jsonl under XDG_CACHE_HOME/arx. The server enforces a cross-process arXiv request delay with a shared filesystem lock. Use locate_cached_arxiv_paper to inspect cached paths without network access."
                    .to_string(),
            ),
        }
    }
}
