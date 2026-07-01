use anyhow::{Context, Result};
use arx::{
    arxiv::{ArxivFetcher, FetchPaperRequest},
    mcp_server::ArxMcpServer,
    paths::xdg_cache_root,
    setup,
};
use clap::{Parser, Subcommand};
use rmcp::{ServiceExt, transport::stdio};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "arx")]
#[command(about = "Cached arXiv fetcher exposed as a stdio MCP server")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Run the stdio MCP server")]
    Serve,
    #[command(about = "Fetch a paper directly and print the same JSON returned by the MCP tool")]
    Fetch {
        arxiv_id: String,
        #[arg(long, default_value_t = true)]
        include_pdf: bool,
        #[arg(long, default_value_t = true)]
        include_source: bool,
        #[arg(long, default_value_t = false)]
        refresh: bool,
    },
    #[command(about = "Print the XDG cache directory used by arx")]
    CacheDir,
    #[command(about = "Print an MCP configuration snippet that launches this binary over stdio")]
    PrintConfig,
    #[command(about = "Install/update the arx entry in Claude Desktop's MCP configuration")]
    InstallClaudeDesktop {
        #[arg(long)]
        config_path: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => run_server().await,
        Command::Fetch {
            arxiv_id,
            include_pdf,
            include_source,
            refresh,
        } => {
            let fetcher = ArxivFetcher::new(xdg_cache_root()?)?;
            let response = fetcher
                .fetch(FetchPaperRequest {
                    arxiv_id,
                    include_pdf: Some(include_pdf),
                    include_source: Some(include_source),
                    refresh: Some(refresh),
                })
                .await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
            Ok(())
        }
        Command::CacheDir => {
            println!("{}", xdg_cache_root()?.display());
            Ok(())
        }
        Command::PrintConfig => {
            let executable = std::env::current_exe().context("locating current executable")?;
            println!(
                "{}",
                serde_json::to_string_pretty(&setup::mcp_config_snippet(&executable))?
            );
            Ok(())
        }
        Command::InstallClaudeDesktop { config_path } => {
            let executable = std::env::current_exe().context("locating current executable")?;
            let config_path = match config_path {
                Some(path) => path,
                None => setup::default_claude_desktop_config_path()?,
            };
            setup::install_claude_desktop_config(&config_path, &executable)?;
            println!("installed arx MCP server in {}", config_path.display());
            Ok(())
        }
    }
}

async fn run_server() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let fetcher = ArxivFetcher::new(xdg_cache_root()?)?;
    tracing::info!(cache_root = %fetcher.cache_root().display(), "starting arx MCP server");
    let service = ArxMcpServer::new(fetcher)
        .serve(stdio())
        .await
        .inspect_err(|error| tracing::error!(?error, "serving error"))?;
    service.waiting().await?;
    Ok(())
}
