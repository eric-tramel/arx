mod mcp_server;
mod setup;

use anyhow::{Context, Result};
use arx_core::{arxiv::ArxivFetcher, paths::xdg_cache_root};
use clap::{ArgAction, Parser, Subcommand};
use mcp_server::ArxMcpServer;
use rmcp::{ServiceExt, transport::stdio};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "arx-mcp")]
#[command(about = "Stdio MCP server for cached arXiv paper retrieval")]
#[command(version, disable_version_flag = true)]
struct Cli {
    #[arg(
        short = 'v',
        long = "version",
        action = ArgAction::Version,
        help = "Print version"
    )]
    _version: Option<bool>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Run the stdio MCP server")]
    Serve,
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
