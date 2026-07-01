use anyhow::Result;
use arx_core::{
    arxiv::{ArxivFetcher, FetchPaperRequest, LocatePaperRequest},
    paths::xdg_cache_root,
};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "arx")]
#[command(about = "Standalone CLI for cached arXiv paper retrieval")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Fetch a paper and print cache paths as JSON")]
    Fetch {
        arxiv_id: String,
        #[arg(long, default_value_t = true)]
        include_pdf: bool,
        #[arg(long, default_value_t = true)]
        include_source: bool,
        #[arg(long, default_value_t = false)]
        refresh: bool,
    },
    #[command(about = "Locate an already-cached paper without network access")]
    Locate { arxiv_id: String },
    #[command(about = "Print the XDG cache directory used by arx")]
    CacheDir,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let fetcher = ArxivFetcher::new(xdg_cache_root()?)?;

    match cli.command {
        Command::Fetch {
            arxiv_id,
            include_pdf,
            include_source,
            refresh,
        } => {
            let response = fetcher
                .fetch(FetchPaperRequest {
                    arxiv_id,
                    include_pdf: Some(include_pdf),
                    include_source: Some(include_source),
                    refresh: Some(refresh),
                })
                .await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
        Command::Locate { arxiv_id } => {
            let response = fetcher.locate(LocatePaperRequest { arxiv_id })?;
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
        Command::CacheDir => {
            println!("{}", fetcher.cache_root().display());
        }
    }

    Ok(())
}
