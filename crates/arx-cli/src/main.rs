use anyhow::{Context, Result, bail};
use arx_core::{
    arxiv::{
        ArxivFetcher, FetchPaperRequest, FetchPaperResponse, FullTextSearchRequest,
        FullTextSearchResponse, LocatePaperRequest, LocatePaperResponse, LookupPapersRequest,
        LookupPapersResponse, MaterialState, PaperMaterialStatus,
    },
    daemon::{
        ArxdClient, DownloadJobState, DownloadJobStatus, DownloadQueueStatusRequest,
        DownloadQueueStatusResponse, QueuedFetchResponse,
    },
    metadata_db::IndexReport,
    paths::xdg_cache_root,
};
use clap::{ArgAction, Parser, Subcommand};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::{
    io::{self, IsTerminal},
    time::Duration,
};
use tokio::time::sleep;

const FETCH_STATUS_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Parser)]
#[command(name = "arx")]
#[command(about = "Standalone CLI for cached arXiv paper retrieval")]
#[command(version, disable_version_flag = true)]
struct Cli {
    #[arg(
        short = 'v',
        long = "version",
        action = ArgAction::Version,
        help = "Print version"
    )]
    _version: bool,
    #[arg(
        short = 'j',
        long,
        global = true,
        help = "Emit machine-readable JSON instead of interactive shell output"
    )]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Fetch a paper through arxd and show progress until it is cached")]
    Fetch {
        #[arg(value_name = "ARXIV_ID", required = true, num_args = 1..)]
        arxiv_ids: Vec<String>,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        include_pdf: bool,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        include_source: bool,
        #[arg(long, default_value_t = false)]
        refresh: bool,
        #[arg(
            short = 'd',
            long,
            default_value_t = false,
            help = "Queue the download and return immediately instead of waiting"
        )]
        detach: bool,
    },
    #[command(about = "Claim paper metadata/status without fetching PDF or source material")]
    Lookup {
        #[arg(value_name = "ARXIV_ID", required = true, num_args = 1..)]
        arxiv_ids: Vec<String>,
        #[arg(long, default_value_t = false)]
        local_only: bool,
        #[arg(long, default_value_t = false)]
        refresh_metadata: bool,
    },
    #[command(about = "Index cached arXiv metadata into the local database")]
    Index,
    #[command(about = "BM25 free-text search across all locally cached paper material")]
    Search {
        #[arg(value_name = "QUERY", required = true, num_args = 1..)]
        query: Vec<String>,
        #[arg(long, help = "Restrict results to a single arXiv id")]
        arxiv_id: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(
            long,
            help = "Search scope: default (title+metadata+body), titles, bibliography, all"
        )]
        scope: Option<String>,
    },
    #[command(about = "Show arxd download queue status")]
    QueueStatus {
        #[arg(long)]
        job_id: Option<String>,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        include_finished: bool,
    },
    #[command(about = "Locate an already-cached paper without network access")]
    Locate { arxiv_id: String },
    #[command(about = "Print the XDG cache directory used by arx")]
    CacheDir,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let json = cli.json;
    let cache_root = xdg_cache_root()?;
    let fetcher = ArxivFetcher::new(cache_root.clone())?;
    let daemon_client = ArxdClient::new(cache_root);
    match cli.command {
        Command::Fetch {
            arxiv_ids,
            include_pdf,
            include_source,
            refresh,
            detach,
        } => {
            let requests: Vec<_> = arxiv_ids
                .into_iter()
                .map(|arxiv_id| FetchPaperRequest {
                    arxiv_id,
                    include_pdf: Some(include_pdf),
                    include_source: Some(include_source),
                    refresh: Some(refresh),
                })
                .collect();
            if detach {
                let responses = enqueue_fetches(&daemon_client, requests).await?;
                if json {
                    print_json_one_or_many(&responses)?;
                } else {
                    print_queued_fetches(&responses);
                }
            } else {
                let responses = fetch_many_blocking(&daemon_client, requests, !json).await?;
                if json {
                    print_json_one_or_many(&responses)?;
                } else {
                    print_fetch_summaries(&responses);
                }
            }
        }
        Command::Lookup {
            arxiv_ids,
            local_only,
            refresh_metadata,
        } => {
            let response = fetcher
                .lookup(LookupPapersRequest {
                    arxiv_ids,
                    fetch_missing_metadata: Some(!local_only),
                    refresh_metadata: Some(refresh_metadata),
                })
                .await?;
            if json {
                print_json_one_or_many(&response.papers)?;
            } else {
                print_lookup_response(&response);
            }
        }
        Command::Index => {
            let response = daemon_client.index().await?;
            if json {
                print_json(&response)?;
            } else {
                print_index_report(&response);
            }
        }
        Command::QueueStatus {
            job_id,
            include_finished,
        } => {
            let response = daemon_client
                .queue_status(DownloadQueueStatusRequest {
                    job_id,
                    include_finished: Some(include_finished),
                })
                .await?;
            if json {
                print_json(&response)?;
            } else {
                print_queue_status(&response);
            }
        }
        Command::Search {
            query,
            arxiv_id,
            limit,
            scope,
        } => {
            let response = fetcher.full_text_search(FullTextSearchRequest {
                query: query.join(" "),
                arxiv_id,
                limit: Some(limit),
                scope,
            })?;
            if json {
                print_json(&response)?;
            } else {
                print_search_response(&response);
            }
        }
        Command::Locate { arxiv_id } => {
            let response = fetcher.locate(LocatePaperRequest { arxiv_id })?;
            if json {
                print_json(&response)?;
            } else {
                print_locate_response(&response);
            }
        }
        Command::CacheDir => {
            println!("{}", fetcher.cache_root().display());
        }
    }

    Ok(())
}

async fn enqueue_fetches(
    daemon_client: &ArxdClient,
    requests: Vec<FetchPaperRequest>,
) -> Result<Vec<QueuedFetchResponse>> {
    let mut responses = Vec::with_capacity(requests.len());
    for request in requests {
        responses.push(daemon_client.enqueue_fetch(request).await?);
    }
    Ok(responses)
}

struct PendingFetch {
    index: usize,
    queued: QueuedFetchResponse,
    progress: Option<ProgressBar>,
    done: bool,
}

async fn fetch_many_blocking(
    daemon_client: &ArxdClient,
    requests: Vec<FetchPaperRequest>,
    show_progress: bool,
) -> Result<Vec<FetchPaperResponse>> {
    let queued = enqueue_fetches(daemon_client, requests).await?;
    let multi_progress = show_progress.then(MultiProgress::new);
    let mut pending = Vec::with_capacity(queued.len());
    for (index, queued) in queued.into_iter().enumerate() {
        let progress = multi_progress
            .as_ref()
            .map(|multi| multi.add(new_fetch_progress_bar(&queued)));
        if let Some(progress) = &progress {
            progress_log(
                progress,
                format!(
                    "{} queued {} as {}",
                    cyan("arxd"),
                    queued.arxiv_id,
                    queued.job_id
                ),
            );
        }
        pending.push(PendingFetch {
            index,
            queued,
            progress,
            done: false,
        });
    }

    let mut responses = vec![None; pending.len()];
    while pending.iter().any(|pending| !pending.done) {
        let mut made_progress = false;
        for pending_fetch in pending.iter_mut().filter(|pending| !pending.done) {
            let job = fetch_job_status(daemon_client, pending_fetch).await?;
            if let Some(progress) = &pending_fetch.progress {
                update_fetch_progress(progress, &job);
            }
            match job.status {
                DownloadJobState::Queued | DownloadJobState::InProgress => {}
                DownloadJobState::Completed => {
                    let Some(response) = job.result else {
                        abandon_fetch_progress(
                            pending_fetch,
                            format!(
                                "{} arxd job {} completed without a result",
                                red("error"),
                                job.job_id
                            ),
                        );
                        bail!("arxd job {} completed without a result", job.job_id);
                    };
                    if let Some(progress) = &pending_fetch.progress {
                        progress.finish_with_message(format!(
                            "{} fetched {} into {}",
                            green("done"),
                            response.arxiv_id,
                            response.cache_dir
                        ));
                    }
                    responses[pending_fetch.index] = Some(response);
                    pending_fetch.done = true;
                    made_progress = true;
                }
                DownloadJobState::Failed => {
                    let error = job
                        .error
                        .unwrap_or_else(|| "unknown arxd error".to_string());
                    abandon_fetch_progress(
                        pending_fetch,
                        format!(
                            "{} arxd job {} failed: {}",
                            red("failed"),
                            job.job_id,
                            error
                        ),
                    );
                    bail!("arxd job {} failed: {}", job.job_id, error);
                }
            }
        }
        if pending.iter().any(|pending| !pending.done) && !made_progress {
            sleep(FETCH_STATUS_POLL_INTERVAL).await;
        }
    }

    responses
        .into_iter()
        .enumerate()
        .map(|(index, response)| {
            response.with_context(|| format!("arxd fetch result {index} missing after completion"))
        })
        .collect()
}

async fn fetch_job_status(
    daemon_client: &ArxdClient,
    pending_fetch: &PendingFetch,
) -> Result<DownloadJobStatus> {
    let status = daemon_client
        .queue_status(DownloadQueueStatusRequest {
            job_id: Some(pending_fetch.queued.job_id.clone()),
            include_finished: Some(true),
        })
        .await?;
    let Some(job) = status.jobs.into_iter().next() else {
        bail!(
            "arxd no longer reports queued job {}",
            pending_fetch.queued.job_id
        );
    };
    Ok(job)
}

fn abandon_fetch_progress(pending_fetch: &PendingFetch, message: String) {
    if let Some(progress) = &pending_fetch.progress {
        progress.abandon_with_message(message);
    }
}

fn print_json<T: serde::Serialize + ?Sized>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn print_json_one_or_many<T: serde::Serialize>(values: &[T]) -> Result<()> {
    if let [value] = values {
        print_json(value)
    } else {
        print_json(values)
    }
}

fn print_queued_fetches(responses: &[QueuedFetchResponse]) {
    for (index, response) in responses.iter().enumerate() {
        if index > 0 {
            println!();
        }
        print_queued_fetch(response);
    }
}

fn print_fetch_summaries(responses: &[FetchPaperResponse]) {
    for (index, response) in responses.iter().enumerate() {
        if index > 0 {
            println!();
        }
        print_fetch_summary(response);
    }
}

fn print_queued_fetch(response: &QueuedFetchResponse) {
    println!(
        "{} queued {} as {}",
        cyan("arxd"),
        response.arxiv_id,
        response.job_id
    );
    println!(
        "{} {} --job-id {}",
        yellow("status:"),
        "arx queue-status",
        response.job_id
    );
    println!(
        "{} queue position {}",
        yellow("position:"),
        response.queue_position
    );
}

fn print_fetch_summary(response: &FetchPaperResponse) {
    println!("{} {}", green("fetched"), response.arxiv_id);
    print_field("cache", Some(response.cache_dir.as_str()));
    print_field("metadata", Some(response.metadata_path.as_str()));
    print_field("database", Some(response.metadata_db_path.as_str()));
    print_field("pdf", response.pdf_path.as_deref());
    print_field("source archive", response.source_archive_path.as_deref());
    print_field("source extracted", response.source_extracted_dir.as_deref());
    print_field("citations", response.citations_jsonl_path.as_deref());
    if let Some(title) = &response.title {
        print_field("title", Some(title));
    }
    if !response.authors.is_empty() {
        println!("{} {}", cyan("authors:"), response.authors.join(", "));
    }
    println!("{} {}", cyan("citations:"), response.citation_count);
    println!(
        "{} {}",
        cyan("network requests:"),
        response.network_requests
    );
}

fn print_index_report(report: &IndexReport) {
    println!(
        "{} indexed {} papers from {} metadata files",
        green("done"),
        report.indexed_papers,
        report.scanned_metadata_files
    );
    print_field("database", Some(report.database_path.as_str()));
    if report.indexed_material_chunks > 0 {
        println!(
            "{} indexed {} searchable material chunks",
            green("done"),
            report.indexed_material_chunks
        );
    }
    if report.removed_papers > 0 {
        println!(
            "{} removed {} stale rows",
            yellow("pruned:"),
            report.removed_papers
        );
    }
}

fn print_search_response(response: &FullTextSearchResponse) {
    if response.results.is_empty() {
        println!(
            "{} no matches in {} indexed chunks (scope: {})",
            yellow("empty"),
            response.indexed_chunks,
            response.scope
        );
        if let Some(note) = &response.note {
            println!("{} {}", yellow("note:"), note);
        }
        return;
    }
    for result in &response.results {
        let location = match (&result.path, result.line_start, result.line_end) {
            (Some(path), Some(start), Some(end)) if start != end => {
                format!("{path}:{start}-{end}")
            }
            (Some(path), Some(start), _) => format!("{path}:{start}"),
            (Some(path), None, _) => path.clone(),
            (None, _, _) => result.field.clone().unwrap_or_else(|| "-".to_string()),
        };
        println!(
            "{:>8.2}  {}  {}  {}",
            result.score,
            cyan(&result.arxiv_id),
            green(&result.source),
            location
        );
        println!("          {}", result.snippet);
    }
}

fn print_queue_status(status: &DownloadQueueStatusResponse) {
    println!(
        "{} queued: {}  in progress: {}  completed: {}  failed: {}",
        cyan("arxd"),
        status.queued_count,
        status.in_progress_count,
        status.completed_count,
        status.failed_count
    );
    if status.jobs.is_empty() {
        println!("{} no matching jobs", green("idle"));
        return;
    }
    for job in &status.jobs {
        let state = match job.status {
            DownloadJobState::Queued => cyan("queued"),
            DownloadJobState::InProgress => yellow("fetching"),
            DownloadJobState::Completed => green("completed"),
            DownloadJobState::Failed => red("failed"),
        };
        let mut details = format!("{} {} {}", state, job.job_id, job.arxiv_id);
        if let Some(position) = job.queue_position {
            details.push_str(&format!(" position {position}"));
        }
        if let Some(elapsed) = job.elapsed_seconds {
            details.push_str(&format!(" elapsed {elapsed}s"));
        }
        if let Some(error) = &job.error {
            details.push_str(&format!(" error {error}"));
        }
        println!("{details}");
    }
}

fn print_lookup_response(response: &LookupPapersResponse) {
    for (index, paper) in response.papers.iter().enumerate() {
        if index > 0 {
            println!();
        }
        print_paper_status(paper);
    }
    if response.network_requests > 0 {
        println!(
            "{} {}",
            cyan("metadata network requests:"),
            response.network_requests
        );
    }
}

fn print_paper_status(paper: &PaperMaterialStatus) {
    let state = if paper.material_state.metadata == MaterialState::Ready {
        green("claimed")
    } else {
        yellow("metadata-missing")
    };
    println!("{} {}", state, paper.arxiv_id);
    print_field("cache", Some(paper.paths.cache_dir.as_str()));
    print_field("metadata", Some(paper.paths.metadata_path.as_str()));
    print_field("pdf", Some(paper.paths.pdf_path.as_str()));
    print_field("source", paper.paths.source_archive_path.as_deref());
    if let Some(metadata) = &paper.metadata {
        if let Some(title) = &metadata.title {
            print_field("title", Some(title));
        }
        if !metadata.authors.is_empty() {
            println!("{} {}", cyan("authors:"), metadata.authors.join(", "));
        }
        if let Some(summary) = &metadata.summary {
            print_field("abstract", Some(summary));
        }
    }
    println!("{} {}", cyan("available:"), paper.available_now.join(", "));
    if !paper.missing.is_empty() {
        println!("{} {}", yellow("missing:"), paper.missing.join(", "));
    }
    if let Some(next_tool) = &paper.next_tool {
        print_field("next", Some(next_tool));
    }
}

fn print_locate_response(response: &LocatePaperResponse) {
    let state = if response.exists {
        green("cached")
    } else {
        yellow("missing")
    };
    println!("{} {}", state, response.arxiv_id);
    print_field("cache", Some(response.cache_dir.as_str()));
    print_field("metadata", response.metadata_path.as_deref());
    print_field("pdf", response.pdf_path.as_deref());
    print_field("source archive", response.source_archive_path.as_deref());
    print_field("source extracted", response.source_extracted_dir.as_deref());
    print_field("citations", response.citations_jsonl_path.as_deref());
}

fn print_field(label: &str, value: Option<&str>) {
    if let Some(value) = value {
        println!("{} {}", cyan(&format!("{label}:")), value);
    }
}

fn progress_log(progress: &ProgressBar, message: String) {
    if io::stderr().is_terminal() {
        progress.println(message);
    } else {
        eprintln!("{message}");
    }
}

fn new_fetch_progress_bar(queued: &QueuedFetchResponse) -> ProgressBar {
    let progress = ProgressBar::new_spinner();
    progress.set_style(fetch_progress_style());
    progress.enable_steady_tick(Duration::from_millis(120));
    progress.set_message(format!(
        "{} {} position {}",
        cyan("queued"),
        queued.arxiv_id,
        queued.queue_position
    ));
    progress
}

fn update_fetch_progress(progress: &ProgressBar, job: &DownloadJobStatus) {
    let message = match job.status {
        DownloadJobState::Queued => format!(
            "{} {} queue position {}",
            cyan("queued"),
            job.arxiv_id,
            job.queue_position.unwrap_or(0),
        ),
        DownloadJobState::InProgress => format!(
            "{} {} elapsed {}s",
            yellow("fetching"),
            job.arxiv_id,
            job.elapsed_seconds.unwrap_or(0),
        ),
        DownloadJobState::Completed => format!("{} {}", green("completed"), job.arxiv_id),
        DownloadJobState::Failed => format!("{} {}", red("failed"), job.arxiv_id),
    };
    progress.set_message(message);
}

fn fetch_progress_style() -> ProgressStyle {
    ProgressStyle::with_template("{spinner:.cyan} {elapsed_precise:.dim} {msg}").unwrap()
}

fn cyan(value: &str) -> String {
    color(36, value)
}

fn green(value: &str) -> String {
    color(32, value)
}

fn yellow(value: &str) -> String {
    color(33, value)
}

fn red(value: &str) -> String {
    color(31, value)
}

fn color(code: u8, value: &str) -> String {
    format!("\x1b[{code}m{value}\x1b[0m")
}
