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
    material_index::{CorpusSearchResult, tokenize},
    metadata_db::IndexReport,
    paths::xdg_cache_root,
};
use clap::{ArgAction, Parser, Subcommand};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::{
    cmp::Ordering,
    collections::HashMap,
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
                let arxiv_ids = unique_search_arxiv_ids(&response);
                let metadata_by_id = if arxiv_ids.is_empty() {
                    HashMap::new()
                } else {
                    search_metadata_by_id(
                        fetcher
                            .lookup(LookupPapersRequest {
                                arxiv_ids,
                                fetch_missing_metadata: Some(false),
                                refresh_metadata: Some(false),
                            })
                            .await?
                            .papers,
                    )
                };
                print_search_response(&response, &metadata_by_id);
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

#[derive(Debug, Clone)]
struct SearchPaperMetadata {
    title: Option<String>,
    authors: Vec<String>,
    year: Option<String>,
}

struct SearchPaperGroup<'a> {
    arxiv_id: &'a str,
    best_score: f64,
    first_index: usize,
    snippets: Vec<&'a CorpusSearchResult>,
}

fn print_search_response(
    response: &FullTextSearchResponse,
    metadata_by_id: &HashMap<String, SearchPaperMetadata>,
) {
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

    let groups = grouped_search_results(&response.results);
    let query_terms = tokenize(&response.query);
    let panel_width = search_panel_width();
    println!(
        "{} top-rated papers for {} · {} section hits · scope {} · indexed chunks {}",
        green("results"),
        italic(&format!("\"{}\"", response.query)),
        response.results.len(),
        cyan(&response.scope),
        response.indexed_chunks
    );
    println!(
        "{} showing at most 3 corroborating snippets per paper",
        dim("note:")
    );

    for (index, group) in groups.iter().enumerate() {
        if index > 0 {
            println!();
        } else {
            println!();
        }
        print_search_panel(
            group,
            metadata_by_id.get(group.arxiv_id),
            &query_terms,
            panel_width,
        );
    }
}

fn unique_search_arxiv_ids(response: &FullTextSearchResponse) -> Vec<String> {
    let mut arxiv_ids = Vec::new();
    for result in &response.results {
        if !arxiv_ids
            .iter()
            .any(|arxiv_id| arxiv_id == &result.arxiv_id)
        {
            arxiv_ids.push(result.arxiv_id.clone());
        }
    }
    arxiv_ids
}

fn search_metadata_by_id(papers: Vec<PaperMaterialStatus>) -> HashMap<String, SearchPaperMetadata> {
    papers
        .into_iter()
        .map(|paper| {
            let metadata = paper.metadata;
            let title = metadata
                .as_ref()
                .and_then(|metadata| metadata.title.clone());
            let authors = metadata
                .as_ref()
                .map(|metadata| metadata.authors.clone())
                .unwrap_or_default();
            let year = metadata
                .as_ref()
                .and_then(|metadata| publication_year(metadata.published.as_deref()))
                .or_else(|| paper.publication_year.map(|year| year.to_string()));
            (
                paper.arxiv_id,
                SearchPaperMetadata {
                    title,
                    authors,
                    year,
                },
            )
        })
        .collect()
}

fn publication_year(published: Option<&str>) -> Option<String> {
    let year = published?.get(0..4)?;
    year.chars()
        .all(|ch| ch.is_ascii_digit())
        .then(|| year.to_string())
}

fn grouped_search_results(results: &[CorpusSearchResult]) -> Vec<SearchPaperGroup<'_>> {
    let mut groups: Vec<SearchPaperGroup<'_>> = Vec::new();
    for (index, result) in results.iter().enumerate() {
        if let Some(group) = groups
            .iter_mut()
            .find(|group| group.arxiv_id == result.arxiv_id)
        {
            group.best_score = group.best_score.max(result.score);
            group.snippets.push(result);
        } else {
            groups.push(SearchPaperGroup {
                arxiv_id: &result.arxiv_id,
                best_score: result.score,
                first_index: index,
                snippets: vec![result],
            });
        }
    }
    groups.sort_by(|left, right| {
        right
            .best_score
            .partial_cmp(&left.best_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.first_index.cmp(&right.first_index))
    });
    groups
}

fn print_search_panel(
    group: &SearchPaperGroup<'_>,
    metadata: Option<&SearchPaperMetadata>,
    query_terms: &[String],
    panel_width: usize,
) {
    let content_width = panel_width.saturating_sub(4);
    println!("╭{}╮", "─".repeat(content_width + 2));
    for line in wrap_text(&paper_title(group.arxiv_id, metadata), content_width) {
        print_panel_line(&italic(&line), content_width);
    }
    let snippets = corroborating_snippets(&group.snippets);
    for line in wrap_text(
        &paper_subtitle(group, metadata, snippets.len()),
        content_width,
    ) {
        print_panel_line(&dim(&line), content_width);
    }
    println!("├{}┤", "─".repeat(content_width + 2));

    for (index, result) in snippets.iter().enumerate() {
        let section = result_section_label(result);
        let location = result_location(result);
        let label = if location == section {
            format!("{}. {} · score {:.2}", index + 1, section, result.score)
        } else {
            format!(
                "{}. {} · {} · score {:.2}",
                index + 1,
                section,
                location,
                result.score
            )
        };
        for line in wrap_text(&label, content_width) {
            print_panel_line(&green(&line), content_width);
        }
        let snippet = collapse_whitespace(&strip_tantivy_markup(&result.snippet));
        let snippet_width = content_width.saturating_sub(4).max(20);
        for line in wrap_text(&snippet, snippet_width) {
            print_panel_line(
                &format!("    {}", highlight_latex_line(&line, query_terms)),
                content_width,
            );
        }
        if index + 1 < snippets.len() {
            print_panel_line("", content_width);
        }
    }
    println!("╰{}╯", "─".repeat(content_width + 2));
}

fn paper_title(arxiv_id: &str, metadata: Option<&SearchPaperMetadata>) -> String {
    metadata
        .and_then(|metadata| metadata.title.as_deref())
        .filter(|title| !title.trim().is_empty())
        .unwrap_or(arxiv_id)
        .to_string()
}

fn paper_subtitle(
    group: &SearchPaperGroup<'_>,
    metadata: Option<&SearchPaperMetadata>,
    snippet_count: usize,
) -> String {
    let year = metadata
        .and_then(|metadata| metadata.year.as_deref())
        .unwrap_or("year unknown");
    let author = metadata
        .map(|metadata| author_label(&metadata.authors))
        .filter(|author| !author.trim().is_empty())
        .unwrap_or_else(|| "unknown author".to_string());
    format!(
        "{} · {} · {} · best score {:.2} · {} snippet{} shown",
        year,
        author,
        group.arxiv_id,
        group.best_score,
        snippet_count,
        if snippet_count == 1 { "" } else { "s" }
    )
}

fn author_label(authors: &[String]) -> String {
    match authors {
        [] => "unknown author".to_string(),
        [author] => author.clone(),
        [first, ..] => format!("{first} et al."),
    }
}

fn corroborating_snippets<'a>(snippets: &[&'a CorpusSearchResult]) -> Vec<&'a CorpusSearchResult> {
    let selected = snippets
        .iter()
        .copied()
        .filter(|result| result.category.as_deref() != Some("title"))
        .take(3)
        .collect::<Vec<_>>();
    if selected.is_empty() {
        snippets.iter().copied().take(3).collect()
    } else {
        selected
    }
}

fn result_section_label(result: &CorpusSearchResult) -> &str {
    if result.source == "metadata" {
        result.field.as_deref().unwrap_or("metadata")
    } else {
        result.category.as_deref().unwrap_or("section")
    }
}

fn result_location(result: &CorpusSearchResult) -> String {
    if result.source == "metadata" {
        return result
            .field
            .clone()
            .unwrap_or_else(|| "metadata".to_string());
    }
    match (&result.path, result.line_start, result.line_end) {
        (Some(path), Some(start), Some(end)) if start != end => {
            format!("{}:{start}-{end}", compact_path(path, &result.arxiv_id))
        }
        (Some(path), Some(start), _) => format!("{}:{start}", compact_path(path, &result.arxiv_id)),
        (Some(path), None, _) => compact_path(path, &result.arxiv_id),
        (None, _, _) => result.field.clone().unwrap_or_else(|| "-".to_string()),
    }
}

fn compact_path(path: &str, arxiv_id: &str) -> String {
    let safe_id = arxiv_id.replace('/', "_");
    let marker = format!("/papers/{safe_id}/");
    if let Some(index) = path.find(&marker) {
        return path[index + marker.len()..].to_string();
    }
    path.rsplit('/').next().unwrap_or(path).to_string()
}

fn search_panel_width() -> usize {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(60, 120)
}

fn print_panel_line(value: &str, content_width: usize) {
    println!("│ {} │", pad_ansi(value, content_width));
}

fn wrap_text(value: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in value.split_whitespace() {
        let word_width = word.chars().count();
        if current.is_empty() {
            if word_width <= width {
                current.push_str(word);
            } else {
                push_split_word(&mut lines, word, width);
            }
        } else if current.chars().count() + 1 + word_width <= width {
            current.push(' ');
            current.push_str(word);
        } else {
            lines.push(current);
            current = String::new();
            if word_width <= width {
                current.push_str(word);
            } else {
                push_split_word(&mut lines, word, width);
            }
        }
    }
    if !current.is_empty() {
        lines.push(current);
    }
    if lines.is_empty() {
        lines.push(String::new());
    }
    lines
}

fn push_split_word(lines: &mut Vec<String>, word: &str, width: usize) {
    let mut current = String::new();
    for ch in word.chars() {
        if current.chars().count() == width {
            lines.push(current);
            current = String::new();
        }
        current.push(ch);
    }
    if !current.is_empty() {
        lines.push(current);
    }
}

fn strip_tantivy_markup(value: &str) -> String {
    value
        .replace("<b>", "")
        .replace("</b>", "")
        .replace("<em>", "")
        .replace("</em>", "")
}

fn collapse_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn highlight_latex_line(line: &str, query_terms: &[String]) -> String {
    let chars: Vec<char> = line.chars().collect();
    let mut highlighted = String::new();
    let mut index = 0;
    while index < chars.len() {
        let ch = chars[index];
        if ch == '\\' {
            let start = index;
            index += 1;
            while index < chars.len() && chars[index].is_alphabetic() {
                index += 1;
            }
            let command = chars[start..index].iter().collect::<String>();
            let command_name = chars[start + 1..index]
                .iter()
                .collect::<String>()
                .to_lowercase();
            if !command_name.is_empty() && query_terms.contains(&command_name) {
                highlighted.push_str(&keyword_hit(&command));
            } else {
                highlighted.push_str(&cyan(&command));
            }
        } else if ch.is_alphanumeric() {
            let start = index;
            index += 1;
            while index < chars.len() && chars[index].is_alphanumeric() {
                index += 1;
            }
            let token = chars[start..index].iter().collect::<String>();
            if query_terms.contains(&token.to_lowercase()) {
                highlighted.push_str(&keyword_hit(&token));
            } else {
                highlighted.push_str(&token);
            }
        } else if matches!(ch, '{' | '}' | '[' | ']' | '(' | ')') {
            highlighted.push_str(&dim(&ch.to_string()));
            index += 1;
        } else if matches!(ch, '$' | '^' | '_') {
            highlighted.push_str(&magenta(&ch.to_string()));
            index += 1;
        } else {
            highlighted.push(ch);
            index += 1;
        }
    }
    highlighted
}

fn pad_ansi(value: &str, width: usize) -> String {
    let visible = visible_width(value);
    if visible >= width {
        return value.to_string();
    }
    format!("{value}{}", " ".repeat(width - visible))
}

fn visible_width(value: &str) -> usize {
    let mut width = 0;
    let mut in_escape = false;
    for ch in value.chars() {
        if in_escape {
            if ch == 'm' {
                in_escape = false;
            }
        } else if ch == '\x1b' {
            in_escape = true;
        } else {
            width += 1;
        }
    }
    width
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

fn magenta(value: &str) -> String {
    color(35, value)
}

fn dim(value: &str) -> String {
    style("2", value)
}

fn italic(value: &str) -> String {
    style("3", value)
}

fn keyword_hit(value: &str) -> String {
    style("1;4;33", value)
}

fn style(code: &str, value: &str) -> String {
    format!("\x1b[{code}m{value}\x1b[0m")
}

fn color(code: u8, value: &str) -> String {
    format!("\x1b[{code}m{value}\x1b[0m")
}
