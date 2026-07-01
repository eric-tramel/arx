use anyhow::{Context, Result, bail};
use arx_core::{
    arxiv::{ArxivFetcher, FetchPaperRequest, FetchPaperResponse, normalize_arxiv_id},
    daemon::{
        ArxdEndpoint, ArxdRequest, ArxdResponse, DownloadJobState, DownloadJobStatus,
        DownloadQueueStatusRequest, DownloadQueueStatusResponse, QueuedFetchResponse,
        remove_endpoint, unix_ms, write_endpoint,
    },
    paths::{arxd_lock_path, xdg_cache_root},
    rate_limit::ARXIV_DELAY,
};
use clap::{Parser, Subcommand};
use fs2::FileExt;
use std::{
    fs::{self, File, OpenOptions},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Semaphore},
    time::{sleep, timeout},
};
use tracing_subscriber::EnvFilter;

const DOWNLOAD_WORKERS: usize = 4;
const STATUS_TOOL_NAME: &str = "get_arxiv_download_queue_status";
const DEFAULT_IDLE_SHUTDOWN: Duration = Duration::from_secs(30);

#[derive(Debug, Parser)]
#[command(name = "arxd")]
#[command(about = "Local arx daemon for queued downloads and metadata indexing")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Run the arx daemon")]
    Serve {
        #[arg(long)]
        cache_root: Option<PathBuf>,
    },
}

#[derive(Debug)]
struct ArxdDaemon {
    cache_root: PathBuf,
    fetcher: Arc<ArxivFetcher>,
    queue: DownloadQueue,
    lock_file: File,
    idle_shutdown: Duration,
    active_requests: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
struct DownloadQueue {
    fetcher: Arc<ArxivFetcher>,
    semaphore: Arc<Semaphore>,
    state: Arc<Mutex<QueueState>>,
}

#[derive(Debug, Default)]
struct QueueState {
    next_job_id: u64,
    jobs: Vec<DownloadJob>,
}

#[derive(Debug, Clone)]
struct DownloadJob {
    job_id: String,
    arxiv_id: String,
    request: FetchPaperRequest,
    status: DownloadJobState,
    queued_at_unix_ms: u64,
    started_at_unix_ms: Option<u64>,
    finished_at_unix_ms: Option<u64>,
    estimated_network_requests: u64,
    result: Option<FetchPaperResponse>,
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Serve { cache_root } => {
            let cache_root = match cache_root {
                Some(cache_root) => cache_root,
                None => xdg_cache_root()?,
            };
            ArxdDaemon::new(cache_root)?.serve().await
        }
    }
}

impl ArxdDaemon {
    fn new(cache_root: PathBuf) -> Result<Self> {
        fs::create_dir_all(&cache_root)
            .with_context(|| format!("creating cache root {}", cache_root.display()))?;
        let lock_file = acquire_daemon_lock(&cache_root)?;
        let fetcher = Arc::new(ArxivFetcher::new(cache_root.clone())?);
        let queue = DownloadQueue::new(fetcher.clone());
        let active_requests = Arc::new(AtomicUsize::new(0));
        Ok(Self {
            cache_root,
            fetcher,
            queue,
            lock_file,
            idle_shutdown: idle_shutdown_duration(),
            active_requests,
        })
    }

    async fn serve(self) -> Result<()> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .context("binding arxd listener")?;
        let local_addr = listener
            .local_addr()
            .context("reading arxd listener address")?;
        write_endpoint(&self.cache_root, &endpoint(&self.cache_root, local_addr)?)?;
        tracing::info!(cache_root = %self.cache_root.display(), %local_addr, "arxd listening");

        loop {
            match timeout(self.idle_shutdown, listener.accept()).await {
                Ok(Ok((stream, _))) => {
                    let fetcher = self.fetcher.clone();
                    let queue = self.queue.clone();
                    let active_requests = self.active_requests.clone();
                    tokio::spawn(async move {
                        let _guard = ActiveRequestGuard::new(active_requests);
                        if let Err(error) = handle_connection(stream, fetcher, queue).await {
                            tracing::warn!(?error, "arxd connection failed");
                        }
                    });
                }
                Ok(Err(error)) => return Err(error).context("accepting arxd connection"),
                Err(_) if !self.has_work().await => break,
                Err(_) => continue,
            }
        }

        remove_endpoint(&self.cache_root)?;
        self.lock_file.unlock().context("unlocking arxd lock")?;
        tracing::info!(cache_root = %self.cache_root.display(), "arxd idle shutdown");
        Ok(())
    }
    async fn has_work(&self) -> bool {
        self.active_requests.load(Ordering::SeqCst) > 0 || self.queue.has_work().await
    }
}

struct ActiveRequestGuard {
    active_requests: Arc<AtomicUsize>,
}

impl ActiveRequestGuard {
    fn new(active_requests: Arc<AtomicUsize>) -> Self {
        active_requests.fetch_add(1, Ordering::SeqCst);
        Self { active_requests }
    }
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.active_requests.fetch_sub(1, Ordering::SeqCst);
    }
}
impl DownloadQueue {
    fn new(fetcher: Arc<ArxivFetcher>) -> Self {
        Self {
            fetcher,
            semaphore: Arc::new(Semaphore::new(DOWNLOAD_WORKERS)),
            state: Arc::new(Mutex::new(QueueState::default())),
        }
    }

    async fn enqueue(&self, request: FetchPaperRequest) -> Result<QueuedFetchResponse> {
        let arxiv_id = normalize_arxiv_id(&request.arxiv_id)?;
        let queued_at_unix_ms = unix_ms();
        let estimated_network_requests = estimated_network_requests(&request);
        let job_id = {
            let mut state = self.state.lock().await;
            state.next_job_id += 1;
            let job_id = format!("download-{}", state.next_job_id);
            state.jobs.push(DownloadJob {
                job_id: job_id.clone(),
                arxiv_id: arxiv_id.clone(),
                request,
                status: DownloadJobState::Queued,
                queued_at_unix_ms,
                started_at_unix_ms: None,
                finished_at_unix_ms: None,
                estimated_network_requests,
                result: None,
                error: None,
            });
            job_id
        };

        self.spawn_worker(job_id.clone());
        let status = self
            .status(DownloadQueueStatusRequest {
                job_id: Some(job_id.clone()),
                include_finished: Some(true),
            })
            .await;
        let job = status
            .jobs
            .into_iter()
            .next()
            .with_context(|| format!("queued job {job_id} disappeared"))?;
        Ok(QueuedFetchResponse {
            job_id,
            arxiv_id,
            status: job.status,
            queue_position: job.queue_position.unwrap_or(0),
            queued_at_unix_ms,
            estimated_seconds_until_start: job.estimated_seconds_until_start,
            estimated_seconds_remaining: job.estimated_seconds_remaining,
            status_tool: STATUS_TOOL_NAME.to_string(),
            message: format!(
                "queued arXiv download; call {STATUS_TOOL_NAME} with this job_id to check progress"
            ),
        })
    }

    async fn status(&self, request: DownloadQueueStatusRequest) -> DownloadQueueStatusResponse {
        let now_unix_ms = unix_ms();
        let include_finished = request.include_finished.unwrap_or(true);
        let jobs = {
            let state = self.state.lock().await;
            state.jobs.clone()
        };
        let queued_count = jobs
            .iter()
            .filter(|job| job.status == DownloadJobState::Queued)
            .count();
        let in_progress_count = jobs
            .iter()
            .filter(|job| job.status == DownloadJobState::InProgress)
            .count();
        let completed_count = jobs
            .iter()
            .filter(|job| job.status == DownloadJobState::Completed)
            .count();
        let failed_count = jobs
            .iter()
            .filter(|job| job.status == DownloadJobState::Failed)
            .count();
        let jobs = queue_statuses(&jobs, now_unix_ms, DOWNLOAD_WORKERS)
            .into_iter()
            .filter(|job| {
                let matches_requested_job = request
                    .job_id
                    .as_ref()
                    .map(|job_id| &job.job_id == job_id)
                    .unwrap_or(true);
                let matches_finished_filter = include_finished
                    || matches!(
                        job.status,
                        DownloadJobState::Queued | DownloadJobState::InProgress
                    );
                matches_requested_job && matches_finished_filter
            })
            .collect();

        DownloadQueueStatusResponse {
            now_unix_ms,
            max_active_workers: DOWNLOAD_WORKERS,
            queued_count,
            in_progress_count,
            completed_count,
            failed_count,
            jobs,
        }
    }

    async fn has_work(&self) -> bool {
        let state = self.state.lock().await;
        state.jobs.iter().any(|job| {
            matches!(
                job.status,
                DownloadJobState::Queued | DownloadJobState::InProgress
            )
        })
    }

    fn spawn_worker(&self, job_id: String) {
        let fetcher = self.fetcher.clone();
        let semaphore = self.semaphore.clone();
        let worker_state = self.state.clone();
        let finish_state = self.state.clone();
        let finish_job_id = job_id.clone();
        let handle =
            tokio::spawn(
                async move { run_fetch_job(fetcher, semaphore, worker_state, job_id).await },
            );

        tokio::spawn(async move {
            finish_worker_task(finish_state, finish_job_id, handle).await;
        });
    }
}

type WorkerResult = Option<std::result::Result<FetchPaperResponse, String>>;

async fn run_fetch_job(
    fetcher: Arc<ArxivFetcher>,
    semaphore: Arc<Semaphore>,
    state: Arc<Mutex<QueueState>>,
    job_id: String,
) -> WorkerResult {
    let permit = match semaphore.acquire_owned().await {
        Ok(permit) => permit,
        Err(error) => {
            return Some(Err(format!("download worker closed: {error}")));
        }
    };

    let request = start_job(&state, &job_id).await?;
    if let Some(delay) = worker_hold_duration() {
        sleep(delay).await;
    }
    let result = fetcher.fetch(request).await.map_err(format_error_chain);
    drop(permit);
    Some(result)
}

async fn finish_worker_task(
    state: Arc<Mutex<QueueState>>,
    job_id: String,
    handle: tokio::task::JoinHandle<WorkerResult>,
) {
    let result = match handle.await {
        Ok(Some(result)) => result,
        Ok(None) => return,
        Err(error) => Err(format!("download worker task failed: {error}")),
    };
    finish_job(&state, &job_id, result).await;
}

fn format_error_chain(error: anyhow::Error) -> String {
    format!("{error:#}")
}

async fn handle_connection(
    stream: TcpStream,
    fetcher: Arc<ArxivFetcher>,
    queue: DownloadQueue,
) -> Result<()> {
    let mut reader = BufReader::new(stream);
    let mut line = Vec::new();
    reader
        .read_until(b'\n', &mut line)
        .await
        .context("reading arxd request")?;
    if line.is_empty() {
        bail!("empty arxd request");
    }
    let request: ArxdRequest = serde_json::from_slice(&line).context("parsing arxd request")?;
    let response = match request {
        ArxdRequest::EnqueueFetch { request } => match queue.enqueue(request).await {
            Ok(response) => ArxdResponse::QueuedFetch { response },
            Err(error) => ArxdResponse::Error {
                message: error.to_string(),
            },
        },
        ArxdRequest::QueueStatus { request } => ArxdResponse::QueueStatus {
            response: queue.status(request).await,
        },
        ArxdRequest::Index => match fetcher.index_with_material() {
            Ok(response) => ArxdResponse::Index { response },
            Err(error) => ArxdResponse::Error {
                message: error.to_string(),
            },
        },
    };

    let payload = serde_json::to_vec(&response).context("serializing arxd response")?;
    let stream = reader.get_mut();
    stream
        .write_all(&payload)
        .await
        .context("writing arxd response")?;
    stream
        .write_all(b"\n")
        .await
        .context("writing arxd response newline")?;
    Ok(())
}

async fn start_job(state: &Arc<Mutex<QueueState>>, job_id: &str) -> Option<FetchPaperRequest> {
    let mut state = state.lock().await;
    let job = state.jobs.iter_mut().find(|job| job.job_id == job_id)?;
    job.status = DownloadJobState::InProgress;
    job.started_at_unix_ms = Some(unix_ms());
    Some(job.request.clone())
}

async fn finish_job(
    state: &Arc<Mutex<QueueState>>,
    job_id: &str,
    result: Result<FetchPaperResponse, String>,
) {
    let mut state = state.lock().await;
    let Some(job) = state.jobs.iter_mut().find(|job| job.job_id == job_id) else {
        return;
    };
    job.finished_at_unix_ms = Some(unix_ms());
    match result {
        Ok(response) => {
            job.status = DownloadJobState::Completed;
            job.result = Some(response);
            job.error = None;
        }
        Err(error) => {
            job.status = DownloadJobState::Failed;
            job.result = None;
            job.error = Some(error);
        }
    }
}

fn queue_statuses(
    jobs: &[DownloadJob],
    now_unix_ms: u64,
    max_active_workers: usize,
) -> Vec<DownloadJobStatus> {
    let worker_count = max_active_workers.max(1);
    let mut worker_available_after = vec![0_u64; worker_count];
    let mut queued_position = 0;
    jobs.iter()
        .map(|job| {
            let estimated_total = estimated_total_seconds(job.estimated_network_requests);
            let elapsed = job.started_at_unix_ms.map(|started| {
                seconds_between(started, job.finished_at_unix_ms.unwrap_or(now_unix_ms))
            });
            let (queue_position, estimated_seconds_until_start, estimated_seconds_remaining) =
                match job.status {
                    DownloadJobState::InProgress => {
                        let elapsed = elapsed.unwrap_or(0);
                        let remaining = estimated_total.saturating_sub(elapsed);
                        let worker_index = next_available_worker(&worker_available_after);
                        worker_available_after[worker_index] =
                            worker_available_after[worker_index].saturating_add(remaining);
                        (None, 0, remaining)
                    }
                    DownloadJobState::Queued => {
                        queued_position += 1;
                        let worker_index = next_available_worker(&worker_available_after);
                        let until_start = worker_available_after[worker_index];
                        let remaining = until_start.saturating_add(estimated_total);
                        worker_available_after[worker_index] =
                            worker_available_after[worker_index].saturating_add(estimated_total);
                        (Some(queued_position), until_start, remaining)
                    }
                    DownloadJobState::Completed | DownloadJobState::Failed => (None, 0, 0),
                };

            DownloadJobStatus {
                job_id: job.job_id.clone(),
                arxiv_id: job.arxiv_id.clone(),
                status: job.status,
                queue_position,
                queued_at_unix_ms: job.queued_at_unix_ms,
                started_at_unix_ms: job.started_at_unix_ms,
                finished_at_unix_ms: job.finished_at_unix_ms,
                estimated_network_requests: job.estimated_network_requests,
                estimated_seconds_until_start,
                estimated_seconds_remaining,
                elapsed_seconds: elapsed,
                request: job.request.clone(),
                result: job.result.clone(),
                error: job.error.clone(),
            }
        })
        .collect()
}

fn next_available_worker(worker_available_after: &[u64]) -> usize {
    worker_available_after
        .iter()
        .enumerate()
        .min_by_key(|(_, available_after)| *available_after)
        .map(|(index, _)| index)
        .unwrap_or(0)
}

fn estimated_network_requests(request: &FetchPaperRequest) -> u64 {
    let include_pdf = request.include_pdf.unwrap_or(true) as u64;
    let include_source = request.include_source.unwrap_or(true) as u64;
    1 + include_pdf + include_source
}

fn estimated_total_seconds(estimated_network_requests: u64) -> u64 {
    estimated_network_requests
        .saturating_mul(ARXIV_DELAY.as_secs())
        .max(1)
}

fn seconds_between(start_unix_ms: u64, end_unix_ms: u64) -> u64 {
    end_unix_ms.saturating_sub(start_unix_ms) / 1_000
}

fn acquire_daemon_lock(cache_root: &Path) -> Result<File> {
    let lock_path = arxd_lock_path(cache_root);
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating arxd lock directory {}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("opening arxd lock {}", lock_path.display()))?;
    file.try_lock_exclusive().with_context(|| {
        format!(
            "locking arxd lock {}; another arxd may already be running",
            lock_path.display()
        )
    })?;
    Ok(file)
}

fn endpoint(cache_root: &Path, addr: SocketAddr) -> Result<ArxdEndpoint> {
    let SocketAddr::V4(addr) = addr else {
        bail!("arxd listener did not bind an IPv4 address: {addr}");
    };
    Ok(ArxdEndpoint {
        host: addr.ip().to_string(),
        port: addr.port(),
        cache_root: cache_root.display().to_string(),
        started_at_unix_ms: unix_ms(),
    })
}

fn idle_shutdown_duration() -> Duration {
    std::env::var("ARXD_IDLE_SHUTDOWN_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_IDLE_SHUTDOWN)
}

fn worker_hold_duration() -> Option<Duration> {
    std::env::var("ARXD_WORKER_HOLD_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|milliseconds| *milliseconds > 0)
        .map(Duration::from_millis)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::task::JoinHandle;

    #[tokio::test]
    async fn finish_worker_task_marks_panicked_in_progress_job_failed() {
        let request = FetchPaperRequest {
            arxiv_id: "2401.12345".to_string(),
            include_pdf: Some(false),
            include_source: Some(false),
            refresh: Some(false),
        };
        let state = Arc::new(Mutex::new(QueueState {
            next_job_id: 1,
            jobs: vec![DownloadJob {
                job_id: "download-1".to_string(),
                arxiv_id: "2401.12345".to_string(),
                estimated_network_requests: estimated_network_requests(&request),
                request,
                status: DownloadJobState::InProgress,
                queued_at_unix_ms: unix_ms(),
                started_at_unix_ms: Some(unix_ms()),
                finished_at_unix_ms: None,
                result: None,
                error: None,
            }],
        }));
        let handle: JoinHandle<WorkerResult> = tokio::spawn(async {
            panic!("worker panic regression sentinel");
        });

        finish_worker_task(state.clone(), "download-1".to_string(), handle).await;

        let state = state.lock().await;
        let job = &state.jobs[0];
        assert_eq!(job.status, DownloadJobState::Failed);
        assert!(job.finished_at_unix_ms.is_some());
        assert!(job.result.is_none());
        let error = job.error.as_deref().expect("panic should record an error");
        assert!(error.contains("download worker task failed"));
        assert!(error.contains("worker panic regression sentinel"));
    }

    #[tokio::test]
    async fn finish_worker_task_records_full_anyhow_context_chain_for_failed_fetch() {
        let request = FetchPaperRequest {
            arxiv_id: "2401.12345".to_string(),
            include_pdf: Some(false),
            include_source: Some(false),
            refresh: Some(false),
        };
        let state = Arc::new(Mutex::new(QueueState {
            next_job_id: 1,
            jobs: vec![DownloadJob {
                job_id: "download-1".to_string(),
                arxiv_id: "2401.12345".to_string(),
                estimated_network_requests: estimated_network_requests(&request),
                request,
                status: DownloadJobState::InProgress,
                queued_at_unix_ms: unix_ms(),
                started_at_unix_ms: Some(unix_ms()),
                finished_at_unix_ms: None,
                result: None,
                error: None,
            }],
        }));
        let url = "http://127.0.0.1:31337/pdf/2401.12345";
        let handle: JoinHandle<WorkerResult> = tokio::spawn(async move {
            let error = anyhow::anyhow!("error decoding response body")
                .context(format!("reading response body from {url}"))
                .context("fetching queued arXiv paper 2401.12345");
            Some(Err(format_error_chain(error)))
        });

        finish_worker_task(state.clone(), "download-1".to_string(), handle).await;

        let state = state.lock().await;
        let job = &state.jobs[0];
        assert_eq!(job.status, DownloadJobState::Failed);
        assert!(job.result.is_none());
        let error = job
            .error
            .as_deref()
            .expect("worker error should be recorded");
        assert!(
            error.contains("fetching queued arXiv paper 2401.12345"),
            "{error}"
        );
        assert!(
            error.contains("reading response body from http://127.0.0.1:31337/pdf/2401.12345"),
            "{error}"
        );
        assert!(error.contains("error decoding response body"), "{error}");
    }
}
