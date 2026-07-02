use anyhow::{Context, Result, bail};
use arx_core::{
    arxiv::{ArxivFetcher, FetchPaperRequest, FetchPaperResponse, normalize_arxiv_id},
    daemon::{
        ArxdEndpoint, ArxdRequest, ArxdResponse, DownloadJobState, DownloadJobStatus,
        DownloadQueueStatusRequest, DownloadQueueStatusResponse, MAX_PERSISTED_FINISHED_JOBS,
        QueuedFetchResponse, read_finished_jobs, remove_endpoint, unix_ms, write_endpoint,
        write_finished_jobs,
    },
    paths::{arxd_lock_path, arxd_log_path, xdg_cache_root},
};
use clap::{Parser, Subcommand};
use fs2::FileExt;
use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
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
use tracing_subscriber::{EnvFilter, fmt::MakeWriter};

const DOWNLOAD_WORKERS: usize = 4;
const STATUS_TOOL_NAME: &str = "get_arxiv_download_queue_status";
const DEFAULT_IDLE_SHUTDOWN: Duration = Duration::from_secs(30);
const DEFAULT_LOG_MAX_BYTES: usize = 1024 * 1024;
const DEFAULT_LOG_BACKUPS: usize = 5;

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
    /// Finished job records loaded from disk at startup (prior runs).
    /// These are prepended to queue status responses when `include_finished` is set.
    persisted_finished: Vec<DownloadJobStatus>,
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
    result: Option<FetchPaperResponse>,
    error: Option<String>,
}

#[derive(Clone)]
struct RollingLogWriter {
    inner: Arc<StdMutex<RollingLogState>>,
}

struct RollingLogState {
    path: PathBuf,
    file: Option<File>,
    current_len: usize,
    max_bytes: usize,
    backups: usize,
}

impl RollingLogWriter {
    fn new(path: PathBuf, max_bytes: usize, backups: usize) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = open_log_file(&path, false)?;
        let current_len = file.metadata()?.len().try_into().unwrap_or(usize::MAX);
        Ok(Self {
            inner: Arc::new(StdMutex::new(RollingLogState {
                path,
                file: Some(file),
                current_len,
                max_bytes: max_bytes.max(1),
                backups,
            })),
        })
    }
}

impl Write for RollingLogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("arxd log writer lock poisoned"))?;
        state.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("arxd log writer lock poisoned"))?;
        match state.file.as_mut() {
            Some(file) => file.flush(),
            None => Ok(()),
        }
    }
}

impl<'writer> MakeWriter<'writer> for RollingLogWriter {
    type Writer = RollingLogWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        self.clone()
    }
}

impl RollingLogState {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.current_len > 0 && self.current_len.saturating_add(buf.len()) > self.max_bytes {
            self.rotate()?;
        }
        self.file
            .as_mut()
            .expect("arxd rolling log file must be open")
            .write_all(buf)?;
        self.current_len = self.current_len.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn rotate(&mut self) -> io::Result<()> {
        if let Some(mut file) = self.file.take() {
            file.flush()?;
        }
        for index in (1..=self.backups).rev() {
            let source = if index == 1 {
                self.path.clone()
            } else {
                numbered_log_path(&self.path, index - 1)
            };
            let destination = numbered_log_path(&self.path, index);
            if index == self.backups {
                remove_if_exists(&destination)?;
            }
            rename_if_exists(&source, &destination)?;
        }
        self.file = Some(open_log_file(&self.path, true)?);
        self.current_len = 0;
        Ok(())
    }
}

fn open_log_file(path: &Path, truncate: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).write(true);
    if truncate {
        options.truncate(true);
    } else {
        options.append(true);
    }
    options.open(path)
}

fn numbered_log_path(path: &Path, index: usize) -> PathBuf {
    let mut file_name = path
        .file_name()
        .map(OsString::from)
        .unwrap_or_else(|| OsString::from("arxd.log"));
    file_name.push(format!(".{index}"));
    path.with_file_name(file_name)
}

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn rename_if_exists(source: &Path, destination: &Path) -> io::Result<()> {
    match fs::rename(source, destination) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn init_logging(cache_root: &Path) -> Result<()> {
    let log_path = arxd_log_path(cache_root);
    let log_writer = RollingLogWriter::new(log_path, DEFAULT_LOG_MAX_BYTES, DEFAULT_LOG_BACKUPS)
        .with_context(|| format!("opening arxd log under {}", cache_root.display()))?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .with_writer(log_writer)
        .with_ansi(false)
        .try_init()
        .map_err(|error| anyhow::anyhow!("initializing arxd file logger: {error}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve { cache_root } => {
            let cache_root = match cache_root {
                Some(cache_root) => cache_root,
                None => xdg_cache_root()?,
            };
            init_logging(&cache_root)?;
            tracing::info!(log_path = %arxd_log_path(&cache_root).display(), "arxd logging initialized");
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
        let persisted_finished = read_finished_jobs(&cache_root);
        let queue = DownloadQueue::new(fetcher.clone(), persisted_finished);
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

        // Persist finished jobs so a restarted arxd can still return them.
        let finished_to_persist = self.queue.finished_jobs_for_persistence().await;
        if let Err(error) = write_finished_jobs(&self.cache_root, &finished_to_persist) {
            tracing::warn!(?error, "failed to persist finished jobs on shutdown");
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
    fn new(fetcher: Arc<ArxivFetcher>, persisted_finished: Vec<DownloadJobStatus>) -> Self {
        Self {
            fetcher,
            semaphore: Arc::new(Semaphore::new(DOWNLOAD_WORKERS)),
            state: Arc::new(Mutex::new(QueueState {
                persisted_finished,
                ..QueueState::default()
            })),
        }
    }

    async fn enqueue(&self, request: FetchPaperRequest) -> Result<QueuedFetchResponse> {
        let arxiv_id = normalize_arxiv_id(&request.arxiv_id)?;
        let queued_at_unix_ms = unix_ms();
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
            status_tool: STATUS_TOOL_NAME.to_string(),
            message: format!(
                "queued arXiv download; call {STATUS_TOOL_NAME} with this job_id to check progress"
            ),
        })
    }

    async fn status(&self, request: DownloadQueueStatusRequest) -> DownloadQueueStatusResponse {
        let now_unix_ms = unix_ms();
        let include_finished = request.include_finished.unwrap_or(true);
        let (live_jobs, persisted_finished) = {
            let state = self.state.lock().await;
            (state.jobs.clone(), state.persisted_finished.clone())
        };
        let queued_count = live_jobs
            .iter()
            .filter(|job| job.status == DownloadJobState::Queued)
            .count();
        let in_progress_count = live_jobs
            .iter()
            .filter(|job| job.status == DownloadJobState::InProgress)
            .count();
        // Count completed/failed from live jobs; persisted are already in terminal state.
        let live_completed_count = live_jobs
            .iter()
            .filter(|job| job.status == DownloadJobState::Completed)
            .count();
        let live_failed_count = live_jobs
            .iter()
            .filter(|job| job.status == DownloadJobState::Failed)
            .count();
        // Persisted finished that are NOT shadowed by a live job with the same job_id.
        let live_job_ids: std::collections::HashSet<&str> =
            live_jobs.iter().map(|j| j.job_id.as_str()).collect();
        let persisted_not_shadowed: Vec<&DownloadJobStatus> = persisted_finished
            .iter()
            .filter(|j| !live_job_ids.contains(j.job_id.as_str()))
            .collect();
        let persisted_completed_count = persisted_not_shadowed
            .iter()
            .filter(|j| j.status == DownloadJobState::Completed)
            .count();
        let persisted_failed_count = persisted_not_shadowed
            .iter()
            .filter(|j| j.status == DownloadJobState::Failed)
            .count();
        let completed_count = live_completed_count + persisted_completed_count;
        let failed_count = live_failed_count + persisted_failed_count;

        let live_statuses = queue_statuses(&live_jobs, now_unix_ms, DOWNLOAD_WORKERS);
        let mut jobs: Vec<DownloadJobStatus> = live_statuses
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

        // Append matching persisted finished jobs (not shadowed by live).
        if include_finished {
            for persisted_job in persisted_not_shadowed {
                let matches_requested_job = request
                    .job_id
                    .as_ref()
                    .map(|job_id| &persisted_job.job_id == job_id)
                    .unwrap_or(true);
                if matches_requested_job {
                    jobs.push(persisted_job.clone());
                }
            }
        }

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

    /// Collect all finished jobs (live + persisted, deduplicated) for persistence on shutdown.
    async fn finished_jobs_for_persistence(&self) -> Vec<DownloadJobStatus> {
        let now_unix_ms = unix_ms();
        let (live_jobs, persisted_finished) = {
            let state = self.state.lock().await;
            (state.jobs.clone(), state.persisted_finished.clone())
        };
        let live_statuses = queue_statuses(&live_jobs, now_unix_ms, DOWNLOAD_WORKERS);
        let live_job_ids: std::collections::HashSet<&str> =
            live_jobs.iter().map(|j| j.job_id.as_str()).collect();

        // Start with persisted jobs that weren't re-run this session (not shadowed).
        let mut combined: Vec<DownloadJobStatus> = persisted_finished
            .into_iter()
            .filter(|j| !live_job_ids.contains(j.job_id.as_str()))
            .collect();
        // Add live finished jobs.
        for job in live_statuses {
            if matches!(
                job.status,
                DownloadJobState::Completed | DownloadJobState::Failed
            ) {
                combined.push(job);
            }
        }
        // Sort by finished_at to produce a deterministic order; missing = 0 (shouldn't happen).
        combined.sort_by_key(|j| j.finished_at_unix_ms.unwrap_or(0));
        // Retain only the most recent MAX_PERSISTED_FINISHED_JOBS.
        let len = combined.len();
        let start = len.saturating_sub(MAX_PERSISTED_FINISHED_JOBS);
        combined[start..].to_vec()
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
    _max_active_workers: usize,
) -> Vec<DownloadJobStatus> {
    let mut queued_position = 0;
    jobs.iter()
        .map(|job| {
            let elapsed = job.started_at_unix_ms.map(|started| {
                seconds_between(started, job.finished_at_unix_ms.unwrap_or(now_unix_ms))
            });
            let queue_position = match job.status {
                DownloadJobState::InProgress => None,
                DownloadJobState::Queued => {
                    queued_position += 1;
                    Some(queued_position)
                }
                DownloadJobState::Completed | DownloadJobState::Failed => None,
            };

            DownloadJobStatus {
                job_id: job.job_id.clone(),
                arxiv_id: job.arxiv_id.clone(),
                status: job.status,
                queue_position,
                queued_at_unix_ms: job.queued_at_unix_ms,
                started_at_unix_ms: job.started_at_unix_ms,
                finished_at_unix_ms: job.finished_at_unix_ms,
                elapsed_seconds: elapsed,
                request: job.request.clone(),
                result: job.result.clone(),
                error: job.error.clone(),
            }
        })
        .collect()
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
    use std::io::Write as _;
    use tempfile::tempdir;
    use tokio::task::JoinHandle;

    #[test]
    fn rolling_log_writer_rotates_current_log_into_numbered_backups_before_overflowing()
    -> std::io::Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("arxd.log");
        let backup_1 = path.with_file_name("arxd.log.1");
        let backup_2 = path.with_file_name("arxd.log.2");
        let mut writer = RollingLogWriter::new(path.clone(), 10, 2)?;

        writer.write_all(b"first\n")?;
        writer.flush()?;

        assert_eq!(fs::read_to_string(&path)?, "first\n");
        assert!(!backup_1.exists());

        writer.write_all(b"second\n")?;
        writer.flush()?;

        assert_eq!(fs::read_to_string(&path)?, "second\n");
        assert_eq!(fs::read_to_string(&backup_1)?, "first\n");
        assert!(!backup_2.exists());

        writer.write_all(b"third\n")?;
        writer.flush()?;

        assert_eq!(fs::read_to_string(&path)?, "third\n");
        assert_eq!(fs::read_to_string(&backup_1)?, "second\n");
        assert_eq!(fs::read_to_string(&backup_2)?, "first\n");

        Ok(())
    }

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
                request,
                status: DownloadJobState::InProgress,
                queued_at_unix_ms: unix_ms(),
                started_at_unix_ms: Some(unix_ms()),
                finished_at_unix_ms: None,
                result: None,
                error: None,
            }],
            ..QueueState::default()
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
                request,
                status: DownloadJobState::InProgress,
                queued_at_unix_ms: unix_ms(),
                started_at_unix_ms: Some(unix_ms()),
                finished_at_unix_ms: None,
                result: None,
                error: None,
            }],
            ..QueueState::default()
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

    fn make_finished_job_status(
        job_id: &str,
        arxiv_id: &str,
        state: DownloadJobState,
        finished_at_unix_ms: u64,
    ) -> DownloadJobStatus {
        use arx_core::arxiv::FetchPaperRequest;
        DownloadJobStatus {
            job_id: job_id.to_string(),
            arxiv_id: arxiv_id.to_string(),
            status: state,
            queue_position: None,
            queued_at_unix_ms: finished_at_unix_ms.saturating_sub(5_000),
            started_at_unix_ms: Some(finished_at_unix_ms.saturating_sub(3_000)),
            finished_at_unix_ms: Some(finished_at_unix_ms),
            elapsed_seconds: Some(3),
            request: FetchPaperRequest {
                arxiv_id: arxiv_id.to_string(),
                include_pdf: Some(false),
                include_source: Some(false),
                refresh: Some(false),
            },
            result: None,
            error: if state == DownloadJobState::Failed {
                Some("simulated failure".to_string())
            } else {
                None
            },
        }
    }

    #[tokio::test]
    async fn finished_jobs_are_returned_after_simulated_restart() {
        // Simulate a prior run: two finished jobs are loaded as persisted_finished.
        let persisted = vec![
            make_finished_job_status(
                "download-1",
                "2401.00001",
                DownloadJobState::Completed,
                1_000,
            ),
            make_finished_job_status("download-2", "2401.00002", DownloadJobState::Failed, 2_000),
        ];
        let fetcher = Arc::new(
            arx_core::arxiv::ArxivFetcher::new(tempfile::tempdir().unwrap().keep()).unwrap(),
        );
        let queue = DownloadQueue::new(fetcher, persisted.clone());

        // Query with include_finished = true — should see both persisted jobs.
        let status = queue
            .status(DownloadQueueStatusRequest {
                job_id: None,
                include_finished: Some(true),
            })
            .await;
        assert_eq!(status.completed_count, 1);
        assert_eq!(status.failed_count, 1);
        assert_eq!(status.jobs.len(), 2);
        let ids: Vec<&str> = status.jobs.iter().map(|j| j.job_id.as_str()).collect();
        assert!(
            ids.contains(&"download-1"),
            "expected download-1 in {ids:?}"
        );
        assert!(
            ids.contains(&"download-2"),
            "expected download-2 in {ids:?}"
        );

        // Query with include_finished = false — persisted jobs should be omitted.
        let status_no_finished = queue
            .status(DownloadQueueStatusRequest {
                job_id: None,
                include_finished: Some(false),
            })
            .await;
        assert_eq!(status_no_finished.jobs.len(), 0);

        // Per-id lookup for a persisted job.
        let single = queue
            .status(DownloadQueueStatusRequest {
                job_id: Some("download-1".to_string()),
                include_finished: Some(true),
            })
            .await;
        assert_eq!(single.jobs.len(), 1);
        assert_eq!(single.jobs[0].arxiv_id, "2401.00001");
        assert_eq!(single.jobs[0].status, DownloadJobState::Completed);
    }

    #[tokio::test]
    async fn finished_jobs_for_persistence_prunes_to_max_and_deduplicates() {
        use arx_core::daemon::MAX_PERSISTED_FINISHED_JOBS;
        // Build more jobs than the cap.
        let count = MAX_PERSISTED_FINISHED_JOBS + 10;
        let mut persisted = Vec::new();
        for i in 0..count {
            persisted.push(make_finished_job_status(
                &format!("download-{i}"),
                &format!("2401.{i:05}"),
                DownloadJobState::Completed,
                i as u64 * 1_000,
            ));
        }
        let fetcher = Arc::new(
            arx_core::arxiv::ArxivFetcher::new(tempfile::tempdir().unwrap().keep()).unwrap(),
        );
        let queue = DownloadQueue::new(fetcher, persisted);

        let to_persist = queue.finished_jobs_for_persistence().await;
        assert_eq!(
            to_persist.len(),
            MAX_PERSISTED_FINISHED_JOBS,
            "should be capped at MAX_PERSISTED_FINISHED_JOBS"
        );
        // Should be the most recent MAX jobs (highest finished_at_unix_ms).
        let min_expected_ts = (count - MAX_PERSISTED_FINISHED_JOBS) as u64 * 1_000;
        assert!(
            to_persist
                .iter()
                .all(|j| j.finished_at_unix_ms.unwrap_or(0) >= min_expected_ts),
            "only most recent jobs should be kept"
        );
    }

    #[test]
    fn write_and_read_finished_jobs_round_trips() {
        use arx_core::daemon::{read_finished_jobs, write_finished_jobs};
        let temp = tempfile::tempdir().unwrap();
        let cache_root = temp.path();

        let jobs = vec![
            make_finished_job_status(
                "download-1",
                "2401.00001",
                DownloadJobState::Completed,
                1_000,
            ),
            make_finished_job_status("download-2", "2401.00002", DownloadJobState::Failed, 2_000),
        ];

        write_finished_jobs(cache_root, &jobs).unwrap();
        let loaded = read_finished_jobs(cache_root);
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].job_id, "download-1");
        assert_eq!(loaded[0].status, DownloadJobState::Completed);
        assert_eq!(loaded[1].job_id, "download-2");
        assert_eq!(loaded[1].status, DownloadJobState::Failed);
        assert_eq!(loaded[1].error.as_deref(), Some("simulated failure"));
    }

    #[test]
    fn read_finished_jobs_returns_empty_on_missing_file() {
        use arx_core::daemon::read_finished_jobs;
        let temp = tempfile::tempdir().unwrap();
        let loaded = read_finished_jobs(temp.path());
        assert!(loaded.is_empty());
    }

    #[test]
    fn write_finished_jobs_prunes_to_max_on_disk() {
        use arx_core::daemon::{
            MAX_PERSISTED_FINISHED_JOBS, read_finished_jobs, write_finished_jobs,
        };
        let temp = tempfile::tempdir().unwrap();
        let cache_root = temp.path();
        let count = MAX_PERSISTED_FINISHED_JOBS + 5;
        let jobs: Vec<DownloadJobStatus> = (0..count)
            .map(|i| {
                make_finished_job_status(
                    &format!("download-{i}"),
                    &format!("2401.{i:05}"),
                    DownloadJobState::Completed,
                    i as u64 * 1_000,
                )
            })
            .collect();

        write_finished_jobs(cache_root, &jobs).unwrap();
        let loaded = read_finished_jobs(cache_root);
        assert_eq!(
            loaded.len(),
            MAX_PERSISTED_FINISHED_JOBS,
            "file should hold at most MAX_PERSISTED_FINISHED_JOBS entries"
        );
        // Should be the last (most recent) entries.
        assert_eq!(
            loaded[0].job_id,
            format!("download-{}", count - MAX_PERSISTED_FINISHED_JOBS)
        );
    }
}
