use crate::{
    arxiv::{FetchPaperRequest, FetchPaperResponse},
    metadata_db::IndexReport,
    paths,
};

pub const MAX_PERSISTED_FINISHED_JOBS: usize = 100;
use anyhow::{Context, Result, bail};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    env,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::sleep,
};

pub const DEFAULT_DAEMON_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct ArxdClient {
    cache_root: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArxdEndpoint {
    pub host: String,
    pub port: u16,
    pub cache_root: String,
    pub started_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ArxdRequest {
    EnqueueFetch { request: FetchPaperRequest },
    QueueStatus { request: DownloadQueueStatusRequest },
    Index,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ArxdResponse {
    QueuedFetch {
        response: QueuedFetchResponse,
    },
    QueueStatus {
        response: DownloadQueueStatusResponse,
    },
    Index {
        response: IndexReport,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DownloadJobState {
    Queued,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QueuedFetchResponse {
    pub job_id: String,
    pub arxiv_id: String,
    pub status: DownloadJobState,
    pub queue_position: usize,
    pub queued_at_unix_ms: u64,
    pub status_tool: String,
    pub message: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct DownloadQueueStatusRequest {
    #[serde(default)]
    pub job_id: Option<String>,
    #[serde(default)]
    pub include_finished: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DownloadQueueStatusResponse {
    pub now_unix_ms: u64,
    pub max_active_workers: usize,
    pub queued_count: usize,
    pub in_progress_count: usize,
    pub completed_count: usize,
    pub failed_count: usize,
    pub jobs: Vec<DownloadJobStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DownloadJobStatus {
    pub job_id: String,
    pub arxiv_id: String,
    pub status: DownloadJobState,
    pub queue_position: Option<usize>,
    pub queued_at_unix_ms: u64,
    pub started_at_unix_ms: Option<u64>,
    pub finished_at_unix_ms: Option<u64>,
    pub elapsed_seconds: Option<u64>,
    pub request: FetchPaperRequest,
    pub result: Option<FetchPaperResponse>,
    pub error: Option<String>,
}

impl ArxdClient {
    pub fn new(cache_root: impl Into<PathBuf>) -> Self {
        Self {
            cache_root: cache_root.into(),
        }
    }

    pub async fn enqueue_fetch(&self, request: FetchPaperRequest) -> Result<QueuedFetchResponse> {
        match self.request(ArxdRequest::EnqueueFetch { request }).await? {
            ArxdResponse::QueuedFetch { response } => Ok(response),
            other => bail!("unexpected arxd response to fetch request: {other:?}"),
        }
    }

    pub async fn queue_status(
        &self,
        request: DownloadQueueStatusRequest,
    ) -> Result<DownloadQueueStatusResponse> {
        match self.request(ArxdRequest::QueueStatus { request }).await? {
            ArxdResponse::QueueStatus { response } => Ok(response),
            other => bail!("unexpected arxd response to queue status request: {other:?}"),
        }
    }

    pub async fn index(&self) -> Result<IndexReport> {
        match self.request(ArxdRequest::Index).await? {
            ArxdResponse::Index { response } => Ok(response),
            other => bail!("unexpected arxd response to index request: {other:?}"),
        }
    }

    async fn request(&self, request: ArxdRequest) -> Result<ArxdResponse> {
        match self.try_request(&request).await {
            Ok(response) => return unpack_error(response),
            Err(first_error) => {
                self.spawn_daemon().with_context(|| {
                    format!("starting arxd after connection failure: {first_error:#}")
                })?;
            }
        }
        self.wait_until_ready().await?;
        let response = self.try_request(&request).await?;
        unpack_error(response)
    }

    async fn try_request(&self, request: &ArxdRequest) -> Result<ArxdResponse> {
        let endpoint = read_endpoint(&self.cache_root)?;
        let mut stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port))
            .await
            .with_context(|| {
                format!("connecting to arxd at {}:{}", endpoint.host, endpoint.port)
            })?;
        let payload = serde_json::to_vec(request).context("serializing arxd request")?;
        stream
            .write_all(&payload)
            .await
            .context("writing arxd request")?;
        stream
            .write_all(b"\n")
            .await
            .context("writing arxd request newline")?;
        let mut reader = BufReader::new(stream);
        let mut line = Vec::new();
        reader
            .read_until(b'\n', &mut line)
            .await
            .context("reading arxd response")?;
        if line.is_empty() {
            bail!("arxd closed connection without a response");
        }
        serde_json::from_slice(&line).context("parsing arxd response")
    }

    fn spawn_daemon(&self) -> Result<()> {
        let executable = arxd_executable()?;
        let mut command = Command::new(&executable);
        command
            .arg("serve")
            .arg("--cache-root")
            .arg(&self.cache_root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // arxd must outlive whichever frontend spawned it: MCP hosts kill
        // their server's whole process group on exit, and an arxd sharing
        // that group dies mid-download without running its shutdown path.
        // Detach it into its own process group so group-directed signals
        // stop at the frontend.
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            command.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
        }
        command
            .spawn()
            .with_context(|| format!("spawning arxd at {}", executable.display()))?;
        Ok(())
    }

    async fn wait_until_ready(&self) -> Result<()> {
        let mut last_error = None;
        for _ in 0..50 {
            match self
                .try_request(&ArxdRequest::QueueStatus {
                    request: DownloadQueueStatusRequest::default(),
                })
                .await
            {
                Ok(response) => {
                    unpack_error(response)?;
                    return Ok(());
                }
                Err(error) => {
                    last_error = Some(error);
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        match last_error {
            Some(error) => Err(error).context("waiting for arxd to become ready"),
            None => bail!("waiting for arxd to become ready"),
        }
    }
}

pub fn write_endpoint(cache_root: &Path, endpoint: &ArxdEndpoint) -> Result<()> {
    let path = paths::arxd_state_path(cache_root);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating arxd state directory {}", parent.display()))?;
    }
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, serde_json::to_vec_pretty(endpoint)?)
        .with_context(|| format!("writing temporary arxd state {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, &path)
        .with_context(|| format!("renaming {} to {}", tmp_path.display(), path.display()))?;
    Ok(())
}

pub fn read_endpoint(cache_root: &Path) -> Result<ArxdEndpoint> {
    let path = paths::arxd_state_path(cache_root);
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("reading arxd state {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing arxd state {}", path.display()))
}

pub fn remove_endpoint(cache_root: &Path) -> Result<()> {
    let path = paths::arxd_state_path(cache_root);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("removing arxd state {}", path.display())),
    }
}

/// Persist a bounded list of finished job records so they survive arxd restart.
/// Only keeps the most recent `MAX_PERSISTED_FINISHED_JOBS` entries.
pub fn write_finished_jobs(cache_root: &Path, jobs: &[DownloadJobStatus]) -> Result<()> {
    let path = paths::arxd_finished_jobs_path(cache_root);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating finished-jobs directory {}", parent.display()))?;
    }
    let truncated: Vec<&DownloadJobStatus> = {
        let len = jobs.len();
        let start = len.saturating_sub(MAX_PERSISTED_FINISHED_JOBS);
        jobs[start..].iter().collect()
    };
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, serde_json::to_vec_pretty(&truncated)?)
        .with_context(|| format!("writing finished jobs to {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, &path)
        .with_context(|| format!("renaming {} to {}", tmp_path.display(), path.display()))?;
    Ok(())
}

/// Load previously persisted finished job records; returns an empty vec on any error
/// (missing file, corrupt JSON) so a fresh start is always valid.
pub fn read_finished_jobs(cache_root: &Path) -> Vec<DownloadJobStatus> {
    let path = paths::arxd_finished_jobs_path(cache_root);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(_) => return Vec::new(),
    };
    serde_json::from_str::<Vec<DownloadJobStatus>>(&text).unwrap_or_default()
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn unpack_error(response: ArxdResponse) -> Result<ArxdResponse> {
    match response {
        ArxdResponse::Error { message } => bail!(message),
        response => Ok(response),
    }
}

fn arxd_executable() -> Result<PathBuf> {
    if let Some(path) = env::var_os("ARXD_BIN") {
        return Ok(PathBuf::from(path));
    }
    let current_exe = env::current_exe().context("locating current executable")?;
    let sibling = current_exe.with_file_name(format!("arxd{}", env::consts::EXE_SUFFIX));
    if sibling.exists() {
        return Ok(sibling);
    }
    Ok(PathBuf::from(format!("arxd{}", env::consts::EXE_SUFFIX)))
}
