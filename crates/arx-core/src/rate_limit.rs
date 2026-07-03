use anyhow::{Context, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const ARXIV_DELAY: Duration = Duration::from_secs(3);

/// Consecutive systemic metadata failures tolerated before metadata requests
/// pause. One blip retries normally; a second consecutive failure means the
/// service is degraded and further attempts only add load.
pub const METADATA_PAUSE_THRESHOLD: u32 = 2;
/// First metadata pause once the threshold is crossed; doubles per further
/// consecutive failure.
pub const METADATA_PAUSE_BASE: Duration = Duration::from_secs(30);
/// Longest pause between metadata probes during an extended arXiv outage.
pub const METADATA_PAUSE_MAX: Duration = Duration::from_secs(900);

#[derive(Debug, Clone)]
pub struct RateLimiter {
    lock_path: PathBuf,
    state_path: PathBuf,
    delay: Duration,
}

/// On-disk state shared by every arx process. `next_allowed_unix_ms` gates
/// all arXiv requests; the metadata fields track export.arxiv.org health so
/// concurrent workers stop hammering a degraded metadata API instead of each
/// discovering the outage independently.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RateLimitState {
    next_allowed_unix_ms: u64,
    #[serde(default)]
    metadata_failure_streak: u32,
    #[serde(default)]
    metadata_paused_until_unix_ms: u64,
}

/// Snapshot of shared arXiv metadata service health.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MetadataHealth {
    pub failure_streak: u32,
    pub paused_until_unix_ms: u64,
}

impl MetadataHealth {
    /// The pause deadline, if metadata requests are currently paused.
    pub fn paused_until(&self, now_unix_ms: u64) -> Option<u64> {
        (self.paused_until_unix_ms > now_unix_ms).then_some(self.paused_until_unix_ms)
    }
}

impl RateLimiter {
    pub fn new(cache_root: impl Into<PathBuf>) -> Self {
        let cache_root = cache_root.into();
        Self::with_delay(cache_root, ARXIV_DELAY)
    }

    pub fn with_delay(cache_root: impl Into<PathBuf>, delay: Duration) -> Self {
        let cache_root = cache_root.into();
        Self {
            lock_path: cache_root.join("arxiv-rate-limit.lock"),
            state_path: cache_root.join("arxiv-rate-limit.json"),
            delay,
        }
    }

    pub async fn run<F, Fut, T>(&self, request: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.run_with_penalty(request, |_| None).await
    }

    /// Run a request under the shared rate-limit lock. When the request fails
    /// and `penalty_for_error` maps the error to a penalty, the shared
    /// next-allowed time is pushed out BEFORE the lock is released, so no
    /// other worker or process can start another request ahead of the
    /// backoff. (Penalizing after release loses that race: a waiting worker
    /// acquires the lock, sees the stale next-allowed time, and immediately
    /// draws another 429 — observed live as paired 429s milliseconds apart.)
    pub async fn run_with_penalty<F, Fut, T, P>(
        &self,
        request: F,
        penalty_for_error: P,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
        P: FnOnce(&anyhow::Error) -> Option<Duration>,
    {
        let guard = self.acquire().await?;
        let wait = guard.wait_duration()?;
        if !wait.is_zero() {
            tracing::info!(?wait, "waiting for global arXiv rate limit");
            tokio::time::sleep(wait).await;
        }

        let mark_result = guard.mark_started();
        if let Err(error) = mark_result {
            return Err(error);
        }

        let result = request().await;
        let penalty_result = match &result {
            Err(error) => match penalty_for_error(error) {
                Some(penalty) => guard.extend_next_allowed(penalty),
                None => Ok(()),
            },
            Ok(_) => Ok(()),
        };
        let release_result = guard.release().and(penalty_result);
        match (result, release_result) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(err)) => Err(err),
            (Err(err), Ok(())) => Err(err),
            (Err(err), Err(release_err)) => Err(err.context(release_err)),
        }
    }

    async fn acquire(&self) -> Result<RateLimitGuard> {
        let lock_path = self.lock_path.clone();
        let state_path = self.state_path.clone();
        let delay = self.delay;
        tokio::task::spawn_blocking(move || RateLimitGuard::acquire(lock_path, state_path, delay))
            .await
            .context("rate-limit lock task panicked")?
    }

    /// Lock-free read of shared metadata health; safe because state writes
    /// are atomic renames. Callers use this to skip doomed metadata attempts
    /// while the shared pause is active.
    pub fn metadata_health(&self) -> Result<MetadataHealth> {
        let state = read_state(&self.state_path)?;
        Ok(MetadataHealth {
            failure_streak: state.metadata_failure_streak,
            paused_until_unix_ms: state.metadata_paused_until_unix_ms,
        })
    }

    /// Record one systemic metadata failure (timeout, 429, 5xx, connect
    /// error) in the shared state. Crossing `METADATA_PAUSE_THRESHOLD`
    /// pauses metadata requests with exponential backoff so a degraded
    /// arXiv sees a handful of probes instead of a retry storm. Must not be
    /// called while this process holds the rate-limit lock.
    pub async fn record_metadata_failure(&self) -> Result<MetadataHealth> {
        let lock_path = self.lock_path.clone();
        let state_path = self.state_path.clone();
        let delay = self.delay;
        tokio::task::spawn_blocking(move || {
            let guard = RateLimitGuard::acquire(lock_path, state_path.clone(), delay)?;
            let mut state = read_state(&state_path)?;
            state.metadata_failure_streak = state.metadata_failure_streak.saturating_add(1);
            if state.metadata_failure_streak >= METADATA_PAUSE_THRESHOLD {
                let paused_until = unix_ms()?.saturating_add(
                    metadata_pause_for_streak(state.metadata_failure_streak).as_millis() as u64,
                );
                // Never shorten a pause another process already extended.
                if paused_until > state.metadata_paused_until_unix_ms {
                    state.metadata_paused_until_unix_ms = paused_until;
                }
            }
            let health = MetadataHealth {
                failure_streak: state.metadata_failure_streak,
                paused_until_unix_ms: state.metadata_paused_until_unix_ms,
            };
            write_state(&state_path, &state)?;
            guard.release()?;
            Ok(health)
        })
        .await
        .context("metadata failure recording task panicked")?
    }

    /// Clear the shared metadata failure streak and pause after a metadata
    /// request reaches arXiv and gets a real answer. Must not be called
    /// while this process holds the rate-limit lock.
    pub async fn record_metadata_recovery(&self) -> Result<()> {
        // Fast path: nothing to clear, skip the lock entirely so the happy
        // path stays write-free.
        let state = read_state(&self.state_path)?;
        if state.metadata_failure_streak == 0 && state.metadata_paused_until_unix_ms == 0 {
            return Ok(());
        }
        let lock_path = self.lock_path.clone();
        let state_path = self.state_path.clone();
        let delay = self.delay;
        tokio::task::spawn_blocking(move || {
            let guard = RateLimitGuard::acquire(lock_path, state_path.clone(), delay)?;
            let mut state = read_state(&state_path)?;
            state.metadata_failure_streak = 0;
            state.metadata_paused_until_unix_ms = 0;
            write_state(&state_path, &state)?;
            guard.release()
        })
        .await
        .context("metadata recovery recording task panicked")?
    }
}

/// Exponential pause schedule: 30s at the threshold, doubling per further
/// consecutive failure, capped at 15 minutes.
fn metadata_pause_for_streak(streak: u32) -> Duration {
    let exponent = streak.saturating_sub(METADATA_PAUSE_THRESHOLD).min(16);
    METADATA_PAUSE_BASE
        .saturating_mul(1u32 << exponent.min(31))
        .min(METADATA_PAUSE_MAX)
}

struct RateLimitGuard {
    file: File,
    state_path: PathBuf,
    delay: Duration,
    released: bool,
}

impl RateLimitGuard {
    fn acquire(lock_path: PathBuf, state_path: PathBuf, delay: Duration) -> Result<Self> {
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating rate-limit directory {}", parent.display()))?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("opening rate-limit lock {}", lock_path.display()))?;
        file.lock_exclusive()
            .with_context(|| format!("locking {}", lock_path.display()))?;

        Ok(Self {
            file,
            state_path,
            delay,
            released: false,
        })
    }

    fn wait_duration(&self) -> Result<Duration> {
        let state = read_state(&self.state_path)?;
        let now = unix_ms()?;
        Ok(if state.next_allowed_unix_ms > now {
            Duration::from_millis(state.next_allowed_unix_ms - now)
        } else {
            Duration::ZERO
        })
    }

    fn mark_started(&self) -> Result<()> {
        let mut state = read_state(&self.state_path)?;
        let now = unix_ms()?;
        state.next_allowed_unix_ms = now.saturating_add(self.delay.as_millis() as u64);
        write_state(&self.state_path, &state)
    }

    /// Push the shared next-allowed time at least `penalty` into the future.
    /// Runs while this guard still holds the lock, so the extended backoff is
    /// visible to the next acquirer before any new request can start.
    fn extend_next_allowed(&self, penalty: Duration) -> Result<()> {
        let mut state = read_state(&self.state_path)?;
        let now = unix_ms()?;
        let penalized = now.saturating_add(penalty.as_millis() as u64);
        if penalized > state.next_allowed_unix_ms {
            state.next_allowed_unix_ms = penalized;
            write_state(&self.state_path, &state)?;
            tracing::info!(?penalty, "arXiv rate limited; extending shared backoff");
        }
        Ok(())
    }

    fn release(mut self) -> Result<()> {
        self.file.unlock().context("unlocking rate-limit file")?;
        self.released = true;
        Ok(())
    }
}

impl Drop for RateLimitGuard {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.file.unlock();
        }
    }
}

fn read_state(path: &Path) -> Result<RateLimitState> {
    match fs::read_to_string(path) {
        Ok(text) => serde_json::from_str(&text)
            .with_context(|| format!("parsing rate-limit state {}", path.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(RateLimitState::default()),
        Err(err) => {
            Err(err).with_context(|| format!("reading rate-limit state {}", path.display()))
        }
    }
}

fn write_state(path: &Path, state: &RateLimitState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating rate-limit state directory {}", parent.display()))?;
    }

    let tmp_path = path.with_extension("json.tmp");
    let mut file = File::create(&tmp_path)
        .with_context(|| format!("creating temporary rate-limit state {}", tmp_path.display()))?;
    serde_json::to_writer(&mut file, state).context("serializing rate-limit state")?;
    file.write_all(b"\n").context("writing state newline")?;
    file.sync_all().context("syncing rate-limit state")?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "renaming temporary rate-limit state {} to {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn unix_ms() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?
        .as_millis() as u64)
}

/// Current unix time in milliseconds, for callers comparing against shared
/// deadlines like `MetadataHealth::paused_until`.
pub fn now_unix_ms() -> Result<u64> {
    unix_ms()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs2::FileExt;
    use std::{
        fs::OpenOptions,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };
    use tempfile::tempdir;

    #[test]
    fn wait_duration_uses_shared_state_without_sleeping() -> Result<()> {
        let temp = tempdir()?;
        let limiter = RateLimiter::with_delay(temp.path(), Duration::ZERO);
        write_state(
            &limiter.state_path,
            &RateLimitState {
                next_allowed_unix_ms: unix_ms()?.saturating_add(60_000),
                ..RateLimitState::default()
            },
        )?;

        let guard = RateLimitGuard::acquire(
            limiter.lock_path.clone(),
            limiter.state_path.clone(),
            Duration::ZERO,
        )?;

        assert!(guard.wait_duration()? > Duration::from_secs(50));
        Ok(())
    }

    #[tokio::test]
    async fn run_with_penalty_extends_backoff_before_releasing_the_lock() -> Result<()> {
        let temp = tempdir()?;
        let limiter = RateLimiter::with_delay(temp.path(), Duration::ZERO);

        let result: Result<()> = limiter
            .run_with_penalty(
                || async { Err(anyhow::anyhow!("simulated 429")) },
                |error| {
                    assert!(format!("{error:#}").contains("429"));
                    Some(Duration::from_secs(30))
                },
            )
            .await;
        assert!(result.is_err());

        // By the time run_with_penalty returns the penalty is already
        // durable, so ANY later acquirer (the race in the old post-release
        // penalize design) must observe it.
        let state = read_state(&limiter.state_path)?;
        let now = unix_ms()?;
        assert!(
            state.next_allowed_unix_ms > now.saturating_add(25_000),
            "penalty should push next-allowed ~30s out"
        );

        // A smaller penalty must not move the next-allowed time earlier.
        let result: Result<()> = limiter
            .run_with_penalty(
                || async { Err(anyhow::anyhow!("simulated 429")) },
                |_| Some(Duration::from_secs(1)),
            )
            .await;
        assert!(result.is_err());
        let state_after = read_state(&limiter.state_path)?;
        assert!(state_after.next_allowed_unix_ms >= state.next_allowed_unix_ms);
        Ok(())
    }

    #[tokio::test]
    async fn successful_run_preserves_metadata_health_fields() -> Result<()> {
        let temp = tempdir()?;
        let limiter = RateLimiter::with_delay(temp.path(), Duration::from_millis(1));
        write_state(
            &limiter.state_path,
            &RateLimitState {
                next_allowed_unix_ms: 0,
                metadata_failure_streak: 5,
                metadata_paused_until_unix_ms: unix_ms()?.saturating_add(60_000),
            },
        )?;

        limiter.run(|| async { Ok(()) }).await?;

        let state = read_state(&limiter.state_path)?;
        assert_eq!(
            state.metadata_failure_streak, 5,
            "mark_started must not wipe metadata health"
        );
        assert!(state.metadata_paused_until_unix_ms > 0);
        Ok(())
    }

    #[tokio::test]
    async fn metadata_failures_pause_after_threshold_with_exponential_backoff() -> Result<()> {
        let temp = tempdir()?;
        let limiter = RateLimiter::with_delay(temp.path(), Duration::ZERO);

        let health = limiter.record_metadata_failure().await?;
        assert_eq!(health.failure_streak, 1);
        assert_eq!(
            health.paused_until(unix_ms()?),
            None,
            "one failure should not pause metadata"
        );

        let health = limiter.record_metadata_failure().await?;
        assert_eq!(health.failure_streak, 2);
        let now = unix_ms()?;
        let first_pause = health
            .paused_until(now)
            .expect("second failure should pause metadata");
        assert!(first_pause >= now + 25_000 && first_pause <= now + 35_000);

        let health = limiter.record_metadata_failure().await?;
        let second_pause = health
            .paused_until(unix_ms()?)
            .expect("third failure should extend the pause");
        assert!(
            second_pause >= now + 55_000,
            "pause should roughly double: {second_pause} vs {now}"
        );

        limiter.record_metadata_recovery().await?;
        let health = limiter.metadata_health()?;
        assert_eq!(health.failure_streak, 0);
        assert_eq!(health.paused_until(unix_ms()?), None);
        Ok(())
    }

    #[test]
    fn metadata_pause_schedule_caps_at_max() {
        assert_eq!(metadata_pause_for_streak(2), Duration::from_secs(30));
        assert_eq!(metadata_pause_for_streak(3), Duration::from_secs(60));
        assert_eq!(metadata_pause_for_streak(4), Duration::from_secs(120));
        assert_eq!(metadata_pause_for_streak(7), METADATA_PAUSE_MAX);
        assert_eq!(metadata_pause_for_streak(u32::MAX), METADATA_PAUSE_MAX);
    }

    #[test]
    fn state_file_without_metadata_fields_still_parses() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("arxiv-rate-limit.json");
        fs::write(&path, "{\"next_allowed_unix_ms\": 12}\n")?;
        let state = read_state(&path)?;
        assert_eq!(state.next_allowed_unix_ms, 12);
        assert_eq!(state.metadata_failure_streak, 0);
        assert_eq!(state.metadata_paused_until_unix_ms, 0);
        Ok(())
    }

    #[tokio::test]
    async fn run_records_next_allowed_at_request_start_and_holds_lock_until_request_finishes()
    -> Result<()> {
        let temp = tempdir()?;
        let delay = Duration::from_millis(250);
        let limiter = RateLimiter::with_delay(temp.path(), delay);
        let request_started_no_earlier_than = unix_ms()?;
        let lock_path = limiter.lock_path.clone();
        let state_path = limiter.state_path.clone();
        let next_allowed_during_request = Arc::new(AtomicU64::new(0));
        let observed_next_allowed = next_allowed_during_request.clone();

        limiter
            .run(|| async {
                let state = read_state(&state_path)?;
                observed_next_allowed.store(state.next_allowed_unix_ms, Ordering::SeqCst);
                assert!(
                    state.next_allowed_unix_ms
                        >= request_started_no_earlier_than
                            .saturating_add(delay.as_millis() as u64),
                    "rate limiter should record the next allowed start before the request future completes"
                );

                let competing_lock = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&lock_path)?;
                assert!(
                    competing_lock.try_lock_exclusive().is_err(),
                    "rate limiter should keep the connection lock held while the request future runs"
                );
                Ok(())
            })
            .await?;

        let state_after_completion = read_state(&limiter.state_path)?;
        assert_eq!(
            state_after_completion.next_allowed_unix_ms,
            next_allowed_during_request.load(Ordering::SeqCst),
            "request completion should not move the next allowed start later"
        );
        let competing_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&limiter.lock_path)?;
        competing_lock.try_lock_exclusive()?;
        competing_lock.unlock()?;
        assert!(limiter.lock_path.exists());
        assert!(limiter.state_path.exists());
        Ok(())
    }
}
