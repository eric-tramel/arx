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

#[derive(Debug, Clone)]
pub struct RateLimiter {
    lock_path: PathBuf,
    state_path: PathBuf,
    delay: Duration,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct RateLimitState {
    next_allowed_unix_ms: u64,
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
        let release_result = guard.release();
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

    /// Push the shared next-allowed time at least `penalty` into the future
    /// so every arx process backs off — used when arXiv answers 429. Never
    /// moves the next-allowed time earlier. Must not be called while this
    /// process holds the rate-limit lock (i.e. from inside `run`'s closure).
    pub async fn penalize(&self, penalty: Duration) -> Result<()> {
        let lock_path = self.lock_path.clone();
        let state_path = self.state_path.clone();
        let delay = self.delay;
        tokio::task::spawn_blocking(move || {
            let guard = RateLimitGuard::acquire(lock_path, state_path.clone(), delay)?;
            let state = read_state(&state_path)?;
            let now = unix_ms()?;
            let penalized = now.saturating_add(penalty.as_millis() as u64);
            if penalized > state.next_allowed_unix_ms {
                write_state(
                    &state_path,
                    &RateLimitState {
                        next_allowed_unix_ms: penalized,
                    },
                )?;
                tracing::info!(?penalty, "arXiv rate limited; extending shared backoff");
            }
            guard.release()
        })
        .await
        .context("rate-limit penalty task panicked")?
    }
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
        let now = unix_ms()?;
        let next_allowed_unix_ms = now.saturating_add(self.delay.as_millis() as u64);
        write_state(
            &self.state_path,
            &RateLimitState {
                next_allowed_unix_ms,
            },
        )
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
    async fn penalize_extends_shared_backoff_without_shortening_it() -> Result<()> {
        let temp = tempdir()?;
        let limiter = RateLimiter::with_delay(temp.path(), Duration::ZERO);

        limiter.penalize(Duration::from_secs(30)).await?;
        let state = read_state(&limiter.state_path)?;
        let now = unix_ms()?;
        assert!(
            state.next_allowed_unix_ms > now.saturating_add(25_000),
            "penalty should push next-allowed ~30s out"
        );

        // A smaller penalty must not move the next-allowed time earlier.
        limiter.penalize(Duration::from_secs(1)).await?;
        let state_after = read_state(&limiter.state_path)?;
        assert_eq!(state_after.next_allowed_unix_ms, state.next_allowed_unix_ms);
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
