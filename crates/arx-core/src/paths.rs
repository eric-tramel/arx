use anyhow::{Context, Result};
use std::{
    env,
    path::{Path, PathBuf},
};

pub const APP_CACHE_DIR: &str = "arx";

pub fn xdg_cache_root() -> Result<PathBuf> {
    if let Ok(path) = env::var("ARX_CACHE_DIR") {
        if !path.trim().is_empty() {
            return Ok(PathBuf::from(path));
        }
    }

    if let Ok(path) = env::var("XDG_CACHE_HOME") {
        if !path.trim().is_empty() {
            return Ok(PathBuf::from(path).join(APP_CACHE_DIR));
        }
    }

    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(dirs::home_dir)
        .context("HOME is not set and no platform home directory is available")?;
    Ok(home.join(".cache").join(APP_CACHE_DIR))
}

pub fn safe_arxiv_id(arxiv_id: &str) -> String {
    arxiv_id
        .chars()
        .map(|ch| match ch {
            '/' | '\\' | ':' => '_',
            ch if ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_' => ch,
            _ => '_',
        })
        .collect()
}

pub fn paper_cache_dir(cache_root: impl Into<PathBuf>, arxiv_id: &str) -> PathBuf {
    cache_root
        .into()
        .join("papers")
        .join(safe_arxiv_id(arxiv_id))
}

pub fn metadata_db_path(cache_root: impl AsRef<Path>) -> PathBuf {
    cache_root.as_ref().join("metadata.sqlite3")
}

pub fn search_index_dir(cache_root: impl AsRef<Path>) -> PathBuf {
    cache_root.as_ref().join("search-index")
}

pub fn arxd_state_path(cache_root: impl AsRef<Path>) -> PathBuf {
    cache_root.as_ref().join("arxd.json")
}

pub fn arxd_lock_path(cache_root: impl AsRef<Path>) -> PathBuf {
    cache_root.as_ref().join("arxd.lock")
}

pub fn arxd_log_path(cache_root: impl AsRef<Path>) -> PathBuf {
    cache_root.as_ref().join("arxd.log")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paper_cache_dir_sanitizes_arxiv_id_path_components_under_cache_root() {
        let root = PathBuf::from("/tmp/arx-cache");

        assert_eq!(
            paper_cache_dir(&root, "hep-th/9901001"),
            root.join("papers").join("hep-th_9901001")
        );
        assert_eq!(
            paper_cache_dir(&root, "https://arxiv.org/abs/2401.12345v2"),
            root.join("papers")
                .join("https___arxiv.org_abs_2401.12345v2")
        );
        assert_eq!(metadata_db_path(&root), root.join("metadata.sqlite3"));
        assert_eq!(arxd_state_path(&root), root.join("arxd.json"));
        assert_eq!(arxd_lock_path(&root), root.join("arxd.lock"));
        assert_eq!(arxd_log_path(&root), root.join("arxd.log"));
    }
}
