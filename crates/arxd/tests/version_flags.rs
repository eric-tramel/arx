use std::{error::Error, fs, path::PathBuf, process::Command};

#[test]
fn version_flags_print_package_version_without_stderr() -> Result<(), Box<dyn Error>> {
    for flag in ["--version", "-v"] {
        let output = Command::new(env!("CARGO_BIN_EXE_arxd"))
            .arg(flag)
            .output()?;

        assert!(
            output.status.success(),
            "arxd {flag} should exit successfully, stdout: {}, stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stderr, b"", "arxd {flag} should not write stderr");
        assert_eq!(
            String::from_utf8(output.stdout)?,
            format!("arxd {}\n", env!("CARGO_PKG_VERSION"))
        );
    }

    Ok(())
}

#[test]
fn serve_command_does_not_require_version_flag() -> Result<(), Box<dyn Error>> {
    let cache_root = version_regression_cache_root();
    let _ = fs::remove_dir_all(&cache_root);

    let output = Command::new(env!("CARGO_BIN_EXE_arxd"))
        .args(["serve", "--cache-root"])
        .arg(&cache_root)
        .env("ARXD_IDLE_SHUTDOWN_MS", "1")
        .output()?;

    let _ = fs::remove_dir_all(&cache_root);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_no_required_version_error("arxd serve", &stderr);
    assert!(
        output.status.success(),
        "arxd serve should exit successfully without requiring -v/--version, stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        stderr
    );

    Ok(())
}

fn version_regression_cache_root() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "arxd-serve-version-regression-{}",
        std::process::id()
    ))
}

fn assert_no_required_version_error(command: &str, stderr: &str) {
    let normalized = stderr.to_ascii_lowercase();
    assert!(
        !(normalized.contains("_version") && normalized.contains("required")),
        "{command} should not fail clap parsing by requiring the internal _version flag, stderr: {stderr}"
    );
}
