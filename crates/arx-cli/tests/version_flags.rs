use std::{error::Error, process::Command};
use tempfile::tempdir;

#[test]
fn version_flags_print_package_version_without_stderr() -> Result<(), Box<dyn Error>> {
    for flag in ["--version", "-v"] {
        let output = Command::new(env!("CARGO_BIN_EXE_arx")).arg(flag).output()?;

        assert!(
            output.status.success(),
            "arx {flag} should exit successfully, stdout: {}, stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stderr, b"", "arx {flag} should not write stderr");
        assert_eq!(
            String::from_utf8(output.stdout)?,
            format!("arx {}\n", env!("CARGO_PKG_VERSION"))
        );
    }

    Ok(())
}

#[test]
fn search_subcommand_does_not_require_or_trigger_custom_version_arg() -> Result<(), Box<dyn Error>>
{
    let temp = tempdir()?;
    let output = Command::new(env!("CARGO_BIN_EXE_arx"))
        .env("XDG_CACHE_HOME", temp.path())
        .env_remove("ARX_CACHE_DIR")
        .args(["search", "delta"])
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "arx search delta should parse and run as a normal subcommand, stdout: {stdout}, stderr: {stderr}"
    );
    assert!(
        !stderr.contains("_version"),
        "normal subcommands should not require the custom version argument, stderr: {stderr}"
    );
    assert_ne!(
        stdout.as_ref(),
        format!("arx {}\n", env!("CARGO_PKG_VERSION")),
        "search without a version flag should not print only the version banner"
    );
    assert!(
        stdout.contains("no matches in") && stdout.contains("indexed chunks"),
        "empty temp cache search should report an empty search result, stdout: {stdout}"
    );

    Ok(())
}
