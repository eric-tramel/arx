use std::{error::Error, process::Command};

#[test]
fn version_flags_print_package_version_without_stderr() -> Result<(), Box<dyn Error>> {
    for flag in ["--version", "-v"] {
        let output = Command::new(env!("CARGO_BIN_EXE_arx-mcp"))
            .arg(flag)
            .output()?;

        assert!(
            output.status.success(),
            "arx-mcp {flag} should exit successfully, stdout: {}, stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stderr, b"", "arx-mcp {flag} should not write stderr");
        assert_eq!(
            String::from_utf8(output.stdout)?,
            format!("arx-mcp {}\n", env!("CARGO_PKG_VERSION"))
        );
    }

    Ok(())
}

#[test]
fn cache_dir_command_does_not_require_version_flag() -> Result<(), Box<dyn Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_arx-mcp"))
        .arg("cache-dir")
        .output()?;

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_no_required_version_error("arx-mcp cache-dir", &stderr);
    assert!(
        output.status.success(),
        "arx-mcp cache-dir should exit successfully without requiring -v/--version, stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        stderr
    );

    Ok(())
}

fn assert_no_required_version_error(command: &str, stderr: &str) {
    let normalized = stderr.to_ascii_lowercase();
    assert!(
        !(normalized.contains("_version") && normalized.contains("required")),
        "{command} should not fail clap parsing by requiring the internal _version flag, stderr: {stderr}"
    );
}
