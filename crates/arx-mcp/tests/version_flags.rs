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
        assert_eq!(
            output.stderr,
            b"",
            "arx-mcp {flag} should not write stderr"
        );
        assert_eq!(
            String::from_utf8(output.stdout)?,
            format!("arx-mcp {}\n", env!("CARGO_PKG_VERSION"))
        );
    }

    Ok(())
}
