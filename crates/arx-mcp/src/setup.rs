use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{
    env, fs,
    path::{Path, PathBuf},
};

pub fn mcp_config_snippet(executable: &Path) -> Value {
    json!({
        "mcpServers": {
            "arx": {
                "command": executable.display().to_string(),
                "args": ["serve"]
            }
        }
    })
}

pub fn default_claude_desktop_config_path() -> Result<PathBuf> {
    if cfg!(target_os = "macos") {
        let home = env::var_os("HOME").context("HOME is not set")?;
        return Ok(PathBuf::from(home)
            .join("Library")
            .join("Application Support")
            .join("Claude")
            .join("claude_desktop_config.json"));
    }

    if cfg!(target_os = "windows") {
        let appdata = env::var_os("APPDATA").context("APPDATA is not set")?;
        return Ok(PathBuf::from(appdata)
            .join("Claude")
            .join("claude_desktop_config.json"));
    }

    let config_home = env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .context("neither XDG_CONFIG_HOME nor HOME is set")?;
    Ok(config_home
        .join("Claude")
        .join("claude_desktop_config.json"))
}

pub fn install_claude_desktop_config(config_path: &Path, executable: &Path) -> Result<()> {
    let mut config = if config_path.exists() {
        let text = fs::read_to_string(config_path)
            .with_context(|| format!("reading {}", config_path.display()))?;
        serde_json::from_str::<Value>(&text)
            .with_context(|| format!("parsing {}", config_path.display()))?
    } else {
        json!({})
    };

    if !config.is_object() {
        config = json!({});
    }

    let object = config
        .as_object_mut()
        .expect("config was just normalized to object");
    let servers = object.entry("mcpServers").or_insert_with(|| json!({}));
    if !servers.is_object() {
        *servers = json!({});
    }
    servers
        .as_object_mut()
        .expect("servers was just normalized to object")
        .insert(
            "arx".to_string(),
            json!({
                "command": executable.display().to_string(),
                "args": ["serve"]
            }),
        );

    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    let tmp_path = config_path.with_extension("json.tmp");
    fs::write(&tmp_path, serde_json::to_vec_pretty(&config)?)
        .with_context(|| format!("writing {}", tmp_path.display()))?;
    fs::rename(&tmp_path, config_path).with_context(|| {
        format!(
            "renaming {} to {}",
            tmp_path.display(),
            config_path.display()
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn config_snippet_points_stdio_server_at_executable() {
        let executable = PathBuf::from("/tmp/arx-bin");

        assert_eq!(
            mcp_config_snippet(&executable),
            json!({
                "mcpServers": {
                    "arx": {
                        "command": executable.display().to_string(),
                        "args": ["serve"]
                    }
                }
            })
        );
    }

    #[test]
    fn install_claude_desktop_config_merges_arx_without_user_config() -> Result<()> {
        let temp = tempdir()?;
        let config_path = temp
            .path()
            .join("Claude")
            .join("claude_desktop_config.json");
        let executable = temp.path().join("bin").join("arx");
        fs::create_dir_all(config_path.parent().unwrap())?;
        fs::write(
            &config_path,
            serde_json::to_vec_pretty(&json!({
                "theme": "dark",
                "mcpServers": {
                    "other": {
                        "command": "other-server",
                        "args": ["--stdio"]
                    },
                    "arx": {
                        "command": "old-arx",
                        "args": ["serve"]
                    }
                }
            }))?,
        )?;

        install_claude_desktop_config(&config_path, &executable)?;

        let merged: Value = serde_json::from_slice(&fs::read(&config_path)?)?;
        assert_eq!(merged["theme"], "dark");
        assert_eq!(merged["mcpServers"]["other"]["command"], "other-server");
        assert_eq!(merged["mcpServers"]["other"]["args"], json!(["--stdio"]));
        assert_eq!(
            merged["mcpServers"]["arx"]["command"],
            executable.display().to_string()
        );
        assert_eq!(merged["mcpServers"]["arx"]["args"], json!(["serve"]));
        assert!(!config_path.with_extension("json.tmp").exists());
        Ok(())
    }
}
