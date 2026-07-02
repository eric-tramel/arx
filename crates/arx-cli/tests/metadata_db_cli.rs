use rusqlite::Connection;
use serde_json::{Value, json};
use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use tempfile::tempdir;

#[test]
fn index_command_writes_cached_metadata_into_xdg_cache_database() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let cache_root = temp.path().join("arx");
    let metadata_path = write_cached_metadata(
        &cache_root,
        "hep-th_9901001",
        metadata_json(
            "hep-th/9901001",
            "Indexed old-style paper",
            &["A. Author", "B. Writer"],
            &["hep-th"],
        ),
    )?;

    let output = arx_command(temp.path())
        .args(["--json", "index"])
        .output()?;

    assert_success(&output);
    let report: Value = serde_json::from_slice(&output.stdout)?;
    let db_path = cache_root.join("metadata.sqlite3");
    assert_eq!(report["database_path"], path_value(&db_path));
    assert_eq!(report["scanned_metadata_files"], 1);
    assert_eq!(report["indexed_papers"], 1);
    assert_eq!(report["removed_papers"], 0);

    let connection = Connection::open(&db_path)?;
    let row = connection.query_row(
        "SELECT safe_id, metadata_path, title, authors_json, categories_json, metadata_json \
         FROM papers WHERE arxiv_id = ?1",
        ["hep-th/9901001"],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        },
    )?;

    assert_eq!(row.0, "hep-th_9901001");
    assert_eq!(Path::new(&row.1), metadata_path.as_path());
    assert_eq!(row.2, "Indexed old-style paper");
    assert_eq!(
        serde_json::from_str::<Value>(&row.3)?,
        json!(["A. Author", "B. Writer"])
    );
    assert_eq!(serde_json::from_str::<Value>(&row.4)?, json!(["hep-th"]));
    assert_eq!(
        serde_json::from_str::<Value>(&row.5)?["arxiv_id"],
        "hep-th/9901001"
    );
    Ok(())
}

#[test]
fn lookup_local_only_returns_cached_metadata_status_without_starting_arxd()
-> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let cache_root = temp.path().join("arx");
    write_cached_metadata(
        &cache_root,
        "2401.12345",
        metadata_json(
            "2401.12345",
            "Cached lookup paper",
            &["Lookup Author"],
            &["cs.CL"],
        ),
    )?;
    write_cached_source_material(&cache_root, "2401.12345")?;
    assert!(!cache_root.join("arxd.json").exists());

    let output = arx_command(temp.path())
        .args([
            "--json",
            "lookup",
            "--local-only",
            "2401.12345",
            "2402.00001",
        ])
        .output()?;

    assert_success(&output);
    assert!(
        !cache_root.join("arxd.json").exists(),
        "local-only lookup should not create arxd daemon state"
    );
    let papers: Value = serde_json::from_slice(&output.stdout)?;
    let papers = papers
        .as_array()
        .ok_or_else(|| format!("lookup should return a JSON array for multiple ids: {papers}"))?;
    assert_eq!(papers.len(), 2);

    let cached = &papers[0];
    assert_eq!(cached["arxiv_id"], "2401.12345");
    assert_eq!(cached["metadata"]["title"], "Cached lookup paper");
    assert_eq!(cached["material_state"]["metadata"], "ready");
    assert_eq!(cached["material_state"]["abstract_text"], "ready");
    assert_eq!(cached["material_state"]["source_archive"], "ready");
    assert_eq!(cached["material_state"]["source_tree"], "ready");
    assert_eq!(cached["material_state"]["citations"], "ready");
    assert_eq!(cached["material_state"]["source_search"], "ready");
    assert_eq!(cached["citation_count"], 1);
    assert_eq!(cached["next_tool"], "full_text_search");
    assert_eq!(
        cached["paths"]["source_extracted_dir"],
        path_value(&cache_root.join("papers/2401.12345/source/extracted"))
    );

    let missing = &papers[1];
    assert_eq!(missing["arxiv_id"], "2402.00001");
    assert_eq!(missing["material_state"]["metadata"], "missing");
    assert_eq!(missing["material_state"]["source_search"], "missing");
    assert_eq!(missing["metadata"], Value::Null);
    assert_eq!(missing["next_tool"], "lookup_arxiv_papers");
    Ok(())
}

#[test]
fn queue_status_first_start_boots_arxd_without_existing_state_file() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let cache_root = temp.path().join("arx");
    assert!(!cache_root.join("arxd.json").exists());

    let output = arx_command(temp.path())
        .args(["--json", "queue-status"])
        .output()?;

    assert_success(&output);
    let response: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(response["queued_count"], 0);
    assert_eq!(response["in_progress_count"], 0);
    assert_eq!(response["completed_count"], 0);
    assert_eq!(response["failed_count"], 0);
    assert_eq!(response["jobs"], json!([]));
    assert!(cache_root.join("arxd.json").exists());
    Ok(())
}

#[test]
fn detach_fetch_is_queued_then_completed_by_arxd_and_indexes_metadata() -> Result<(), Box<dyn Error>>
{
    let temp = tempdir()?;
    let cache_root = temp.path().join("arx");
    let target_metadata_path = write_cached_metadata(
        &cache_root,
        "2401.12345",
        metadata_json(
            "2401.12345",
            "Cached target paper",
            &["Target Author"],
            &["cs.CL"],
        ),
    )?;
    write_cached_metadata(
        &cache_root,
        "2402.00001",
        metadata_json(
            "2402.00001",
            "Sibling cached paper",
            &["Sibling Author"],
            &["cs.AI"],
        ),
    )?;

    let output = arx_command(temp.path())
        .env("ARXD_IDLE_SHUTDOWN_MS", "1000")
        .args([
            "--json",
            "fetch",
            "2401.12345",
            "--include-pdf=false",
            "--include-source=false",
            "--detach",
        ])
        .output()?;

    assert_success(&output);
    let queued: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(queued["arxiv_id"], "2401.12345");
    assert_eq!(queued["status"], "queued");
    assert_eq!(queued["queue_position"], 1);
    assert_eq!(queued["status_tool"], "get_arxiv_download_queue_status");
    let job_id = queued["job_id"]
        .as_str()
        .ok_or("queued response should contain a job id")?;

    let completed = wait_for_completed_job(temp.path(), job_id)?;
    assert_eq!(completed["status"], "completed");
    let response = &completed["result"];
    let db_path = cache_root.join("metadata.sqlite3");
    assert_eq!(response["arxiv_id"], "2401.12345");
    assert_eq!(response["metadata_path"], path_value(&target_metadata_path));
    assert_eq!(response["metadata_db_path"], path_value(&db_path));
    assert_eq!(response["indexed_metadata_records"], 2);
    assert_eq!(response["cache_hit"], true);
    assert_eq!(response["network_requests"], 0);
    assert_eq!(response["title"], "Cached target paper");
    assert_eq!(response["authors"], json!(["Target Author"]));
    assert!(response["pdf_path"].is_null());
    assert!(response["source_archive_path"].is_null());
    assert!(response["citations_jsonl_path"].is_null());

    let connection = Connection::open(&db_path)?;
    let indexed_count: i64 =
        connection.query_row("SELECT COUNT(*) FROM papers", [], |row| row.get(0))?;
    assert_eq!(indexed_count, 2);
    let sibling_title: String = connection.query_row(
        "SELECT title FROM papers WHERE arxiv_id = ?1",
        ["2402.00001"],
        |row| row.get(0),
    )?;
    assert_eq!(sibling_title, "Sibling cached paper");
    Ok(())
}

#[test]
fn detach_fetch_accepts_multiple_arxiv_ids_and_returns_json_array() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let cache_root = temp.path().join("arx");
    for arxiv_id in ["2404.00001", "2404.00002"] {
        write_cached_metadata(
            &cache_root,
            arxiv_id,
            metadata_json(
                arxiv_id,
                &format!("Detached multi paper {arxiv_id}"),
                &["Multi Author"],
                &["cs.DL"],
            ),
        )?;
    }

    let output = arx_command(temp.path())
        .env("ARXD_WORKER_HOLD_MS", "150")
        .env("ARXD_IDLE_SHUTDOWN_MS", "1000")
        .args([
            "--json",
            "fetch",
            "2404.00001",
            "2404.00002",
            "--include-pdf=false",
            "--include-source=false",
            "--detach",
        ])
        .output()?;

    assert_success(&output);
    let queued: Value = serde_json::from_slice(&output.stdout)?;
    let queued = queued
        .as_array()
        .ok_or("multi-id detach JSON should be an array")?;
    assert_eq!(queued.len(), 2);
    assert_eq!(queued[0]["arxiv_id"], "2404.00001");
    assert_eq!(queued[1]["arxiv_id"], "2404.00002");

    for job in queued {
        let job_id = job["job_id"]
            .as_str()
            .ok_or("queued response should contain a job id")?;
        let completed = wait_for_completed_job(temp.path(), job_id)?;
        assert_eq!(completed["status"], "completed");
    }
    Ok(())
}

#[test]
fn fetch_without_detach_blocks_and_prints_interactive_summary() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let cache_root = temp.path().join("arx");
    let metadata_path = write_cached_metadata(
        &cache_root,
        "2403.12345",
        metadata_json(
            "2403.12345",
            "Blocking cached paper",
            &["Blocking Author"],
            &["cs.HC"],
        ),
    )?;

    let output = arx_command(temp.path())
        .env("ARXD_WORKER_HOLD_MS", "150")
        .env("ARXD_IDLE_SHUTDOWN_MS", "1000")
        .args([
            "fetch",
            "2403.12345",
            "--include-pdf=false",
            "--include-source=false",
        ])
        .output()?;

    assert_success(&output);
    assert!(
        serde_json::from_slice::<Value>(&output.stdout).is_err(),
        "default fetch stdout should be interactive text, not JSON"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("fetched"),
        "stdout should confirm fetch: {stdout}"
    );
    assert!(
        stdout.contains(&metadata_path.display().to_string()),
        "stdout should show metadata path: {stdout}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("queued"),
        "stderr should show progress: {stderr}"
    );

    let db_path = cache_root.join("metadata.sqlite3");
    let connection = Connection::open(&db_path)?;
    let title: String = connection.query_row(
        "SELECT title FROM papers WHERE arxiv_id = ?1",
        ["2403.12345"],
        |row| row.get(0),
    )?;
    assert_eq!(title, "Blocking cached paper");
    Ok(())
}

#[test]
fn fetch_without_detach_accepts_multiple_arxiv_ids_and_prints_summaries()
-> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let cache_root = temp.path().join("arx");
    for arxiv_id in ["2405.00001", "2405.00002"] {
        write_cached_metadata(
            &cache_root,
            arxiv_id,
            metadata_json(
                arxiv_id,
                &format!("Blocking multi paper {arxiv_id}"),
                &["Blocking Multi Author"],
                &["cs.IR"],
            ),
        )?;
    }

    let output = arx_command(temp.path())
        .env("ARXD_WORKER_HOLD_MS", "100")
        .env("ARXD_IDLE_SHUTDOWN_MS", "1000")
        .args([
            "fetch",
            "2405.00001",
            "2405.00002",
            "--include-pdf=false",
            "--include-source=false",
        ])
        .output()?;

    assert_success(&output);
    assert!(
        serde_json::from_slice::<Value>(&output.stdout).is_err(),
        "default multi fetch stdout should be interactive text, not JSON"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("2405.00001"),
        "stdout should include first id: {stdout}"
    );
    assert!(
        stdout.contains("2405.00002"),
        "stdout should include second id: {stdout}"
    );
    assert_eq!(stdout.matches("fetched").count(), 2);

    let connection = Connection::open(cache_root.join("metadata.sqlite3"))?;
    let indexed_count: i64 =
        connection.query_row("SELECT COUNT(*) FROM papers", [], |row| row.get(0))?;
    assert_eq!(indexed_count, 2);
    Ok(())
}

#[test]
fn concurrent_fetch_commands_share_one_arxd_queue_and_allows_cached_jobs_to_overlap()
-> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let cache_root = temp.path().join("arx");
    let arxiv_ids = ["2401.00001", "2401.00002", "2401.00003"];
    for arxiv_id in arxiv_ids {
        write_cached_metadata(
            &cache_root,
            arxiv_id,
            metadata_json(
                arxiv_id,
                &format!("Cached paper {arxiv_id}"),
                &["A. Author"],
                &["cs.AI"],
            ),
        )?;
    }

    let mut children = Vec::new();
    for arxiv_id in arxiv_ids {
        let child = arx_command(temp.path())
            .env("ARXD_WORKER_HOLD_MS", "1000")
            .args([
                "--json",
                "fetch",
                arxiv_id,
                "--include-pdf=false",
                "--include-source=false",
                "--detach",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        children.push(child);
    }

    let mut job_ids = Vec::new();
    for child in children {
        let output = child.wait_with_output()?;
        assert_success(&output);
        let queued: Value = serde_json::from_slice(&output.stdout)?;
        job_ids.push(
            queued["job_id"]
                .as_str()
                .ok_or("queued response should contain a job id")?
                .to_string(),
        );
    }
    job_ids.sort();
    job_ids.dedup();
    assert_eq!(job_ids.len(), 3);

    let pressure = wait_for_queue_pressure(temp.path(), 0, arxiv_ids.len() as u64)?;
    assert!(
        pressure["max_active_workers"]
            .as_u64()
            .is_some_and(|workers| workers >= arxiv_ids.len() as u64),
        "daemon should allow every cached metadata-only job to run concurrently: {pressure}"
    );
    assert_eq!(pressure["queued_count"], 0);
    assert_eq!(pressure["in_progress_count"], arxiv_ids.len() as u64);

    let in_progress_ids = pressure["jobs"]
        .as_array()
        .ok_or("queue status jobs should be an array")?
        .iter()
        .filter(|job| job["status"] == "in_progress")
        .map(|job| {
            job["job_id"]
                .as_str()
                .ok_or("in-progress job should contain a job id")
                .map(ToOwned::to_owned)
        })
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(in_progress_ids.len(), arxiv_ids.len());
    for job_id in &job_ids {
        assert!(
            in_progress_ids.contains(job_id),
            "queued job {job_id} should be in progress while workers are held: {pressure}"
        );
    }

    for job_id in &job_ids {
        let completed = wait_for_completed_job(temp.path(), job_id)?;
        assert_eq!(completed["status"], "completed");
    }

    wait_for_daemon_state_removed(&cache_root)?;
    let output = arx_command(temp.path())
        .args(["--json", "queue-status"])
        .output()?;
    assert_success(&output);
    let restarted: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(restarted["queued_count"], 0);
    assert_eq!(restarted["in_progress_count"], 0);
    assert!(cache_root.join("arxd.json").exists());
    Ok(())
}

#[test]
fn legacy_walk_fetch_until_year_options_are_rejected_by_cli_parser() -> Result<(), Box<dyn Error>> {
    for legacy_option in ["--walk-until-year", "--fetch-until-year"] {
        let temp = tempdir()?;
        let output = arx_command(temp.path())
            .args([
                "fetch",
                "2401.12345",
                legacy_option,
                "2020",
                "--include-pdf=false",
                "--include-source=false",
            ])
            .output()?;

        assert!(
            !output.status.success(),
            "{legacy_option} should be rejected, stdout: {}, stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(legacy_option),
            "stderr should name rejected option {legacy_option}, got: {stderr}"
        );
        assert!(
            !temp.path().join("arx").join("metadata.sqlite3").exists(),
            "parser rejection should happen before the command opens the metadata database"
        );
    }
    Ok(())
}

fn arx_command(xdg_cache_home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_arx"));
    command
        .env("XDG_CACHE_HOME", xdg_cache_home)
        .env("ARXD_BIN", arxd_binary())
        .env("ARXD_IDLE_SHUTDOWN_MS", "250")
        .env_remove("ARX_CACHE_DIR");
    command
}

fn arxd_binary() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_arxd") {
        return PathBuf::from(path);
    }
    let arx_bin = PathBuf::from(env!("CARGO_BIN_EXE_arx"));
    let parent = arx_bin
        .parent()
        .expect("arx binary path should have a parent directory");
    let debug_dir = if parent.file_name().is_some_and(|name| name == "deps") {
        parent
            .parent()
            .expect("target/debug/deps should have target/debug parent")
    } else {
        parent
    };
    debug_dir.join(format!("arxd{}", std::env::consts::EXE_SUFFIX))
}

fn wait_for_completed_job(xdg_cache_home: &Path, job_id: &str) -> Result<Value, Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let output = arx_command(xdg_cache_home)
            .args(["--json", "queue-status", "--job-id", job_id])
            .output()?;
        assert_success(&output);
        let response: Value = serde_json::from_slice(&output.stdout)?;
        if let Some(job) = response["jobs"].as_array().and_then(|jobs| jobs.first()) {
            match job["status"].as_str() {
                Some("completed") => return Ok(job.clone()),
                Some("failed") => return Err(format!("job failed: {job}").into()),
                _ => {}
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for job {job_id}").into());
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_queue_pressure(
    xdg_cache_home: &Path,
    queued_count: u64,
    in_progress_count: u64,
) -> Result<Value, Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let output = arx_command(xdg_cache_home)
            .args(["--json", "queue-status"])
            .output()?;
        assert_success(&output);
        let response: Value = serde_json::from_slice(&output.stdout)?;
        if response["queued_count"] == queued_count
            && response["in_progress_count"] == in_progress_count
        {
            return Ok(response);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for queue pressure queued={queued_count} in_progress={in_progress_count}; last response: {response}"
            )
            .into());
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_daemon_state_removed(cache_root: &Path) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let state_path = cache_root.join("arxd.json");
    while state_path.exists() {
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for {} to be removed",
                state_path.display()
            )
            .into());
        }
        thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

fn write_cached_metadata(
    cache_root: &Path,
    safe_id: &str,
    metadata: Value,
) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let paper_dir = cache_root.join("papers").join(safe_id);
    fs::create_dir_all(&paper_dir)?;
    let metadata_path = paper_dir.join("metadata.json");
    fs::write(&metadata_path, serde_json::to_vec_pretty(&metadata)?)?;
    Ok(metadata_path)
}

fn write_cached_source_material(cache_root: &Path, safe_id: &str) -> Result<(), Box<dyn Error>> {
    let paper_dir = cache_root.join("papers").join(safe_id);
    let source_dir = paper_dir.join("source");
    let extracted_dir = source_dir.join("extracted");
    fs::create_dir_all(&extracted_dir)?;
    let source_file = extracted_dir.join("main.tex");
    fs::write(
        &source_file,
        "A cached source line cites arXiv:2101.00001 for offline lookup.\n",
    )?;
    let archive_path = source_dir.join("e-print.tar");
    fs::write(&archive_path, b"local deterministic source archive fixture")?;
    fs::write(
        source_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&json!({
            "source_archive_path": archive_path.display().to_string(),
            "source_extracted_dir": extracted_dir.display().to_string(),
        }))?,
    )?;
    fs::write(
        paper_dir.join("citations.jsonl"),
        serde_json::to_string(&json!({
            "citing_arxiv_id": safe_id.replace('_', "/"),
            "cited_arxiv_id": "2101.00001",
            "source_file": source_file.display().to_string(),
            "line": 1,
            "context": "A cached source line cites arXiv:2101.00001 for offline lookup."
        }))? + "\n",
    )?;
    Ok(())
}

fn metadata_json(arxiv_id: &str, title: &str, authors: &[&str], categories: &[&str]) -> Value {
    json!({
        "arxiv_id": arxiv_id,
        "abs_url": format!("https://arxiv.org/abs/{arxiv_id}"),
        "pdf_url": format!("https://arxiv.org/pdf/{arxiv_id}"),
        "title": title,
        "authors": authors,
        "summary": "Synthetic cached metadata used by offline CLI tests.",
        "published": "2024-01-01T00:00:00Z",
        "updated": "2024-01-02T00:00:00Z",
        "categories": categories,
    })
}

fn path_value(path: &Path) -> Value {
    Value::String(path.display().to_string())
}

fn assert_success(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "command failed, stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
