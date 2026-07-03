# arx

`arx` is a local arXiv cache with a daemon-backed architecture:

- `arxd`: the single local backend daemon. It owns the download queue, metadata indexing, SQLite writes, and arXiv rate limiting. If it is not running, frontends start it; when its queue is idle it shuts down after a short grace period.
- `arx`: a standalone CLI frontend for queuing downloads, indexing cached metadata, locating cached files, and inspecting daemon status.
- `arx-mcp`: a stdio MCP frontend that exposes the same daemon-backed operations to agents.

The shared core downloads arXiv metadata, PDF, and TeX/source files into an XDG cache, records arXiv-to-arXiv citations discovered in the source as JSONL, and stores indexed metadata in a SQLite database under the cache root.

The cache is safe for multi-agent use on one machine: arxd is the only queue manager for a cache root, and arXiv network requests are serialized through the existing shared filesystem lock and rate-limit state.

## Workspace layout

```text
crates/
  arx-core/  # arXiv fetching, cache paths, daemon protocol, source extraction, citations, rate limiting
  arxd/      # local backend daemon; owns queue and metadata database updates
  arx-cli/   # standalone `arx` frontend; boots arxd when needed
  arx-mcp/   # stdio `arx-mcp` MCP frontend and MCP setup helpers
```

## Features

- Standalone CLI:
  - `arx fetch <ID>`
  - `arx lookup <ID>`
  - `arx search <QUERY>`
  - `arx index`
  - `arx locate <ID>`
  - `arx cache-dir`
  - `arx queue-status`
- Local backend daemon:
  - `arxd serve`: owns the per-cache-root queue and database updates; frontends start it automatically.
- MCP stdio server with four tools:
  - `lookup_arxiv_papers`: metadata, abstract, local material readiness, and cache paths; fetches only missing metadata.
  - `full_text_search`: BM25-ranked search over cached material for all papers, or one paper via `arxiv_id`. Scope controls which content is searched: `default` (title+metadata+body, no bibliography), `titles`, `bibliography`, `all`. Empty results include a `note` explaining why.
  - `fetch_arxiv_paper`: ask arxd to queue/cache a paper (PDF/source) and return a job id immediately.
  - `get_arxiv_download_queue_status`: inspect queued, active, completed, and failed downloads.
- Downloads arXiv metadata, PDF, and source/e-print bundle.
- Extracts source archives when possible.
- Writes arXiv-to-arXiv citations discovered in source/BibTeX files to `citations.jsonl`.
- Caches under XDG cache directories for future hits.
- Stores indexed metadata in `<cache-root>/metadata.sqlite3`.
- Keeps arxd connection state in `<cache-root>/arxd.json` and the daemon singleton lock in `<cache-root>/arxd.lock`.
- Enforces a cross-process 3 second delay between arXiv requests.
- Includes MCP setup helpers in `arx-mcp` so harnesses can launch the frontend directly from config.
- All shipped command-line binaries (`arx`, `arxd`, and `arx-mcp`) support `--version` / `-v`.

## Install

Homebrew (macOS and Linux):

```bash
brew install eric-tramel/tap/arx
```

This installs `arx`, `arxd`, and `arx-mcp` from
[eric-tramel/homebrew-tap](https://github.com/eric-tramel/homebrew-tap).

Or install from the latest GitHub release:

```bash
curl -fsSL https://raw.githubusercontent.com/eric-tramel/arx/main/install.sh | sh
```

Windows PowerShell:

```powershell
irm https://raw.githubusercontent.com/eric-tramel/arx/main/install.ps1 | iex
```

The installers download the matching release archive and install `arx`, `arxd`, and `arx-mcp`.

Override defaults:

```bash
ARX_VERSION=v0.1.1 ARX_INSTALL_DIR=$HOME/bin sh install.sh
```

```powershell
$env:ARX_VERSION = "v0.1.1"
$env:ARX_INSTALL_DIR = "$HOME\\bin"
.\\install.ps1
```

Install all binaries from a checkout instead:

```bash
cargo install --path crates/arx-cli
cargo install --path crates/arx-mcp
cargo install --path crates/arxd
```

Or run from the workspace:

```bash
cargo run -p arx -- fetch 0704.0001
cargo run -p arx-mcp -- serve
```

Local development builds can reuse compilation artifacts across worktrees by
running `rustc` through `sccache`. Configure it per-user rather than in this
repository, so source builds (including the Homebrew formula) work without
sccache installed:

```toml
# ~/.cargo/config.toml
[build]
rustc-wrapper = "sccache"
```

GitHub Actions sets `RUSTC_WRAPPER=sccache` for release/test jobs.

`arx-mcp serve` is the default MCP command, so an MCP client can launch `arx-mcp` with no arguments or with `serve`. `arx-mcp` starts `arxd` when a tool needs daemon work.

## Standalone CLI usage

Claim metadata/status first. `lookup` never downloads PDF/source material; by default it fetches only missing Atom metadata and returns local readiness for PDF/source/citations/search material.

```bash
arx lookup 0704.0001
arx lookup 0704.0001 0704.0002
arx lookup 0704.0001 --local-only
```

Fetch a paper outside MCP. By default this is interactive: it waits for arxd to finish, shows progress, then prints a human-readable summary.

```bash
arx fetch 0704.0001
```

Fetch multiple papers in one command:

```bash
arx fetch 0704.0001 0704.0002 hep-th/9901001
```

Queue and return immediately:

```bash
arx fetch 0704.0001 --detach
arx fetch 0704.0001 -d
arx fetch 0704.0001 0704.0002 --detach
```

Check daemon queue status:

```bash
arx queue-status
arx queue-status --job-id download-1
```

Machine-readable output is opt-in with `--json`/`-j`:

```bash
arx --json lookup 0704.0001
arx --json lookup 0704.0001 --local-only
arx --json fetch 0704.0001
arx -j fetch 0704.0001 --detach
arx -j fetch 0704.0001 0704.0002 --detach
arx --json queue-status --job-id download-1
```

Index cached metadata into the local database. This also rebuilds the
persistent full-text search index (a [Tantivy](https://github.com/quickwit-oss/tantivy)
index at `<cache-dir>/search-index/` covering metadata, citations, and
TeX/source paragraphs) used by `arx search`:

```bash
arx index
```

The search index is derived data: arx wipes and rebuilds it automatically
on version mismatch or corruption, and `arx index` regenerates it from the
paper cache at any time. arxd is the only writer; the CLI and MCP server
open it read-only per query.

BM25-ranked free-text search across all locally cached paper material,
without network access:

```bash
arx search reward model training
arx search "mixture of experts" --limit 5
arx search calibration --arxiv-id 0704.0001
arx search attention --scope titles          # title-only (find papers by name)
arx search vaswani --scope bibliography      # search citation records and .bib files
```

By default the search scope is `default`, which covers titles, metadata
(abstract, authors, categories), and TeX/source body paragraphs. Bibliography
content (.bib, .bbl files and citation records) is **excluded** from the
default scope to prevent hits in one paper's reference list from polluting
searches for papers *about* a topic. Use `--scope bibliography` to search
bibliography content explicitly, or `--scope all` for everything. When results
are empty, the `note` field explains why and what to do next (e.g. fetch the
TeX source to index the body).

Locate cached files without network access:

```bash
arx locate 0704.0001
```

Print the cache directory:

```bash
arx cache-dir
```

Refresh existing cached files:

```bash
arx fetch 0704.0001 --refresh
```

Skip PDF or source downloads:

```bash
arx fetch 0704.0001 --include-pdf=false
arx fetch 0704.0001 --include-source=false
```

JSON fetch output depends on mode. Blocking JSON mode prints the final fetched paper response after arxd completes:

```json
{
  "arxiv_id": "0704.0001",
  "cache_dir": "/home/user/.cache/arx/papers/0704.0001",
  "metadata_path": "/home/user/.cache/arx/papers/0704.0001/metadata.json",
  "metadata_db_path": "/home/user/.cache/arx/metadata.sqlite3",
  "indexed_metadata_records": 12,
  "pdf_path": "/home/user/.cache/arx/papers/0704.0001/paper.pdf",
  "source_archive_path": "/home/user/.cache/arx/papers/0704.0001/source/e-print.tar.gz",
  "source_extracted_dir": "/home/user/.cache/arx/papers/0704.0001/source/extracted",
  "citations_jsonl_path": "/home/user/.cache/arx/papers/0704.0001/citations.jsonl",
  "title": "A cached arXiv paper",
  "authors": ["A. Author"],
  "citation_count": 5,
  "cache_hit": false,
  "network_requests": 3,
  "rate_limit_delay_seconds": 3
}
```

Detached JSON mode prints the queued daemon job:

```json
{
  "job_id": "download-1",
  "arxiv_id": "0704.0001",
  "status": "queued",
  "queue_position": 1,
  "queued_at_unix_ms": 1782864000000,
  "status_tool": "get_arxiv_download_queue_status",
  "message": "queued arXiv download; call get_arxiv_download_queue_status with this job_id to check progress"
}
```

When more than one arXiv id is requested with `--json`, fetch and lookup output are JSON arrays in the same order as the requested ids. `lookup` entries are metadata-first paper objects with per-paper `material_state`, cached `metadata` when available, `available_now`, `missing`, and a `next_tool` hint.

## MCP usage

Run the MCP server directly:

```bash
arx-mcp serve
```

The MCP server publishes exactly four tools to keep the agent-facing surface small. Start with `lookup_arxiv_papers` to get a stable local paper object, cached metadata/abstract, cache paths, and explicit material readiness without fetching PDF/source. Use `full_text_search` for BM25-ranked snippets over cached metadata/citations/extracted TeX — across every cached paper by default, or scoped to one paper with `arxiv_id`; the search index maintains itself (updated on fetch, self-healing on first search), so agents never manage indexing. Use `fetch_arxiv_paper` to queue PDF/source acquisition and `get_arxiv_download_queue_status` to inspect daemon jobs. Cache maintenance (`arx index`, `arx locate`) lives in the CLI, not the MCP surface.

Print a ready-to-copy MCP config snippet:

```bash
arx-mcp print-config
```

Example shape:

```json
{
  "mcpServers": {
    "arx": {
      "command": "/path/to/arx-mcp",
      "args": ["serve"]
    }
  }
}
```

For Claude Desktop, install or update the `arx` MCP entry directly:

```bash
arx-mcp install-claude-desktop
```

To write a specific config file instead of the platform default:

```bash
arx-mcp install-claude-desktop --config-path /path/to/claude_desktop_config.json
```

## Agent plugin installation

The agent plugins do not install `arx`. Install `arx`, `arxd`, and `arx-mcp` first with Homebrew, a release installer, or `cargo install`, then make sure `arx-mcp` and `arxd` are on `PATH` from the same install directory.

The plugin-managed MCP launcher is supported on Unix, macOS, and Linux. It resolves installed `arx-mcp` and `arxd` binaries from `PATH`, exports `ARXD_BIN`, and runs `arx-mcp serve`. It rejects binaries from the current project directory or any Git worktree so an agent harness does not accidentally launch a checkout build such as `target/debug/arx-mcp`.

Use `/path/to/arx` below as the path to this repository checkout, not a path to an `arx` binary.

Codex:

```bash
codex plugin marketplace add /path/to/arx
codex plugin add arx@arx
```

Claude Code:

```bash
claude plugin marketplace add /path/to/arx --scope local
claude plugin install arx@arx --scope local
```

Pi:

```bash
pi install /path/to/arx/plugins/arx -l --approve
```

Oh My Pi:

```bash
cd /path/to/arx
omp plugin link ./plugins/arx --local
```

Hermes:

```bash
hermes plugins install eric-tramel/arx/plugins/arx --enable
hermes arx setup
hermes mcp test arx
```

`plugins/hermes-arx` remains a symlink alias to the same plugin for older install notes, but new installs should target `plugins/arx`.

Windows users should configure MCP manually with `arx-mcp print-config`; the plugin launcher is POSIX shell based.

## Cache layout

Cache root resolution:

1. `$ARX_CACHE_DIR`, when set.
2. `$XDG_CACHE_HOME/arx`, when set.
3. `~/.cache/arx`.

The metadata index database is stored at:

```text
<cache-root>/metadata.sqlite3
```

Run `arx index` to ask arxd to scan existing `metadata.json` files into the database. `arx fetch` queues the fetch in arxd and, unless `--detach` is passed, waits while showing interactive progress. arxd runs the same index step before fetching a paper, then writes the fetched paper's metadata into the database.

arxd state and log files live at:

```text
<cache-root>/arxd.json
<cache-root>/arxd.lock
<cache-root>/arxd.log
```

Paper files are stored under:

```text
<cache-root>/papers/<safe-arxiv-id>/
  metadata.json
  paper.pdf
  citations.jsonl
  source/
    manifest.json
    e-print.tar.gz
    extracted/
```

Old-style arXiv IDs such as `hep-th/9901001` are path-sanitized, e.g. `hep-th_9901001`.

## Rate limiting

arXiv asks API clients to make no more than one request every three seconds. `arx-core` enforces that with shared files in the cache root:

```text
<cache-root>/arxiv-rate-limit.lock
<cache-root>/arxiv-rate-limit.json
```

Every metadata, PDF, or source request must acquire the same lock and update the same next-allowed timestamp. This keeps the delay consistent across multiple agents, standalone CLI runs, and multiple MCP server processes on the same system.

## Citations JSONL

When source files are fetched, `arx-core` scans text source files for arXiv references such as:

- `arXiv:2401.12345`
- `https://arxiv.org/abs/2401.12345`
- `https://arxiv.org/pdf/hep-th/9901001`
- BibTeX `archivePrefix = {arXiv}` with `eprint = {...}`

Each discovered non-self arXiv citation is written once to `citations.jsonl`:

```jsonl
{"citing_arxiv_id":"2401.12345v2","cited_arxiv_id":"2101.00001v2","source_file":".../main.tex","line":12,"context":"See arXiv:2101.00001v2."}
```

## Release CI and distribution

GitHub Actions builds release binaries on every push to `main` and every `v*` version tag:

- Linux x86_64: `arx-x86_64-unknown-linux-gnu.tar.gz`
- macOS arm64: `arx-aarch64-apple-darwin.tar.gz`
- Windows x86_64: `arx-x86_64-pc-windows-msvc.zip`

Pushes to `main` produce workflow artifacts for verification. Version tags publish the same archives and `.sha256` files to GitHub Releases.

Distribution strategy: GitHub Releases plus tiny `install.sh` and `install.ps1` bootstrap installers. Release archives include `arx`, `arxd`, and `arx-mcp` together so frontends can boot the daemon from the same install directory. The Homebrew tap consumes the same tagged source and release flow; Scoop/cargo-dist can be layered on later using the same release archives.

Create a release:

```bash
git tag v0.1.1
git push origin v0.1.1
```

## Development

```bash
cargo fmt
cargo test --workspace
cargo run -p arx -- locate 0704.0001
cargo run -p arx-mcp -- print-config
cargo run -p arxd -- serve
```

MCP smoke testing can be done with any MCP client that supports stdio servers, or by using the generated config from `arx-mcp print-config`.

## arXiv use

This tool stores arXiv content locally for personal/research use. Respect arXiv's API terms and copyright limitations when using or redistributing downloaded content.

Relevant arXiv API terms: <https://info.arxiv.org/help/api/tou.html>

## License

MIT
