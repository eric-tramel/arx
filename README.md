# arx

`arx` is a local arXiv cache with two separately runnable surfaces:

- `arx`: a standalone core CLI for fetching, locating, and inspecting cached arXiv papers without any agent or MCP runtime.
- `arx-mcp`: a stdio MCP server that exposes the same core fetcher to agents.

The shared core downloads arXiv metadata, PDF, and TeX/source files into an XDG cache and records arXiv-to-arXiv citations discovered in the source as JSONL.

The cache is safe for multi-agent use on one machine: arXiv network requests are serialized through a shared filesystem lock and a shared rate-limit state file, so separate CLI and MCP server processes respect the same delay.

## Workspace layout

```text
crates/
  arx-core/  # arXiv fetching, cache paths, source extraction, citations, rate limiting
  arx-cli/   # standalone `arx` binary; no MCP dependency
  arx-mcp/   # stdio `arx-mcp` MCP server and MCP setup helpers
```

## Features

- Standalone CLI:
  - `arx fetch <ID>`
  - `arx locate <ID>`
  - `arx cache-dir`
- MCP stdio server with two tools:
  - `fetch_arxiv_paper`: download/cache a paper and return paths.
  - `locate_cached_arxiv_paper`: return cached paths without network access.
- Downloads arXiv metadata, PDF, and source/e-print bundle.
- Extracts source archives when possible.
- Writes arXiv-to-arXiv citations discovered in source/BibTeX files to `citations.jsonl`.
- Caches under XDG cache directories for future hits.
- Enforces a cross-process 3 second delay between arXiv requests.
- Includes MCP setup helpers in `arx-mcp` so harnesses can launch the server directly from config.

## Install

Recommended install from the latest GitHub release:

```bash
curl -fsSL https://raw.githubusercontent.com/eric-tramel/arx/main/install.sh | sh
```

Windows PowerShell:

```powershell
irm https://raw.githubusercontent.com/eric-tramel/arx/main/install.ps1 | iex
```

The installers download the matching release archive and install both binaries: `arx` and `arx-mcp`.

Override defaults:

```bash
ARX_VERSION=v0.1.0 ARX_INSTALL_DIR=$HOME/bin sh install.sh
```

```powershell
$env:ARX_VERSION = "v0.1.0"
$env:ARX_INSTALL_DIR = "$HOME\\bin"
.\\install.ps1
```

Install both binaries from a checkout instead:

```bash
cargo install --path crates/arx-cli
cargo install --path crates/arx-mcp
```

Or run from the workspace:

```bash
cargo run -p arx -- fetch 0704.0001
cargo run -p arx-mcp -- serve
```

`arx-mcp serve` is the default MCP command, so an MCP client can launch `arx-mcp` with no arguments or with `serve`.

## Standalone CLI usage

Fetch a paper outside MCP:

```bash
arx fetch 0704.0001
```

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

Fetch output is structured JSON:

```json
{
  "arxiv_id": "0704.0001",
  "cache_dir": "/home/user/.cache/arx/papers/0704.0001",
  "metadata_path": "/home/user/.cache/arx/papers/0704.0001/metadata.json",
  "pdf_path": "/home/user/.cache/arx/papers/0704.0001/paper.pdf",
  "source_archive_path": "/home/user/.cache/arx/papers/0704.0001/source/e-print.tar.gz",
  "source_extracted_dir": "/home/user/.cache/arx/papers/0704.0001/source/extracted",
  "citations_jsonl_path": "/home/user/.cache/arx/papers/0704.0001/citations.jsonl",
  "cache_hit": false,
  "network_requests": 3,
  "rate_limit_delay_seconds": 3
}
```

## MCP usage

Run the MCP server directly:

```bash
arx-mcp serve
```

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

## Cache layout

Cache root resolution:

1. `$ARX_CACHE_DIR`, when set.
2. `$XDG_CACHE_HOME/arx`, when set.
3. `~/.cache/arx`.

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

Distribution strategy: GitHub Releases plus tiny `install.sh` and `install.ps1` bootstrap installers. This keeps install friction low across Linux/macOS/Windows without adding package-manager infrastructure before there is demand. Homebrew/Scoop/cargo-dist can be layered on later using the same release archives.

Create a release:

```bash
git tag v0.1.0
git push origin v0.1.0
```

## Development

```bash
cargo fmt
cargo test --workspace
cargo run -p arx -- locate 0704.0001
cargo run -p arx-mcp -- print-config
```

MCP smoke testing can be done with any MCP client that supports stdio servers, or by using the generated config from `arx-mcp print-config`.

## arXiv use

This tool stores arXiv content locally for personal/research use. Respect arXiv's API terms and copyright limitations when using or redistributing downloaded content.

Relevant arXiv API terms: <https://info.arxiv.org/help/api/tou.html>

## License

MIT
