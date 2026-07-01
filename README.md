# arx

`arx` is a local stdio MCP server for fetching arXiv papers by arXiv ID. It downloads metadata, PDF, and TeX/source files into an XDG cache and returns the local file paths to agents through MCP tools.

The server is built for multi-agent use on one machine: arXiv network requests are serialized through a shared filesystem lock and a shared rate-limit state file so separate MCP server processes still respect the same delay.

## Features

- MCP stdio server with two tools:
  - `fetch_arxiv_paper`: download/cache a paper and return paths.
  - `locate_cached_arxiv_paper`: return cached paths without network access.
- Downloads arXiv metadata, PDF, and source/e-print bundle.
- Extracts source archives when possible.
- Writes arXiv-to-arXiv citations discovered in source/BibTeX files to `citations.jsonl`.
- Caches under XDG cache directories for future hits.
- Enforces a cross-process 3 second delay between arXiv requests.
- Includes setup helpers so harnesses can launch the server directly from MCP configuration.

## Install

```bash
cargo install --path .
```

Or run from a checkout:

```bash
cargo run -- serve
```

`serve` is the default subcommand, so an MCP client can launch the binary with no arguments or with `serve`.

## MCP configuration

Print a ready-to-copy config snippet:

```bash
arx print-config
```

Example shape:

```json
{
  "mcpServers": {
    "arx": {
      "command": "/path/to/arx",
      "args": ["serve"]
    }
  }
}
```

For Claude Desktop, install or update the `arx` MCP entry directly:

```bash
arx install-claude-desktop
```

To write a specific config file instead of the platform default:

```bash
arx install-claude-desktop --config-path /path/to/claude_desktop_config.json
```

## CLI usage

Fetch a paper outside MCP:

```bash
arx fetch 0704.0001
```

Fetch output is the same structured JSON returned by the MCP tool:

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

arXiv asks API clients to make no more than one request every three seconds. `arx` enforces that with shared files in the cache root:

```text
<cache-root>/arxiv-rate-limit.lock
<cache-root>/arxiv-rate-limit.json
```

Every metadata, PDF, or source request must acquire the same lock and update the same next-allowed timestamp. This keeps the delay consistent across multiple agents and multiple MCP server processes on the same system.

## Citations JSONL

When source files are fetched, `arx` scans text source files for arXiv references such as:

- `arXiv:2401.12345`
- `https://arxiv.org/abs/2401.12345`
- `https://arxiv.org/pdf/hep-th/9901001`
- BibTeX `archivePrefix = {arXiv}` with `eprint = {...}`

Each discovered non-self arXiv citation is written once to `citations.jsonl`:

```jsonl
{"citing_arxiv_id":"2401.12345v2","cited_arxiv_id":"2101.00001v2","source_file":".../main.tex","line":12,"context":"See arXiv:2101.00001v2."}
```

## Development

```bash
cargo fmt
cargo test
cargo run -- serve
```

MCP smoke testing can be done with any MCP client that supports stdio servers, or by using the generated config from `arx print-config`.

## arXiv use

This tool stores arXiv content locally for personal/research use. Respect arXiv's API terms and copyright limitations when using or redistributing downloaded content.

Relevant arXiv API terms: <https://info.arxiv.org/help/api/tou.html>

## License

MIT
