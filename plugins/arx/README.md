# arx Agent Plugin

This plugin vendors Agent Skills and MCP configuration for using `arx` from Codex, Claude Code, Pi, Oh My Pi, and compatible harnesses.

## Requirements

- Unix, macOS, or Linux for plugin-managed MCP launch.
- `arx-mcp` and `arxd` installed on `PATH` from the same install directory.
- Installed binaries must be outside the current project directory and outside Git worktrees. This prevents a harness from accidentally running a local checkout binary.

Windows users can still configure MCP manually with `arx-mcp print-config`.

This plugin does not install `arx`; install the binaries first with Homebrew, a release installer, or `cargo install`.

## Skills

- `arx-paper-metadata`: inspect arXiv metadata, abstracts, readiness, and cache paths.
- `arx-paper-fetch`: queue missing PDF/source material through `arxd` and inspect durable job status.
- `arx-full-text-search`: search cached local paper text with `full_text_search`.

## MCP

The plugin configures one MCP server named `arx`. The launcher resolves both `arx-mcp` and `arxd`, requires them to come from the same install directory, exports `ARXD_BIN`, then runs:

```sh
arx-mcp serve
```

The MCP server exposes the existing four arx tools:

- `lookup_arxiv_papers`
- `fetch_arxiv_paper`
- `get_arxiv_download_queue_status`
- `full_text_search`

## Installation

Use `/path/to/arx` as the path to the repository checkout that contains this plugin. It is not a path to an `arx-mcp` or `arxd` binary.

Codex:

```sh
codex plugin marketplace add /path/to/arx
codex plugin add arx@arx
```

Claude Code:

```sh
claude plugin marketplace add /path/to/arx --scope local
claude plugin install arx@arx --scope local
```

Pi:

```sh
pi install /path/to/arx/plugins/arx -l --approve
```

Oh My Pi:

```sh
cd /path/to/arx
omp plugin link ./plugins/arx --local
```
