# arx Agent Plugin

This plugin vendors Agent Skills and MCP configuration for using `arx` from Codex, Claude Code, Pi, Oh My Pi, and compatible harnesses.

## Requirements

- `arx-mcp` installed on the agent harness `PATH`.
- `arxd` installed where `arx-mcp` can find it: `ARXD_BIN`, a sibling `arxd` next to `arx-mcp`, or `arxd` on `PATH`.

If a harness cannot inherit your shell `PATH`, configure MCP manually with the absolute-path snippet from `arx-mcp print-config`.

This plugin does not install `arx`; install the binaries first with Homebrew, a release installer, or `cargo install`.

## Skills

- `arx-paper-metadata`: inspect arXiv metadata, abstracts, readiness, and cache paths.
- `arx-paper-fetch`: queue missing PDF/source material through `arxd` and inspect durable job status.
- `arx-full-text-search`: search cached local paper text with `full_text_search`.

## MCP

The plugin configures one MCP server named `arx` and launches:

```sh
arx-mcp serve
```

The MCP server exposes the existing four arx tools:

- `lookup_arxiv_papers`
- `fetch_arxiv_paper`
- `get_arxiv_download_queue_status`
- `full_text_search`

## Installation

Install `arx-mcp` and `arxd` first, then install this plugin from GitHub. You do not need an `arx` source checkout.

Codex:

```sh
codex plugin marketplace add eric-tramel/arx --sparse .agents --sparse plugins/arx
codex plugin add arx@arx
```

Claude Code:

```sh
claude plugin marketplace add eric-tramel/arx --scope user --sparse .claude-plugin plugins/arx
claude plugin install arx@arx --scope user
```

Pi:

```sh
pi install git:github.com/eric-tramel/arx --approve
```

Oh My Pi:

```sh
omp plugin install github:eric-tramel/arx
```

Hermes:

```sh
hermes plugins install eric-tramel/arx/plugins/arx --enable
hermes arx setup
hermes mcp test arx
```
