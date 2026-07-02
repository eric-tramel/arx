# arx Hermes Plugin

This plugin gives Hermes the same arx guidance that the Codex, Claude Code, Pi, and Oh My Pi plugin root bundles, while keeping paper operations in the `arx-mcp` MCP server.

Install from this repository:

```bash
hermes plugins install eric-tramel/arx/plugins/hermes-arx --enable
hermes arx setup
hermes mcp test arx
```

What it adds:

- `arx:arx-paper-metadata`, `arx:arx-paper-fetch`, and `arx:arx-full-text-search` skills.
- `hermes arx setup`, which registers the bundled launcher as `mcp_servers.arx`.

The plugin does not install `arx`. It expects `arx-mcp` and `arxd` installed on `PATH` from the same install directory.

After setup, run `hermes mcp test arx` when you want a live MCP connection check.
