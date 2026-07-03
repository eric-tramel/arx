# arx Hermes Setup

Run:

```bash
hermes arx setup
hermes mcp test arx
```

The setup command registers `arx-mcp serve` as `mcp_servers.arx`. Make sure the harness can find `arx-mcp` on `PATH`; `arx-mcp` will resolve `arxd` from `ARXD_BIN`, a sibling install, or `PATH`.
