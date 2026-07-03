# arx Hermes Setup

Run:

```bash
hermes arx setup
hermes mcp test arx
```

The setup command registers the bundled launcher as `mcp_servers.arx`. The launcher resolves `arx-mcp` and `arxd` from installed binaries on `PATH`, exports `ARXD_BIN`, and starts `arx-mcp serve`.
