"""Hermes plugin for arx MCP setup and skills."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

try:
    from hermes_constants import get_hermes_home
except Exception:  # pragma: no cover - Hermes provides this at runtime.
    get_hermes_home = None  # type: ignore[assignment]


PLUGIN_DIR = Path(__file__).resolve().parent
MCP_SERVER_NAME = "arx"
LAUNCHER = PLUGIN_DIR / "scripts" / "launch-arx-mcp.sh"
REQUIRED_MCP_TOOLS = {
    "lookup_arxiv_papers",
    "fetch_arxiv_paper",
    "get_arxiv_download_queue_status",
    "full_text_search",
}
SKILLS = (
    (
        "arx-paper-metadata",
        "Review arXiv metadata, abstracts, readiness, and cache paths with arx.",
    ),
    (
        "arx-paper-fetch",
        "Queue missing arXiv paper material through arxd and track durable job status.",
    ),
    (
        "arx-full-text-search",
        "Search cached arx-indexed paper text with BM25 snippets and scopes.",
    ),
)
TRUTHY_STRINGS = {"1", "true", "yes", "on"}


def register(ctx) -> None:
    """Register Hermes-native arx skills and setup command."""
    for name, description in SKILLS:
        ctx.register_skill(name, PLUGIN_DIR / "skills" / name / "SKILL.md", description)

    ctx.register_cli_command(
        name="arx",
        help="Manage arx MCP integration",
        setup_fn=_setup_cli,
        handler_fn=_handle_cli,
        description="Configure Hermes' arx MCP integration.",
    )


def _setup_cli(parser) -> None:
    subparsers = parser.add_subparsers(dest="arx_action")
    setup = subparsers.add_parser("setup", help="Register arx MCP in this Hermes profile")
    setup.add_argument(
        "--force",
        action="store_true",
        help="Replace any existing arx MCP registration",
    )
    setup.add_argument("--json", action="store_true", help="Print machine-readable JSON")


def _handle_cli(args) -> None:
    action = getattr(args, "arx_action", None) or "setup"
    if action != "setup":
        result = {
            "ok": False,
            "summary": f"Unsupported arx action: {action}",
            "steps": [{"status": "error", "message": "Use `hermes arx setup`."}],
        }
    else:
        result = _setup_mcp(force=bool(getattr(args, "force", False)))
    _print_result(result, as_json=bool(getattr(args, "json", False)))
    raise SystemExit(0 if result["ok"] else 1)


def _setup_mcp(*, force: bool) -> dict[str, Any]:
    steps: list[dict[str, str]] = []
    launcher_issue = _launcher_issue()
    if launcher_issue:
        return {
            "ok": False,
            "summary": "arx MCP setup needs attention.",
            "steps": [{"status": "error", "message": launcher_issue}],
        }

    state = _mcp_config_state()
    if _mcp_registration_ready(state) and not force:
        steps.append({"status": "ok", "message": "arx MCP is already configured."})
    else:
        write = _write_mcp_registration()
        after_write = _mcp_config_state()
        if write["ok"] and _mcp_registration_ready(after_write):
            steps.append({"status": "ok", "message": "Registered arx MCP server."})
        else:
            detail = _join_details(write["summary"], _mcp_registration_problem(after_write))
            steps.append(
                {
                    "status": "error",
                    "message": "Failed to register arx MCP server.",
                    "detail": detail,
                }
            )

    ok = all(step["status"] != "error" for step in steps)
    return {
        "ok": ok,
        "summary": "arx MCP setup complete." if ok else "arx MCP setup needs attention.",
        "steps": steps,
        "next_steps": [
            "Start or restart Hermes so the MCP tool list is refreshed.",
            "Run `hermes mcp test arx` to verify the live MCP connection.",
        ],
    }


def _launcher_issue() -> str:
    if not LAUNCHER.is_file():
        return f"Missing launcher: {LAUNCHER}"
    if not os.access(LAUNCHER, os.X_OK):
        return f"Launcher is not executable: {LAUNCHER}"
    return ""


def _write_mcp_registration() -> dict[str, Any]:
    try:
        config = _load_config_data()
        if not isinstance(config, dict):
            raise ValueError("Hermes config root must be a mapping.")
        servers = config.get("mcp_servers")
        if not isinstance(servers, dict):
            servers = {}
            config["mcp_servers"] = servers
        servers[MCP_SERVER_NAME] = {
            "command": str(LAUNCHER),
            "args": [],
            "enabled": True,
        }
        _save_config_data(config)
        return {"ok": True, "summary": f"file: {_hermes_config_path()} (updated)"}
    except Exception as exc:
        return {"ok": False, "summary": str(exc)}


def _load_config_data() -> dict[str, Any]:
    try:
        from hermes_cli.config import load_config
    except (ImportError, ModuleNotFoundError):
        return _load_json_config()

    config = load_config()
    return config if isinstance(config, dict) else {}


def _save_config_data(config: dict[str, Any]) -> None:
    try:
        from hermes_cli.config import save_config
    except (ImportError, ModuleNotFoundError):
        _save_json_config(config)
        return

    save_config(config)


def _load_json_config() -> dict[str, Any]:
    config_path = _hermes_config_path()
    if not config_path.exists():
        return {}
    text = config_path.read_text(encoding="utf-8").strip()
    if not text:
        return {}
    loaded = json.loads(text)
    if not isinstance(loaded, dict):
        raise ValueError("Hermes config root must be a mapping.")
    return loaded


def _save_json_config(config: dict[str, Any]) -> None:
    config_path = _hermes_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _mcp_config_state() -> dict[str, Any]:
    config_path = _hermes_config_path()
    state = {
        "config_path": str(config_path),
        "config_exists": config_path.exists(),
        "config_error": "",
        "configured": False,
        "enabled": False,
        "expected": False,
        "detail": "",
        "tool_filter_issue": "",
    }
    if not config_path.exists():
        return state

    try:
        data = _load_config_data()
    except Exception as exc:
        state["config_error"] = f"Could not parse Hermes config: {exc}"
        return state

    servers = data.get("mcp_servers") if isinstance(data, dict) else None
    server = servers.get(MCP_SERVER_NAME) if isinstance(servers, dict) else None
    if not isinstance(server, dict):
        return state

    state["configured"] = True
    state["enabled"] = _truthy_value(server.get("enabled"), default=True)
    command = server.get("command")
    args = server.get("args") or []
    args_list = args if isinstance(args, list) else [args]
    normalized_args = [str(arg) for arg in args_list]
    state["expected"] = _is_expected_mcp_launch(command, normalized_args)
    state["detail"] = f"command={command!r} args={normalized_args!r}"
    state["tool_filter_issue"] = _tool_filter_issue(server.get("tools"))
    return state


def _tool_filter_issue(tools: Any) -> str:
    if not isinstance(tools, dict):
        return ""
    include = _string_list(tools.get("include"))
    exclude = _string_list(tools.get("exclude"))
    if include is not None:
        missing = REQUIRED_MCP_TOOLS.difference(include)
        if missing:
            return "arx MCP include filter hides required tools: " + ", ".join(sorted(missing))
    if exclude is not None:
        blocked = REQUIRED_MCP_TOOLS.intersection(exclude)
        if blocked:
            return "arx MCP exclude filter blocks required tools: " + ", ".join(sorted(blocked))
    return ""


def _is_expected_mcp_launch(command: Any, args: list[str]) -> bool:
    if args:
        return False
    try:
        return Path(str(command)).resolve() == LAUNCHER.resolve()
    except OSError:
        return False


def _mcp_registration_ready(state: dict[str, Any]) -> bool:
    return (
        state["configured"]
        and state["enabled"]
        and state["expected"]
        and not state["config_error"]
        and not state["tool_filter_issue"]
    )


def _mcp_registration_problem(state: dict[str, Any]) -> str:
    if state["config_error"]:
        return state["config_error"]
    if not state["configured"]:
        return f"No `mcp_servers.{MCP_SERVER_NAME}` entry found."
    if not state["enabled"]:
        return f"`mcp_servers.{MCP_SERVER_NAME}` is disabled."
    if not state["expected"]:
        return f"`mcp_servers.{MCP_SERVER_NAME}` does not launch `{LAUNCHER}`. {state['detail']}"
    if state["tool_filter_issue"]:
        return state["tool_filter_issue"]
    return ""


def _hermes_config_path() -> Path:
    if get_hermes_home is not None:
        return get_hermes_home() / "config.yaml"
    if os.environ.get("HERMES_HOME"):
        return Path(os.environ["HERMES_HOME"]) / "config.yaml"
    return Path(os.path.expanduser("~/.hermes/config.yaml"))


def _truthy_value(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in TRUTHY_STRINGS
    return bool(value)


def _string_list(value: Any) -> set[str] | None:
    if isinstance(value, str):
        return {value}
    if isinstance(value, list):
        return {str(item) for item in value}
    return None


def _print_result(result: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(_format_result(result))


def _format_result(result: dict[str, Any]) -> str:
    lines = [result.get("summary", "arx status")]
    entries = result.get("steps") or []
    for item in entries:
        status = str(item.get("status", "")).upper()
        lines.append(f"[{status}] {item.get('message')}")
        if item.get("detail"):
            lines.append(f"  {item['detail']}")
    if result.get("next_steps"):
        lines.append("Next steps:")
        lines.extend(f"- {step}" for step in result["next_steps"])
    return "\n".join(lines)


def _join_details(*parts: str) -> str:
    return " ".join(part for part in parts if part)
