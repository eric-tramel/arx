#!/usr/bin/env python3
"""Validate arx cross-harness plugin packaging."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import tomllib
from pathlib import Path
from selectors import DefaultSelector, EVENT_READ
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_TOOLS = {
    "lookup_arxiv_papers",
    "fetch_arxiv_paper",
    "get_arxiv_download_queue_status",
    "full_text_search",
}
SKILL_NAMES = {
    "arx-paper-metadata",
    "arx-paper-fetch",
    "arx-full-text-search",
}


class ValidationError(RuntimeError):
    pass


def main() -> int:
    checks = [
        validate_json_manifests,
        validate_versions,
        validate_skills,
        validate_drift,
        validate_mcp_configs,
        validate_no_machine_paths,
        validate_hermes_plugin,
        validate_mcp_smoke,
    ]

    for check in checks:
        check()
        print(f"ok: {check.__name__}")
    return 0


def validate_json_manifests() -> None:
    for path in [
        ROOT / "package.json",
        ROOT / "plugins/arx/.codex-plugin/plugin.json",
        ROOT / "plugins/arx/.claude-plugin/plugin.json",
        ROOT / "plugins/arx/package.json",
        ROOT / ".mcp.json",
        ROOT / "plugins/arx/.mcp.json",
        ROOT / "plugins/arx/.claude-mcp.json",
        ROOT / ".agents/plugins/marketplace.json",
        ROOT / ".claude-plugin/marketplace.json",
    ]:
        load_json(path)


def validate_versions() -> None:
    cargo_version = workspace_version()
    codex = load_json(ROOT / "plugins/arx/.codex-plugin/plugin.json")
    claude = load_json(ROOT / "plugins/arx/.claude-plugin/plugin.json")
    root_package = load_json(ROOT / "package.json")
    package = load_json(ROOT / "plugins/arx/package.json")
    hermes = parse_top_level_yaml(ROOT / "plugins/arx/plugin.yaml")

    for name, version in [
        ("codex plugin", codex.get("version")),
        ("claude plugin", claude.get("version")),
        ("root Pi/OMP package", root_package.get("version")),
        ("Pi/OMP package", package.get("version")),
        ("hermes plugin", hermes.get("version")),
    ]:
        if version != cargo_version:
            raise ValidationError(f"{name} version {version!r} does not match Cargo version {cargo_version!r}")

    pi_manifest = root_package.get("pi")
    if not isinstance(pi_manifest, dict) or pi_manifest.get("skills") != ["plugins/arx/skills"]:
        raise ValidationError("root package.json must expose plugins/arx/skills for Pi remote installs")


def validate_skills() -> None:
    root = ROOT / "plugins/arx/skills"
    seen: set[str] = set()
    for skill in sorted(root.glob("*/SKILL.md")):
        frontmatter = parse_skill_frontmatter(skill)
        name = frontmatter.get("name")
        description = frontmatter.get("description")
        if name != skill.parent.name:
            raise ValidationError(f"{skill}: frontmatter name {name!r} does not match directory")
        if not description:
            raise ValidationError(f"{skill}: missing description")
        seen.add(str(name))
    if seen != SKILL_NAMES:
        raise ValidationError(f"{root}: expected skills {sorted(SKILL_NAMES)}, found {sorted(seen)}")


def validate_drift() -> None:
    symlinks = [
        (ROOT / "plugins/hermes-arx", ROOT / "plugins/arx"),
        (ROOT / "skills", ROOT / "plugins/arx/skills"),
        (ROOT / ".mcp.json", ROOT / "plugins/arx/.mcp.json"),
    ]
    for alias, target in symlinks:
        if not alias.is_symlink():
            raise ValidationError(f"{alias.relative_to(ROOT)} must be a symlink")
        if alias.resolve() != target.resolve():
            raise ValidationError(f"{alias.relative_to(ROOT)} does not point at {target.relative_to(ROOT)}")


def validate_mcp_configs() -> None:
    codex_mcp = load_json(ROOT / "plugins/arx/.mcp.json")["mcpServers"]["arx"]
    if codex_mcp.get("command") != "arx-mcp" or codex_mcp.get("args") != ["serve"]:
        raise ValidationError("plugins/arx/.mcp.json must launch `arx-mcp serve`")

    claude_mcp = load_json(ROOT / "plugins/arx/.claude-mcp.json")["mcpServers"]["arx"]
    if claude_mcp.get("command") != "arx-mcp" or claude_mcp.get("args") != ["serve"]:
        raise ValidationError("plugins/arx/.claude-mcp.json must launch `arx-mcp serve`")

    codex_market = load_json(ROOT / ".agents/plugins/marketplace.json")
    codex_plugins = codex_market.get("plugins")
    if not isinstance(codex_plugins, list) or codex_plugins[0]["source"]["path"] != "./plugins/arx":
        raise ValidationError("Codex marketplace must point at ./plugins/arx")

    claude_market = load_json(ROOT / ".claude-plugin/marketplace.json")
    claude_plugins = claude_market.get("plugins")
    if not isinstance(claude_plugins, list) or claude_plugins[0]["source"] != "./plugins/arx":
        raise ValidationError("Claude marketplace must point at ./plugins/arx")


def validate_no_machine_paths() -> None:
    forbidden = ["/Users/eric", "target/debug"]
    for path in list((ROOT / "plugins").rglob("*")) + [
        ROOT / "package.json",
        ROOT / ".mcp.json",
        ROOT / ".agents/plugins/marketplace.json",
        ROOT / ".claude-plugin/marketplace.json",
    ]:
        if not path.is_file():
            continue
        if path.suffix in {".pyc", ".pyo"} or "__pycache__" in path.parts:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for marker in forbidden:
            if marker in text:
                raise ValidationError(f"{path}: contains machine-specific marker {marker!r}")


def validate_hermes_plugin() -> None:
    module = import_hermes_plugin()
    ctx = FakeHermesContext()
    module.register(ctx)
    if {item[0] for item in ctx.skills} != SKILL_NAMES:
        raise ValidationError("Hermes plugin did not register the expected skills")
    if ctx.tools:
        raise ValidationError(f"Hermes plugin tools mismatch: {ctx.tools}")
    if ctx.commands != ["arx"]:
        raise ValidationError(f"Hermes plugin commands mismatch: {ctx.commands}")

    with tempfile.TemporaryDirectory(prefix="arx-hermes-plugin-") as tmp:
        base = Path(tmp)
        old_home = os.environ.get("HERMES_HOME")
        try:
            os.environ["HERMES_HOME"] = str(base / "hermes-home")
            config_path = Path(os.environ["HERMES_HOME"]) / "config.yaml"
            config_path.parent.mkdir(parents=True)
            config_path.write_text(
                json.dumps(
                    {
                        "mcp_servers": {
                            "arx": {
                                "command": module.MCP_COMMAND,
                                "args": module.MCP_ARGS,
                                "enabled": True,
                                "tools": {"include": ["full_text_search"]},
                            }
                        }
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            result = module._setup_mcp(force=False)
            if not result["ok"]:
                raise ValidationError(f"Hermes setup failed in temp home: {result}")
            state = module._mcp_config_state()
            if not module._mcp_registration_ready(state):
                raise ValidationError(f"Hermes setup did not produce a ready registration: {state}")
        finally:
            if old_home is None:
                os.environ.pop("HERMES_HOME", None)
            else:
                os.environ["HERMES_HOME"] = old_home


def validate_mcp_smoke() -> None:
    run_checked(["cargo", "build", "--locked", "-p", "arx-mcp", "-p", "arxd"], cwd=ROOT, timeout=240)
    arx_mcp = ROOT / "target/debug/arx-mcp"
    arxd = ROOT / "target/debug/arxd"
    if not arx_mcp.is_file() or not arxd.is_file():
        raise ValidationError("cargo build did not produce arx-mcp and arxd")

    with tempfile.TemporaryDirectory(prefix="arx-mcp-smoke-") as tmp:
        base = Path(tmp)
        bin_dir = base / "bin"
        work = base / "work"
        cache = base / "cache"
        bin_dir.mkdir()
        work.mkdir()
        cache.mkdir()
        shutil.copy2(arx_mcp, bin_dir / "arx-mcp")
        shutil.copy2(arxd, bin_dir / "arxd")
        make_executable(bin_dir / "arx-mcp")
        make_executable(bin_dir / "arxd")

        env = os.environ.copy()
        env["PATH"] = f"{bin_dir}{os.pathsep}/usr/bin{os.pathsep}/bin"
        env["ARX_CACHE_DIR"] = str(cache)
        env["ARXD_IDLE_SHUTDOWN_MS"] = "150"

        proc = subprocess.Popen(
            ["arx-mcp", "serve"],
            cwd=work,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        assert proc.stdin is not None
        assert proc.stdout is not None
        selector = DefaultSelector()
        selector.register(proc.stdout, EVENT_READ)
        try:
            send_json(proc, 1, "initialize", {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "arx-plugin-validator", "version": "0"},
            })
            send_notification(proc, "notifications/initialized", {})
            send_json(proc, 2, "tools/list", {})
            send_json(
                proc,
                3,
                "tools/call",
                {"name": "get_arxiv_download_queue_status", "arguments": {"include_finished": True}},
            )

            responses: dict[int, dict[str, Any]] = {}
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and (2 not in responses or 3 not in responses):
                events = selector.select(timeout=0.25)
                if not events:
                    if proc.poll() is not None:
                        break
                    continue
                line = proc.stdout.readline()
                if not line:
                    continue
                try:
                    message = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValidationError(f"MCP server emitted non-JSON line: {line!r}") from exc
                if isinstance(message.get("id"), int):
                    responses[message["id"]] = message

            if 2 not in responses:
                raise ValidationError(f"MCP tools/list did not respond. stderr={read_stderr(proc)!r}")
            if "error" in responses[2]:
                raise ValidationError(f"MCP tools/list returned error: {responses[2]['error']}")
            tool_names = {tool["name"] for tool in responses[2].get("result", {}).get("tools", [])}
            if tool_names != EXPECTED_TOOLS:
                raise ValidationError(f"MCP tools mismatch: expected {sorted(EXPECTED_TOOLS)}, found {sorted(tool_names)}")

            if 3 not in responses:
                raise ValidationError(f"MCP queue-status call did not respond. stderr={read_stderr(proc)!r}")
            if "error" in responses[3]:
                raise ValidationError(f"MCP queue-status returned error: {responses[3]['error']}")
        finally:
            selector.close()
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)


class FakeHermesContext:
    def __init__(self) -> None:
        self.skills: list[tuple[str, Path, str]] = []
        self.tools: list[str] = []
        self.commands: list[str] = []

    def register_skill(self, name: str, path: Path, description: str) -> None:
        self.skills.append((name, path, description))

    def register_tool(self, *, name: str, **_kwargs: Any) -> None:
        self.tools.append(name)

    def register_cli_command(self, *, name: str, **_kwargs: Any) -> None:
        self.commands.append(name)


def import_hermes_plugin() -> Any:
    path = ROOT / "plugins/arx/__init__.py"
    spec = importlib.util.spec_from_file_location("arx_hermes_plugin_validation", path)
    if spec is None or spec.loader is None:
        raise ValidationError("Could not load Hermes plugin module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def send_json(proc: subprocess.Popen[str], msg_id: int, method: str, params: dict[str, Any]) -> None:
    assert proc.stdin is not None
    proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": msg_id, "method": method, "params": params}) + "\n")
    proc.stdin.flush()


def send_notification(proc: subprocess.Popen[str], method: str, params: dict[str, Any]) -> None:
    assert proc.stdin is not None
    proc.stdin.write(json.dumps({"jsonrpc": "2.0", "method": method, "params": params}) + "\n")
    proc.stdin.flush()


def read_stderr(proc: subprocess.Popen[str]) -> str:
    if proc.stderr is None:
        return ""
    try:
        return proc.stderr.read()
    except Exception:
        return ""


def workspace_version() -> str:
    cargo = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    try:
        return cargo["workspace"]["package"]["version"]
    except KeyError as exc:
        raise ValidationError("Could not find [workspace.package].version") from exc


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValidationError(f"{path}: invalid JSON: {exc}") from exc


def parse_top_level_yaml(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith(" ") or line.startswith("-") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def parse_skill_frontmatter(path: Path) -> dict[str, str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != "---":
        raise ValidationError(f"{path}: missing YAML frontmatter")
    values: dict[str, str] = {}
    for line in lines[1:]:
        if line == "---":
            return values
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        values[key.strip()] = value.strip()
    raise ValidationError(f"{path}: unterminated YAML frontmatter")


def make_executable(path: Path) -> None:
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def run_checked(command: list[str], *, cwd: Path, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode != 0:
        raise ValidationError(
            f"{' '.join(command)} failed with {result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
