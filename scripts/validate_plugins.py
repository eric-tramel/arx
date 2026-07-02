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
        validate_launcher_behavior,
        validate_hermes_plugin,
        validate_mcp_smoke,
    ]

    for check in checks:
        check()
        print(f"ok: {check.__name__}")
    return 0


def validate_json_manifests() -> None:
    for path in [
        ROOT / "plugins/arx/.codex-plugin/plugin.json",
        ROOT / "plugins/arx/.claude-plugin/plugin.json",
        ROOT / "plugins/arx/package.json",
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
    package = load_json(ROOT / "plugins/arx/package.json")
    hermes = parse_top_level_yaml(ROOT / "plugins/hermes-arx/plugin.yaml")

    for name, version in [
        ("codex plugin", codex.get("version")),
        ("claude plugin", claude.get("version")),
        ("Pi/OMP package", package.get("version")),
        ("hermes plugin", hermes.get("version")),
    ]:
        if version != cargo_version:
            raise ValidationError(f"{name} version {version!r} does not match Cargo version {cargo_version!r}")


def validate_skills() -> None:
    for root in [ROOT / "plugins/arx/skills", ROOT / "plugins/hermes-arx/skills"]:
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
    compare_trees(ROOT / "plugins/arx/skills", ROOT / "plugins/hermes-arx/skills")
    compare_files(
        ROOT / "plugins/arx/scripts/launch-arx-mcp.sh",
        ROOT / "plugins/hermes-arx/scripts/launch-arx-mcp.sh",
    )
    inline = load_json(ROOT / "plugins/arx/.mcp.json")["mcpServers"]["arx"]["args"][2]
    script = (ROOT / "plugins/arx/scripts/launch-arx-mcp.sh").read_text(encoding="utf-8")
    if inline != script:
        raise ValidationError("plugins/arx/.mcp.json inline launcher drifted from launch-arx-mcp.sh")


def validate_mcp_configs() -> None:
    codex_mcp = load_json(ROOT / "plugins/arx/.mcp.json")["mcpServers"]["arx"]
    if codex_mcp.get("command") != "/bin/sh":
        raise ValidationError("plugins/arx/.mcp.json must launch through /bin/sh")
    args = codex_mcp.get("args")
    if not isinstance(args, list) or len(args) != 3 or "ARXD_BIN" not in args[2] or "arx-mcp" not in args[2]:
        raise ValidationError("plugins/arx/.mcp.json does not contain the inline arx launcher")

    claude_mcp = load_json(ROOT / "plugins/arx/.claude-mcp.json")["mcpServers"]["arx"]
    if claude_mcp.get("command") != "${CLAUDE_PLUGIN_ROOT}/scripts/launch-arx-mcp.sh":
        raise ValidationError("plugins/arx/.claude-mcp.json must use the Claude plugin root launcher")

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


def validate_launcher_behavior() -> None:
    launcher = ROOT / "plugins/arx/scripts/launch-arx-mcp.sh"
    run_checked(["sh", "-n", str(launcher)], cwd=ROOT)

    for label, command in launcher_commands():
        validate_launcher_matrix(label, command)


def validate_launcher_matrix(label: str, command: list[str]) -> None:
    with tempfile.TemporaryDirectory(prefix="arx-plugin-launcher-") as tmp:
        base = Path(tmp)
        work = base / "work"
        work.mkdir()
        good_bin = base / "good-bin"
        good_bin.mkdir()
        write_fake_binary(
            good_bin / "arx-mcp",
            "printf 'args=%s\\n' \"$*\"\nprintf 'ARXD_BIN=%s\\n' \"${ARXD_BIN:-}\"\n",
        )
        write_fake_binary(good_bin / "arxd", "printf 'arxd fake\\n'\n")

        result = run_launcher_command(command, cwd=work, path=path_value(good_bin))
        assert_returncode(result, 0, f"{label}: trusted fake binaries should launch")
        if f"ARXD_BIN={(good_bin / 'arxd').resolve()}" not in result.stdout:
            raise ValidationError(f"{label}: launcher did not export ARXD_BIN to arx-mcp")
        if "args=serve" not in result.stdout:
            raise ValidationError(f"{label}: launcher did not exec arx-mcp with serve")

        only_arxd = base / "only-arxd"
        only_arxd.mkdir()
        write_fake_binary(only_arxd / "arxd", "exit 0\n")
        assert_returncode(
            run_launcher_command(command, cwd=work, path=path_value(only_arxd)),
            127,
            f"{label}: missing arx-mcp should exit 127",
        )

        only_mcp = base / "only-mcp"
        only_mcp.mkdir()
        write_fake_binary(only_mcp / "arx-mcp", "exit 0\n")
        assert_returncode(
            run_launcher_command(command, cwd=work, path=path_value(only_mcp)),
            127,
            f"{label}: missing arxd should exit 127",
        )

        rel_work = base / "relative-work"
        rel_work.mkdir()
        rel_bin = rel_work / "relbin"
        rel_bin.mkdir()
        write_fake_binary(rel_bin / "arx-mcp", "exit 0\n")
        assert_returncode(
            run_launcher_command(command, cwd=rel_work, path=f"relbin:{path_value(good_bin)}"),
            126,
            f"{label}: relative PATH hit should exit 126",
        )

        empty_work = base / "empty-work"
        empty_work.mkdir()
        write_fake_binary(empty_work / "arx-mcp", "exit 0\n")
        assert_returncode(
            run_launcher_command(command, cwd=empty_work, path=f":{path_value(good_bin)}"),
            126,
            f"{label}: empty PATH hit should exit 126",
        )

        project = base / "project"
        project_bin = project / "bin"
        project_bin.mkdir(parents=True)
        write_fake_binary(project_bin / "arx-mcp", "exit 0\n")
        write_fake_binary(project_bin / "arxd", "exit 0\n")
        assert_returncode(
            run_launcher_command(command, cwd=project, path=path_value(project_bin)),
            126,
            f"{label}: project-local binaries should exit 126",
        )

        gitroot = base / "gitroot"
        git_bin = gitroot / "out" / "bin"
        git_bin.mkdir(parents=True)
        (gitroot / ".git").write_text("gitdir: elsewhere\n", encoding="utf-8")
        write_fake_binary(git_bin / "arx-mcp", "exit 0\n")
        write_fake_binary(git_bin / "arxd", "exit 0\n")
        assert_returncode(
            run_launcher_command(command, cwd=work, path=path_value(git_bin)),
            126,
            f"{label}: Git worktree binaries should exit 126",
        )

        link_bin = base / "link-bin"
        link_bin.mkdir()
        (link_bin / "arx-mcp").symlink_to(git_bin / "arx-mcp")
        write_fake_binary(link_bin / "arxd", "exit 0\n")
        assert_returncode(
            run_launcher_command(command, cwd=work, path=path_value(link_bin)),
            126,
            f"{label}: symlink into Git worktree should exit 126",
        )

        bin1 = base / "bin1"
        bin2 = base / "bin2"
        bin1.mkdir()
        bin2.mkdir()
        write_fake_binary(bin1 / "arx-mcp", "exit 0\n")
        write_fake_binary(bin2 / "arxd", "exit 0\n")
        assert_returncode(
            run_launcher_command(command, cwd=work, path=f"{path_value(bin1)}:{path_value(bin2)}"),
            126,
            f"{label}: mismatched binary directories should exit 126",
        )


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
                                "command": str(module.LAUNCHER),
                                "args": [],
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

    launcher = ROOT / "plugins/arx/scripts/launch-arx-mcp.sh"
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
            [str(launcher)],
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
    path = ROOT / "plugins/hermes-arx/__init__.py"
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


def compare_trees(left: Path, right: Path) -> None:
    left_files = sorted(path.relative_to(left) for path in left.rglob("*") if path.is_file())
    right_files = sorted(path.relative_to(right) for path in right.rglob("*") if path.is_file())
    if left_files != right_files:
        raise ValidationError(f"tree drift between {left} and {right}")
    for rel in left_files:
        compare_files(left / rel, right / rel)


def compare_files(left: Path, right: Path) -> None:
    if left.read_bytes() != right.read_bytes():
        raise ValidationError(f"file drift between {left} and {right}")


def write_fake_binary(path: Path, body: str) -> None:
    path.write_text("#!/bin/sh\nset -eu\n" + body, encoding="utf-8")
    make_executable(path)


def make_executable(path: Path) -> None:
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def path_value(*entries: Path) -> str:
    return os.pathsep.join(str(entry) for entry in entries)


def launcher_commands() -> list[tuple[str, list[str]]]:
    inline = load_json(ROOT / "plugins/arx/.mcp.json")["mcpServers"]["arx"]
    return [
        ("script", [str(ROOT / "plugins/arx/scripts/launch-arx-mcp.sh")]),
        ("inline", [inline["command"], *inline["args"]]),
    ]


def run_launcher_command(command: list[str], *, cwd: Path, path: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PATH"] = path
    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def assert_returncode(result: subprocess.CompletedProcess[str], expected: int, label: str) -> None:
    if result.returncode != expected:
        raise ValidationError(
            f"{label}: expected return code {expected}, got {result.returncode}. "
            f"stdout={result.stdout!r} stderr={result.stderr!r}"
        )


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
