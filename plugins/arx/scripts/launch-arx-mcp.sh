#!/bin/sh
set -eu

fail() {
  code="$1"
  label="$2"
  shift 2
  printf '%s\n' "arx plugin launch error: $label" >&2
  for line do
    printf '%s\n' "$line" >&2
  done
  exit "$code"
}

canonical_path() {
  target="$1"
  depth=0
  while [ -L "$target" ]; do
    depth=$((depth + 1))
    [ "$depth" -le 40 ] || return 1
    link="$(readlink "$target")" || return 1
    case "$link" in
      /*) target="$link" ;;
      *) target="${target%/*}/$link" ;;
    esac
  done

  parent="${target%/*}"
  name="${target##*/}"
  [ "$parent" = "$target" ] && parent=.
  parent="$(CDPATH= cd "$parent" 2>/dev/null && pwd -P)" || return 1
  printf '%s/%s\n' "$parent" "$name"
}

has_git_ancestor() {
  scan="${1%/*}"
  [ "$scan" = "$1" ] && scan=.
  scan="$(CDPATH= cd "$scan" 2>/dev/null && pwd -P)" || return 1

  while :; do
    [ -e "$scan/.git" ] && return 0
    [ "$scan" = / ] && return 1
    scan="${scan%/*}"
    [ -z "$scan" ] && scan=/
  done
}

is_under_project() {
  binary="$1"
  project="$2"
  [ "$project" = / ] && return 1
  case "$binary" in
    "$project"|"$project"/*) return 0 ;;
    *) return 1 ;;
  esac
}

resolve_from_path() {
  name="$1"
  rest="${PATH:-}"
  last=0
  project_dir="$(pwd -P 2>/dev/null || pwd)"

  while :; do
    case "$rest" in
      *:*)
        entry="${rest%%:*}"
        rest="${rest#*:}"
        ;;
      *)
        entry="$rest"
        last=1
        ;;
    esac

    if [ -z "$entry" ]; then
      candidate="./$name"
      if [ -x "$candidate" ] && [ ! -d "$candidate" ]; then
        fail 126 binary_untrusted \
          "Refusing to run $name from an empty PATH segment." \
          "Restart the harness with installed arx binaries on an absolute PATH entry."
      fi
    else
      case "$entry" in
        /*)
          candidate="$entry/$name"
          if [ -x "$candidate" ] && [ ! -d "$candidate" ]; then
            canonical="$(canonical_path "$candidate")" || fail 126 binary_untrusted \
              "Could not canonicalize $name at $candidate." \
              "Restart the harness with installed arx binaries on an absolute PATH entry."

            if is_under_project "$canonical" "$project_dir"; then
              fail 126 binary_untrusted \
                "Refusing to run $name from the current project directory." \
                "Install arx first and put the install bin directory on PATH."
            fi

            if has_git_ancestor "$canonical"; then
              fail 126 binary_untrusted \
                "Refusing to run $name from a Git worktree." \
                "Install arx first and put the install bin directory on PATH."
            fi

            printf '%s\n' "$canonical"
            return 0
          fi
          ;;
        *)
          candidate="$entry/$name"
          if [ -x "$candidate" ] && [ ! -d "$candidate" ]; then
            fail 126 binary_untrusted \
              "Refusing to run $name from relative PATH entry: $entry" \
              "Restart the harness with installed arx binaries on an absolute PATH entry."
          fi
          ;;
      esac
    fi

    [ "$last" -eq 1 ] && break
  done

  fail 127 binary_missing \
    "The arx plugin requires $name on PATH." \
    "Install arx binaries and restart the agent harness."
}

arx_mcp_bin="$(resolve_from_path arx-mcp)"
arxd_bin="$(resolve_from_path arxd)"

if [ "${arx_mcp_bin%/*}" != "${arxd_bin%/*}" ]; then
  fail 126 binary_mismatch \
    "arx-mcp and arxd must resolve from the same install directory." \
    "arx-mcp: $arx_mcp_bin" \
    "arxd: $arxd_bin"
fi

export ARXD_BIN="$arxd_bin"
exec "$arx_mcp_bin" serve
