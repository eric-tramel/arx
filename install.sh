#!/usr/bin/env sh
set -eu

owner="eric-tramel"
repo="arx"
version="${ARX_VERSION:-latest}"
install_dir="${ARX_INSTALL_DIR:-$HOME/.local/bin}"

os="$(uname -s)"
arch="$(uname -m)"

case "$os:$arch" in
  Linux:x86_64|Linux:amd64)
    target="x86_64-unknown-linux-gnu"
    archive="arx-${target}.tar.gz"
    ;;
  Darwin:arm64|Darwin:aarch64)
    target="aarch64-apple-darwin"
    archive="arx-${target}.tar.gz"
    ;;
  *)
    echo "unsupported platform: $os $arch" >&2
    echo "supported by this installer: Linux x86_64, macOS arm64" >&2
    echo "Windows users should run install.ps1 from PowerShell." >&2
    exit 1
    ;;
esac

if [ "$version" = "latest" ]; then
  url="https://github.com/${owner}/${repo}/releases/latest/download/${archive}"
else
  url="https://github.com/${owner}/${repo}/releases/download/${version}/${archive}"
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT INT TERM

echo "Downloading $url"
curl --fail --location --show-error --silent "$url" --output "$tmp_dir/$archive"

tar -xzf "$tmp_dir/$archive" -C "$tmp_dir"
mkdir -p "$install_dir"

binary_root="$tmp_dir/arx-${target}"
install -m 0755 "$binary_root/arx" "$install_dir/arx"
install -m 0755 "$binary_root/arx-mcp" "$install_dir/arx-mcp"
install -m 0755 "$binary_root/arxd" "$install_dir/arxd"

echo "Installed arx, arxd, and arx-mcp to $install_dir"
case ":$PATH:" in
  *":$install_dir:"*) ;;
  *) echo "Add $install_dir to PATH if it is not already available." ;;
esac
