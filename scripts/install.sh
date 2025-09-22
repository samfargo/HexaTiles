#!/usr/bin/env bash
set -euo pipefail

OWNER="hexatiles"
REPO="hexatiles"

die() { echo "install: $*" >&2; exit 1; }

if ! command -v curl >/dev/null 2>&1; then
  die "curl is required"
fi

LATEST="${1:-}"
if [ -z "$LATEST" ]; then
  LATEST=$(curl -fsSL "https://api.github.com/repos/${OWNER}/${REPO}/releases/latest" | grep -o '"tag_name": *"[^"]*"' | sed 's/.*"\(.*\)"/\1/')
fi
[ -n "$LATEST" ] || die "could not determine latest release"

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
case "$ARCH" in
  x86_64|amd64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) die "unsupported arch: $ARCH" ;;
esac

EXT="tar.gz"
if [ "$OS" = "mingw" ] || [ "$OS" = "msys" ] || [ "$OS" = "cygwin" ]; then
  OS=windows
  EXT=zip
fi

NAME="hexatiles_${OS}_${ARCH}"
ASSET_URL="https://github.com/${OWNER}/${REPO}/releases/download/${LATEST}/${NAME}.${EXT}"

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

echo "Downloading ${ASSET_URL}"
curl -fsSL "$ASSET_URL" -o "$TMPDIR/pkg.${EXT}"

case "$EXT" in
  tar.gz)
    tar -C "$TMPDIR" -xzf "$TMPDIR/pkg.${EXT}" || die "failed to extract archive"
    ;;
  zip)
    unzip -q "$TMPDIR/pkg.${EXT}" -d "$TMPDIR" || die "failed to unzip archive"
    ;;
esac

DEST="/usr/local/bin"
if [ ! -w "$DEST" ]; then
  DEST="$HOME/.local/bin"
  mkdir -p "$DEST"
  echo "Installing to $DEST (add to PATH if needed)"
fi

install -m 0755 "$TMPDIR/hexatiles" "$DEST/hexatiles"
echo "hexatiles installed to $DEST/hexatiles"
echo "Try: hexatiles --help"


