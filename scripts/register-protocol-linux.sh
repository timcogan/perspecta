#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEFAULT_BIN="$PROJECT_ROOT/target/release/perspecta"
BIN_PATH="${1:-$DEFAULT_BIN}"
LOGO_SOURCE="${2:-$PROJECT_ROOT/assets/perspecta.svg}"

if [[ ! -x "$BIN_PATH" ]]; then
  echo "Binary not found or not executable: $BIN_PATH"
  echo "Build first with: cargo build --release"
  exit 1
fi

DESKTOP_DIR="$HOME/.local/share/applications"
DESKTOP_FILE="$DESKTOP_DIR/perspecta.desktop"
ICON_DIR="$HOME/.local/share/icons/hicolor/scalable/apps"
ICON_FILE="$ICON_DIR/perspecta.svg"

mkdir -p "$DESKTOP_DIR" "$ICON_DIR"

if [[ -f "$LOGO_SOURCE" ]]; then
  cp "$LOGO_SOURCE" "$ICON_FILE"
else
  echo "Warning: logo not found at $LOGO_SOURCE (desktop entry will still be registered)."
fi

cat > "$DESKTOP_FILE" <<EOF
[Desktop Entry]
Name=Perspecta Viewer
Comment=Open DICOM studies in Perspecta
Exec=$BIN_PATH %u
Icon=perspecta
Terminal=false
Type=Application
MimeType=x-scheme-handler/perspecta;
Categories=Graphics;Science;MedicalSoftware;
NoDisplay=true
EOF

if command -v xdg-mime >/dev/null 2>&1; then
  xdg-mime default perspecta.desktop x-scheme-handler/perspecta
else
  echo "xdg-mime not found; install xdg-utils to complete protocol registration."
  exit 1
fi

if command -v update-desktop-database >/dev/null 2>&1; then
  update-desktop-database "$DESKTOP_DIR" >/dev/null 2>&1 || true
fi

if command -v gtk-update-icon-cache >/dev/null 2>&1; then
  gtk-update-icon-cache -q -t -f "$HOME/.local/share/icons/hicolor" >/dev/null 2>&1 || true
fi

echo "Registered perspecta:// handler using:"
echo "  $DESKTOP_FILE"
if [[ -f "$ICON_FILE" ]]; then
  echo "Installed app icon:"
  echo "  $ICON_FILE"
fi
echo "You can test with:"
echo "  xdg-open 'perspecta://open?path=example-data%2Fexample.dcm'"
