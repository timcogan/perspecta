#!/usr/bin/env bash
set -euo pipefail

DEFAULT_BIN="$(pwd)/target/release/perspecta"
BIN_PATH="${1:-$DEFAULT_BIN}"

if [[ ! -x "$BIN_PATH" ]]; then
  echo "Binary not found or not executable: $BIN_PATH"
  echo "Build first with: cargo build --release"
  exit 1
fi

DESKTOP_DIR="$HOME/.local/share/applications"
DESKTOP_FILE="$DESKTOP_DIR/perspecta.desktop"

mkdir -p "$DESKTOP_DIR"

cat > "$DESKTOP_FILE" <<EOF
[Desktop Entry]
Name=Perspecta Viewer
Comment=Open DICOM studies in Perspecta
Exec=$BIN_PATH %u
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

echo "Registered perspecta:// handler using:"
echo "  $DESKTOP_FILE"
echo "You can test with:"
echo "  xdg-open 'perspecta://open?path=example-data%2Fexample.dcm'"
