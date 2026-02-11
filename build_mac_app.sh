#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

TEMPLATE_SOURCE="${1:-}"
if [ -z "$TEMPLATE_SOURCE" ]; then
  while IFS= read -r CANDIDATE; do
    if [[ "$CANDIDATE" != *"filled"* ]] && [[ "$CANDIDATE" != *"~$"* ]]; then
      TEMPLATE_SOURCE="$CANDIDATE"
      break
    fi
  done < <(find "$ROOT_DIR" -maxdepth 1 -type f -name "*.xlsx" | sort)
fi

if [ -z "$TEMPLATE_SOURCE" ]; then
  echo "No template XLSX found. Pass one explicitly:"
  echo "  ./build_mac_app.sh '/path/to/template.xlsx'"
  exit 1
fi

if [ ! -f "$TEMPLATE_SOURCE" ]; then
  echo "Template file not found: $TEMPLATE_SOURCE"
  exit 1
fi

ASSET_DIR="$ROOT_DIR/.build_assets"
mkdir -p "$ASSET_DIR"
cp "$TEMPLATE_SOURCE" "$ASSET_DIR/default_template.xlsx"

if python -m PyInstaller --version >/dev/null 2>&1; then
  PYINSTALLER_CMD=(python -m PyInstaller)
elif command -v pyinstaller >/dev/null 2>&1; then
  PYINSTALLER_CMD=(pyinstaller)
else
  echo "PyInstaller is not installed."
  echo "Install it once, then rerun:"
  echo "  python -m pip install pyinstaller"
  exit 1
fi

"${PYINSTALLER_CMD[@]}" \
  --noconfirm \
  --clean \
  --windowed \
  --name PayrollConverterApp \
  --add-data "payroll_roster.json:." \
  --add-data "payroll_workspace_ui.html:." \
  --add-data "$ASSET_DIR/default_template.xlsx:." \
  payroll_mac_app.py

echo ""
echo "Build complete:"
echo "  dist/PayrollConverterApp.app"
echo "Bundled default template:"
echo "  $TEMPLATE_SOURCE"
