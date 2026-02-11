#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_NAME="PayrollConverter.app"
APP_DIR="$ROOT_DIR/$APP_NAME"
CONTENTS_DIR="$APP_DIR/Contents"
MACOS_DIR="$CONTENTS_DIR/MacOS"
RESOURCES_DIR="$CONTENTS_DIR/Resources"
PAYLOAD_DIR="$RESOURCES_DIR/app"

TEMPLATE_SOURCE="${1:-}"
if [ -z "$TEMPLATE_SOURCE" ]; then
  while IFS= read -r CANDIDATE; do
    if [[ "$CANDIDATE" != *"filled"* ]] && [[ "$CANDIDATE" != *"~$"* ]]; then
      TEMPLATE_SOURCE="$CANDIDATE"
      break
    fi
  done < <(find "$ROOT_DIR" -maxdepth 1 -type f -name "*.xlsx" | sort)
fi

rm -rf "$APP_DIR"
mkdir -p "$MACOS_DIR" "$PAYLOAD_DIR"

cat > "$CONTENTS_DIR/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key>
  <string>PayrollConverter</string>
  <key>CFBundleDisplayName</key>
  <string>PayrollConverter</string>
  <key>CFBundleIdentifier</key>
  <string>com.local.payrollconverter</string>
  <key>CFBundleVersion</key>
  <string>1.0</string>
  <key>CFBundleShortVersionString</key>
  <string>1.0</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleExecutable</key>
  <string>PayrollConverter</string>
  <key>LSMinimumSystemVersion</key>
  <string>11.0</string>
</dict>
</plist>
PLIST

cat > "$MACOS_DIR/PayrollConverter" <<'SCRIPT'
#!/bin/bash
set -euo pipefail

APP_CONTENTS="$(cd "$(dirname "$0")/.." && pwd)"
PAYLOAD_DIR="$APP_CONTENTS/Resources/app"

PYTHON_BIN=""
for CANDIDATE in /usr/bin/python3 /opt/homebrew/bin/python3 /usr/local/bin/python3; do
  if [ -x "$CANDIDATE" ]; then
    PYTHON_BIN="$CANDIDATE"
    break
  fi
done

if [ -z "$PYTHON_BIN" ]; then
  osascript -e 'display alert "Python 3 not found" message "Install Python 3 to run PayrollConverter.app." as critical'
  exit 1
fi

cd "$PAYLOAD_DIR"
exec "$PYTHON_BIN" payroll_mac_app.py
SCRIPT

chmod +x "$MACOS_DIR/PayrollConverter"

cp "$ROOT_DIR/payroll_mac_app.py" "$PAYLOAD_DIR/"
cp "$ROOT_DIR/simplify_timecard_csv.py" "$PAYLOAD_DIR/"
cp "$ROOT_DIR/fill_payroll_workbook_from_hours.py" "$PAYLOAD_DIR/"
if [ -f "$ROOT_DIR/payroll_workspace_ui.html" ]; then
  cp "$ROOT_DIR/payroll_workspace_ui.html" "$PAYLOAD_DIR/"
fi

if [ -f "$ROOT_DIR/payroll_roster.json" ]; then
  cp "$ROOT_DIR/payroll_roster.json" "$PAYLOAD_DIR/"
fi

if [ -n "$TEMPLATE_SOURCE" ] && [ -f "$TEMPLATE_SOURCE" ]; then
  cp "$TEMPLATE_SOURCE" "$PAYLOAD_DIR/default_template.xlsx"
fi

echo "Created $APP_DIR"
echo "Double-click in Finder to run."
if [ -n "$TEMPLATE_SOURCE" ] && [ -f "$TEMPLATE_SOURCE" ]; then
  echo "Bundled default template: $TEMPLATE_SOURCE"
fi
