#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

usage() {
  cat <<'EOF'
Usage:
  scripts/sync_week_to_github.sh --latest
  scripts/sync_week_to_github.sh <week_start> [week_end]

Examples:
  scripts/sync_week_to_github.sh --latest
  scripts/sync_week_to_github.sh 2026-01-31 2026-02-06
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

export_args=()
commit_label=""

if [[ "${1}" == "--latest" ]]; then
  export_args+=(--latest)
  commit_label="latest payroll week"
else
  week_start="${1}"
  week_end="${2:-}"
  export_args+=(--week-start "${week_start}")
  if [[ -n "${week_end}" ]]; then
    export_args+=(--week-end "${week_end}")
    commit_label="${week_start} to ${week_end}"
  else
    commit_label="${week_start}"
  fi
fi

exported_file="$(python scripts/export_payroll_period.py "${export_args[@]}")"
echo "Exported: ${exported_file}"

# Quick smoke check for key Python files before pushing.
python -m py_compile \
  payroll_web_app.py \
  fill_payroll_workbook_from_hours.py \
  simplify_timecard_csv.py \
  scripts/export_payroll_period.py

git add "${exported_file}"

if git diff --cached --quiet; then
  echo "No data changes to commit."
  exit 0
fi

git commit -m "Update saved payroll period (${commit_label})"
git push origin main
echo "Pushed to origin/main."
