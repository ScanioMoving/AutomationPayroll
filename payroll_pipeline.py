#!/usr/bin/env python3
"""Run payroll pipeline: simplify batch report, then fill workbook with hours + tips."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def default_hours_output(batch_report_csv: Path, include_weekly_overtime: bool) -> Path:
    suffix = "_simple_reg.csv" if include_weekly_overtime else "_simple.csv"
    return batch_report_csv.with_name(f"{batch_report_csv.stem}{suffix}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pipeline: simplify batch report CSV, then fill payroll XLSX using "
            "simplified hours and tips."
        )
    )
    parser.add_argument("workbook", help="Path to payroll workbook (.xlsx).")
    parser.add_argument("batch_report_csv", help="Path to raw batch report CSV.")
    parser.add_argument("tips_csv", help="Path to tip report CSV.")
    parser.add_argument(
        "--hours-output",
        help=(
            "Path for simplified hours CSV. Defaults to "
            "<batch_report>_simple_reg.csv (or _simple.csv with --exclude-weekly-overtime)."
        ),
    )
    parser.add_argument(
        "--tip-summary-output",
        help="Path for simplified tip summary CSV (Name,Commission).",
    )
    parser.add_argument(
        "--filled-workbook-output",
        help="Path for filled workbook output (.xlsx).",
    )
    parser.add_argument(
        "--exclude-weekly-overtime",
        action="store_true",
        help="Exclude WEEKLY OVERTIME adjustments when simplifying batch report.",
    )
    return parser.parse_args()


def run_step(command: list[str], step_name: str) -> None:
    print(f"{step_name}...", flush=True)
    subprocess.run(command, check=True)


def main() -> None:
    args = parse_args()

    script_dir = Path(__file__).resolve().parent
    simplify_script = script_dir / "simplify_timecard_csv.py"
    fill_script = script_dir / "fill_payroll_workbook_from_hours.py"

    if not simplify_script.exists():
        raise FileNotFoundError(f"Missing script: {simplify_script}")
    if not fill_script.exists():
        raise FileNotFoundError(f"Missing script: {fill_script}")

    workbook_path = Path(args.workbook)
    batch_report_csv = Path(args.batch_report_csv)
    tips_csv = Path(args.tips_csv)

    include_weekly_overtime = not args.exclude_weekly_overtime
    hours_output = (
        Path(args.hours_output)
        if args.hours_output
        else default_hours_output(batch_report_csv, include_weekly_overtime)
    )
    filled_workbook_output = (
        Path(args.filled_workbook_output)
        if args.filled_workbook_output
        else workbook_path.with_name(f"{workbook_path.stem}_filled_with_tips.xlsx")
    )
    tip_summary_output = (
        Path(args.tip_summary_output)
        if args.tip_summary_output
        else tips_csv.with_name(f"{tips_csv.stem}_simple.csv")
    )

    simplify_command = [
        sys.executable,
        str(simplify_script),
        str(batch_report_csv),
        "-o",
        str(hours_output),
    ]
    if args.exclude_weekly_overtime:
        simplify_command.append("--exclude-weekly-overtime")

    fill_command = [
        sys.executable,
        str(fill_script),
        str(workbook_path),
        str(hours_output),
        "--tips-csv",
        str(tips_csv),
        "--tip-summary-output",
        str(tip_summary_output),
        "-o",
        str(filled_workbook_output),
    ]

    try:
        run_step(simplify_command, "Step 1/2: Simplifying batch report")
        run_step(fill_command, "Step 2/2: Filling workbook with hours and tips")
    except subprocess.CalledProcessError as exc:
        raise SystemExit(exc.returncode) from exc

    print("Pipeline complete.", flush=True)
    print(f"Simplified hours CSV: {hours_output}", flush=True)
    print(f"Simplified tips CSV: {tip_summary_output}", flush=True)
    print(f"Filled workbook: {filled_workbook_output}", flush=True)


if __name__ == "__main__":
    main()
