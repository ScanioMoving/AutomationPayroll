#!/usr/bin/env python3
"""Flatten a batch timecard CSV into Name/Company/Hours rows."""

from __future__ import annotations

import argparse
import csv
import re
from collections import OrderedDict
from datetime import datetime
from pathlib import Path

EXPECTED_COLUMNS = 19
TIME_PATTERN = re.compile(r"\d{1,2}:\d{2}\s*[AP]M", re.IGNORECASE)


def clean(value: str) -> str:
    return (value or "").strip()


def collapse_spaces(value: str) -> str:
    return " ".join(clean(value).split())


def is_employee_name(cell_value: str) -> bool:
    if not cell_value:
        return False

    lowered = cell_value.lower()
    if lowered.startswith("timecard report"):
        return False
    if lowered.startswith("pay period:"):
        return False
    if lowered == "sea and air":
        return False
    return True


def parse_hhmm_to_minutes(value: str) -> int | None:
    text = clean(value).replace(" ", "")
    if not text:
        return None

    sign = 1
    if text[0] in "+-":
        if text[0] == "-":
            sign = -1
        text = text[1:]

    if ":" not in text:
        return None

    hours, minutes = text.split(":", 1)
    if hours == "":
        hours = "0"
    if minutes == "":
        minutes = "0"

    if not hours.isdigit() or not minutes.isdigit():
        return None

    return sign * (int(hours) * 60 + int(minutes))


def parse_clock_to_minutes(value: str) -> int | None:
    text = clean(value).upper()
    if not TIME_PATTERN.fullmatch(text):
        return None

    parsed = datetime.strptime(text, "%I:%M %p")
    return parsed.hour * 60 + parsed.minute


def duration_from_in_out(in_value: str, out_value: str) -> int | None:
    start = parse_clock_to_minutes(in_value)
    end = parse_clock_to_minutes(out_value)
    if start is None or end is None:
        return None

    duration = end - start
    if duration < 0:
        duration += 24 * 60
    return duration


def format_minutes_as_hhmm(minutes: int) -> str:
    sign = "-" if minutes < 0 else ""
    abs_minutes = abs(minutes)
    hours, mins = divmod(abs_minutes, 60)
    return f"{sign}{hours}:{mins:02d}"


def flatten_timecard(
    input_path: Path, include_weekly_overtime: bool
) -> OrderedDict[tuple[str, str], int]:
    totals: OrderedDict[tuple[str, str], int] = OrderedDict()
    current_employee: str | None = None

    with input_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if len(row) < EXPECTED_COLUMNS:
                row += [""] * (EXPECTED_COLUMNS - len(row))

            first_col = collapse_spaces(row[0])
            if is_employee_name(first_col):
                current_employee = first_col
                continue

            if not current_employee:
                continue

            department = collapse_spaces(row[3])
            if not department:
                continue

            marker = collapse_spaces(row[6]).upper()
            in_time = clean(row[5])
            out_time = clean(row[7])
            reg_value = clean(row[10])

            if marker == "WEEKLY OVERTIME":
                if include_weekly_overtime:
                    minutes = parse_hhmm_to_minutes(reg_value)
                else:
                    # Explicitly exclude weekly overtime adjustment rows.
                    continue
            else:
                minutes = duration_from_in_out(in_time, out_time)
                if minutes is None:
                    # Fallback for reports that only contain hh:mm values.
                    minutes = parse_hhmm_to_minutes(reg_value)

            if minutes is None:
                continue

            key = (current_employee, department)
            if key not in totals:
                totals[key] = 0
            totals[key] += minutes

    return totals


def build_output_path(
    input_path: Path, output_path: str | None, include_weekly_overtime: bool
) -> Path:
    if output_path:
        return Path(output_path)
    suffix = "_simple_reg.csv" if include_weekly_overtime else "_simple.csv"
    return input_path.with_name(f"{input_path.stem}{suffix}")


def write_flat_csv(output_path: Path, totals: OrderedDict[tuple[str, str], int]) -> None:
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Name", "Company", "Hours at Company"])
        for (name, company), minutes in totals.items():
            writer.writerow([name, company, format_minutes_as_hhmm(minutes)])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convert a batch timecard report to a simple table with "
            "Name, Company, and Hours at Company."
        )
    )
    parser.add_argument("input_csv", help="Path to the original batch report CSV.")
    parser.add_argument(
        "-o",
        "--output",
        help=(
            "Path for the simplified CSV. Defaults to <input>_simple_reg.csv "
            "(or <input>_simple.csv with --exclude-weekly-overtime)."
        ),
    )
    parser.add_argument(
        "--exclude-weekly-overtime",
        action="store_true",
        help=(
            "Exclude WEEKLY OVERTIME adjustment rows. "
            "By default, weekly overtime REG adjustments are included."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input_csv)
    include_weekly_overtime = not args.exclude_weekly_overtime
    output_path = build_output_path(
        input_path, args.output, include_weekly_overtime
    )
    totals = flatten_timecard(
        input_path, include_weekly_overtime=include_weekly_overtime
    )
    write_flat_csv(output_path, totals)
    print(f"Wrote {len(totals)} rows to {output_path}")


if __name__ == "__main__":
    main()
