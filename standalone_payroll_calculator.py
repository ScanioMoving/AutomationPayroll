#!/usr/bin/env python3
"""Standalone payroll calculator based on Name/Company/Hours CSV input."""

from __future__ import annotations

import argparse
import csv
import difflib
import json
import re
import sys
import unicodedata
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS = {"a": NS_MAIN}
CELL_REF_RE = re.compile(r"([A-Z]+)(\d+)")

HOME_COMPANIES = (
    "scanio_moving",
    "scanio_storage",
    "sea_and_air_intl",
    "flat_price",
)

HOME_COMPANY_LABEL = {
    "scanio_moving": "SCANIO MOVING",
    "scanio_storage": "SCANIO STORAGE",
    "sea_and_air_intl": "SEA AND AIR INT-L",
    "flat_price": "FLAT PRICE",
}

BILLED_COMPANIES = ("scanio", "sea_and_air", "flat_price")

BILLED_COMPANY_LABEL = {
    "scanio": "SCANIO",
    "sea_and_air": "SEA AND AIR",
    "flat_price": "FLAT PRICE",
}

DEFAULT_BURDEN = {
    "scanio_moving": 1.18,
    "scanio_storage": 1.24,
    "sea_and_air_intl": 1.18,
    "flat_price": 1.18,
}


@dataclass
class EmployeeConfig:
    name: str
    home_company: str
    rate: float
    burden_multiplier: float


def normalize_spaces(value: str) -> str:
    return " ".join((value or "").strip().split())


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return " ".join(token for token in text.split() if token)


def first_last(normalized_name: str) -> tuple[str, str]:
    tokens = normalized_name.split()
    if not tokens:
        return ("", "")
    return (tokens[0], tokens[-1])


def parse_cell_ref(cell_ref: str) -> tuple[str, int]:
    match = CELL_REF_RE.fullmatch(cell_ref)
    if not match:
        raise ValueError(f"Invalid cell reference: {cell_ref}")
    return match.group(1), int(match.group(2))


def get_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    strings: list[str] = []
    for item in root.findall("a:si", NS):
        text = "".join(node.text or "" for node in item.findall(".//a:t", NS))
        strings.append(text)
    return strings


def get_row_cells(row_elem: ET.Element) -> dict[str, ET.Element]:
    cells: dict[str, ET.Element] = {}
    for cell in row_elem.findall("a:c", NS):
        col, _ = parse_cell_ref(cell.attrib["r"])
        cells[col] = cell
    return cells


def get_string_cell_value(cell: ET.Element | None, shared_strings: list[str]) -> str | None:
    if cell is None or cell.attrib.get("t") != "s":
        return None
    value_node = cell.find("a:v", NS)
    if value_node is None or value_node.text is None:
        return None
    return shared_strings[int(value_node.text)]


def get_numeric_cell_value(cell: ET.Element | None) -> float | None:
    if cell is None:
        return None
    value_node = cell.find("a:v", NS)
    if value_node is None or value_node.text is None:
        return None
    try:
        return float(value_node.text)
    except ValueError:
        return None


def parse_home_company(label: str) -> str | None:
    text = normalize_text(label)
    if not text:
        return None
    if "scanio moving" in text:
        return "scanio_moving"
    if "scanio storage" in text:
        return "scanio_storage"
    if "sea" in text and "air" in text:
        return "sea_and_air_intl"
    if "flat" in text:
        return "flat_price"
    return None


def parse_billed_company(label: str) -> str | None:
    text = normalize_text(label)
    if not text:
        return None
    if "scanio" in text:
        return "scanio"
    if "sea" in text and "air" in text:
        return "sea_and_air"
    if "flat" in text:
        return "flat_price"
    return None


def parse_hour_text_to_decimal(value: str) -> float:
    text = normalize_spaces(value).replace(" ", "")
    if not text:
        return 0.0

    sign = 1.0
    if text[0] in "+-":
        if text[0] == "-":
            sign = -1.0
        text = text[1:]

    if ":" in text:
        hour_part, minute_part = text.split(":", 1)
        if hour_part == "":
            hour_part = "0"
        if minute_part == "":
            minute_part = "0"
        hours = int(hour_part)
        minutes = int(minute_part)
        return sign * (hours + minutes / 60.0)

    return sign * float(text)


def format_decimal(value: float) -> str:
    text = f"{value:.4f}".rstrip("0").rstrip(".")
    return text if text else "0"


def seed_roster_from_workbook(workbook_path: Path) -> list[EmployeeConfig]:
    with zipfile.ZipFile(workbook_path, "r") as zf:
        shared_strings = get_shared_strings(zf)
        sheet = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))

    sheet_data = sheet.find("a:sheetData", NS)
    if sheet_data is None:
        raise ValueError("Could not find sheet data in workbook.")

    roster: list[EmployeeConfig] = []
    current_home_company: str | None = None

    for row_elem in sheet_data.findall("a:row", NS):
        cells = get_row_cells(row_elem)
        label = get_string_cell_value(cells.get("B"), shared_strings)

        if label:
            home = parse_home_company(label)
            if home:
                current_home_company = home
                continue

            if normalize_text(label) == "total":
                current_home_company = None
                continue

        if current_home_company is None:
            continue

        if not label:
            continue

        rate = get_numeric_cell_value(cells.get("C"))
        if rate is None:
            continue

        roster.append(
            EmployeeConfig(
                name=normalize_spaces(label),
                home_company=current_home_company,
                rate=float(rate),
                burden_multiplier=DEFAULT_BURDEN[current_home_company],
            )
        )

    deduped: dict[str, EmployeeConfig] = {}
    for entry in roster:
        key = normalize_text(entry.name)
        deduped[key] = entry
    return list(deduped.values())


def write_roster(roster_path: Path, roster: list[EmployeeConfig]) -> None:
    payload = {
        "employees": [
            {
                "name": entry.name,
                "home_company": entry.home_company,
                "rate": entry.rate,
                "burden_multiplier": entry.burden_multiplier,
            }
            for entry in sorted(roster, key=lambda item: normalize_text(item.name))
        ]
    }
    roster_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def read_roster(roster_path: Path) -> list[EmployeeConfig]:
    data = json.loads(roster_path.read_text(encoding="utf-8"))
    employees = data.get("employees", [])
    roster: list[EmployeeConfig] = []
    for item in employees:
        home = item["home_company"]
        burden = item.get("burden_multiplier", DEFAULT_BURDEN.get(home, 1.18))
        roster.append(
            EmployeeConfig(
                name=normalize_spaces(item["name"]),
                home_company=home,
                rate=float(item["rate"]),
                burden_multiplier=float(burden),
            )
        )
    return roster


def ensure_roster(
    roster_path: Path, seed_workbook: Path | None
) -> list[EmployeeConfig]:
    if roster_path.exists():
        return read_roster(roster_path)

    if seed_workbook is None:
        raise ValueError(
            f"Roster file not found at {roster_path}. "
            "Pass --seed-workbook to generate it."
        )

    roster = seed_roster_from_workbook(seed_workbook)
    write_roster(roster_path, roster)
    return roster


def read_hours_csv(hours_csv_path: Path) -> tuple[dict[str, dict[str, float]], list[str]]:
    by_name: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    unknown_companies: list[str] = []

    with hours_csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        required = {"Name", "Company", "Hours at Company"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Hours CSV missing columns: {sorted(missing)}")

        for row in reader:
            name = normalize_spaces(row["Name"])
            if not name:
                continue

            billed_company = parse_billed_company(row["Company"])
            if billed_company is None:
                company = normalize_spaces(row["Company"])
                if company and company not in unknown_companies:
                    unknown_companies.append(company)
                continue

            hours = parse_hour_text_to_decimal(row["Hours at Company"])
            by_name[name][billed_company] += hours

    return by_name, unknown_companies


def map_source_to_roster(
    source_names: list[str], roster: list[EmployeeConfig]
) -> tuple[dict[str, EmployeeConfig], list[str]]:
    roster_by_exact: dict[str, list[EmployeeConfig]] = defaultdict(list)
    roster_by_first_last: dict[tuple[str, str], list[EmployeeConfig]] = defaultdict(list)

    for entry in roster:
        normalized = normalize_text(entry.name)
        roster_by_exact[normalized].append(entry)
        roster_by_first_last[first_last(normalized)].append(entry)

    used: set[str] = set()
    mapped: dict[str, EmployeeConfig] = {}
    unmatched: list[str] = []

    for source_name in source_names:
        normalized_source = normalize_text(source_name)
        candidates = [
            item
            for item in roster_by_exact.get(normalized_source, [])
            if normalize_text(item.name) not in used
        ]

        chosen: EmployeeConfig | None = None
        if len(candidates) == 1:
            chosen = candidates[0]
        else:
            fl = first_last(normalized_source)
            fl_candidates = [
                item
                for item in roster_by_first_last.get(fl, [])
                if normalize_text(item.name) not in used
            ]
            if len(fl_candidates) == 1:
                chosen = fl_candidates[0]
            else:
                scored: list[tuple[float, EmployeeConfig]] = []
                source_tokens = normalized_source.split()
                source_first = source_tokens[0] if source_tokens else ""
                source_last = source_tokens[-1] if source_tokens else ""

                for entry in roster:
                    key = normalize_text(entry.name)
                    if key in used:
                        continue
                    entry_norm = normalize_text(entry.name)
                    entry_tokens = entry_norm.split()
                    entry_first = entry_tokens[0] if entry_tokens else ""
                    entry_last = entry_tokens[-1] if entry_tokens else ""
                    score = difflib.SequenceMatcher(None, normalized_source, entry_norm).ratio()
                    if source_last and source_last == entry_last:
                        score += 0.08
                    if source_first and source_first == entry_first:
                        score += 0.05
                    scored.append((score, entry))

                scored.sort(key=lambda pair: pair[0], reverse=True)
                if scored:
                    best_score, best_entry = scored[0]
                    second_score = scored[1][0] if len(scored) > 1 else 0.0
                    if best_score >= 0.78 and (best_score - second_score >= 0.03):
                        chosen = best_entry

        if chosen is None:
            unmatched.append(source_name)
            continue

        mapped[source_name] = chosen
        used.add(normalize_text(chosen.name))

    return mapped, unmatched


def prompt_choice(question: str, choices: list[tuple[str, str]]) -> str:
    print(question)
    for idx, (_, label) in enumerate(choices, start=1):
        print(f"  {idx}. {label}")
    while True:
        value = input("Select option number: ").strip()
        if not value.isdigit():
            print("Please enter a number.")
            continue
        idx = int(value)
        if not (1 <= idx <= len(choices)):
            print("Invalid option.")
            continue
        return choices[idx - 1][0]


def prompt_float(question: str) -> float:
    while True:
        value = input(question).strip()
        try:
            return float(value)
        except ValueError:
            print("Please enter a numeric value.")


def resolve_unknown_names(
    unknown_names: list[str],
    roster: list[EmployeeConfig],
    non_interactive: bool,
) -> list[EmployeeConfig]:
    if not unknown_names:
        return []

    if non_interactive:
        missing = ", ".join(unknown_names)
        raise ValueError(
            "Unknown names found and --non-interactive is set: "
            f"{missing}. Add them to roster first."
        )

    if not sys.stdin.isatty():
        missing = ", ".join(unknown_names)
        raise ValueError(
            "Unknown names found but input is not interactive: "
            f"{missing}. Re-run in a terminal or use --non-interactive."
        )

    choices = [
        ("scanio_moving", "Scanio Moving"),
        ("scanio_storage", "Scanio Storage"),
        ("sea_and_air_intl", "Sea and Air Int-L"),
        ("flat_price", "Flat Price"),
    ]

    created: list[EmployeeConfig] = []
    for name in unknown_names:
        print(f"\nNew name detected: {name}")
        home_company = prompt_choice(
            "Which home company should this person be added to?", choices
        )
        rate = prompt_float("Enter hourly rate for this person: ")
        entry = EmployeeConfig(
            name=name,
            home_company=home_company,
            rate=rate,
            burden_multiplier=DEFAULT_BURDEN[home_company],
        )
        roster.append(entry)
        created.append(entry)

    return created


def build_calculation_rows(
    source_hours_by_name: dict[str, dict[str, float]],
    source_to_employee: dict[str, EmployeeConfig],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for source_name, buckets in source_hours_by_name.items():
        employee = source_to_employee[source_name]
        scanio_hours = float(buckets.get("scanio", 0.0))
        sea_and_air_hours = float(buckets.get("sea_and_air", 0.0))
        flat_price_hours = float(buckets.get("flat_price", 0.0))
        total_hours = scanio_hours + sea_and_air_hours + flat_price_hours

        regular_hours = min(total_hours, 40.0)
        overtime_hours = max(total_hours - 40.0, 0.0)

        base_pay = regular_hours * employee.rate
        overtime_premium = overtime_hours * (employee.rate * 0.5)
        total_pay = base_pay + overtime_hours * employee.rate + overtime_premium

        if total_hours > 0:
            scanio_pct = scanio_hours / total_hours
            sea_and_air_pct = sea_and_air_hours / total_hours
            flat_price_pct = flat_price_hours / total_hours
        else:
            scanio_pct = 0.0
            sea_and_air_pct = 0.0
            flat_price_pct = 0.0

        alloc_pay_scanio = total_pay * scanio_pct
        alloc_pay_sea_and_air = total_pay * sea_and_air_pct
        alloc_pay_flat_price = total_pay * flat_price_pct

        burden = employee.burden_multiplier
        alloc_cost_scanio = alloc_pay_scanio * burden
        alloc_cost_sea_and_air = alloc_pay_sea_and_air * burden
        alloc_cost_flat_price = alloc_pay_flat_price * burden

        rows.append(
            {
                "name": employee.name,
                "source_name": source_name,
                "home_company": employee.home_company,
                "rate": employee.rate,
                "scanio_hours": scanio_hours,
                "sea_and_air_hours": sea_and_air_hours,
                "flat_price_hours": flat_price_hours,
                "total_hours": total_hours,
                "regular_hours": regular_hours,
                "overtime_hours": overtime_hours,
                "base_pay": base_pay,
                "overtime_premium": overtime_premium,
                "total_pay": total_pay,
                "scanio_pct": scanio_pct,
                "sea_and_air_pct": sea_and_air_pct,
                "flat_price_pct": flat_price_pct,
                "alloc_pay_scanio": alloc_pay_scanio,
                "alloc_pay_sea_and_air": alloc_pay_sea_and_air,
                "alloc_pay_flat_price": alloc_pay_flat_price,
                "burden_multiplier": burden,
                "alloc_cost_scanio": alloc_cost_scanio,
                "alloc_cost_sea_and_air": alloc_cost_sea_and_air,
                "alloc_cost_flat_price": alloc_cost_flat_price,
            }
        )

    rows.sort(key=lambda row: (HOME_COMPANY_LABEL[row["home_company"]], row["name"].lower()))
    return rows


def write_details_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    header = [
        "Name",
        "Home Company",
        "Rate",
        "Scanio Hours",
        "Sea and Air Hours",
        "Flat Price Hours",
        "Total Hours",
        "Regular Hours",
        "Overtime Hours",
        "Base Pay",
        "Overtime Premium",
        "Total Pay",
        "Scanio %",
        "Sea and Air %",
        "Flat Price %",
        "Alloc Pay -> Scanio",
        "Alloc Pay -> Sea and Air",
        "Alloc Pay -> Flat Price",
        "Burden Multiplier",
        "Alloc Cost -> Scanio",
        "Alloc Cost -> Sea and Air",
        "Alloc Cost -> Flat Price",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for row in rows:
            writer.writerow(
                [
                    row["name"],
                    HOME_COMPANY_LABEL[row["home_company"]],
                    format_decimal(row["rate"]),
                    format_decimal(row["scanio_hours"]),
                    format_decimal(row["sea_and_air_hours"]),
                    format_decimal(row["flat_price_hours"]),
                    format_decimal(row["total_hours"]),
                    format_decimal(row["regular_hours"]),
                    format_decimal(row["overtime_hours"]),
                    format_decimal(row["base_pay"]),
                    format_decimal(row["overtime_premium"]),
                    format_decimal(row["total_pay"]),
                    format_decimal(row["scanio_pct"]),
                    format_decimal(row["sea_and_air_pct"]),
                    format_decimal(row["flat_price_pct"]),
                    format_decimal(row["alloc_pay_scanio"]),
                    format_decimal(row["alloc_pay_sea_and_air"]),
                    format_decimal(row["alloc_pay_flat_price"]),
                    format_decimal(row["burden_multiplier"]),
                    format_decimal(row["alloc_cost_scanio"]),
                    format_decimal(row["alloc_cost_sea_and_air"]),
                    format_decimal(row["alloc_cost_flat_price"]),
                ]
            )


def write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    totals_by_home: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    alloc_cost_by_home_to_billed: dict[str, dict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    employees_by_home: dict[str, int] = defaultdict(int)

    for row in rows:
        home = row["home_company"]
        employees_by_home[home] += 1
        totals_by_home[home]["total_hours"] += row["total_hours"]
        totals_by_home[home]["total_pay"] += row["total_pay"]
        alloc_cost_by_home_to_billed[home]["scanio"] += row["alloc_cost_scanio"]
        alloc_cost_by_home_to_billed[home]["sea_and_air"] += row["alloc_cost_sea_and_air"]
        alloc_cost_by_home_to_billed[home]["flat_price"] += row["alloc_cost_flat_price"]

    scanio_to_sea = (
        alloc_cost_by_home_to_billed["sea_and_air_intl"]["scanio"]
        - alloc_cost_by_home_to_billed["scanio_moving"]["sea_and_air"]
        - alloc_cost_by_home_to_billed["scanio_storage"]["sea_and_air"]
    )
    scanio_to_flat = (
        alloc_cost_by_home_to_billed["flat_price"]["scanio"]
        - alloc_cost_by_home_to_billed["scanio_moving"]["flat_price"]
        - alloc_cost_by_home_to_billed["scanio_storage"]["flat_price"]
    )
    sea_to_flat = (
        alloc_cost_by_home_to_billed["flat_price"]["sea_and_air"]
        - alloc_cost_by_home_to_billed["sea_and_air_intl"]["flat_price"]
    )

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "Home Company",
                "Employee Count",
                "Total Hours",
                "Total Pay",
                "Cost Attributed to Scanio",
                "Cost Attributed to Sea and Air",
                "Cost Attributed to Flat Price",
            ]
        )
        for home in HOME_COMPANIES:
            writer.writerow(
                [
                    HOME_COMPANY_LABEL[home],
                    employees_by_home.get(home, 0),
                    format_decimal(totals_by_home[home]["total_hours"]),
                    format_decimal(totals_by_home[home]["total_pay"]),
                    format_decimal(alloc_cost_by_home_to_billed[home]["scanio"]),
                    format_decimal(alloc_cost_by_home_to_billed[home]["sea_and_air"]),
                    format_decimal(alloc_cost_by_home_to_billed[home]["flat_price"]),
                ]
            )

        writer.writerow([])
        writer.writerow(["Reimbursement Balances", "Amount"])
        writer.writerow(["Due from Scanio to Sea & Air", format_decimal(scanio_to_sea)])
        writer.writerow(["Due from Scanio to Flat Price", format_decimal(scanio_to_flat)])
        writer.writerow(["Due from Sea & Air to Flat Price", format_decimal(sea_to_flat)])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Standalone payroll calculations from simplified CSV "
            "(Name, Company, Hours at Company)."
        )
    )
    parser.add_argument(
        "hours_csv",
        help="Input hours CSV (for example: Batch_Report_..._simple_reg.csv).",
    )
    parser.add_argument(
        "--roster",
        default="payroll_roster.json",
        help="Path to roster JSON (default: payroll_roster.json).",
    )
    parser.add_argument(
        "--seed-workbook",
        help=(
            "Workbook used to seed roster if roster file does not exist. "
            "Rates/home companies are read from sheet1."
        ),
    )
    parser.add_argument(
        "--details-output",
        help="Output CSV for employee-level payroll details.",
    )
    parser.add_argument(
        "--summary-output",
        help="Output CSV for company-level summary and reimbursements.",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Fail if unknown names are found instead of prompting.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    hours_csv_path = Path(args.hours_csv)
    roster_path = Path(args.roster)
    seed_workbook_path = Path(args.seed_workbook) if args.seed_workbook else None

    details_output = (
        Path(args.details_output)
        if args.details_output
        else hours_csv_path.with_name(f"{hours_csv_path.stem}_payroll_details.csv")
    )
    summary_output = (
        Path(args.summary_output)
        if args.summary_output
        else hours_csv_path.with_name(f"{hours_csv_path.stem}_payroll_summary.csv")
    )

    roster = ensure_roster(roster_path, seed_workbook_path)
    source_hours_by_name, unknown_companies = read_hours_csv(hours_csv_path)
    source_names = list(source_hours_by_name.keys())

    source_to_roster, unmatched_names = map_source_to_roster(source_names, roster)
    if unmatched_names:
        created = resolve_unknown_names(unmatched_names, roster, args.non_interactive)
        if created:
            write_roster(roster_path, roster)
        source_to_roster, still_unmatched = map_source_to_roster(source_names, roster)
        if still_unmatched:
            missing = ", ".join(still_unmatched)
            raise ValueError(f"Could not map names after update: {missing}")

    rows = build_calculation_rows(source_hours_by_name, source_to_roster)
    write_details_csv(details_output, rows)
    write_summary_csv(summary_output, rows)

    print(f"Wrote details CSV: {details_output}")
    print(f"Wrote summary CSV: {summary_output}")
    print(f"Mapped names: {len(source_to_roster)}/{len(source_names)}")

    if unknown_companies:
        print("Unknown billed companies skipped from input:")
        for company in unknown_companies:
            print(f"  - {company}")


if __name__ == "__main__":
    main()
