#!/usr/bin/env python3
"""Fill payroll workbook hours and optional tips from CSV inputs."""

from __future__ import annotations

import argparse
import csv
import difflib
import json
import re
import unicodedata
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS = {"a": NS_MAIN}
CELL_REF_RE = re.compile(r"([A-Z]+)(\d+)")

COMPANY_TO_COLUMN = {
    "scanio": "K",
    "sea_and_air": "M",
    "flat_price": "O",
}

TIP_SOURCE_TO_COMMISSION_COLUMN = {
    "scanio": "H",
    "sea_and_air": "I",
    "flat_price": "J",
}

HOME_COMPANY_TO_TIP_SOURCE = {
    "scanio_moving": "scanio",
    "scanio_storage": "scanio",
    "sea_and_air_intl": "sea_and_air",
    "flat_price": "flat_price",
}

B101_SAFE_FORMULA = (
    'IF(C93=0,"No Scanio/SeaAir Reimbursement",'
    'IF(C93>0,"Scanio Owes Sea & Air","Sea & Air Owes Scanio"))'
)

COMPANY_ROW_SLOTS = {
    "scanio_moving": list(range(5, 26)),
    "scanio_storage": list(range(33, 40)),
    "sea_and_air_intl": list(range(47, 57)),
    "flat_price": list(range(64, 86)),
}

def normalize_spaces(value: str) -> str:
    return " ".join((value or "").strip().split())


def normalize_company(company: str) -> str | None:
    text = normalize_spaces(company).upper()
    if not text:
        return None
    if "SCANIO" in text:
        return "scanio"
    if "SEA" in text and "AIR" in text:
        return "sea_and_air"
    if "FLAT" in text:
        return "flat_price"
    return None


def parse_home_company_label(value: str) -> str | None:
    text = normalize_spaces(value).upper()
    if not text:
        return None
    if "SCANIO MOVING" in text:
        return "scanio_moving"
    if "SCANIO STORAGE" in text:
        return "scanio_storage"
    if "SEA" in text and "AIR" in text:
        return "sea_and_air_intl"
    if "FLAT" in text:
        return "flat_price"
    return None


def normalize_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "")
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    tokens = [token for token in text.split() if token]
    return " ".join(tokens)


def name_first_last(normalized_name: str) -> tuple[str, str]:
    tokens = normalized_name.split()
    if not tokens:
        return ("", "")
    return (tokens[0], tokens[-1])


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


def parse_number(value: str) -> float:
    text = normalize_spaces(value).replace("$", "").replace(",", "")
    if not text:
        return 0.0
    return float(text)


def parse_tip_source_from_note(note: str) -> str | None:
    text = normalize_spaces(note).lower()
    if not text:
        return None

    tokens = re.findall(r"[a-z]+", text)
    for token in tokens:
        if token in {"sc", "scanio"}:
            return "scanio"
        if token in {"sa"}:
            return "sea_and_air"
        if token in {"fp"}:
            return "flat_price"

    if "sea" in text and "air" in text:
        return "sea_and_air"
    if "flat" in text:
        return "flat_price"
    if "scanio" in text:
        return "scanio"
    return None


def pick_fallback_tip_source(
    tip_breakdown: dict[str, float], home_company: str
) -> str:
    nonzero_sources = [
        (source, amount) for source, amount in tip_breakdown.items() if abs(amount) > 1e-9
    ]
    if nonzero_sources:
        nonzero_sources.sort(key=lambda pair: pair[1], reverse=True)
        return nonzero_sources[0][0]
    return HOME_COMPANY_TO_TIP_SOURCE[home_company]


def format_decimal_for_excel(value: float) -> str:
    if abs(value) < 1e-12:
        return "0"
    text = f"{value:.10f}".rstrip("0").rstrip(".")
    return text if text else "0"


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
        column, _ = parse_cell_ref(cell.attrib["r"])
        cells[column] = cell
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


def set_numeric_cell(row_elem: ET.Element, row_number: int, column: str, value: float) -> None:
    target_ref = f"{column}{row_number}"
    target_cell: ET.Element | None = None
    for cell in row_elem.findall("a:c", NS):
        if cell.attrib.get("r") == target_ref:
            target_cell = cell
            break

    if target_cell is None:
        target_cell = ET.SubElement(row_elem, f"{{{NS_MAIN}}}c", {"r": target_ref})

    if "t" in target_cell.attrib:
        del target_cell.attrib["t"]

    for child in list(target_cell):
        if child.tag in {f"{{{NS_MAIN}}}v", f"{{{NS_MAIN}}}f", f"{{{NS_MAIN}}}is"}:
            target_cell.remove(child)

    value_node = ET.SubElement(target_cell, f"{{{NS_MAIN}}}v")
    value_node.text = format_decimal_for_excel(value)


def get_or_create_row(sheet_data: ET.Element, row_number: int) -> ET.Element:
    for row_elem in sheet_data.findall("a:row", NS):
        if int(row_elem.attrib.get("r", "0")) == row_number:
            return row_elem
    return ET.SubElement(sheet_data, f"{{{NS_MAIN}}}row", {"r": str(row_number)})


def set_formula_string_cell(
    sheet_data: ET.Element, row_number: int, column: str, formula: str
) -> None:
    row_elem = get_or_create_row(sheet_data, row_number)
    target_ref = f"{column}{row_number}"
    target_cell: ET.Element | None = None
    for cell in row_elem.findall("a:c", NS):
        if cell.attrib.get("r") == target_ref:
            target_cell = cell
            break

    if target_cell is None:
        target_cell = ET.SubElement(row_elem, f"{{{NS_MAIN}}}c", {"r": target_ref})

    target_cell.attrib["t"] = "str"
    for child in list(target_cell):
        if child.tag in {f"{{{NS_MAIN}}}v", f"{{{NS_MAIN}}}f", f"{{{NS_MAIN}}}is"}:
            target_cell.remove(child)

    formula_node = ET.SubElement(target_cell, f"{{{NS_MAIN}}}f")
    formula_node.text = formula


def set_inline_string_cell(row_elem: ET.Element, row_number: int, column: str, value: str) -> None:
    target_ref = f"{column}{row_number}"
    target_cell: ET.Element | None = None
    for cell in row_elem.findall("a:c", NS):
        if cell.attrib.get("r") == target_ref:
            target_cell = cell
            break

    if target_cell is None:
        target_cell = ET.SubElement(row_elem, f"{{{NS_MAIN}}}c", {"r": target_ref})

    target_cell.attrib["t"] = "inlineStr"

    for child in list(target_cell):
        if child.tag in {
            f"{{{NS_MAIN}}}v",
            f"{{{NS_MAIN}}}f",
            f"{{{NS_MAIN}}}is",
        }:
            target_cell.remove(child)

    is_node = ET.SubElement(target_cell, f"{{{NS_MAIN}}}is")
    text_node = ET.SubElement(is_node, f"{{{NS_MAIN}}}t")
    text_node.text = value


def load_hours_csv(csv_path: Path) -> tuple[dict[str, dict[str, float]], list[str]]:
    totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    unknown_companies: list[str] = []

    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        required = {"Name", "Company", "Hours at Company"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV is missing required columns: {sorted(missing)}")

        for row in reader:
            raw_name = normalize_spaces(row["Name"])
            if not raw_name:
                continue

            bucket = normalize_company(row["Company"])
            if bucket is None:
                company = normalize_spaces(row["Company"])
                if company and company not in unknown_companies:
                    unknown_companies.append(company)
                continue

            hours = parse_hour_text_to_decimal(row["Hours at Company"])
            totals[raw_name][bucket] += hours

    return totals, unknown_companies


def load_roster(roster_path: Path) -> list[dict[str, Any]]:
    payload = json.loads(roster_path.read_text(encoding="utf-8"))
    employees = payload.get("employees", [])
    result: list[dict[str, Any]] = []
    for employee in employees:
        name = normalize_spaces(employee.get("name", ""))
        home_company = employee.get("home_company")
        rate = employee.get("rate")
        if (
            not name
            or home_company not in COMPANY_ROW_SLOTS
            or not isinstance(rate, (int, float))
        ):
            continue
        result.append({"name": name, "home_company": home_company, "rate": float(rate)})
    return result


def build_employee_rows_from_roster(
    sheet_data: ET.Element, roster_entries: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in roster_entries:
        grouped[entry["home_company"]].append(entry)

    for company in grouped:
        grouped[company].sort(key=lambda item: normalize_name(item["name"]))

    employee_rows: list[dict[str, Any]] = []
    fill_columns = {"G", "H", "I", "J", "K", "M", "O"}

    for company, slots in COMPANY_ROW_SLOTS.items():
        company_entries = grouped.get(company, [])
        if len(company_entries) > len(slots):
            raise ValueError(
                f"Roster has {len(company_entries)} employees for {company}, "
                f"but template supports only {len(slots)} rows."
            )

        for idx, row_number in enumerate(slots):
            row_elem = get_or_create_row(sheet_data, row_number)
            if idx < len(company_entries):
                entry = company_entries[idx]
                set_inline_string_cell(row_elem, row_number, "B", entry["name"])
                set_numeric_cell(row_elem, row_number, "C", entry["rate"])
                employee_rows.append(
                    {
                        "row_number": row_number,
                        "row_elem": row_elem,
                        "workbook_name": entry["name"],
                        "home_company": company,
                    }
                )
            else:
                set_inline_string_cell(row_elem, row_number, "B", "")
                set_numeric_cell(row_elem, row_number, "C", 0.0)
                for column in fill_columns:
                    set_numeric_cell(row_elem, row_number, column, 0.0)

    return employee_rows


def load_tips_csv(
    tips_csv_path: Path,
) -> tuple[dict[str, float], dict[str, dict[str, float]], list[str]]:
    totals: dict[str, float] = defaultdict(float)
    totals_by_source: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    unknown_tip_notes: list[str] = []
    format_mode: str | None = None

    with tips_csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        for raw_row in reader:
            row = list(raw_row)
            if len(row) < 10:
                row += [""] * (10 - len(row))

            first = normalize_spaces(row[0])
            second = normalize_spaces(row[1])

            if first.upper() == "NAME" and second.upper() == "COMMISSION":
                format_mode = "simple"
                continue

            if first.upper() == "EMP L NAME" and second.upper() == "EMP F NAME":
                format_mode = "raw"
                continue

            if format_mode == "simple":
                if not first:
                    continue
                try:
                    amount = parse_number(second)
                except ValueError:
                    continue
                totals[first] += amount
                continue

            if format_mode == "raw":
                if not first or not second:
                    continue
                name = normalize_spaces(f"{second} {first}")
                try:
                    cash_tips = parse_number(row[5])
                except ValueError:
                    cash_tips = 0.0
                try:
                    card_tips = parse_number(row[7])
                except ValueError:
                    card_tips = 0.0
                amount = cash_tips + card_tips
                totals[name] += amount

                note = normalize_spaces(row[3])
                source = parse_tip_source_from_note(note)
                if source is None:
                    marker = f"{name} | NOTE: {note if note else '<blank>'}"
                    if marker not in unknown_tip_notes:
                        unknown_tip_notes.append(marker)
                    continue
                totals_by_source[name][source] += amount

    return (
        dict(totals),
        {name: dict(source_totals) for name, source_totals in totals_by_source.items()},
        unknown_tip_notes,
    )


def write_tip_summary_csv(summary_csv_path: Path, tip_totals: dict[str, float]) -> None:
    with summary_csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Name", "Commission"])
        for name in sorted(tip_totals.keys(), key=lambda value: normalize_name(value)):
            writer.writerow([name, f"{tip_totals[name]:.2f}"])


def match_names(
    workbook_names: list[str], source_names: list[str]
) -> tuple[dict[str, str], list[str]]:
    normalized_to_workbook: dict[str, list[str]] = defaultdict(list)
    first_last_to_workbook: dict[tuple[str, str], list[str]] = defaultdict(list)

    for workbook_name in workbook_names:
        normalized = normalize_name(workbook_name)
        normalized_to_workbook[normalized].append(workbook_name)
        first_last_to_workbook[name_first_last(normalized)].append(workbook_name)

    used_workbook_names: set[str] = set()
    source_to_workbook: dict[str, str] = {}
    unmatched_sources: list[str] = []

    for source_name in source_names:
        normalized_source = normalize_name(source_name)
        exact_matches = [
            candidate
            for candidate in normalized_to_workbook.get(normalized_source, [])
            if candidate not in used_workbook_names
        ]

        chosen: str | None = None
        if len(exact_matches) == 1:
            chosen = exact_matches[0]
        else:
            source_key = name_first_last(normalized_source)
            first_last_matches = [
                candidate
                for candidate in first_last_to_workbook.get(source_key, [])
                if candidate not in used_workbook_names
            ]
            if len(first_last_matches) == 1:
                chosen = first_last_matches[0]
            else:
                source_tokens = normalized_source.split()
                source_last = source_tokens[-1] if source_tokens else ""
                source_first = source_tokens[0] if source_tokens else ""

                scored: list[tuple[float, str]] = []
                for workbook_name in workbook_names:
                    if workbook_name in used_workbook_names:
                        continue
                    normalized_workbook = normalize_name(workbook_name)
                    workbook_tokens = normalized_workbook.split()
                    workbook_first = workbook_tokens[0] if workbook_tokens else ""
                    workbook_last = workbook_tokens[-1] if workbook_tokens else ""
                    score = difflib.SequenceMatcher(
                        None, normalized_source, normalized_workbook
                    ).ratio()
                    if source_last and source_last == workbook_last:
                        score += 0.08
                    if source_first and source_first == workbook_first:
                        score += 0.05
                    scored.append((score, workbook_name))

                scored.sort(reverse=True)
                if scored:
                    best_score, best_name = scored[0]
                    second_score = scored[1][0] if len(scored) > 1 else 0.0
                    # Require both minimum quality and separation to avoid bad auto-matches.
                    if best_score >= 0.78 and (best_score - second_score >= 0.03):
                        chosen = best_name

        if chosen is None:
            unmatched_sources.append(source_name)
            continue

        source_to_workbook[source_name] = chosen
        used_workbook_names.add(chosen)

    return source_to_workbook, unmatched_sources


def ensure_recalc_on_open(workbook_xml: ET.Element) -> None:
    calc = workbook_xml.find("a:calcPr", NS)
    if calc is None:
        calc = ET.SubElement(workbook_xml, f"{{{NS_MAIN}}}calcPr")
    calc.set("fullCalcOnLoad", "1")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fill payroll workbook company-hour columns using a simplified "
            "Name/Company/Hours CSV. Optionally also fill commissions from a tip report."
        )
    )
    parser.add_argument("workbook", help="Path to payroll .xlsx template/workbook.")
    parser.add_argument("hours_csv", help="Path to simplified CSV (simple_reg style).")
    parser.add_argument(
        "--roster",
        help=(
            "Optional roster JSON (employees with name/home_company/rate). "
            "If provided, workbook employee rows are rebuilt from roster."
        ),
    )
    parser.add_argument(
        "--tips-csv",
        help=(
            "Optional tips CSV. Accepts the raw tip report format or a simple "
            "Name,Commission CSV."
        ),
    )
    parser.add_argument(
        "--tip-summary-output",
        help=(
            "Optional output path for simplified tip totals CSV (Name,Commission). "
            "Defaults to <tips_csv>_simple.csv when --tips-csv is provided."
        ),
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output .xlsx path. Defaults to <workbook>_filled.xlsx",
    )
    return parser.parse_args()


def fill_workbook(
    workbook_path: Path,
    hours_csv_path: Path,
    output_path: Path,
    roster_path: Path | None = None,
    tips_csv_path: Path | None = None,
    tip_summary_output_path: Path | None = None,
) -> dict[str, Any]:
    workbook_path = Path(workbook_path)
    hours_csv_path = Path(hours_csv_path)
    output_path = Path(output_path)
    roster_path = Path(roster_path) if roster_path else None
    tips_csv_path = Path(tips_csv_path) if tips_csv_path else None
    tip_summary_output_path = Path(tip_summary_output_path) if tip_summary_output_path else None

    hours_by_source_name, unknown_companies = load_hours_csv(hours_csv_path)
    source_names = list(hours_by_source_name.keys())
    (
        tip_totals_by_source_name,
        tip_source_breakdown_by_source_name,
        unknown_tip_notes,
    ) = load_tips_csv(tips_csv_path) if tips_csv_path else ({}, {}, [])
    tip_source_names = list(tip_totals_by_source_name.keys())

    with zipfile.ZipFile(workbook_path, "r") as zin:
        shared_strings = get_shared_strings(zin)
        sheet1_root = ET.fromstring(zin.read("xl/worksheets/sheet1.xml"))
        workbook_root = ET.fromstring(zin.read("xl/workbook.xml"))

        sheet_data = sheet1_root.find("a:sheetData", NS)
        if sheet_data is None:
            raise ValueError("Could not find sheetData in xl/worksheets/sheet1.xml")

        if roster_path:
            roster_entries = load_roster(roster_path)
            employee_rows = build_employee_rows_from_roster(sheet_data, roster_entries)
        else:
            employee_rows = []
            current_home_company: str | None = None
            for row_elem in sheet_data.findall("a:row", NS):
                row_number = int(row_elem.attrib["r"])
                cell_map = get_row_cells(row_elem)

                section_label = get_string_cell_value(cell_map.get("B"), shared_strings)
                if section_label:
                    parsed_home_company = parse_home_company_label(section_label)
                    if parsed_home_company:
                        current_home_company = parsed_home_company
                    elif normalize_name(section_label) == "total":
                        current_home_company = None

                workbook_name = get_string_cell_value(cell_map.get("B"), shared_strings)
                rate = get_numeric_cell_value(cell_map.get("C"))
                if workbook_name and rate is not None and current_home_company:
                    employee_rows.append(
                        {
                            "row_number": row_number,
                            "row_elem": row_elem,
                            "workbook_name": workbook_name,
                            "home_company": current_home_company,
                        }
                    )

        workbook_names = [entry["workbook_name"] for entry in employee_rows]
        source_to_workbook, unmatched_sources = match_names(workbook_names, source_names)
        workbook_to_source = {workbook: source for source, workbook in source_to_workbook.items()}
        tip_source_to_workbook, unmatched_tip_sources = match_names(
            workbook_names, tip_source_names
        )
        workbook_to_tip_source = {
            workbook: source for source, workbook in tip_source_to_workbook.items()
        }

        if tip_summary_output_path:
            canonical_tip_totals: dict[str, float] = defaultdict(float)
            for source_name, amount in tip_totals_by_source_name.items():
                mapped_name = tip_source_to_workbook.get(source_name, source_name)
                canonical_tip_totals[mapped_name] += amount
            write_tip_summary_csv(tip_summary_output_path, canonical_tip_totals)

        for entry in employee_rows:
            workbook_name = entry["workbook_name"]
            row_number = entry["row_number"]
            row_elem = entry["row_elem"]

            source_name = workbook_to_source.get(workbook_name)
            buckets = hours_by_source_name.get(source_name, {}) if source_name else {}

            set_numeric_cell(
                row_elem, row_number, COMPANY_TO_COLUMN["scanio"], buckets.get("scanio", 0.0)
            )
            set_numeric_cell(
                row_elem,
                row_number,
                COMPANY_TO_COLUMN["sea_and_air"],
                buckets.get("sea_and_air", 0.0),
            )
            set_numeric_cell(
                row_elem,
                row_number,
                COMPANY_TO_COLUMN["flat_price"],
                buckets.get("flat_price", 0.0),
            )

            if tip_totals_by_source_name:
                tip_source_name = workbook_to_tip_source.get(workbook_name)
                tip_total = (
                    tip_totals_by_source_name.get(tip_source_name, 0.0)
                    if tip_source_name
                    else 0.0
                )
                tip_breakdown = (
                    dict(tip_source_breakdown_by_source_name.get(tip_source_name, {}))
                    if tip_source_name
                    else {}
                )

                assigned_amount = sum(tip_breakdown.values())
                remainder = tip_total - assigned_amount
                if abs(remainder) > 1e-9:
                    fallback_source = pick_fallback_tip_source(
                        tip_breakdown, entry["home_company"]
                    )
                    tip_breakdown[fallback_source] = (
                        tip_breakdown.get(fallback_source, 0.0) + remainder
                    )

                for source_key, column in TIP_SOURCE_TO_COMMISSION_COLUMN.items():
                    set_numeric_cell(row_elem, row_number, column, tip_breakdown.get(source_key, 0.0))

                # Keep employee total commission in the existing "comm" column.
                set_numeric_cell(row_elem, row_number, "G", tip_total)

        # Replace Google Sheets-only formula with Excel-compatible IF formula.
        set_formula_string_cell(sheet_data, 101, "B", B101_SAFE_FORMULA)

        ensure_recalc_on_open(workbook_root)

        sheet1_bytes = ET.tostring(sheet1_root, encoding="utf-8", xml_declaration=True)
        workbook_bytes = ET.tostring(workbook_root, encoding="utf-8", xml_declaration=True)

        with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename == "xl/worksheets/sheet1.xml":
                    data = sheet1_bytes
                elif item.filename == "xl/workbook.xml":
                    data = workbook_bytes
                zout.writestr(item, data)

    return {
        "output_path": output_path,
        "tip_summary_output_path": tip_summary_output_path,
        "source_to_workbook": source_to_workbook,
        "source_names": source_names,
        "tip_source_to_workbook": tip_source_to_workbook,
        "tip_source_names": tip_source_names,
        "tip_totals_by_source_name": tip_totals_by_source_name,
        "unmatched_sources": unmatched_sources,
        "unmatched_tip_sources": unmatched_tip_sources,
        "unknown_tip_notes": unknown_tip_notes,
        "unknown_companies": unknown_companies,
        "roster_path": roster_path,
    }


def main() -> None:
    args = parse_args()
    workbook_path = Path(args.workbook)
    hours_csv_path = Path(args.hours_csv)
    roster_path = Path(args.roster) if args.roster else None
    tips_csv_path = Path(args.tips_csv) if args.tips_csv else None
    tip_summary_output_path = (
        Path(args.tip_summary_output)
        if args.tip_summary_output
        else (
            tips_csv_path.with_name(f"{tips_csv_path.stem}_simple.csv")
            if tips_csv_path
            else None
        )
    )
    output_path = (
        Path(args.output)
        if args.output
        else workbook_path.with_name(f"{workbook_path.stem}_filled.xlsx")
    )

    result = fill_workbook(
        workbook_path=workbook_path,
        hours_csv_path=hours_csv_path,
        output_path=output_path,
        roster_path=roster_path,
        tips_csv_path=tips_csv_path,
        tip_summary_output_path=tip_summary_output_path,
    )

    print(f"Wrote filled workbook: {result['output_path']}")
    print(
        f"Matched hour names: "
        f"{len(result['source_to_workbook'])}/{len(result['source_names'])}"
    )

    if result["tip_totals_by_source_name"]:
        print(
            f"Matched tip names: "
            f"{len(result['tip_source_to_workbook'])}/{len(result['tip_source_names'])}"
        )
        print("Filled commissions by source in H/I/J and total commission in G.")
        if result["tip_summary_output_path"]:
            print(f"Wrote simplified tip CSV: {result['tip_summary_output_path']}")

    if result["unmatched_sources"]:
        print("Unmatched source names (not found in workbook):")
        for name in result["unmatched_sources"]:
            print(f"  - {name}")

    if result["unmatched_tip_sources"]:
        print("Unmatched tip names (not found in workbook):")
        for name in result["unmatched_tip_sources"]:
            print(f"  - {name}")

    if result["unknown_tip_notes"]:
        print(
            "Tip rows with unrecognized NOTE source "
            "(assigned to employee's existing tip company; otherwise home company):"
        )
        for note_line in result["unknown_tip_notes"]:
            print(f"  - {note_line}")

    if result["unknown_companies"]:
        print("Unknown companies skipped:")
        for company in result["unknown_companies"]:
            print(f"  - {company}")

    if result["roster_path"]:
        print(f"Roster applied: {result['roster_path']}")


if __name__ == "__main__":
    main()
