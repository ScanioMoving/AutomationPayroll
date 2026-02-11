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
from copy import deepcopy
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS = {"a": NS_MAIN}
NS_REL = "http://schemas.openxmlformats.org/package/2006/relationships"
CALC_CHAIN_REL_TYPE = (
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/calcChain"
)
CELL_REF_RE = re.compile(r"([A-Z]+)(\d+)")
NOTE_AMOUNT_RE = re.compile(r"(?<![A-Za-z0-9])(?:\$)?(\d+(?:\.\d+)?)")
SHEET_DATA_XML_RE = re.compile(
    r"<(?:\w+:)?sheetData\b[^>]*>.*?</(?:\w+:)?sheetData>", re.DOTALL
)
CALC_PR_XML_RE = re.compile(
    r"<(?:\w+:)?calcPr\b[^>]*(?:/>|>.*?</(?:\w+:)?calcPr>)", re.DOTALL
)
FORMULA_CELL_REF_RE = re.compile(r"(\$?)([A-Z]{1,3})(\$?)(\d+)")

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

def reimbursement_status_formula(due_row: int) -> str:
    return (
        f'IF(C{due_row}=0,"No Scanio/SeaAir Reimbursement",'
        f'IF(C{due_row}>0,"Scanio Owes Sea & Air","Sea & Air Owes Scanio"))'
    )

COMPANY_ROW_SLOTS = {
    "scanio_moving": list(range(5, 26)),
    "scanio_storage": list(range(33, 40)),
    "sea_and_air_intl": list(range(47, 57)),
    "flat_price": list(range(64, 86)),
}

COMPANY_BURDEN_MULTIPLIER = {
    "scanio_moving": 1.18,
    "scanio_storage": 1.24,
    "sea_and_air_intl": 1.18,
    "flat_price": 1.18,
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


def parse_tip_amount_from_note(note: str) -> float | None:
    text = normalize_spaces(note).lower()
    if not text:
        return None

    # Support note-only entries such as "sc insu 15 mats @ 48.26".
    if not any(keyword in text for keyword in ("insu", "ins", "mat", "mats", "@")):
        return None

    matches = NOTE_AMOUNT_RE.findall(text)
    if not matches:
        return None

    values = [float(value) for value in matches]
    total = sum(values)
    return total if total > 0 else None


def parse_tip_source_from_note(note: str) -> str | None:
    text = normalize_spaces(note).lower()
    if not text:
        return None

    if "long island" in text or "montia" in text:
        return "flat_price"

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


def column_index(column: str) -> int:
    value = 0
    for char in column:
        value = value * 26 + (ord(char) - ord("A") + 1)
    return value


def insert_cell_in_order(row_elem: ET.Element, new_cell: ET.Element, target_column: str) -> None:
    target_idx = column_index(target_column)
    children = list(row_elem)
    for child_idx, child in enumerate(children):
        if child.tag != f"{{{NS_MAIN}}}c":
            continue
        ref = child.attrib.get("r", "")
        match = CELL_REF_RE.fullmatch(ref)
        if not match:
            continue
        if column_index(match.group(1)) > target_idx:
            row_elem.insert(child_idx, new_cell)
            return
    row_elem.append(new_cell)


def shift_formula_for_row_insert(formula: str, start_row: int, delta: int) -> str:
    if not formula or delta == 0:
        return formula

    def repl(match: re.Match[str]) -> str:
        col_abs, col, row_abs, row_text = match.groups()
        row_number = int(row_text)
        if row_number >= start_row:
            row_number += delta
        return f"{col_abs}{col}{row_abs}{row_number}"

    return FORMULA_CELL_REF_RE.sub(repl, formula)


def shift_formula_for_row_copy(formula: str, delta: int) -> str:
    if not formula or delta == 0:
        return formula

    def repl(match: re.Match[str]) -> str:
        col_abs, col, row_abs, row_text = match.groups()
        row_number = int(row_text)
        if not row_abs:
            row_number += delta
        return f"{col_abs}{col}{row_abs}{row_number}"

    return FORMULA_CELL_REF_RE.sub(repl, formula)


def shift_ref_rows(ref_text: str, start_row: int, delta: int) -> str:
    if not ref_text or delta == 0:
        return ref_text

    parts = ref_text.split(":", 1)
    adjusted: list[str] = []
    for part in parts:
        match = FORMULA_CELL_REF_RE.fullmatch(part)
        if not match:
            adjusted.append(part)
            continue
        col_abs, col, row_abs, row_text = match.groups()
        row_number = int(row_text)
        if row_number >= start_row:
            row_number += delta
        adjusted.append(f"{col_abs}{col}{row_abs}{row_number}")

    return ":".join(adjusted)


def shift_rows_in_sheet(sheet_root: ET.Element, start_row: int, delta: int) -> None:
    if delta == 0:
        return

    sheet_data = sheet_root.find("a:sheetData", NS)
    if sheet_data is None:
        return

    rows = sheet_data.findall("a:row", NS)
    rows_sorted = sorted(
        rows,
        key=lambda node: int(node.attrib.get("r", "0")),
        reverse=delta > 0,
    )
    for row_elem in rows_sorted:
        row_number = int(row_elem.attrib.get("r", "0"))
        if row_number >= start_row:
            row_elem.attrib["r"] = str(row_number + delta)
        for cell in row_elem.findall("a:c", NS):
            ref = cell.attrib.get("r")
            if ref:
                col, cell_row = parse_cell_ref(ref)
                if cell_row >= start_row:
                    cell.attrib["r"] = f"{col}{cell_row + delta}"
            formula = cell.find("a:f", NS)
            if formula is not None and formula.text:
                formula.text = shift_formula_for_row_insert(formula.text, start_row, delta)
            formula_ref = formula.attrib.get("ref") if formula is not None else None
            if formula is not None and formula_ref:
                formula.attrib["ref"] = shift_ref_rows(formula_ref, start_row, delta)

    merge_cells = sheet_root.find("a:mergeCells", NS)
    if merge_cells is not None:
        for merge_cell in merge_cells.findall("a:mergeCell", NS):
            ref = merge_cell.attrib.get("ref")
            if ref:
                merge_cell.attrib["ref"] = shift_ref_rows(ref, start_row, delta)

    dimension = sheet_root.find("a:dimension", NS)
    if dimension is not None:
        ref = dimension.attrib.get("ref")
        if ref:
            dimension.attrib["ref"] = shift_ref_rows(ref, start_row, delta)


def insert_row_in_order(sheet_data: ET.Element, row_elem: ET.Element) -> None:
    target_row = int(row_elem.attrib.get("r", "0"))
    children = list(sheet_data)
    for child_idx, child in enumerate(children):
        if child.tag != f"{{{NS_MAIN}}}row":
            continue
        existing_row = int(child.attrib.get("r", "0"))
        if existing_row > target_row:
            sheet_data.insert(child_idx, row_elem)
            return
    sheet_data.append(row_elem)


def clone_template_row(sheet_data: ET.Element, template_row: ET.Element, target_row: int) -> None:
    source_row = int(template_row.attrib.get("r", "0"))
    row_clone = deepcopy(template_row)
    row_clone.attrib["r"] = str(target_row)

    delta = target_row - source_row
    for cell in row_clone.findall("a:c", NS):
        ref = cell.attrib.get("r")
        if ref:
            col, _ = parse_cell_ref(ref)
            cell.attrib["r"] = f"{col}{target_row}"
        formula = cell.find("a:f", NS)
        if formula is not None and formula.text:
            formula.text = shift_formula_for_row_copy(formula.text, delta)
            formula.attrib.pop("ref", None)
        value_node = cell.find("a:v", NS)
        if formula is not None and value_node is not None:
            cell.remove(value_node)

    insert_row_in_order(sheet_data, row_clone)


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


def set_numeric_cell(
    row_elem: ET.Element,
    row_number: int,
    column: str,
    value: float,
    preserve_formula: bool = False,
) -> None:
    target_ref = f"{column}{row_number}"
    target_cell: ET.Element | None = None
    for cell in row_elem.findall("a:c", NS):
        if cell.attrib.get("r") == target_ref:
            target_cell = cell
            break

    if preserve_formula and target_cell is not None and target_cell.find("a:f", NS) is not None:
        return

    if target_cell is None:
        target_cell = ET.Element(f"{{{NS_MAIN}}}c", {"r": target_ref})
        insert_cell_in_order(row_elem, target_cell, column)

    if "t" in target_cell.attrib:
        del target_cell.attrib["t"]

    for child in list(target_cell):
        if child.tag in {f"{{{NS_MAIN}}}v", f"{{{NS_MAIN}}}f", f"{{{NS_MAIN}}}is"}:
            target_cell.remove(child)

    value_node = ET.SubElement(target_cell, f"{{{NS_MAIN}}}v")
    value_node.text = format_decimal_for_excel(value)


def get_or_create_row(sheet_data: ET.Element, row_number: int) -> ET.Element:
    existing_rows = sheet_data.findall("a:row", NS)
    for row_elem in existing_rows:
        if int(row_elem.attrib.get("r", "0")) == row_number:
            return row_elem

    new_row = ET.Element(f"{{{NS_MAIN}}}row", {"r": str(row_number)})
    children = list(sheet_data)
    for child_idx, child in enumerate(children):
        if child.tag != f"{{{NS_MAIN}}}row":
            continue
        try:
            existing_number = int(child.attrib.get("r", "0"))
        except ValueError:
            continue
        if existing_number > row_number:
            sheet_data.insert(child_idx, new_row)
            return new_row

    sheet_data.append(new_row)
    return new_row


def set_formula_cell(
    sheet_data: ET.Element,
    row_number: int,
    column: str,
    formula: str,
    cell_type: str | None = None,
) -> None:
    row_elem = get_or_create_row(sheet_data, row_number)
    target_ref = f"{column}{row_number}"
    target_cell: ET.Element | None = None
    for cell in row_elem.findall("a:c", NS):
        if cell.attrib.get("r") == target_ref:
            target_cell = cell
            break

    if target_cell is None:
        target_cell = ET.Element(f"{{{NS_MAIN}}}c", {"r": target_ref})
        insert_cell_in_order(row_elem, target_cell, column)

    if cell_type:
        target_cell.attrib["t"] = cell_type
    elif "t" in target_cell.attrib:
        del target_cell.attrib["t"]

    for child in list(target_cell):
        if child.tag in {f"{{{NS_MAIN}}}v", f"{{{NS_MAIN}}}f", f"{{{NS_MAIN}}}is"}:
            target_cell.remove(child)

    formula_node = ET.SubElement(target_cell, f"{{{NS_MAIN}}}f")
    formula_node.text = formula


def set_formula_string_cell(
    sheet_data: ET.Element, row_number: int, column: str, formula: str
) -> None:
    set_formula_cell(sheet_data, row_number, column, formula, cell_type="str")


def set_inline_string_cell(row_elem: ET.Element, row_number: int, column: str, value: str) -> None:
    target_ref = f"{column}{row_number}"
    target_cell: ET.Element | None = None
    for cell in row_elem.findall("a:c", NS):
        if cell.attrib.get("r") == target_ref:
            target_cell = cell
            break

    if target_cell is None:
        target_cell = ET.Element(f"{{{NS_MAIN}}}c", {"r": target_ref})
        insert_cell_in_order(row_elem, target_cell, column)

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


def map_row_with_insertions(
    original_row: int, insertions: list[tuple[int, int]]
) -> int:
    mapped = original_row
    for start_row, delta in insertions:
        if mapped >= start_row:
            mapped += delta
    return mapped


def refresh_company_summary_formulas(
    sheet_data: ET.Element, company_slots: dict[str, list[int]]
) -> int:
    section_rows: dict[str, dict[str, int]] = {}

    for company, slots in company_slots.items():
        if not slots:
            continue
        start_row = min(slots)
        end_row = max(slots)
        total_row = end_row + 1
        amount_row = end_row + 3
        section_rows[company] = {
            "start_row": start_row,
            "end_row": end_row,
            "total_row": total_row,
            "amount_row": amount_row,
        }

        set_formula_cell(sheet_data, total_row, "D", f"SUM(D{start_row}:D{end_row})")
        set_formula_cell(sheet_data, total_row, "M", f"SUM(M{start_row}:M{end_row})")
        set_formula_cell(sheet_data, total_row, "O", f"SUM(O{start_row}:O{end_row})")
        set_formula_cell(sheet_data, total_row, "Q", f"SUM(Q{start_row}:Q{end_row})")

        burden = COMPANY_BURDEN_MULTIPLIER[company]
        burden_text = format_decimal_for_excel(burden)
        set_formula_cell(sheet_data, amount_row, "E", f"E{total_row}+G{total_row}")
        set_formula_cell(
            sheet_data, amount_row, "G", f"SUM(H{amount_row}:J{amount_row})/{burden_text}"
        )
        set_formula_cell(sheet_data, amount_row, "H", f"H{total_row}*{burden_text}")

        if company == "scanio_moving":
            set_formula_cell(sheet_data, total_row, "L", f"K{total_row}/Q{total_row}")
            set_formula_cell(sheet_data, total_row, "N", f"M{total_row}/Q{total_row}")
            set_formula_cell(sheet_data, total_row, "P", f"O{total_row}/D{total_row}")
            set_formula_cell(
                sheet_data, amount_row, "K", f"E{total_row}*L{total_row}*{burden_text}"
            )
            set_formula_cell(
                sheet_data, amount_row, "M", f"E{total_row}*N{total_row}*{burden_text}"
            )
            set_formula_cell(
                sheet_data, amount_row, "O", f"E{total_row}*P{total_row}*{burden_text}"
            )
        else:
            set_formula_cell(
                sheet_data,
                amount_row,
                "K",
                f"SUMPRODUCT(E{start_row}:E{end_row},L{start_row}:L{end_row})*{burden_text}",
            )
            set_formula_cell(
                sheet_data,
                amount_row,
                "M",
                f"SUMPRODUCT(E{start_row}:E{end_row},N{start_row}:N{end_row})*{burden_text}",
            )
            set_formula_cell(
                sheet_data,
                amount_row,
                "O",
                f"SUMPRODUCT(E{start_row}:E{end_row},P{start_row}:P{end_row})*{burden_text}",
            )

        set_formula_cell(
            sheet_data,
            amount_row,
            "Q",
            f"(K{amount_row}+M{amount_row}+O{amount_row})/{burden_text}",
        )
        set_formula_cell(sheet_data, amount_row + 1, "E", f"G{amount_row}+Q{amount_row}")

    scanio_totals = section_rows["scanio_moving"]
    storage_totals = section_rows["scanio_storage"]
    sea_totals = section_rows["sea_and_air_intl"]
    flat_totals = section_rows["flat_price"]

    overtime_row = flat_totals["total_row"] + 5
    due_row = flat_totals["total_row"] + 7
    reimbursement_row = flat_totals["total_row"] + 15

    set_formula_cell(
        sheet_data,
        overtime_row,
        "F",
        (
            f"F{scanio_totals['total_row']}"
            f"+F{storage_totals['total_row']}"
            f"+F{sea_totals['total_row']}"
            f"+F{flat_totals['total_row']}"
        ),
    )
    set_formula_cell(
        sheet_data,
        due_row,
        "C",
        (
            f"K{sea_totals['amount_row']}+H{sea_totals['amount_row']}"
            f"-M{scanio_totals['amount_row']}-I{scanio_totals['amount_row']}"
            f"-M{storage_totals['amount_row']}-I{storage_totals['amount_row']}"
        ),
    )
    set_formula_cell(
        sheet_data,
        due_row,
        "Q",
        (
            f"Q{flat_totals['total_row']}"
            f"+Q{sea_totals['total_row']}"
            f"+Q{storage_totals['total_row']}"
            f"+Q{scanio_totals['total_row']}"
        ),
    )
    set_formula_cell(
        sheet_data,
        reimbursement_row,
        "F",
        (
            f"G{flat_totals['amount_row']}"
            f"+G{sea_totals['amount_row']}"
            f"+G{storage_totals['amount_row']}"
            f"+G{scanio_totals['amount_row']}"
        ),
    )
    return reimbursement_row


def set_employee_row_formulas(sheet_data: ET.Element, row_number: int) -> None:
    set_formula_cell(sheet_data, row_number, "D", f"SUM(K{row_number}:O{row_number})")
    set_formula_cell(
        sheet_data,
        row_number,
        "E",
        (
            f"IF(D{row_number}>40,"
            f"(D{row_number}-40)*(C{row_number}*1.5)+(C{row_number}*40),"
            f"D{row_number}*C{row_number})"
        ),
    )
    set_formula_cell(
        sheet_data,
        row_number,
        "F",
        f"IF(D{row_number}>40,(D{row_number}-40)*(C{row_number}*0.5),0)",
    )
    set_formula_cell(sheet_data, row_number, "G", f"SUM(H{row_number}:J{row_number})")
    set_formula_cell(sheet_data, row_number, "L", f"IFERROR(K{row_number}/Q{row_number},0)")
    set_formula_cell(sheet_data, row_number, "N", f"IFERROR(M{row_number}/Q{row_number},0)")
    set_formula_cell(sheet_data, row_number, "P", f"IFERROR(O{row_number}/Q{row_number},0)")
    set_formula_cell(sheet_data, row_number, "Q", f"K{row_number}+M{row_number}+O{row_number}")


def build_employee_rows_from_roster(
    sheet_root: ET.Element,
    sheet_data: ET.Element,
    roster_entries: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in roster_entries:
        grouped[entry["home_company"]].append(entry)

    for company in grouped:
        grouped[company].sort(key=lambda item: normalize_name(item["name"]))

    insertions: list[tuple[int, int]] = []
    company_slots: dict[str, list[int]] = {}
    for company, base_slots in COMPANY_ROW_SLOTS.items():
        slots = [map_row_with_insertions(row_number, insertions) for row_number in base_slots]
        overflow = len(grouped.get(company, [])) - len(slots)
        if overflow > 0:
            insert_at = slots[-1] + 1
            template_row = get_or_create_row(sheet_data, slots[-1])
            shift_rows_in_sheet(sheet_root, insert_at, overflow)
            for index in range(overflow):
                clone_template_row(sheet_data, template_row, insert_at + index)
            insertions.append((insert_at, overflow))
            slots.extend(insert_at + index for index in range(overflow))
        company_slots[company] = slots

    reimbursement_row = refresh_company_summary_formulas(sheet_data, company_slots)

    employee_rows: list[dict[str, Any]] = []
    fill_columns = ["H", "I", "J", "K", "M", "O"]

    for company, slots in company_slots.items():
        company_entries = grouped.get(company, [])

        for idx, row_number in enumerate(slots):
            row_elem = get_or_create_row(sheet_data, row_number)
            set_inline_string_cell(row_elem, row_number, "A", "")
            set_employee_row_formulas(sheet_data, row_number)
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
                # Keep any template commission-total formula in column G.
                set_numeric_cell(row_elem, row_number, "G", 0.0, preserve_formula=True)

    return employee_rows, reimbursement_row


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
                note = normalize_spaces(row[3])
                try:
                    cash_tips = parse_number(row[5])
                except ValueError:
                    cash_tips = 0.0
                try:
                    card_tips = parse_number(row[7])
                except ValueError:
                    card_tips = 0.0
                amount = cash_tips + card_tips
                if abs(amount) < 1e-9:
                    inferred = parse_tip_amount_from_note(note)
                    if inferred is not None:
                        amount = inferred
                totals[name] += amount

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
    calc.set("forceFullCalc", "1")
    calc.set("calcMode", "auto")


def ensure_all_rows_visible(sheet_xml: ET.Element) -> None:
    sheet_data = sheet_xml.find("a:sheetData", NS)
    if sheet_data is None:
        return

    for row_elem in sheet_data.findall("a:row", NS):
        # Force rows visible in exported workbook while keeping row sizing/group metadata.
        row_elem.attrib.pop("hidden", None)


def merge_sheet_data_into_original_xml(
    original_sheet_xml_bytes: bytes, sheet_data: ET.Element
) -> bytes:
    """Replace only the sheetData block to preserve workbook-specific root namespaces."""
    original_xml = original_sheet_xml_bytes.decode("utf-8")

    # Keep default SpreadsheetML namespace in fragment output.
    ET.register_namespace("", NS_MAIN)
    sheet_data_xml = ET.tostring(sheet_data, encoding="unicode")

    merged_xml, replacements = SHEET_DATA_XML_RE.subn(
        sheet_data_xml, original_xml, count=1
    )
    if replacements != 1:
        raise ValueError("Could not merge updated sheetData into sheet1.xml")
    return merged_xml.encode("utf-8")


def merge_calc_pr_into_original_workbook_xml(
    original_workbook_xml_bytes: bytes, workbook_root: ET.Element
) -> bytes:
    original_xml = original_workbook_xml_bytes.decode("utf-8")
    calc = workbook_root.find("a:calcPr", NS)
    if calc is None:
        # Fallback: keep original if calcPr cannot be located.
        return original_workbook_xml_bytes

    ET.register_namespace("", NS_MAIN)
    calc_xml = ET.tostring(calc, encoding="unicode")
    merged_xml, replacements = CALC_PR_XML_RE.subn(calc_xml, original_xml, count=1)
    if replacements != 1:
        # Fallback to original to avoid damaging workbook XML if pattern is unexpected.
        return original_workbook_xml_bytes
    return merged_xml.encode("utf-8")


def remove_calc_chain_relationship(workbook_rels_bytes: bytes) -> tuple[bytes, bool]:
    root = ET.fromstring(workbook_rels_bytes)
    removed = False
    for rel in list(root):
        if rel.tag != f"{{{NS_REL}}}Relationship":
            continue
        if rel.attrib.get("Type") == CALC_CHAIN_REL_TYPE:
            root.remove(rel)
            removed = True
    if not removed:
        return workbook_rels_bytes, False

    ET.register_namespace("", NS_REL)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True), True


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
        workbook_original_bytes = zin.read("xl/workbook.xml")
        workbook_root = ET.fromstring(workbook_original_bytes)
        sheet1_original_bytes = zin.read("xl/worksheets/sheet1.xml")
        sheet1_root = ET.fromstring(sheet1_original_bytes)
        workbook_rels_path = "xl/_rels/workbook.xml.rels"
        workbook_rels_bytes = (
            zin.read(workbook_rels_path)
            if workbook_rels_path in zin.namelist()
            else None
        )

        sheet_data = sheet1_root.find("a:sheetData", NS)
        if sheet_data is None:
            raise ValueError("Could not find sheetData in xl/worksheets/sheet1.xml")

        reimbursement_row = 101
        if roster_path:
            roster_entries = load_roster(roster_path)
            employee_rows, reimbursement_row = build_employee_rows_from_roster(
                sheet1_root, sheet_data, roster_entries
            )
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
                set_numeric_cell(row_elem, row_number, "G", tip_total, preserve_formula=True)

        # Replace Google Sheets-only formula with Excel-compatible IF formula.
        due_row = reimbursement_row - 8
        set_formula_string_cell(
            sheet_data,
            reimbursement_row,
            "B",
            reimbursement_status_formula(due_row),
        )

        # Preserve full line visibility in generated workbook.
        ensure_all_rows_visible(sheet1_root)
        ensure_recalc_on_open(workbook_root)

        sheet1_bytes = merge_sheet_data_into_original_xml(sheet1_original_bytes, sheet_data)
        workbook_bytes = merge_calc_pr_into_original_workbook_xml(
            workbook_original_bytes, workbook_root
        )
        updated_workbook_rels_bytes = workbook_rels_bytes
        remove_calc_chain_part = False
        if workbook_rels_bytes is not None:
            updated_workbook_rels_bytes, remove_calc_chain_part = (
                remove_calc_chain_relationship(workbook_rels_bytes)
            )

        with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                if remove_calc_chain_part and item.filename == "xl/calcChain.xml":
                    continue
                data = zin.read(item.filename)
                if item.filename == "xl/worksheets/sheet1.xml":
                    data = sheet1_bytes
                elif item.filename == "xl/workbook.xml":
                    data = workbook_bytes
                elif (
                    updated_workbook_rels_bytes is not None
                    and item.filename == workbook_rels_path
                ):
                    data = updated_workbook_rels_bytes
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
