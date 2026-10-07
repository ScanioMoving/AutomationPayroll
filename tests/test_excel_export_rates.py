import csv
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from xml.etree import ElementTree as ET
import zipfile

from fill_payroll_workbook_from_hours import (
    COMPANY_ROW_SLOTS,
    NS,
    apply_company_burden_multipliers,
    fill_workbook,
)


TEMPLATE = Path(__file__).resolve().parents[1] / "Copy of Payroll Weekly 01.31.26- 02.06.26.xlsx"
EXPECTED_FACTORS = {
    "scanio_moving": "1.2",
    "scanio_storage": "1.22",
    "sea_and_air_intl": "1.16",
    "flat_price": "1.17",
}


def read_sheet(path):
    with zipfile.ZipFile(path) as workbook:
        return ET.fromstring(workbook.read("xl/worksheets/sheet1.xml"))


def cell_at(sheet, address):
    return sheet.find(f".//a:c[@r='{address}']", NS)


class ExcelExportRatesTest(unittest.TestCase):
    def export(self, directory, overflow=False, use_roster=True):
        employees = []
        amount_rows = {}
        inserted = 0
        for company, slots in COMPANY_ROW_SLOTS.items():
            count = len(slots) + 1 if overflow else 1
            employees.extend(
                {"name": f"{company} Employee {index:02d}", "home_company": company, "rate": 20}
                for index in range(count)
            )
            inserted += max(0, count - len(slots))
            amount_rows[company] = max(slots) + inserted + 3

        hours = directory / "hours.csv"
        with hours.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["Name", "Company", "Hours at Company"])
            for employee in employees:
                for company, total in (("Scanio", "20"), ("Sea and Air", "15"), ("Flat Price", "10")):
                    writer.writerow([employee["name"], company, total])
        roster = directory / "roster.json"
        roster.write_text(json.dumps({"employees": employees}), encoding="utf-8")
        output = directory / "export.xlsx"
        fill_workbook(TEMPLATE, hours, output, roster_path=roster if use_roster else None)
        return read_sheet(output), amount_rows

    def assert_company_factors(self, sheet, amount_rows):
        for company, row in amount_rows.items():
            factor = EXPECTED_FACTORS[company]
            for column, operator in (("G", "/"), ("H", "*"), ("K", "*"), ("M", "*"), ("O", "*"), ("Q", "/")):
                formula = cell_at(sheet, f"{column}{row}").find("a:f", NS).text
                self.assertTrue(formula.endswith(operator + factor), (company, column, formula))
                self.assertNotIn("1.18", formula)
            # The shared commission formulas must retain their master and followers.
            master = cell_at(sheet, f"H{row}").find("a:f", NS)
            self.assertEqual(master.get("ref"), f"H{row}:J{row}")
            for column in ("I", "J"):
                follower = cell_at(sheet, f"{column}{row}").find("a:f", NS)
                self.assertEqual(follower.get("t"), "shared")
                self.assertEqual(follower.get("si"), master.get("si"))
                self.assertIsNone(follower.text)

    def test_existing_template_gets_new_rates_without_other_formula_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            sheet, rows = self.export(Path(tmp))
        self.assert_company_factors(sheet, rows)
        original = read_sheet(TEMPLATE)
        changed = set()
        employee_inputs = {f"{column}{row}" for slots in COMPANY_ROW_SLOTS.values() for row in slots for column in ("A", "B", "C", "H", "I", "J", "K", "M", "O")}
        for cell in original.findall(".//a:c", NS):
            if cell.get("r") in employee_inputs:
                continue
            formula = cell.find("a:f", NS)
            if formula is None:
                continue
            updated = cell_at(sheet, cell.get("r")).find("a:f", NS)
            if ET.tostring(formula) != ET.tostring(updated):
                changed.add(cell.get("r"))
        expected = {f"{column}{rows[company]}" for company in EXPECTED_FACTORS for column in ("G", "H", "K", "M", "O", "Q")}
        # The exporter also replaces the existing Google Sheets reimbursement formula.
        self.assertEqual(changed - {"B101"}, expected)

    def test_dynamic_rows_use_company_rates_after_all_sections_expand(self):
        with tempfile.TemporaryDirectory() as tmp:
            sheet, rows = self.export(Path(tmp), overflow=True)
        self.assert_company_factors(sheet, rows)

    def test_export_without_roster_updates_saved_template_rates(self):
        with tempfile.TemporaryDirectory() as tmp:
            sheet, rows = self.export(Path(tmp), use_roster=False)
        self.assert_company_factors(sheet, rows)

    def test_unshared_commission_formulas_and_cached_values(self):
        sheet_data = ET.fromstring('''<sheetData xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
          <row r="28">
            <c r="I28"><f>I26 * 1.1800</f><v>118</v></c>
            <c r="J28"><f>J26*1.18</f><v>236</v></c>
          </row>
        </sheetData>''')
        apply_company_burden_multipliers(sheet_data, COMPANY_ROW_SLOTS)
        for column in ("I", "J"):
            cell = cell_at(sheet_data, f"{column}28")
            self.assertTrue(cell.find("a:f", NS).text.endswith("*1.2"))
            self.assertIsNone(cell.find("a:v", NS))
        first_pass = ET.tostring(sheet_data)
        apply_company_burden_multipliers(sheet_data, COMPANY_ROW_SLOTS)
        self.assertEqual(ET.tostring(sheet_data), first_pass)

    def test_website_download_includes_hours_commissions_and_company_factors(self):
        import payroll_web_app as web

        payload = {
            "week_start": "2026-10-03",
            "employees": [
                {
                    "name": f"Test Employee {index}",
                    "payrollCompany": label,
                    "rate": 20,
                    "days": [{"hours": [20, 15, 10], "commissions": [30, 40, 50]}],
                }
                for index, (_, label) in enumerate(web.COMPANY_OPTIONS)
            ],
        }
        body = json.dumps(payload).encode()
        handler = SimpleNamespace(
            require_auth=lambda: SimpleNamespace(user_id=1),
            headers={"Content-Length": str(len(body))},
            rfile=io.BytesIO(body),
        )
        with patch.object(web, "get_default_template_path", return_value=TEMPLATE), patch.object(web, "file_response") as response:
            web.PayrollWebRequestHandler.handle_workspace_export_xlsx(handler)
        self.assertEqual(response.call_args.kwargs["filename"], "payroll_week_2026-10-03_to_2026-10-09_filled.xlsx")
        with zipfile.ZipFile(io.BytesIO(response.call_args.args[1])) as workbook:
            self.assertIsNone(workbook.testzip())
            sheet = ET.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
            calc = ET.fromstring(workbook.read("xl/workbook.xml")).find("a:calcPr", NS)
            self.assertEqual(calc.get("fullCalcOnLoad"), "1")
        self.assert_company_factors(sheet, {company: max(slots) + 3 for company, slots in COMPANY_ROW_SLOTS.items()})
        for slots in COMPANY_ROW_SLOTS.values():
            for column, expected in (("H", 30), ("I", 40), ("J", 50), ("K", 20), ("M", 15), ("O", 10)):
                value = cell_at(sheet, f"{column}{slots[0]}").find("a:v", NS).text
                self.assertEqual(float(value), expected)


if __name__ == "__main__":
    unittest.main()
