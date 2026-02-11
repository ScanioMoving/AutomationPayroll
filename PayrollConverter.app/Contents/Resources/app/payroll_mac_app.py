#!/usr/bin/env python3
"""Local Mac payroll app with drag/drop upload and employee management."""

from __future__ import annotations

import cgi
import io
import json
import shutil
import socket
import sys
import tempfile
import traceback
import webbrowser
import zipfile
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from fill_payroll_workbook_from_hours import fill_workbook, load_tips_csv, match_names
from simplify_timecard_csv import flatten_timecard, write_flat_csv

def get_bundle_app_dir() -> Path:
    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            return Path(meipass)
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


APP_DIR = get_bundle_app_dir()
PREFERRED_DATA_DIR = Path.home() / "Library" / "Application Support" / "PayrollAutomationApp"
FALLBACK_DATA_DIR = APP_DIR / ".payroll_app_data"


def select_data_dir() -> Path:
    for candidate in (PREFERRED_DATA_DIR, FALLBACK_DATA_DIR):
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            probe = candidate / ".write_probe"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            return candidate
        except Exception:
            continue
    raise RuntimeError("Could not find writable app data directory.")


DATA_DIR = select_data_dir()

ROSTER_PATH = DATA_DIR / "payroll_roster.json"
SETTINGS_PATH = DATA_DIR / "settings.json"
DEFAULT_TEMPLATE_COPY_PATH = DATA_DIR / "default_template.xlsx"
BUNDLED_TEMPLATE_CANDIDATE_NAMES = (
    "default_template.xlsx",
    "Copy of Payroll Weekly 01.31.26- 02.06.26.xlsx",
)

DEFAULT_BURDEN_BY_COMPANY = {
    "scanio_moving": 1.18,
    "scanio_storage": 1.24,
    "sea_and_air_intl": 1.18,
    "flat_price": 1.18,
}

COMPANY_OPTIONS = [
    ("scanio_moving", "Scanio Moving"),
    ("scanio_storage", "Scanio Storage"),
    ("sea_and_air_intl", "Sea and Air Int-L"),
    ("flat_price", "Flat Price"),
]

COMPANY_LABEL_TO_KEY = {label: key for key, label in COMPANY_OPTIONS}


def normalize_spaces(value: str) -> str:
    return " ".join((value or "").strip().split())


def safe_filename(name: str, fallback: str) -> str:
    cleaned = "".join(ch for ch in (name or "") if ch.isalnum() or ch in (" ", ".", "-", "_"))
    cleaned = normalize_spaces(cleaned).replace(" ", "_")
    return cleaned if cleaned else fallback


def find_free_port(host: str = "127.0.0.1") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def read_json_file(path: Path, default_payload: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default_payload
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default_payload


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def default_roster_payload() -> dict[str, Any]:
    bundled = APP_DIR / "payroll_roster.json"
    if bundled.exists():
        try:
            return json.loads(bundled.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"employees": []}


def ensure_roster_file() -> None:
    if not ROSTER_PATH.exists():
        write_json_file(ROSTER_PATH, default_roster_payload())


def ensure_settings_file() -> None:
    bundled_template: Path | None = None

    for candidate_name in BUNDLED_TEMPLATE_CANDIDATE_NAMES:
        candidate = APP_DIR / candidate_name
        if candidate.exists():
            bundled_template = candidate
            break

    if bundled_template is None:
        for candidate in sorted(APP_DIR.glob("*.xlsx")):
            if "filled" in candidate.name.lower():
                continue
            if candidate.name.startswith("~$"):
                continue
            bundled_template = candidate
            break

    if not DEFAULT_TEMPLATE_COPY_PATH.exists() and bundled_template and bundled_template.exists():
        shutil.copy2(bundled_template, DEFAULT_TEMPLATE_COPY_PATH)

    default_template = str(DEFAULT_TEMPLATE_COPY_PATH) if DEFAULT_TEMPLATE_COPY_PATH.exists() else ""
    default_payload = {"default_template_path": default_template}
    settings = read_json_file(SETTINGS_PATH, default_payload)
    configured = str(settings.get("default_template_path", "")).strip()
    configured_path = Path(configured).expanduser() if configured else Path("")
    if not configured or not configured_path.exists():
        settings["default_template_path"] = default_template
    elif "default_template_path" not in settings:
        settings["default_template_path"] = default_template
    write_json_file(SETTINGS_PATH, settings)


def read_roster_payload() -> dict[str, Any]:
    ensure_roster_file()
    payload = read_json_file(ROSTER_PATH, {"employees": []})
    employees = payload.get("employees")
    if not isinstance(employees, list):
        payload["employees"] = []
    return payload


def write_roster_payload(payload: dict[str, Any]) -> None:
    employees = payload.get("employees", [])
    employees.sort(key=lambda item: normalize_spaces(item.get("name", "")).lower())
    write_json_file(ROSTER_PATH, {"employees": employees})


def roster_employees() -> list[dict[str, Any]]:
    payload = read_roster_payload()
    output: list[dict[str, Any]] = []
    for item in payload.get("employees", []):
        name = normalize_spaces(str(item.get("name", "")))
        company = item.get("home_company")
        rate = item.get("rate")
        burden = item.get("burden_multiplier")
        if not name or company not in DEFAULT_BURDEN_BY_COMPANY:
            continue
        try:
            rate_value = float(rate)
        except Exception:
            rate_value = 0.0
        try:
            burden_value = float(burden)
        except Exception:
            burden_value = DEFAULT_BURDEN_BY_COMPANY[company]
        output.append(
            {
                "name": name,
                "home_company": company,
                "rate": rate_value,
                "burden_multiplier": burden_value,
            }
        )
    return output


def read_settings() -> dict[str, Any]:
    ensure_settings_file()
    return read_json_file(SETTINGS_PATH, {"default_template_path": ""})


def write_settings(settings: dict[str, Any]) -> None:
    write_json_file(SETTINGS_PATH, settings)


def infer_default_rate(company: str, employees: list[dict[str, Any]]) -> float:
    company_rates = [float(item["rate"]) for item in employees if item["home_company"] == company]
    if not company_rates:
        return 0.0
    return round(sum(company_rates) / len(company_rates), 2)


def parse_bool_flag(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def extract_source_names_from_batch(
    batch_csv_path: Path, exclude_weekly_overtime: bool
) -> tuple[set[str], Path]:
    include_weekly_overtime = not exclude_weekly_overtime
    totals = flatten_timecard(batch_csv_path, include_weekly_overtime=include_weekly_overtime)
    simplified_hours = batch_csv_path.with_name(
        f"{batch_csv_path.stem}{'_simple.csv' if exclude_weekly_overtime else '_simple_reg.csv'}"
    )
    write_flat_csv(simplified_hours, totals)
    names = {name for name, _ in totals.keys()}
    return names, simplified_hours


def extract_source_names_from_tips(tip_csv_path: Path) -> tuple[set[str], Path]:
    tip_totals, _, _ = load_tips_csv(tip_csv_path)
    names = set(tip_totals.keys())
    simple_tips = tip_csv_path.with_name(f"{tip_csv_path.stem}_simple.csv")
    with simple_tips.open("w", newline="", encoding="utf-8") as handle:
        writer = csv_writer(handle)
        writer.writerow(["Name", "Commission"])
        for name in sorted(names, key=lambda value: value.lower()):
            writer.writerow([name, f"{tip_totals[name]:.2f}"])
    return names, simple_tips


def csv_writer(handle: io.TextIOBase):
    import csv

    return csv.writer(handle)


def get_file_field(form: cgi.FieldStorage, field_name: str) -> tuple[str, bytes] | None:
    if field_name not in form:
        return None
    field = form[field_name]
    if isinstance(field, list):
        field = field[0]
    filename = getattr(field, "filename", None)
    if not filename:
        return None
    data = field.file.read()
    if not isinstance(data, (bytes, bytearray)):
        return None
    return (filename, bytes(data))


def parse_multipart_form(handler: BaseHTTPRequestHandler) -> cgi.FieldStorage:
    content_type = handler.headers.get("Content-Type", "")
    return cgi.FieldStorage(
        fp=handler.rfile,
        headers=handler.headers,
        environ={"REQUEST_METHOD": "POST", "CONTENT_TYPE": content_type},
        keep_blank_values=True,
    )


def json_response(handler: BaseHTTPRequestHandler, payload: dict[str, Any], status: int = 200) -> None:
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def text_response(handler: BaseHTTPRequestHandler, text: str, status: int = 200) -> None:
    body = text.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/plain; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def file_response(
    handler: BaseHTTPRequestHandler, payload: bytes, filename: str, content_type: str
) -> None:
    handler.send_response(200)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Disposition", f'attachment; filename="{filename}"')
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


def parse_json_body(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    length = int(handler.headers.get("Content-Length", "0"))
    raw = handler.rfile.read(length) if length > 0 else b"{}"
    return json.loads(raw.decode("utf-8"))


def default_template_path() -> Path | None:
    settings = read_settings()
    configured = settings.get("default_template_path", "")
    if configured:
        candidate = Path(configured)
        if candidate.exists():
            return candidate
    if DEFAULT_TEMPLATE_COPY_PATH.exists():
        return DEFAULT_TEMPLATE_COPY_PATH
    return None


def run_fill_script(
    template_path: Path,
    hours_csv_path: Path,
    tip_csv_path: Path,
    tip_summary_path: Path,
    output_xlsx_path: Path,
) -> tuple[bool, str]:
    try:
        result = fill_workbook(
            workbook_path=template_path,
            hours_csv_path=hours_csv_path,
            output_path=output_xlsx_path,
            roster_path=ROSTER_PATH,
            tips_csv_path=tip_csv_path,
            tip_summary_output_path=tip_summary_path,
        )
    except Exception as exc:
        return False, str(exc)

    message_lines = [
        f"Wrote filled workbook: {result['output_path']}",
        (
            f"Matched hour names: "
            f"{len(result['source_to_workbook'])}/{len(result['source_names'])}"
        ),
    ]
    if result["tip_totals_by_source_name"]:
        message_lines.append(
            f"Matched tip names: "
            f"{len(result['tip_source_to_workbook'])}/{len(result['tip_source_names'])}"
        )
    if result["unmatched_sources"]:
        message_lines.append(
            "Unmatched source names: " + ", ".join(result["unmatched_sources"])
        )
    if result["unmatched_tip_sources"]:
        message_lines.append(
            "Unmatched tip names: " + ", ".join(result["unmatched_tip_sources"])
        )
    return True, "\n".join(message_lines)


HTML_PAGE = """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Payroll Converter</title>
  <style>
    :root {
      --bg: #f4f6f8;
      --card: #ffffff;
      --line: #d7dde4;
      --text: #132235;
      --muted: #59687a;
      --brand: #0f766e;
      --brand-dark: #115e59;
      --danger: #b91c1c;
    }
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
      background: linear-gradient(180deg, #eef2f7 0%, #f8fafc 100%);
      color: var(--text);
    }
    .wrap {
      max-width: 1080px;
      margin: 24px auto 40px auto;
      padding: 0 16px;
    }
    .panel {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 16px;
      margin-bottom: 16px;
      box-shadow: 0 8px 24px rgba(14, 34, 56, 0.06);
    }
    h1, h2 {
      margin: 0 0 10px 0;
      font-weight: 700;
    }
    h1 { font-size: 24px; }
    h2 { font-size: 18px; }
    .muted { color: var(--muted); font-size: 14px; }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 12px;
      margin-top: 12px;
    }
    .drop {
      border: 2px dashed #aab8c6;
      border-radius: 10px;
      padding: 14px;
      min-height: 82px;
      cursor: pointer;
      background: #fbfdff;
    }
    .drop.active {
      border-color: var(--brand);
      background: #ecfdf5;
    }
    .drop .title { font-weight: 600; margin-bottom: 6px; }
    .drop .file { font-size: 13px; color: var(--muted); word-break: break-all; }
    input[type="file"] { display: none; }
    .row {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
      margin-top: 12px;
    }
    button {
      border: 1px solid var(--brand);
      background: var(--brand);
      color: white;
      border-radius: 8px;
      padding: 10px 14px;
      font-weight: 600;
      cursor: pointer;
    }
    button.secondary {
      background: white;
      color: var(--brand);
    }
    button.danger {
      border-color: var(--danger);
      background: var(--danger);
    }
    button:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .status {
      margin-top: 10px;
      font-size: 14px;
      white-space: pre-wrap;
    }
    .status.error { color: var(--danger); }
    .unknown-list {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      margin-top: 10px;
    }
    .unknown-row {
      display: grid;
      grid-template-columns: minmax(220px, 1fr) 200px 120px;
      gap: 10px;
      align-items: center;
      margin-bottom: 8px;
    }
    .unknown-row:last-child { margin-bottom: 0; }
    select, input[type="number"] {
      border: 1px solid #c6d2de;
      border-radius: 7px;
      padding: 8px;
      font-size: 14px;
      width: 100%;
      box-sizing: border-box;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 8px;
      font-size: 14px;
    }
    th, td {
      border-bottom: 1px solid #e3e9ef;
      text-align: left;
      padding: 8px 6px;
    }
    th { color: #324559; font-weight: 700; }
    .small { font-size: 12px; color: var(--muted); }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="panel">
      <h1>Payroll Converter</h1>
      <div class="muted">Drop your files, review new names, then convert.</div>
      <div class="grid">
        <div class="drop" id="batchDrop">
          <div class="title">Batch CSV (required)</div>
          <div class="file" id="batchFileLabel">Drop file or click to choose</div>
          <input type="file" id="batchInput" accept=".csv" />
        </div>
        <div class="drop" id="tipDrop">
          <div class="title">Tip CSV (required)</div>
          <div class="file" id="tipFileLabel">Drop file or click to choose</div>
          <input type="file" id="tipInput" accept=".csv" />
        </div>
        <div class="drop" id="templateDrop">
          <div class="title">Template XLSX (optional override)</div>
          <div class="file" id="templateFileLabel">Drop file or click to choose</div>
          <input type="file" id="templateInput" accept=".xlsx" />
        </div>
      </div>
      <div class="row">
        <label><input type="checkbox" id="excludeWeeklyOt" checked /> Use raw worked hours (exclude weekly overtime adjustments)</label>
      </div>
      <div class="row">
        <button id="checkBtn" class="secondary">Check New Names</button>
        <button id="convertBtn">Convert and Download</button>
      </div>
      <div class="status" id="status"></div>
      <div id="unknownContainer" style="display:none;">
        <h2 style="margin-top:12px;">New Name Assignment</h2>
        <div class="small">Assign company for names not in current employee roster.</div>
        <div class="unknown-list" id="unknownList"></div>
      </div>
    </div>

    <div class="panel">
      <h2>Default Template</h2>
      <div class="muted" id="defaultTemplateText">Loading...</div>
      <div class="row">
        <div class="drop" id="setTemplateDrop" style="max-width:460px;">
          <div class="title">Set Default Template XLSX</div>
          <div class="file" id="setTemplateFileLabel">Drop file or click to choose</div>
          <input type="file" id="setTemplateInput" accept=".xlsx" />
        </div>
        <button id="saveTemplateBtn" class="secondary">Save As Default</button>
      </div>
    </div>

    <div class="panel">
      <h2>Manage Current Employees</h2>
      <div class="small">Edit hourly rate, then save. You can also remove employees who left.</div>
      <div class="row">
        <button id="refreshEmployeesBtn" class="secondary">Refresh</button>
        <button id="saveEmployeesBtn" class="secondary">Save Rate Changes</button>
        <button id="removeEmployeesBtn" class="danger">Remove Selected</button>
      </div>
      <table>
        <thead>
          <tr>
            <th style="width:36px;"></th>
            <th>Name</th>
            <th>Company</th>
            <th>Rate</th>
          </tr>
        </thead>
        <tbody id="employeesTbody"></tbody>
      </table>
    </div>
  </div>

  <script>
    const files = { batch: null, tip: null, template: null, defaultTemplate: null };
    let unknownNames = [];
    let unknownDefaults = {};

    const companyOptions = [
      { value: "scanio_moving", label: "Scanio Moving" },
      { value: "scanio_storage", label: "Scanio Storage" },
      { value: "sea_and_air_intl", label: "Sea and Air Int-L" },
      { value: "flat_price", label: "Flat Price" },
    ];

    function escapeHtml(value) {
      return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function setStatus(text, isError=false) {
      const el = document.getElementById("status");
      el.textContent = text || "";
      el.classList.toggle("error", !!isError);
    }

    function bindDrop(dropId, inputId, key, labelId) {
      const drop = document.getElementById(dropId);
      const input = document.getElementById(inputId);
      const label = document.getElementById(labelId);

      function setFile(file) {
        files[key] = file;
        label.textContent = file ? file.name : "Drop file or click to choose";
      }

      drop.addEventListener("click", () => input.click());
      input.addEventListener("change", () => {
        setFile(input.files && input.files[0] ? input.files[0] : null);
      });

      drop.addEventListener("dragover", (e) => {
        e.preventDefault();
        drop.classList.add("active");
      });
      drop.addEventListener("dragleave", () => drop.classList.remove("active"));
      drop.addEventListener("drop", (e) => {
        e.preventDefault();
        drop.classList.remove("active");
        if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
          setFile(e.dataTransfer.files[0]);
        }
      });
    }

    function buildUnknownAssignments(names, defaults) {
      const container = document.getElementById("unknownContainer");
      const list = document.getElementById("unknownList");
      list.innerHTML = "";
      if (!names || names.length === 0) {
        container.style.display = "none";
        return;
      }
      container.style.display = "block";
      for (const name of names) {
        const row = document.createElement("div");
        row.className = "unknown-row";
        row.dataset.name = name;

        const nameDiv = document.createElement("div");
        nameDiv.textContent = name;
        row.appendChild(nameDiv);

        const select = document.createElement("select");
        select.className = "unknown-company";
        const suggestedCompany = defaults[name]?.home_company || "scanio_moving";
        for (const option of companyOptions) {
          const opt = document.createElement("option");
          opt.value = option.value;
          opt.textContent = option.label;
          if (option.value === suggestedCompany) opt.selected = true;
          select.appendChild(opt);
        }
        row.appendChild(select);

        const rateInput = document.createElement("input");
        rateInput.type = "number";
        rateInput.className = "unknown-rate";
        rateInput.step = "0.01";
        rateInput.min = "0";
        const suggestedRate = defaults[name]?.rate;
        rateInput.value = suggestedRate !== undefined ? suggestedRate : "";
        rateInput.placeholder = "Rate (optional)";
        row.appendChild(rateInput);

        list.appendChild(row);
      }
    }

    function collectAssignments() {
      const rows = Array.from(document.querySelectorAll(".unknown-row"));
      return rows.map((row) => ({
        name: row.dataset.name,
        home_company: row.querySelector(".unknown-company").value,
        rate: row.querySelector(".unknown-rate").value
      }));
    }

    async function checkNewNames() {
      setStatus("Checking names...");
      if (!files.batch || !files.tip) {
        setStatus("Batch CSV and Tip CSV are required.", true);
        return;
      }

      const fd = new FormData();
      fd.append("batch_csv", files.batch, files.batch.name);
      fd.append("tip_csv", files.tip, files.tip.name);
      fd.append("exclude_weekly_overtime", document.getElementById("excludeWeeklyOt").checked ? "1" : "0");

      const res = await fetch("/api/preview", { method: "POST", body: fd });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        setStatus(data.error || "Preview failed.", true);
        return;
      }

      unknownNames = data.unknown_names || [];
      unknownDefaults = data.default_assignments || {};
      buildUnknownAssignments(unknownNames, unknownDefaults);
      if (unknownNames.length === 0) {
        setStatus("No new names found. Ready to convert.");
      } else {
        setStatus("New names found. Assign company and optional rate, then convert.");
      }
    }

    async function convertAndDownload() {
      setStatus("Converting...");
      if (!files.batch || !files.tip) {
        setStatus("Batch CSV and Tip CSV are required.", true);
        return;
      }

      const fd = new FormData();
      fd.append("batch_csv", files.batch, files.batch.name);
      fd.append("tip_csv", files.tip, files.tip.name);
      if (files.template) {
        fd.append("template_xlsx", files.template, files.template.name);
      }
      fd.append("exclude_weekly_overtime", document.getElementById("excludeWeeklyOt").checked ? "1" : "0");
      fd.append("assignments_json", JSON.stringify(collectAssignments()));

      const res = await fetch("/api/convert", { method: "POST", body: fd });
      const contentType = res.headers.get("Content-Type") || "";
      if (!res.ok || contentType.includes("application/json")) {
        let data = {};
        try { data = await res.json(); } catch {}
        setStatus(data.error || "Convert failed.", true);
        return;
      }

      const blob = await res.blob();
      const disposition = res.headers.get("Content-Disposition") || "";
      const filenameMatch = disposition.match(/filename="([^"]+)"/);
      const filename = filenameMatch ? filenameMatch[1] : "payroll_output.xlsx";
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
      setStatus("Done. Download started.");
      await loadEmployees();
      await loadSettings();
    }

    async function loadEmployees() {
      const res = await fetch("/api/employees");
      const data = await res.json();
      const tbody = document.getElementById("employeesTbody");
      tbody.innerHTML = "";
      for (const emp of (data.employees || [])) {
        const tr = document.createElement("tr");
        tr.dataset.name = emp.name;
        tr.innerHTML = `
          <td><input type="checkbox" data-name="${escapeHtml(emp.name)}" /></td>
          <td>${escapeHtml(emp.name)}</td>
          <td>${escapeHtml(emp.home_company_label)}</td>
          <td><input type="number" class="employee-rate" min="0" step="0.01" value="${Number(emp.rate)}" /></td>
        `;
        tbody.appendChild(tr);
      }
    }

    async function saveEmployeeRates() {
      const rows = Array.from(document.querySelectorAll("#employeesTbody tr"));
      if (rows.length === 0) {
        setStatus("No employees to update.");
        return;
      }

      const employees = [];
      for (const row of rows) {
        const name = (row.dataset.name || "").trim();
        const rateInput = row.querySelector(".employee-rate");
        const rateText = (rateInput?.value || "").trim();
        const rate = Number(rateText);
        if (!name) {
          continue;
        }
        if (!Number.isFinite(rate) || rate < 0) {
          setStatus(`Invalid rate for ${name}.`, true);
          return;
        }
        employees.push({ name, rate });
      }

      const res = await fetch("/api/employees/update", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ employees })
      });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        setStatus(data.error || "Failed to save rates.", true);
        return;
      }
      setStatus(`Updated rates for ${data.updated_count} employees.`);
      await loadEmployees();
    }

    async function removeSelectedEmployees() {
      const checks = Array.from(document.querySelectorAll('#employeesTbody input[type="checkbox"]:checked'));
      const names = checks.map((el) => el.dataset.name);
      if (names.length === 0) {
        return;
      }
      const res = await fetch("/api/employees/remove", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ names })
      });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        setStatus(data.error || "Failed to remove employees.", true);
        return;
      }
      setStatus(`Removed ${data.removed_count} employees.`);
      await loadEmployees();
    }

    async function loadSettings() {
      const res = await fetch("/api/settings");
      const data = await res.json();
      const text = document.getElementById("defaultTemplateText");
      if (data.default_template_path) {
        text.textContent = `Current default template: ${data.default_template_path}`;
      } else {
        text.textContent = "No default template saved. Upload one below or include template in Convert step.";
      }
    }

    async function saveDefaultTemplate() {
      if (!files.defaultTemplate) {
        setStatus("Choose a template file first.", true);
        return;
      }
      const fd = new FormData();
      fd.append("template_xlsx", files.defaultTemplate, files.defaultTemplate.name);
      const res = await fetch("/api/template", { method: "POST", body: fd });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        setStatus(data.error || "Failed to save default template.", true);
        return;
      }
      setStatus("Default template saved.");
      await loadSettings();
    }

    bindDrop("batchDrop", "batchInput", "batch", "batchFileLabel");
    bindDrop("tipDrop", "tipInput", "tip", "tipFileLabel");
    bindDrop("templateDrop", "templateInput", "template", "templateFileLabel");
    bindDrop("setTemplateDrop", "setTemplateInput", "defaultTemplate", "setTemplateFileLabel");

    document.getElementById("checkBtn").addEventListener("click", checkNewNames);
    document.getElementById("convertBtn").addEventListener("click", convertAndDownload);
    document.getElementById("refreshEmployeesBtn").addEventListener("click", loadEmployees);
    document.getElementById("saveEmployeesBtn").addEventListener("click", saveEmployeeRates);
    document.getElementById("removeEmployeesBtn").addEventListener("click", removeSelectedEmployees);
    document.getElementById("saveTemplateBtn").addEventListener("click", saveDefaultTemplate);

    loadEmployees();
    loadSettings();
  </script>
</body>
</html>
"""


class PayrollRequestHandler(BaseHTTPRequestHandler):
    server_version = "PayrollApp/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/":
            body = HTML_PAGE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/api/employees":
            employees = roster_employees()
            payload = {
                "employees": [
                    {
                        "name": item["name"],
                        "home_company": item["home_company"],
                        "home_company_label": dict(COMPANY_OPTIONS)[item["home_company"]],
                        "rate": item["rate"],
                    }
                    for item in sorted(employees, key=lambda entry: entry["name"].lower())
                ]
            }
            json_response(self, payload)
            return

        if path == "/api/settings":
            settings = read_settings()
            json_response(self, settings)
            return

        text_response(self, "Not Found", status=404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        try:
            if path == "/api/preview":
                self.handle_preview()
                return
            if path == "/api/convert":
                self.handle_convert()
                return
            if path == "/api/employees/update":
                self.handle_update_employees()
                return
            if path == "/api/employees/remove":
                self.handle_remove_employees()
                return
            if path == "/api/template":
                self.handle_set_template()
                return
            text_response(self, "Not Found", status=404)
        except Exception as exc:
            payload = {
                "ok": False,
                "error": f"{exc}",
                "trace": traceback.format_exc(limit=2),
            }
            json_response(self, payload, status=500)

    def handle_preview(self) -> None:
        form = parse_multipart_form(self)
        batch_file = get_file_field(form, "batch_csv")
        tip_file = get_file_field(form, "tip_csv")

        if batch_file is None or tip_file is None:
            json_response(
                self,
                {"ok": False, "error": "Batch CSV and Tip CSV are required."},
                status=400,
            )
            return

        exclude_weekly_overtime = parse_bool_flag(form.getfirst("exclude_weekly_overtime"), True)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            batch_name, batch_bytes = batch_file
            tip_name, tip_bytes = tip_file

            batch_path = tmp / safe_filename(batch_name, "batch.csv")
            tip_path = tmp / safe_filename(tip_name, "tips.csv")
            batch_path.write_bytes(batch_bytes)
            tip_path.write_bytes(tip_bytes)

            batch_names, _ = extract_source_names_from_batch(batch_path, exclude_weekly_overtime)
            tip_totals, _, _ = load_tips_csv(tip_path)
            source_names = sorted(set(batch_names) | set(tip_totals.keys()))

        employees = roster_employees()
        roster_names = [item["name"] for item in employees]
        _, unmatched = match_names(roster_names, source_names)

        default_assignments: dict[str, dict[str, Any]] = {}
        for unknown_name in unmatched:
            suggested_company = "scanio_moving"
            default_assignments[unknown_name] = {
                "home_company": suggested_company,
                "rate": infer_default_rate(suggested_company, employees),
            }

        json_response(
            self,
            {
                "ok": True,
                "unknown_names": unmatched,
                "default_assignments": default_assignments,
            },
        )

    def handle_convert(self) -> None:
        form = parse_multipart_form(self)
        batch_file = get_file_field(form, "batch_csv")
        tip_file = get_file_field(form, "tip_csv")
        template_file = get_file_field(form, "template_xlsx")

        if batch_file is None or tip_file is None:
            json_response(
                self,
                {"ok": False, "error": "Batch CSV and Tip CSV are required."},
                status=400,
            )
            return

        exclude_weekly_overtime = parse_bool_flag(form.getfirst("exclude_weekly_overtime"), True)
        assignments_raw = form.getfirst("assignments_json", "[]")
        try:
            assignments_list = json.loads(assignments_raw)
        except Exception:
            assignments_list = []

        assignment_map: dict[str, dict[str, Any]] = {}
        for item in assignments_list if isinstance(assignments_list, list) else []:
            name = normalize_spaces(str(item.get("name", "")))
            company = item.get("home_company")
            rate_value = item.get("rate", "")
            if not name or company not in DEFAULT_BURDEN_BY_COMPANY:
                continue
            assignment_map[name] = {"home_company": company, "rate": rate_value}

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            batch_name, batch_bytes = batch_file
            tip_name, tip_bytes = tip_file

            batch_path = tmp / safe_filename(batch_name, "batch.csv")
            tip_path = tmp / safe_filename(tip_name, "tips.csv")
            batch_path.write_bytes(batch_bytes)
            tip_path.write_bytes(tip_bytes)

            batch_names, simplified_hours_path = extract_source_names_from_batch(
                batch_path, exclude_weekly_overtime
            )
            tip_totals, _, _ = load_tips_csv(tip_path)
            source_names = sorted(set(batch_names) | set(tip_totals.keys()))

            employees = roster_employees()
            roster_names = [item["name"] for item in employees]
            _, unmatched = match_names(roster_names, source_names)

            missing_assignments = [name for name in unmatched if name not in assignment_map]
            if missing_assignments:
                json_response(
                    self,
                    {
                        "ok": False,
                        "error": (
                            "Missing company assignment for new names: "
                            + ", ".join(missing_assignments)
                        ),
                    },
                    status=400,
                )
                return

            if unmatched:
                payload = read_roster_payload()
                current = roster_employees()
                existing_names = {item["name"] for item in current}
                for unknown_name in unmatched:
                    if unknown_name in existing_names:
                        continue
                    assigned = assignment_map[unknown_name]
                    company = assigned["home_company"]
                    provided_rate = normalize_spaces(str(assigned.get("rate", "")))
                    if provided_rate:
                        try:
                            rate = float(provided_rate)
                        except ValueError:
                            rate = infer_default_rate(company, current)
                    else:
                        rate = infer_default_rate(company, current)

                    entry = {
                        "name": unknown_name,
                        "home_company": company,
                        "rate": rate,
                        "burden_multiplier": DEFAULT_BURDEN_BY_COMPANY[company],
                    }
                    payload.setdefault("employees", []).append(entry)
                    current.append(entry)
                write_roster_payload(payload)

            if template_file is not None:
                template_name, template_bytes = template_file
                template_path = tmp / safe_filename(template_name, "template.xlsx")
                template_path.write_bytes(template_bytes)
            else:
                configured_template = default_template_path()
                if configured_template is None:
                    json_response(
                        self,
                        {
                            "ok": False,
                            "error": (
                                "No template provided and no default template configured. "
                                "Upload a template XLSX."
                            ),
                        },
                        status=400,
                    )
                    return
                template_path = configured_template

            tip_summary_path = tip_path.with_name(f"{tip_path.stem}_simple.csv")
            filled_workbook_name = (
                f"{template_path.stem}_filled_with_tips.xlsx"
                if template_path.suffix.lower() == ".xlsx"
                else "payroll_filled_with_tips.xlsx"
            )
            filled_workbook_path = tmp / filled_workbook_name

            ok, fill_output = run_fill_script(
                template_path=template_path,
                hours_csv_path=simplified_hours_path,
                tip_csv_path=tip_path,
                tip_summary_path=tip_summary_path,
                output_xlsx_path=filled_workbook_path,
            )
            if not ok:
                json_response(
                    self,
                    {"ok": False, "error": "Workbook fill failed:\n" + fill_output},
                    status=500,
                )
                return

            file_response(
                self,
                filled_workbook_path.read_bytes(),
                filename=filled_workbook_path.name,
                content_type=(
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                ),
            )

    def handle_remove_employees(self) -> None:
        data = parse_json_body(self)
        names = data.get("names", [])
        if not isinstance(names, list):
            json_response(self, {"ok": False, "error": "Invalid names payload."}, status=400)
            return
        names_set = {normalize_spaces(str(name)) for name in names if normalize_spaces(str(name))}
        payload = read_roster_payload()
        employees = payload.get("employees", [])
        before = len(employees)
        payload["employees"] = [
            item
            for item in employees
            if normalize_spaces(str(item.get("name", ""))) not in names_set
        ]
        after = len(payload["employees"])
        write_roster_payload(payload)
        json_response(self, {"ok": True, "removed_count": before - after})

    def handle_update_employees(self) -> None:
        data = parse_json_body(self)
        updates = data.get("employees", [])
        if not isinstance(updates, list):
            json_response(
                self,
                {"ok": False, "error": "Invalid employees payload."},
                status=400,
            )
            return

        payload = read_roster_payload()
        employees = payload.get("employees", [])
        by_name: dict[str, dict[str, Any]] = {}
        for employee in employees:
            key = normalize_spaces(str(employee.get("name", "")))
            if key:
                by_name[key] = employee

        updated_count = 0
        for item in updates:
            if not isinstance(item, dict):
                continue
            name = normalize_spaces(str(item.get("name", "")))
            if not name or name not in by_name:
                continue

            raw_rate = item.get("rate")
            try:
                rate = float(raw_rate)
            except (TypeError, ValueError):
                json_response(
                    self,
                    {"ok": False, "error": f"Invalid rate for {name}."},
                    status=400,
                )
                return

            if rate < 0:
                json_response(
                    self,
                    {"ok": False, "error": f"Rate cannot be negative for {name}."},
                    status=400,
                )
                return

            by_name[name]["rate"] = rate
            updated_count += 1

        write_roster_payload(payload)
        json_response(self, {"ok": True, "updated_count": updated_count})

    def handle_set_template(self) -> None:
        form = parse_multipart_form(self)
        template_file = get_file_field(form, "template_xlsx")
        if template_file is None:
            json_response(self, {"ok": False, "error": "Template XLSX is required."}, status=400)
            return
        filename, file_bytes = template_file
        template_name = safe_filename(filename, "default_template.xlsx")
        if not template_name.lower().endswith(".xlsx"):
            template_name += ".xlsx"
        target = DATA_DIR / template_name
        target.write_bytes(file_bytes)
        shutil.copy2(target, DEFAULT_TEMPLATE_COPY_PATH)
        settings = read_settings()
        settings["default_template_path"] = str(DEFAULT_TEMPLATE_COPY_PATH)
        write_settings(settings)
        json_response(self, {"ok": True, "default_template_path": str(DEFAULT_TEMPLATE_COPY_PATH)})

    def log_message(self, format: str, *args: Any) -> None:
        # Keep terminal noise low.
        return


def run_app() -> None:
    ensure_roster_file()
    ensure_settings_file()
    host = "127.0.0.1"
    port = find_free_port(host)
    server = ThreadingHTTPServer((host, port), PayrollRequestHandler)
    url = f"http://{host}:{port}/"
    print(f"Payroll app running at {url}")
    webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    run_app()
