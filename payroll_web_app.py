#!/usr/bin/env python3
"""Online payroll web app with auth, SQLite, and background conversion workers."""

from __future__ import annotations

import cgi
import hashlib
import hmac
import json
import os
import re
import secrets
import shutil
import sqlite3
import tempfile
import threading
import time
import traceback
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from xml.etree import ElementTree as ET
import zipfile

from fill_payroll_workbook_from_hours import fill_workbook, load_tips_csv, match_names
from simplify_timecard_csv import flatten_timecard, write_flat_csv

APP_ROOT = Path(__file__).resolve().parent


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "")
    if not raw:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def resolve_data_dir() -> Path:
    configured = os.environ.get("PAYROLL_DATA_DIR", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return APP_ROOT / ".payroll_web_data"


DATA_DIR = resolve_data_dir()
USERS_DIR = DATA_DIR / "users"
DB_PATH = DATA_DIR / "payroll_web.db"
WORKSPACE_UI_FILENAME = "payroll_workspace_ui.html"

COMPANY_OPTIONS = [
    ("scanio_moving", "Scanio Moving"),
    ("scanio_storage", "Scanio Storage"),
    ("sea_and_air_intl", "Sea and Air Int-L"),
    ("flat_price", "Flat Price"),
]

DEFAULT_BURDEN_BY_COMPANY = {
    "scanio_moving": 1.18,
    "scanio_storage": 1.24,
    "sea_and_air_intl": 1.18,
    "flat_price": 1.18,
}

TEMPLATE_COMPANY_ROW_SLOTS = {
    "scanio_moving": list(range(5, 26)),
    "scanio_storage": list(range(33, 40)),
    "sea_and_air_intl": list(range(47, 57)),
    "flat_price": list(range(64, 86)),
}

TRACKED_COMPANY_EXPORT_LABELS = {
    0: "Scanio",
    1: "Sea and Air",
    2: "Flat Price",
}

TRACKED_COMPANY_TIP_NOTE = {
    0: "sc",
    1: "sa",
    2: "fp",
}

BUNDLED_TEMPLATE_CANDIDATE_NAMES = (
    "default_template.xlsx",
    "Payroll master.xlsx",
    "Copy of Payroll Weekly 01.31.26- 02.06.26.xlsx",
)

XLSX_NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
XLSX_NS = {"a": XLSX_NS_MAIN}
CELL_REF_RE = re.compile(r"([A-Z]+)(\d+)")
SESSION_TTL_SECONDS = 60 * 60 * 24 * 7
SESSION_COOKIE_NAME = os.environ.get("PAYROLL_SESSION_COOKIE_NAME", "payroll_session").strip() or "payroll_session"
SESSION_COOKIE_SAMESITE = os.environ.get("PAYROLL_COOKIE_SAMESITE", "Lax").strip().title() or "Lax"
if SESSION_COOKIE_SAMESITE not in {"Lax", "Strict", "None"}:
    SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE = env_bool("PAYROLL_COOKIE_SECURE", False) or SESSION_COOKIE_SAMESITE == "None"
SESSION_TTL_SECONDS = env_int("PAYROLL_SESSION_TTL_SECONDS", SESSION_TTL_SECONDS)
ALLOW_SELF_REGISTRATION = env_bool("PAYROLL_ALLOW_REGISTRATION", True)

JOB_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="payroll-agent")
JOB_FUTURES: dict[str, Any] = {}
JOB_FUTURES_LOCK = threading.Lock()


@dataclass
class AuthUser:
    user_id: int
    email: str


def now_ts() -> int:
    return int(time.time())


def normalize_spaces(value: str) -> str:
    return " ".join((value or "").strip().split())


def parse_iso_date(value: str) -> date | None:
    text = normalize_spaces(value)
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def format_us_date(value: date) -> str:
    return value.strftime("%m/%d/%Y")


def load_workspace_ui_html() -> str:
    candidate = APP_ROOT / WORKSPACE_UI_FILENAME
    if candidate.exists():
        try:
            return candidate.read_text(encoding="utf-8")
        except Exception:
            pass
    return (
        "<!doctype html><html><body>"
        "<h1>Payroll Weekly Workspace</h1>"
        "<p>UI file not found. Expected: "
        + str(candidate)
        + "</p>"
        "<p>Open <a href='/converter'>/converter</a> for converter mode.</p>"
        "</body></html>"
    )


def safe_filename(name: str, fallback: str) -> str:
    cleaned = "".join(ch for ch in (name or "") if ch.isalnum() or ch in (" ", ".", "-", "_"))
    cleaned = normalize_spaces(cleaned).replace(" ", "_")
    return cleaned if cleaned else fallback


def hash_password(password: str, salt_hex: str | None = None) -> tuple[str, str]:
    salt = bytes.fromhex(salt_hex) if salt_hex else secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return salt.hex(), digest.hex()


def verify_password(password: str, salt_hex: str, digest_hex: str) -> bool:
    _, computed = hash_password(password, salt_hex=salt_hex)
    return hmac.compare_digest(computed, digest_hex)


def user_dir(user_id: int) -> Path:
    path = USERS_DIR / str(user_id)
    path.mkdir(parents=True, exist_ok=True)
    (path / "templates").mkdir(parents=True, exist_ok=True)
    (path / "jobs").mkdir(parents=True, exist_ok=True)
    return path


def db_conn() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_storage() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    USERS_DIR.mkdir(parents=True, exist_ok=True)
    with db_conn() as con:
        con.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                password_salt TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                home_company TEXT NOT NULL,
                rate REAL NOT NULL,
                burden_multiplier REAL NOT NULL,
                is_hidden INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                UNIQUE(user_id, name COLLATE NOCASE),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS settings (
                user_id INTEGER PRIMARY KEY,
                default_template_path TEXT,
                updated_at INTEGER NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                error_text TEXT,
                output_path TEXT,
                output_filename TEXT,
                log_text TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS payroll_weeks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                week_start TEXT NOT NULL,
                week_end TEXT NOT NULL,
                pay_period TEXT NOT NULL,
                period_note TEXT,
                payload_json TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(user_id, week_start),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            """
        )
        employee_cols = {str(row["name"]) for row in con.execute("PRAGMA table_info(employees)").fetchall()}
        if "is_hidden" not in employee_cols:
            con.execute("ALTER TABLE employees ADD COLUMN is_hidden INTEGER NOT NULL DEFAULT 0")


def create_session(user_id: int) -> str:
    token = secrets.token_urlsafe(40)
    ts = now_ts()
    with db_conn() as con:
        con.execute(
            "INSERT INTO sessions(token, user_id, created_at, expires_at) VALUES(?,?,?,?)",
            (token, user_id, ts, ts + SESSION_TTL_SECONDS),
        )
    return token


def clear_expired_sessions() -> None:
    with db_conn() as con:
        con.execute("DELETE FROM sessions WHERE expires_at < ?", (now_ts(),))


def auth_user_from_handler(handler: BaseHTTPRequestHandler) -> AuthUser | None:
    clear_expired_sessions()
    cookie_header = handler.headers.get("Cookie", "")
    cookie = SimpleCookie()
    cookie.load(cookie_header)
    morsel = cookie.get(SESSION_COOKIE_NAME)
    if morsel is None:
        return None
    token = morsel.value
    with db_conn() as con:
        row = con.execute(
            """
            SELECT u.id AS user_id, u.email
            FROM sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.token = ? AND s.expires_at >= ?
            """,
            (token, now_ts()),
        ).fetchone()
    if row is None:
        return None
    return AuthUser(user_id=int(row["user_id"]), email=str(row["email"]))


def set_session_cookie(handler: BaseHTTPRequestHandler, token: str) -> None:
    cookie = SimpleCookie()
    cookie[SESSION_COOKIE_NAME] = token
    cookie[SESSION_COOKIE_NAME]["path"] = "/"
    cookie[SESSION_COOKIE_NAME]["httponly"] = True
    cookie[SESSION_COOKIE_NAME]["samesite"] = SESSION_COOKIE_SAMESITE
    cookie[SESSION_COOKIE_NAME]["max-age"] = SESSION_TTL_SECONDS
    if SESSION_COOKIE_SECURE:
        cookie[SESSION_COOKIE_NAME]["secure"] = True
    handler.send_header("Set-Cookie", cookie.output(header="").strip())


def clear_session_cookie(handler: BaseHTTPRequestHandler) -> None:
    cookie = SimpleCookie()
    cookie[SESSION_COOKIE_NAME] = ""
    cookie[SESSION_COOKIE_NAME]["path"] = "/"
    cookie[SESSION_COOKIE_NAME]["max-age"] = 0
    cookie[SESSION_COOKIE_NAME]["httponly"] = True
    cookie[SESSION_COOKIE_NAME]["samesite"] = SESSION_COOKIE_SAMESITE
    if SESSION_COOKIE_SECURE:
        cookie[SESSION_COOKIE_NAME]["secure"] = True
    handler.send_header("Set-Cookie", cookie.output(header="").strip())


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


def html_response(handler: BaseHTTPRequestHandler, html: str, status: int = 200) -> None:
    body = html.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def file_response(handler: BaseHTTPRequestHandler, data: bytes, filename: str, content_type: str) -> None:
    handler.send_response(200)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Disposition", f'attachment; filename="{filename}"')
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def redirect_response(handler: BaseHTTPRequestHandler, location: str, status: int = 302) -> None:
    handler.send_response(status)
    handler.send_header("Location", location)
    handler.send_header("Content-Length", "0")
    handler.end_headers()


def parse_json_body(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    length = int(handler.headers.get("Content-Length", "0"))
    raw = handler.rfile.read(length) if length > 0 else b"{}"
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def parse_multipart_form(handler: BaseHTTPRequestHandler) -> cgi.FieldStorage:
    content_type = handler.headers.get("Content-Type", "")
    return cgi.FieldStorage(
        fp=handler.rfile,
        headers=handler.headers,
        environ={"REQUEST_METHOD": "POST", "CONTENT_TYPE": content_type},
        keep_blank_values=True,
    )


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


def csv_writer(handle: Any) -> Any:
    import csv

    return csv.writer(handle)


def parse_bool_flag(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def infer_default_rate(company: str, employees: list[dict[str, Any]]) -> float:
    company_rates = [float(item["rate"]) for item in employees if item["home_company"] == company]
    if not company_rates:
        return 0.0
    return round(sum(company_rates) / len(company_rates), 2)


def read_shared_strings_from_xlsx(zf: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    strings: list[str] = []
    for item in root.findall("a:si", XLSX_NS):
        text = "".join(node.text or "" for node in item.findall(".//a:t", XLSX_NS))
        strings.append(text)
    return strings


def parse_cell_ref(cell_ref: str) -> tuple[str, int]:
    match = CELL_REF_RE.fullmatch(cell_ref)
    if not match:
        return ("", 0)
    return (match.group(1), int(match.group(2)))


def cell_string_value(cell: ET.Element | None, shared_strings: list[str]) -> str | None:
    if cell is None:
        return None
    cell_type = cell.attrib.get("t")
    if cell_type == "s":
        value_node = cell.find("a:v", XLSX_NS)
        if value_node is None or value_node.text is None:
            return None
        try:
            idx = int(value_node.text)
            if 0 <= idx < len(shared_strings):
                return shared_strings[idx]
        except Exception:
            return None
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell.findall(".//a:t", XLSX_NS))
    value_node = cell.find("a:v", XLSX_NS)
    if value_node is not None and value_node.text is not None:
        return value_node.text
    return None


def cell_numeric_value(cell: ET.Element | None) -> float | None:
    if cell is None:
        return None
    value_node = cell.find("a:v", XLSX_NS)
    if value_node is None or value_node.text is None:
        return None
    try:
        return float(value_node.text)
    except Exception:
        return None


def template_employees(template_path: Path | None) -> list[dict[str, Any]]:
    if template_path is None or not template_path.exists():
        return []

    try:
        with zipfile.ZipFile(template_path, "r") as zf:
            if "xl/worksheets/sheet1.xml" not in zf.namelist():
                return []

            shared_strings = read_shared_strings_from_xlsx(zf)
            sheet_root = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
            sheet_data = sheet_root.find("a:sheetData", XLSX_NS)
            if sheet_data is None:
                return []

            row_lookup: dict[int, dict[str, ET.Element]] = {}
            for row_elem in sheet_data.findall("a:row", XLSX_NS):
                try:
                    row_num = int(row_elem.attrib.get("r", "0"))
                except Exception:
                    continue
                cell_map: dict[str, ET.Element] = {}
                for cell in row_elem.findall("a:c", XLSX_NS):
                    col, _ = parse_cell_ref(cell.attrib.get("r", ""))
                    if col:
                        cell_map[col] = cell
                row_lookup[row_num] = cell_map

            output: list[dict[str, Any]] = []
            seen: set[str] = set()
            for home_company, rows in TEMPLATE_COMPANY_ROW_SLOTS.items():
                for row_num in rows:
                    cells = row_lookup.get(row_num, {})
                    raw_name = cell_string_value(cells.get("B"), shared_strings)
                    name = normalize_spaces(str(raw_name or ""))
                    if not name:
                        continue
                    key = name.lower()
                    if key in seen:
                        continue
                    seen.add(key)

                    rate = cell_numeric_value(cells.get("C"))
                    output.append(
                        {
                            "name": name,
                            "home_company": home_company,
                            "rate": float(rate) if rate is not None else 0.0,
                            "burden_multiplier": DEFAULT_BURDEN_BY_COMPANY[home_company],
                        }
                    )

            return output
    except Exception:
        return []


def get_default_template_path(user_id: int) -> Path | None:
    with db_conn() as con:
        row = con.execute("SELECT default_template_path FROM settings WHERE user_id = ?", (user_id,)).fetchone()
    if row is None:
        return None
    path_text = normalize_spaces(str(row["default_template_path"] or ""))
    if not path_text:
        return None
    path = Path(path_text)
    return path if path.exists() else None


def set_default_template_path(user_id: int, template_path: Path) -> None:
    with db_conn() as con:
        con.execute(
            """
            INSERT INTO settings(user_id, default_template_path, updated_at)
            VALUES(?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                default_template_path = excluded.default_template_path,
                updated_at = excluded.updated_at
            """,
            (user_id, str(template_path), now_ts()),
        )


def get_employees(user_id: int, include_hidden: bool = False) -> list[dict[str, Any]]:
    hidden_filter = "" if include_hidden else "AND is_hidden = 0"
    with db_conn() as con:
        rows = con.execute(
            """
            SELECT name, home_company, rate, burden_multiplier, is_hidden
            FROM employees
            WHERE user_id = ?
            """
            + hidden_filter
            + """
            ORDER BY lower(name)
            """,
            (user_id,),
        ).fetchall()
    return [
        {
            "name": str(row["name"]),
            "home_company": str(row["home_company"]),
            "rate": float(row["rate"]),
            "burden_multiplier": float(row["burden_multiplier"]),
            "is_hidden": bool(int(row["is_hidden"])),
        }
        for row in rows
    ]


def upsert_employee(user_id: int, name: str, home_company: str, rate: float) -> None:
    if home_company not in DEFAULT_BURDEN_BY_COMPANY:
        raise ValueError("Invalid company")
    burden = DEFAULT_BURDEN_BY_COMPANY[home_company]
    with db_conn() as con:
        con.execute(
            """
            INSERT INTO employees(user_id, name, home_company, rate, burden_multiplier, is_hidden, updated_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(user_id, name) DO UPDATE SET
                home_company = excluded.home_company,
                rate = excluded.rate,
                burden_multiplier = excluded.burden_multiplier,
                is_hidden = 0,
                updated_at = excluded.updated_at
            """,
            (user_id, name, home_company, rate, burden, 0, now_ts()),
        )


def remove_employees(user_id: int, names: list[str]) -> int:
    cleaned = [normalize_spaces(name) for name in names if normalize_spaces(name)]
    if not cleaned:
        return 0
    placeholders = ",".join(["?"] * len(cleaned))
    with db_conn() as con:
        before = con.execute("SELECT COUNT(*) FROM employees WHERE user_id = ?", (user_id,)).fetchone()[0]
        con.execute(
            f"DELETE FROM employees WHERE user_id = ? AND lower(name) IN ({placeholders})",
            [user_id, *[name.lower() for name in cleaned]],
        )
        after = con.execute("SELECT COUNT(*) FROM employees WHERE user_id = ?", (user_id,)).fetchone()[0]
    return int(before - after)


def set_employees_hidden(user_id: int, names: list[str], hidden: bool) -> int:
    cleaned = [normalize_spaces(name) for name in names if normalize_spaces(name)]
    if not cleaned:
        return 0
    placeholders = ",".join(["?"] * len(cleaned))
    with db_conn() as con:
        con.execute(
            f"""
            UPDATE employees
            SET is_hidden = ?, updated_at = ?
            WHERE user_id = ? AND lower(name) IN ({placeholders})
            """,
            [1 if hidden else 0, now_ts(), user_id, *[name.lower() for name in cleaned]],
        )
        changed = int(con.execute("SELECT changes()").fetchone()[0])
    return changed


def sync_employees_from_template(user_id: int, template_path: Path) -> dict[str, int]:
    items = template_employees(template_path)
    updated = 0
    added = 0
    with db_conn() as con:
        for item in items:
            name = normalize_spaces(item["name"])
            if not name:
                continue
            existing = con.execute(
                "SELECT home_company, rate FROM employees WHERE user_id = ? AND lower(name) = ?",
                (user_id, name.lower()),
            ).fetchone()
            home_company = item["home_company"]
            rate = safe_float(item["rate"], 0.0)
            burden = DEFAULT_BURDEN_BY_COMPANY.get(home_company, 1.18)
            if existing is None:
                con.execute(
                    """
                    INSERT INTO employees(user_id, name, home_company, rate, burden_multiplier, updated_at)
                    VALUES(?,?,?,?,?,?)
                    """,
                    (user_id, name, home_company, rate, burden, now_ts()),
                )
                added += 1
            else:
                old_home = str(existing["home_company"])
                old_rate = float(existing["rate"])
                if old_home != home_company or abs(old_rate - rate) > 1e-9:
                    con.execute(
                        """
                        UPDATE employees
                        SET home_company = ?, rate = ?, burden_multiplier = ?, updated_at = ?
                        WHERE user_id = ? AND lower(name) = ?
                        """,
                        (home_company, rate, burden, now_ts(), user_id, name.lower()),
                    )
                    updated += 1
    return {"template_count": len(items), "updated_count": updated, "added_count": added}


def write_roster_json(path: Path, employees: list[dict[str, Any]]) -> None:
    payload = {
        "employees": [
            {
                "name": entry["name"],
                "home_company": entry["home_company"],
                "rate": safe_float(entry["rate"], 0.0),
            }
            for entry in employees
        ]
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def home_company_from_label(label: str) -> str:
    text = normalize_spaces(label).lower()
    if "scanio storage" in text:
        return "scanio_storage"
    if "scanio" in text:
        return "scanio_moving"
    if "sea" in text and "air" in text:
        return "sea_and_air_intl"
    if "flat" in text:
        return "flat_price"
    return "scanio_moving"


def split_tip_name(name: str) -> tuple[str, str]:
    tokens = normalize_spaces(name).split()
    if not tokens:
        return ("", "")
    if len(tokens) == 1:
        return (tokens[0], tokens[0])
    return (tokens[-1], " ".join(tokens[:-1]))


def aggregate_workspace_employees(rows: list[Any]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    for item in rows:
        if not isinstance(item, dict):
            continue
        name = normalize_spaces(str(item.get("name", "")))
        if not name:
            continue
        key = name.lower()
        entry = merged.setdefault(
            key,
            {
                "name": name,
                "payroll_company_label": "",
                "rate": 0.0,
                "hours": [0.0, 0.0, 0.0],
                "commissions": [0.0, 0.0, 0.0],
            },
        )
        payroll_label = normalize_spaces(str(item.get("payrollCompany", "")))
        if payroll_label:
            entry["payroll_company_label"] = payroll_label

        rate_value = safe_float(item.get("rate"), entry["rate"])
        if rate_value > 0 or entry["rate"] <= 0:
            entry["rate"] = rate_value

        days = item.get("days", [])
        if not isinstance(days, list):
            continue
        for day in days:
            if not isinstance(day, dict):
                continue
            hours = day.get("hours", [])
            commissions = day.get("commissions", [])
            if isinstance(hours, list):
                for idx in range(3):
                    if idx < len(hours):
                        entry["hours"][idx] += safe_float(hours[idx], 0.0)
            if isinstance(commissions, list):
                for idx in range(3):
                    if idx < len(commissions):
                        entry["commissions"][idx] += safe_float(commissions[idx], 0.0)

    output: list[dict[str, Any]] = []
    for merged_entry in merged.values():
        home_company = home_company_from_label(merged_entry["payroll_company_label"])
        output.append(
            {
                "name": merged_entry["name"],
                "home_company": home_company,
                "rate": float(merged_entry["rate"]),
                "hours": [float(value) for value in merged_entry["hours"]],
                "commissions": [float(value) for value in merged_entry["commissions"]],
            }
        )
    output.sort(key=lambda item: item["name"].lower())
    return output


def write_workspace_hours_csv(path: Path, employees: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv_writer(handle)
        writer.writerow(["Name", "Company", "Hours at Company"])
        for employee in employees:
            for idx in range(3):
                hours = safe_float(employee["hours"][idx], 0.0)
                if abs(hours) < 1e-9:
                    continue
                writer.writerow(
                    [
                        employee["name"],
                        TRACKED_COMPANY_EXPORT_LABELS[idx],
                        f"{hours:.2f}",
                    ]
                )


def write_workspace_tips_csv(path: Path, employees: list[dict[str, Any]]) -> None:
    fallback_note = {
        "scanio_moving": "sc",
        "scanio_storage": "sc",
        "sea_and_air_intl": "sa",
        "flat_price": "fp",
    }
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv_writer(handle)
        writer.writerow(
            [
                "EMP L NAME",
                "EMP F NAME",
                "EMP #",
                "NOTES",
                "JOB",
                "CASH TIPS",
                "VOID",
                "CARD TIPS",
                "OTHER",
                "TOTAL",
            ]
        )
        for employee in employees:
            last_name, first_name = split_tip_name(employee["name"])
            if not last_name or not first_name:
                continue
            wrote_row = False
            for idx in range(3):
                amount = safe_float(employee["commissions"][idx], 0.0)
                if abs(amount) < 1e-9:
                    continue
                wrote_row = True
                writer.writerow(
                    [
                        last_name,
                        first_name,
                        "",
                        TRACKED_COMPANY_TIP_NOTE[idx],
                        "",
                        f"{amount:.2f}",
                        "",
                        "0",
                        "",
                        "",
                    ]
                )
            if not wrote_row:
                writer.writerow(
                    [
                        last_name,
                        first_name,
                        "",
                        fallback_note.get(employee["home_company"], "sc"),
                        "",
                        "0",
                        "",
                        "0",
                        "",
                        "",
                    ]
                )


def write_workspace_roster_json(path: Path, employees: list[dict[str, Any]]) -> None:
    payload = {
        "employees": [
            {
                "name": employee["name"],
                "home_company": employee["home_company"],
                "rate": float(employee["rate"]),
                "burden_multiplier": DEFAULT_BURDEN_BY_COMPANY[employee["home_company"]],
            }
            for employee in employees
        ]
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def bundled_template_path() -> Path | None:
    for name in BUNDLED_TEMPLATE_CANDIDATE_NAMES:
        candidate = APP_ROOT / name
        if candidate.exists():
            return candidate
    return None


def ensure_user_default_template(user_id: int) -> Path | None:
    existing = get_default_template_path(user_id)
    if existing is not None and existing.exists():
        return existing

    bundled = bundled_template_path()
    if bundled is None:
        return None

    udir = user_dir(user_id)
    default_copy = udir / "templates" / "default_template.xlsx"
    if not default_copy.exists():
        shutil.copy2(bundled, default_copy)
    set_default_template_path(user_id, default_copy)
    sync_employees_from_template(user_id, default_copy)
    return default_copy


def ensure_user_employees_seeded(user_id: int) -> None:
    template = ensure_user_default_template(user_id)
    if template is None:
        return
    if not get_employees(user_id):
        sync_employees_from_template(user_id, template)


def save_payroll_week(
    user_id: int,
    week_start: str,
    week_end: str,
    pay_period: str,
    period_note: str,
    payload_json: str,
) -> int:
    ts = now_ts()
    with db_conn() as con:
        con.execute(
            """
            INSERT INTO payroll_weeks(
                user_id, week_start, week_end, pay_period, period_note, payload_json, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(user_id, week_start) DO UPDATE SET
                week_end = excluded.week_end,
                pay_period = excluded.pay_period,
                period_note = excluded.period_note,
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            """,
            (user_id, week_start, week_end, pay_period, period_note, payload_json, ts, ts),
        )
        row = con.execute(
            "SELECT id FROM payroll_weeks WHERE user_id = ? AND week_start = ?",
            (user_id, week_start),
        ).fetchone()
    return int(row["id"]) if row is not None else 0


def list_payroll_weeks(user_id: int, limit: int = 200) -> list[dict[str, Any]]:
    with db_conn() as con:
        rows = con.execute(
            """
            SELECT id, week_start, week_end, pay_period, period_note, created_at, updated_at
            FROM payroll_weeks
            WHERE user_id = ?
            ORDER BY week_start DESC, updated_at DESC
            LIMIT ?
            """,
            (user_id, max(1, min(1000, int(limit)))),
        ).fetchall()
    return [
        {
            "id": int(row["id"]),
            "week_start": str(row["week_start"]),
            "week_end": str(row["week_end"]),
            "pay_period": str(row["pay_period"]),
            "period_note": str(row["period_note"] or ""),
            "created_at": int(row["created_at"]),
            "updated_at": int(row["updated_at"]),
        }
        for row in rows
    ]


def get_payroll_week(user_id: int, period_id: int) -> dict[str, Any] | None:
    with db_conn() as con:
        row = con.execute(
            """
            SELECT id, week_start, week_end, pay_period, period_note, payload_json, created_at, updated_at
            FROM payroll_weeks
            WHERE user_id = ? AND id = ?
            """,
            (user_id, period_id),
        ).fetchone()
    if row is None:
        return None
    try:
        payload = json.loads(str(row["payload_json"] or "{}"))
    except Exception:
        payload = {}
    return {
        "id": int(row["id"]),
        "week_start": str(row["week_start"]),
        "week_end": str(row["week_end"]),
        "pay_period": str(row["pay_period"]),
        "period_note": str(row["period_note"] or ""),
        "created_at": int(row["created_at"]),
        "updated_at": int(row["updated_at"]),
        "payload": payload,
    }


def extract_source_names_from_batch(batch_csv: Path, exclude_weekly_overtime: bool, out_csv: Path) -> list[str]:
    include_weekly_overtime = not exclude_weekly_overtime
    totals = flatten_timecard(batch_csv, include_weekly_overtime=include_weekly_overtime)
    write_flat_csv(out_csv, totals)
    return sorted({name for (name, _company) in totals.keys()})


def create_job(user_id: int) -> str:
    job_id = secrets.token_hex(12)
    ts = now_ts()
    with db_conn() as con:
        con.execute(
            "INSERT INTO jobs(id, user_id, status, created_at, updated_at) VALUES(?,?,?,?,?)",
            (job_id, user_id, "queued", ts, ts),
        )
    return job_id


def update_job(job_id: str, **fields: Any) -> None:
    if not fields:
        return
    columns = []
    values: list[Any] = []
    for key, value in fields.items():
        columns.append(f"{key} = ?")
        values.append(value)
    columns.append("updated_at = ?")
    values.append(now_ts())
    values.append(job_id)
    with db_conn() as con:
        con.execute(f"UPDATE jobs SET {', '.join(columns)} WHERE id = ?", values)


def list_jobs(user_id: int, limit: int = 30) -> list[dict[str, Any]]:
    with db_conn() as con:
        rows = con.execute(
            """
            SELECT id, status, created_at, updated_at, error_text, output_filename
            FROM jobs
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (user_id, limit),
        ).fetchall()
    return [
        {
            "id": str(row["id"]),
            "status": str(row["status"]),
            "created_at": int(row["created_at"]),
            "updated_at": int(row["updated_at"]),
            "error_text": str(row["error_text"] or ""),
            "output_filename": str(row["output_filename"] or ""),
        }
        for row in rows
    ]


def get_job(user_id: int, job_id: str) -> sqlite3.Row | None:
    with db_conn() as con:
        row = con.execute(
            "SELECT * FROM jobs WHERE user_id = ? AND id = ?",
            (user_id, job_id),
        ).fetchone()
    return row


def process_job(
    *,
    user_id: int,
    job_id: str,
    batch_path: Path,
    tip_path: Path,
    template_override_path: Path | None,
    exclude_weekly_overtime: bool,
    assignment_map: dict[str, dict[str, Any]],
) -> None:
    try:
        update_job(job_id, status="running")

        job_dir = user_dir(user_id) / "jobs" / job_id
        job_dir.mkdir(parents=True, exist_ok=True)

        simplified_hours = job_dir / "hours_simple.csv"
        tip_summary = job_dir / "tips_simple.csv"
        roster_json = job_dir / "roster.json"

        batch_names = extract_source_names_from_batch(batch_path, exclude_weekly_overtime, simplified_hours)
        tip_totals, _, _ = load_tips_csv(tip_path)
        source_names = sorted(set(batch_names) | set(tip_totals.keys()))

        employees = get_employees(user_id)
        roster_names = [item["name"] for item in employees]
        _, unmatched = match_names(roster_names, source_names)

        missing_assignments = [name for name in unmatched if name not in assignment_map]
        if missing_assignments:
            raise ValueError("Missing company assignment for: " + ", ".join(missing_assignments))

        if unmatched:
            latest = get_employees(user_id)
            for unknown_name in unmatched:
                assigned = assignment_map[unknown_name]
                company = str(assigned.get("home_company", "scanio_moving"))
                if company not in DEFAULT_BURDEN_BY_COMPANY:
                    company = "scanio_moving"
                rate_text = normalize_spaces(str(assigned.get("rate", "")))
                if rate_text:
                    try:
                        rate = float(rate_text)
                    except ValueError:
                        rate = infer_default_rate(company, latest)
                else:
                    rate = infer_default_rate(company, latest)
                upsert_employee(user_id, unknown_name, company, rate)

        employees = get_employees(user_id)
        write_roster_json(roster_json, employees)

        template_path = template_override_path or get_default_template_path(user_id)
        if template_path is None or not template_path.exists():
            raise ValueError("No default template configured.")

        output_filename = f"{template_path.stem}_filled_with_tips.xlsx"
        output_path = job_dir / output_filename

        result = fill_workbook(
            workbook_path=template_path,
            hours_csv_path=simplified_hours,
            output_path=output_path,
            roster_path=roster_json,
            tips_csv_path=tip_path,
            tip_summary_output_path=tip_summary,
        )

        update_job(
            job_id,
            status="completed",
            output_path=str(output_path),
            output_filename=output_filename,
            log_text=(
                f"Matched hour names: {len(result['source_to_workbook'])}/{len(result['source_names'])}\\n"
                f"Matched tip names: {len(result['tip_source_to_workbook'])}/{len(result['tip_source_names'])}"
            ),
        )
    except Exception as exc:
        update_job(job_id, status="failed", error_text=str(exc)[:4000], log_text=traceback.format_exc()[:8000])


def submit_job(
    *,
    user_id: int,
    job_id: str,
    batch_path: Path,
    tip_path: Path,
    template_override_path: Path | None,
    exclude_weekly_overtime: bool,
    assignment_map: dict[str, dict[str, Any]],
) -> None:
    future = JOB_EXECUTOR.submit(
        process_job,
        user_id=user_id,
        job_id=job_id,
        batch_path=batch_path,
        tip_path=tip_path,
        template_override_path=template_override_path,
        exclude_weekly_overtime=exclude_weekly_overtime,
        assignment_map=assignment_map,
    )
    with JOB_FUTURES_LOCK:
        JOB_FUTURES[job_id] = future


LOGIN_PAGE = """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Payroll Login</title>
  <style>
    :root {
      --bg: #f4f7fb;
      --panel: #ffffff;
      --line: #d5deea;
      --text: #162435;
      --muted: #5d6b7d;
      --brand: #0f766e;
      --brand-dark: #0b5d56;
      --danger: #b42318;
      --ok: #137333;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", Helvetica, Arial, sans-serif;
      background: linear-gradient(180deg, #ebf1f8 0%, #f8fafc 100%);
      color: var(--text);
    }
    .wrap {
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 18px;
    }
    .card {
      width: min(520px, 100%);
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      box-shadow: 0 10px 26px rgba(16, 35, 56, 0.10);
      padding: 20px;
    }
    h1 { margin: 0 0 8px; font-size: 26px; }
    .muted { color: var(--muted); font-size: 14px; margin-bottom: 12px; }
    .row { display: flex; gap: 8px; flex-wrap: wrap; }
    input, button {
      font: inherit;
      border-radius: 9px;
      border: 1px solid #bdcad9;
      padding: 10px 12px;
    }
    input { width: 100%; margin-bottom: 8px; }
    button {
      border-color: var(--brand);
      background: var(--brand);
      color: white;
      font-weight: 700;
      cursor: pointer;
    }
    button.secondary {
      background: white;
      color: var(--brand-dark);
    }
    .status {
      font-size: 13px;
      margin-top: 8px;
      min-height: 18px;
      white-space: pre-wrap;
    }
    .error { color: var(--danger); }
    .ok { color: var(--ok); }
    .help { margin-top: 10px; font-size: 13px; color: var(--muted); }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>Payroll Weekly Online</h1>
      <div class="muted">Login to access the weekly sheet, PDF output, and saved payroll history.</div>
      <input id="email" type="email" placeholder="Email" />
      <input id="password" type="password" placeholder="Password (min 8 chars)" />
      <div class="row">
        <button id="loginBtn" type="button">Login</button>
        <button id="registerBtn" class="secondary" type="button">Register</button>
      </div>
      <div id="status" class="status"></div>
      <div class="help">No default username/password. Register once, then login.</div>
    </div>
  </div>
  <script>
    function setStatus(text, mode) {
      const el = document.getElementById("status");
      el.textContent = text || "";
      el.className = "status" + (mode ? " " + mode : "");
    }

    async function postJson(url, payload) {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload || {})
      });
      let data = {};
      try { data = await response.json(); } catch {}
      return { response, data };
    }

    function formPayload() {
      return {
        email: document.getElementById("email").value.trim(),
        password: document.getElementById("password").value
      };
    }

    async function login() {
      const { response, data } = await postJson("/api/auth/login", formPayload());
      if (!response.ok || !data.ok) {
        setStatus(data.error || "Login failed.", "error");
        return;
      }
      window.location.href = "/workspace";
    }

    async function registerUser() {
      const { response, data } = await postJson("/api/auth/register", formPayload());
      if (!response.ok || !data.ok) {
        setStatus(data.error || "Register failed.", "error");
        return;
      }
      setStatus("Account created. You can login now.", "ok");
    }

    document.getElementById("loginBtn").addEventListener("click", login);
    document.getElementById("registerBtn").addEventListener("click", registerUser);
  </script>
</body>
</html>
"""


HTML_PAGE = """<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Payroll Web</title>
  <style>
    :root {
      --bg: #f3f4f6;
      --panel: #ffffff;
      --line: #cfd8e3;
      --text: #132033;
      --muted: #5c6b80;
      --brand: #0f766e;
      --brand-2: #0b5d56;
      --danger: #b42318;
      --ok: #137333;
      --alt-col: #f7fafc;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", Helvetica, Arial, sans-serif;
      background: linear-gradient(180deg, #ecf2f9 0%, #f8fafc 100%);
      color: var(--text);
    }
    .wrap {
      max-width: 1220px;
      margin: 20px auto 40px;
      padding: 0 16px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      box-shadow: 0 10px 28px rgba(18, 35, 56, 0.08);
      padding: 14px;
      margin-bottom: 14px;
    }
    h1, h2 { margin: 0 0 10px; }
    h1 { font-size: 24px; }
    h2 { font-size: 18px; }
    .muted { color: var(--muted); font-size: 13px; }
    .row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 10px; }
    input, select, button {
      font: inherit;
      border-radius: 8px;
      border: 1px solid #becbda;
      padding: 8px 10px;
    }
    button {
      border-color: var(--brand);
      background: var(--brand);
      color: white;
      font-weight: 700;
      cursor: pointer;
    }
    button.secondary { background: white; color: var(--brand); }
    button.danger { border-color: var(--danger); background: var(--danger); }
    button:disabled { opacity: .55; cursor: not-allowed; }
    .drop {
      border: 2px dashed #a9b9cc;
      border-radius: 10px;
      min-height: 86px;
      padding: 10px;
      background: #fbfdff;
      cursor: pointer;
    }
    .drop.active { border-color: var(--brand); background: #ecfdf5; }
    .drop .title { font-weight: 700; margin-bottom: 6px; }
    .drop .file { font-size: 12px; color: var(--muted); word-break: break-all; }
    input[type=file] { display: none; }
    .status { white-space: pre-wrap; font-size: 13px; margin-top: 8px; }
    .status.error { color: var(--danger); }
    .status.ok { color: var(--ok); }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; padding: 8px 6px; border-bottom: 1px solid #e4ebf3; }
    th { background: #eef4fa; }
    th:nth-child(even), td:nth-child(even) { background: var(--alt-col); }
    .hidden { display: none; }
    .unknown-list { border: 1px solid var(--line); border-radius: 8px; padding: 8px; margin-top: 8px; }
    .unknown-row { display: grid; grid-template-columns: minmax(220px, 1fr) 200px 120px; gap: 8px; margin-bottom: 8px; }
    .unknown-row:last-child { margin-bottom: 0; }
    .badge { display: inline-block; padding: 3px 7px; border-radius: 999px; font-size: 12px; font-weight: 700; }
    .queued { background: #eef2ff; color: #243c96; }
    .running { background: #fff7ed; color: #9a3412; }
    .completed { background: #ecfdf3; color: #166534; }
    .failed { background: #fef2f2; color: #991b1b; }
  </style>
</head>
<body>
  <div class=\"wrap\">
    <div class=\"panel\">
      <h1>Payroll Online</h1>
      <div class=\"muted\">Login + database + background conversion agents</div>
    </div>

    <div id=\"authPanel\" class=\"panel\">
      <h2>Login</h2>
      <div class=\"row\">
        <input id=\"email\" type=\"email\" placeholder=\"email\" style=\"min-width:260px\" />
        <input id=\"password\" type=\"password\" placeholder=\"password\" style=\"min-width:220px\" />
        <button id=\"loginBtn\">Login</button>
        <button id=\"registerBtn\" class=\"secondary\">Register</button>
      </div>
      <div id=\"authStatus\" class=\"status\"></div>
    </div>

    <div id=\"appPanel\" class=\"hidden\">
      <div class=\"panel\">
        <div class=\"row\" style=\"justify-content:space-between\">
          <div><strong id=\"meEmail\"></strong></div>
          <button id=\"logoutBtn\" class=\"secondary\">Logout</button>
        </div>
      </div>

      <div class=\"panel\">
        <h2>Template</h2>
        <div id=\"defaultTemplateText\" class=\"muted\"></div>
        <div class=\"grid\" style=\"margin-top:8px\">
          <label id=\"setTemplateDrop\" class=\"drop\">
            <div class=\"title\">Default Template (XLSX)</div>
            <div id=\"setTemplateFileLabel\" class=\"file\">Drop template or click</div>
            <input id=\"setTemplateInput\" type=\"file\" accept=\".xlsx\" />
          </label>
        </div>
        <div class=\"row\" style=\"margin-top:8px\">
          <button id=\"saveTemplateBtn\">Save as Default</button>
        </div>
      </div>

      <div class=\"panel\">
        <h2>Convert</h2>
        <div class=\"grid\">
          <label id=\"batchDrop\" class=\"drop\">
            <div class=\"title\">Batch CSV</div>
            <div id=\"batchFileLabel\" class=\"file\">Drop batch csv</div>
            <input id=\"batchInput\" type=\"file\" accept=\".csv\" />
          </label>
          <label id=\"tipDrop\" class=\"drop\">
            <div class=\"title\">Tip CSV</div>
            <div id=\"tipFileLabel\" class=\"file\">Drop tip csv</div>
            <input id=\"tipInput\" type=\"file\" accept=\".csv\" />
          </label>
          <label id=\"templateDrop\" class=\"drop\">
            <div class=\"title\">Override Template (Optional)</div>
            <div id=\"templateFileLabel\" class=\"file\">Drop template if not using default</div>
            <input id=\"templateInput\" type=\"file\" accept=\".xlsx\" />
          </label>
        </div>
        <div class=\"row\" style=\"margin-top:8px\">
          <label><input id=\"excludeWeeklyOt\" type=\"checkbox\" /> Exclude weekly overtime</label>
          <button id=\"checkBtn\" class=\"secondary\">Check New Names</button>
          <button id=\"convertBtn\">Queue Conversion Job</button>
        </div>
        <div id=\"unknownPanel\" class=\"hidden\">
          <h3 style=\"margin:8px 0 6px\">Assign New Names</h3>
          <div id=\"unknownList\" class=\"unknown-list\"></div>
        </div>
        <div id=\"convertStatus\" class=\"status\"></div>
      </div>

      <div class=\"panel\">
        <h2>Jobs (Agents)</h2>
        <div class=\"muted\">Queued jobs run in background workers. Download when complete.</div>
        <table>
          <thead><tr><th>ID</th><th>Status</th><th>Created</th><th>Updated</th><th>Output</th><th>Error</th></tr></thead>
          <tbody id=\"jobsTbody\"></tbody>
        </table>
      </div>

      <div class=\"panel\">
        <h2>Manage Employees</h2>
        <div class=\"row\">
          <input id=\"newEmployeeName\" type=\"text\" placeholder=\"Employee Name\" style=\"min-width:260px\" />
          <select id=\"newEmployeeCompany\"></select>
          <input id=\"newEmployeeRate\" type=\"number\" min=\"0\" step=\"0.01\" placeholder=\"Rate\" />
          <button id=\"addEmployeeBtn\">Add</button>
          <button id=\"saveEmployeesBtn\" class=\"secondary\">Save Changes</button>
          <button id=\"removeEmployeesBtn\" class=\"danger\">Remove Selected</button>
          <button id=\"refreshEmployeesBtn\" class=\"secondary\">Refresh</button>
        </div>
        <table>
          <thead><tr><th></th><th>Name</th><th>Company</th><th>Rate</th></tr></thead>
          <tbody id=\"employeesTbody\"></tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    const files = { batch: null, tip: null, template: null, defaultTemplate: null };
    let unknownNames = [];
    let unknownDefaults = {};
    let jobsPollTimer = null;

    const companyOptions = [
      { value: "scanio_moving", label: "Scanio Moving" },
      { value: "scanio_storage", label: "Scanio Storage" },
      { value: "sea_and_air_intl", label: "Sea and Air Int-L" },
      { value: "flat_price", label: "Flat Price" }
    ];

    function setStatus(id, text, isError=false, isOk=false) {
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = text;
      el.className = "status" + (isError ? " error" : "") + (isOk ? " ok" : "");
    }

    function escapeHtml(text) {
      return String(text || "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }

    function bindDrop(dropId, inputId, fileKey, labelId) {
      const drop = document.getElementById(dropId);
      const input = document.getElementById(inputId);
      const label = document.getElementById(labelId);

      function setFile(file) {
        files[fileKey] = file || null;
        label.textContent = file ? `${file.name} (${Math.round(file.size/1024)} KB)` : "No file selected";
      }

      drop.addEventListener("click", () => input.click());
      input.addEventListener("change", () => setFile(input.files?.[0] || null));

      drop.addEventListener("dragover", (e) => { e.preventDefault(); drop.classList.add("active"); });
      drop.addEventListener("dragleave", () => drop.classList.remove("active"));
      drop.addEventListener("drop", (e) => {
        e.preventDefault();
        drop.classList.remove("active");
        const file = e.dataTransfer?.files?.[0];
        if (file) {
          const dt = new DataTransfer();
          dt.items.add(file);
          input.files = dt.files;
          setFile(file);
        }
      });
    }

    function initCompanyOptions() {
      const newCompany = document.getElementById("newEmployeeCompany");
      newCompany.innerHTML = "";
      for (const option of companyOptions) {
        const o = document.createElement("option");
        o.value = option.value;
        o.textContent = option.label;
        newCompany.appendChild(o);
      }
    }

    async function apiJson(url, options={}) {
      const res = await fetch(url, options);
      let data = {};
      try { data = await res.json(); } catch {}
      return { res, data };
    }

    async function register() {
      const email = document.getElementById("email").value.trim();
      const password = document.getElementById("password").value;
      const { res, data } = await apiJson("/api/auth/register", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password })
      });
      if (!res.ok || !data.ok) {
        setStatus("authStatus", data.error || "Register failed", true);
        return;
      }
      setStatus("authStatus", "Registered. You can login now.", false, true);
    }

    async function login() {
      const email = document.getElementById("email").value.trim();
      const password = document.getElementById("password").value;
      const { res, data } = await apiJson("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password })
      });
      if (!res.ok || !data.ok) {
        setStatus("authStatus", data.error || "Login failed", true);
        return;
      }
      await loadMe();
      setStatus("authStatus", "", false);
    }

    async function logout() {
      await fetch("/api/auth/logout", { method: "POST" });
      document.getElementById("appPanel").classList.add("hidden");
      document.getElementById("authPanel").classList.remove("hidden");
      if (jobsPollTimer) {
        clearInterval(jobsPollTimer);
        jobsPollTimer = null;
      }
    }

    async function loadMe() {
      const { res, data } = await apiJson("/api/me");
      if (!res.ok || !data.ok) {
        document.getElementById("appPanel").classList.add("hidden");
        document.getElementById("authPanel").classList.remove("hidden");
        return;
      }
      document.getElementById("meEmail").textContent = data.email;
      document.getElementById("authPanel").classList.add("hidden");
      document.getElementById("appPanel").classList.remove("hidden");
      await loadEmployees();
      await loadSettings();
      await loadJobs();
      if (!jobsPollTimer) {
        jobsPollTimer = setInterval(loadJobs, 2500);
      }
    }

    async function loadSettings() {
      const { res, data } = await apiJson("/api/settings");
      const text = document.getElementById("defaultTemplateText");
      if (!res.ok || !data.ok) {
        text.textContent = "Failed to load settings";
        return;
      }
      text.textContent = data.default_template_path
        ? `Current default template: ${data.default_template_path}`
        : "No default template saved yet.";
    }

    async function saveDefaultTemplate() {
      if (!files.defaultTemplate) {
        setStatus("convertStatus", "Choose a template first", true);
        return;
      }
      const fd = new FormData();
      fd.append("template_xlsx", files.defaultTemplate, files.defaultTemplate.name);
      const { res, data } = await apiJson("/api/template", { method: "POST", body: fd });
      if (!res.ok || !data.ok) {
        setStatus("convertStatus", data.error || "Failed to save template", true);
        return;
      }
      setStatus(
        "convertStatus",
        `Template saved. Synced ${data.synced_template_employees} employees (${data.updated_employees} updated, ${data.added_employees} added).`,
        false,
        true
      );
      await loadEmployees();
      await loadSettings();
    }

    function buildUnknownAssignments(names, defaults) {
      const panel = document.getElementById("unknownPanel");
      const list = document.getElementById("unknownList");
      list.innerHTML = "";
      if (!names || names.length === 0) {
        panel.classList.add("hidden");
        return;
      }
      panel.classList.remove("hidden");
      for (const name of names) {
        const row = document.createElement("div");
        row.className = "unknown-row";
        row.dataset.name = name;

        const nameNode = document.createElement("div");
        nameNode.textContent = name;
        row.appendChild(nameNode);

        const select = document.createElement("select");
        select.className = "unknown-company";
        for (const option of companyOptions) {
          const o = document.createElement("option");
          o.value = option.value;
          o.textContent = option.label;
          select.appendChild(o);
        }
        const suggestedCompany = defaults[name]?.home_company;
        if (suggestedCompany) select.value = suggestedCompany;
        row.appendChild(select);

        const rateInput = document.createElement("input");
        rateInput.type = "number";
        rateInput.className = "unknown-rate";
        rateInput.step = "0.01";
        rateInput.min = "0";
        const suggestedRate = defaults[name]?.rate;
        rateInput.value = suggestedRate !== undefined ? suggestedRate : "";
        rateInput.placeholder = "Rate";
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
      if (!files.batch || !files.tip) {
        setStatus("convertStatus", "Batch CSV and Tip CSV are required", true);
        return;
      }
      setStatus("convertStatus", "Checking names...");
      const fd = new FormData();
      fd.append("batch_csv", files.batch, files.batch.name);
      fd.append("tip_csv", files.tip, files.tip.name);
      fd.append("exclude_weekly_overtime", document.getElementById("excludeWeeklyOt").checked ? "1" : "0");
      const { res, data } = await apiJson("/api/preview", { method: "POST", body: fd });
      if (!res.ok || !data.ok) {
        setStatus("convertStatus", data.error || "Preview failed", true);
        return;
      }
      unknownNames = data.unknown_names || [];
      unknownDefaults = data.default_assignments || {};
      buildUnknownAssignments(unknownNames, unknownDefaults);
      setStatus("convertStatus", unknownNames.length ? "New names found. Assign them, then queue job." : "No new names.", false, true);
    }

    async function queueConversion() {
      if (!files.batch || !files.tip) {
        setStatus("convertStatus", "Batch CSV and Tip CSV are required", true);
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

      const { res, data } = await apiJson("/api/jobs/submit", { method: "POST", body: fd });
      if (!res.ok || !data.ok) {
        setStatus("convertStatus", data.error || "Job submit failed", true);
        return;
      }
      setStatus("convertStatus", `Job queued: ${data.job_id}`, false, true);
      await loadJobs();
    }

    function fmtTs(ts) {
      if (!ts) return "";
      return new Date(ts * 1000).toLocaleString();
    }

    async function loadJobs() {
      const { res, data } = await apiJson("/api/jobs");
      if (!res.ok || !data.ok) return;
      const tbody = document.getElementById("jobsTbody");
      tbody.innerHTML = "";
      for (const job of (data.jobs || [])) {
        const tr = document.createElement("tr");
        const out = (job.status === "completed" && job.output_filename)
          ? `<a href="/api/jobs/${encodeURIComponent(job.id)}/download">${escapeHtml(job.output_filename)}</a>`
          : "";
        tr.innerHTML = `
          <td>${escapeHtml(job.id)}</td>
          <td><span class=\"badge ${escapeHtml(job.status)}\">${escapeHtml(job.status)}</span></td>
          <td>${escapeHtml(fmtTs(job.created_at))}</td>
          <td>${escapeHtml(fmtTs(job.updated_at))}</td>
          <td>${out}</td>
          <td>${escapeHtml(job.error_text || "")}</td>
        `;
        tbody.appendChild(tr);
      }
    }

    async function loadEmployees() {
      const { res, data } = await apiJson("/api/employees");
      if (!res.ok || !data.ok) {
        return;
      }
      const tbody = document.getElementById("employeesTbody");
      tbody.innerHTML = "";
      for (const emp of (data.employees || [])) {
        const companySelectHtml = companyOptions
          .map((option) => `<option value="${option.value}"${option.value === emp.home_company ? " selected" : ""}>${option.label}</option>`)
          .join("");
        const tr = document.createElement("tr");
        tr.dataset.name = emp.name;
        tr.innerHTML = `
          <td><input type=\"checkbox\" data-name=\"${escapeHtml(emp.name)}\" /></td>
          <td>${escapeHtml(emp.name)}</td>
          <td><select class=\"employee-company\">${companySelectHtml}</select></td>
          <td><input type=\"number\" class=\"employee-rate\" min=\"0\" step=\"0.01\" value=\"${Number(emp.rate)}\" /></td>
        `;
        tbody.appendChild(tr);
      }
    }

    async function addEmployee() {
      const name = document.getElementById("newEmployeeName").value.trim();
      const home_company = document.getElementById("newEmployeeCompany").value;
      const rate = Number(document.getElementById("newEmployeeRate").value || "0");
      if (!name) {
        setStatus("convertStatus", "Employee name is required", true);
        return;
      }
      const { res, data } = await apiJson("/api/employees/add", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, home_company, rate })
      });
      if (!res.ok || !data.ok) {
        setStatus("convertStatus", data.error || "Failed to add employee", true);
        return;
      }
      setStatus("convertStatus", `Added employee: ${name}`, false, true);
      document.getElementById("newEmployeeName").value = "";
      document.getElementById("newEmployeeRate").value = "";
      await loadEmployees();
    }

    async function saveEmployees() {
      const rows = Array.from(document.querySelectorAll("#employeesTbody tr"));
      const employees = [];
      for (const row of rows) {
        const name = (row.dataset.name || "").trim();
        const home_company = row.querySelector(".employee-company")?.value || "";
        const rate = Number(row.querySelector(".employee-rate")?.value || "0");
        if (!name) continue;
        employees.push({ name, home_company, rate });
      }
      const { res, data } = await apiJson("/api/employees/update", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ employees })
      });
      if (!res.ok || !data.ok) {
        setStatus("convertStatus", data.error || "Failed to update employees", true);
        return;
      }
      setStatus("convertStatus", `Updated ${data.updated_count} employees`, false, true);
      await loadEmployees();
    }

    async function removeSelectedEmployees() {
      const checks = Array.from(document.querySelectorAll('#employeesTbody input[type="checkbox"]:checked'));
      const names = checks.map((node) => node.dataset.name || "").filter(Boolean);
      if (names.length === 0) return;
      const { res, data } = await apiJson("/api/employees/remove", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ names })
      });
      if (!res.ok || !data.ok) {
        setStatus("convertStatus", data.error || "Failed to remove", true);
        return;
      }
      setStatus("convertStatus", `Removed ${data.removed_count} employees`, false, true);
      await loadEmployees();
    }

    bindDrop("batchDrop", "batchInput", "batch", "batchFileLabel");
    bindDrop("tipDrop", "tipInput", "tip", "tipFileLabel");
    bindDrop("templateDrop", "templateInput", "template", "templateFileLabel");
    bindDrop("setTemplateDrop", "setTemplateInput", "defaultTemplate", "setTemplateFileLabel");
    initCompanyOptions();

    document.getElementById("registerBtn").addEventListener("click", register);
    document.getElementById("loginBtn").addEventListener("click", login);
    document.getElementById("logoutBtn").addEventListener("click", logout);
    document.getElementById("saveTemplateBtn").addEventListener("click", saveDefaultTemplate);
    document.getElementById("checkBtn").addEventListener("click", checkNewNames);
    document.getElementById("convertBtn").addEventListener("click", queueConversion);
    document.getElementById("addEmployeeBtn").addEventListener("click", addEmployee);
    document.getElementById("saveEmployeesBtn").addEventListener("click", saveEmployees);
    document.getElementById("removeEmployeesBtn").addEventListener("click", removeSelectedEmployees);
    document.getElementById("refreshEmployeesBtn").addEventListener("click", loadEmployees);

    loadMe();
  </script>
</body>
</html>
"""


class PayrollWebRequestHandler(BaseHTTPRequestHandler):
    def require_auth(self) -> AuthUser | None:
        user = auth_user_from_handler(self)
        if user is None:
            json_response(self, {"ok": False, "error": "Unauthorized"}, status=401)
            return None
        return user

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        try:
            if path == "/":
                user = auth_user_from_handler(self)
                redirect_response(self, "/workspace" if user else "/login")
                return
            if path == "/login":
                user = auth_user_from_handler(self)
                if user is not None:
                    redirect_response(self, "/workspace")
                    return
                html_response(self, LOGIN_PAGE)
                return
            if path == "/workspace":
                user = auth_user_from_handler(self)
                if user is None:
                    redirect_response(self, "/login")
                    return
                html_response(self, load_workspace_ui_html())
                return
            if path == "/converter":
                user = auth_user_from_handler(self)
                if user is None:
                    redirect_response(self, "/login")
                    return
                html_response(self, HTML_PAGE)
                return
            if path == "/healthz":
                text_response(self, "ok")
                return
            if path == "/api/me":
                user = auth_user_from_handler(self)
                if user is None:
                    json_response(self, {"ok": False, "error": "Unauthorized"}, status=401)
                    return
                json_response(self, {"ok": True, "user_id": user.user_id, "email": user.email})
                return
            if path == "/api/workspace/employees":
                user = self.require_auth()
                if user is None:
                    return
                ensure_user_employees_seeded(user.user_id)
                query = parse_qs(parsed.query)
                include_hidden = parse_bool_flag((query.get("include_hidden") or ["0"])[0], False)
                rows = get_employees(user.user_id, include_hidden=include_hidden)
                json_response(
                    self,
                    {
                        "ok": True,
                        "employees": [
                            {
                                "name": item["name"],
                                "home_company": item["home_company"],
                                "home_company_label": dict(COMPANY_OPTIONS)[item["home_company"]],
                                "rate": item["rate"],
                                "is_hidden": bool(item.get("is_hidden", False)),
                            }
                            for item in rows
                        ],
                    },
                )
                return
            if path == "/api/workspace/periods":
                user = self.require_auth()
                if user is None:
                    return
                query = parse_qs(parsed.query)
                try:
                    limit = max(1, min(500, int((query.get("limit") or ["200"])[0])))
                except Exception:
                    limit = 200
                json_response(self, {"ok": True, "periods": list_payroll_weeks(user.user_id, limit=limit)})
                return
            workspace_match = re.fullmatch(r"/api/workspace/periods/(\d+)", path)
            if workspace_match:
                user = self.require_auth()
                if user is None:
                    return
                period = get_payroll_week(user.user_id, int(workspace_match.group(1)))
                if period is None:
                    json_response(self, {"ok": False, "error": "Saved week not found"}, status=404)
                    return
                json_response(self, {"ok": True, "period": period})
                return
            if path == "/api/settings":
                user = self.require_auth()
                if user is None:
                    return
                ensure_user_default_template(user.user_id)
                template_path = get_default_template_path(user.user_id)
                json_response(
                    self,
                    {
                        "ok": True,
                        "default_template_path": str(template_path) if template_path else "",
                        "company_options": [{"value": key, "label": label} for key, label in COMPANY_OPTIONS],
                    },
                )
                return
            if path == "/api/employees":
                user = self.require_auth()
                if user is None:
                    return
                ensure_user_employees_seeded(user.user_id)
                query = parse_qs(parsed.query)
                include_hidden = parse_bool_flag((query.get("include_hidden") or ["0"])[0], False)
                json_response(
                    self,
                    {"ok": True, "employees": get_employees(user.user_id, include_hidden=include_hidden)},
                )
                return
            if path == "/api/jobs":
                user = self.require_auth()
                if user is None:
                    return
                query = parse_qs(parsed.query)
                try:
                    limit = max(1, min(100, int((query.get("limit") or ["30"])[0])))
                except Exception:
                    limit = 30
                json_response(self, {"ok": True, "jobs": list_jobs(user.user_id, limit=limit)})
                return

            match = re.fullmatch(r"/api/jobs/([a-f0-9]+)/download", path)
            if match:
                user = self.require_auth()
                if user is None:
                    return
                job_id = match.group(1)
                row = get_job(user.user_id, job_id)
                if row is None:
                    json_response(self, {"ok": False, "error": "Job not found"}, status=404)
                    return
                if str(row["status"]) != "completed":
                    json_response(self, {"ok": False, "error": "Job not complete"}, status=400)
                    return
                output_path = Path(str(row["output_path"] or ""))
                if not output_path.exists():
                    json_response(self, {"ok": False, "error": "Output file missing"}, status=404)
                    return
                file_response(
                    self,
                    output_path.read_bytes(),
                    filename=str(row["output_filename"] or output_path.name),
                    content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                return

            text_response(self, "Not Found", status=404)
        except Exception as exc:
            json_response(
                self,
                {
                    "ok": False,
                    "error": str(exc),
                    "trace": traceback.format_exc(limit=2),
                },
                status=500,
            )

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        try:
            if path == "/api/auth/register":
                self.handle_register()
                return
            if path == "/api/auth/login":
                self.handle_login()
                return
            if path == "/api/auth/logout":
                self.handle_logout()
                return
            if path == "/api/workspace/export-xlsx":
                self.handle_workspace_export_xlsx()
                return
            if path == "/api/workspace/save":
                self.handle_workspace_save()
                return
            if path == "/api/template":
                self.handle_set_template()
                return
            if path == "/api/preview":
                self.handle_preview()
                return
            if path == "/api/jobs/submit":
                self.handle_submit_job()
                return
            if path == "/api/employees/add":
                self.handle_add_employee()
                return
            if path == "/api/employees/update":
                self.handle_update_employees()
                return
            if path == "/api/employees/remove":
                self.handle_remove_employees()
                return
            if path == "/api/employees/hide":
                self.handle_hide_employees()
                return
            text_response(self, "Not Found", status=404)
        except Exception as exc:
            json_response(
                self,
                {
                    "ok": False,
                    "error": str(exc),
                    "trace": traceback.format_exc(limit=2),
                },
                status=500,
            )

    def handle_register(self) -> None:
        if not ALLOW_SELF_REGISTRATION:
            json_response(
                self,
                {"ok": False, "error": "Registration is disabled. Ask an admin to create your account."},
                status=403,
            )
            return

        data = parse_json_body(self)
        email = normalize_spaces(str(data.get("email", ""))).lower()
        password = str(data.get("password", ""))
        if not email or "@" not in email:
            json_response(self, {"ok": False, "error": "Valid email required"}, status=400)
            return
        if len(password) < 8:
            json_response(self, {"ok": False, "error": "Password must be at least 8 characters"}, status=400)
            return

        salt, digest = hash_password(password)
        try:
            with db_conn() as con:
                con.execute(
                    "INSERT INTO users(email, password_salt, password_hash, created_at) VALUES(?,?,?,?)",
                    (email, salt, digest, now_ts()),
                )
        except sqlite3.IntegrityError:
            json_response(self, {"ok": False, "error": "Email already exists"}, status=409)
            return

        json_response(self, {"ok": True})

    def handle_login(self) -> None:
        data = parse_json_body(self)
        email = normalize_spaces(str(data.get("email", ""))).lower()
        password = str(data.get("password", ""))

        with db_conn() as con:
            row = con.execute(
                "SELECT id, password_salt, password_hash FROM users WHERE email = ?",
                (email,),
            ).fetchone()
        if row is None:
            json_response(self, {"ok": False, "error": "Invalid credentials"}, status=401)
            return
        if not verify_password(password, str(row["password_salt"]), str(row["password_hash"])):
            json_response(self, {"ok": False, "error": "Invalid credentials"}, status=401)
            return

        token = create_session(int(row["id"]))
        payload = json.dumps({"ok": True}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        set_session_cookie(self, token)
        self.end_headers()
        self.wfile.write(payload)

    def handle_logout(self) -> None:
        cookie_header = self.headers.get("Cookie", "")
        cookie = SimpleCookie()
        cookie.load(cookie_header)
        morsel = cookie.get(SESSION_COOKIE_NAME)
        if morsel is not None:
            with db_conn() as con:
                con.execute("DELETE FROM sessions WHERE token = ?", (morsel.value,))
        payload = json.dumps({"ok": True}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        clear_session_cookie(self)
        self.end_headers()
        self.wfile.write(payload)

    def handle_workspace_export_xlsx(self) -> None:
        user = self.require_auth()
        if user is None:
            return

        data = parse_json_body(self)
        raw_rows = data.get("employees", [])
        if not isinstance(raw_rows, list):
            json_response(
                self,
                {"ok": False, "error": "Invalid payload: employees must be a list."},
                status=400,
            )
            return

        workspace_rows = aggregate_workspace_employees(raw_rows)
        if not workspace_rows:
            json_response(
                self,
                {"ok": False, "error": "No named employees found in workspace data."},
                status=400,
            )
            return

        template_path = get_default_template_path(user.user_id) or ensure_user_default_template(user.user_id)
        if template_path is None or not template_path.exists():
            json_response(
                self,
                {
                    "ok": False,
                    "error": "No default template configured. Upload/set a template first.",
                },
                status=400,
            )
            return

        week_start_text = normalize_spaces(str(data.get("week_start", "")))
        week_end_text = normalize_spaces(str(data.get("week_end", "")))
        week_start_date = parse_iso_date(week_start_text)
        week_end_date = parse_iso_date(week_end_text)
        if week_start_date is not None and week_end_date is None:
            week_end_date = week_start_date + timedelta(days=6)

        if week_start_date is not None and week_end_date is not None:
            output_filename = (
                "payroll_week_"
                + week_start_date.isoformat()
                + "_to_"
                + week_end_date.isoformat()
                + "_filled.xlsx"
            )
        else:
            output_filename = safe_filename(f"{template_path.stem}_workspace_filled.xlsx", "payroll_workspace_filled.xlsx")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            hours_csv = tmp / "workspace_hours.csv"
            tips_csv = tmp / "workspace_tips.csv"
            roster_json = tmp / "workspace_roster.json"
            output_xlsx = tmp / output_filename

            write_workspace_hours_csv(hours_csv, workspace_rows)
            write_workspace_tips_csv(tips_csv, workspace_rows)
            write_workspace_roster_json(roster_json, workspace_rows)

            try:
                fill_workbook(
                    workbook_path=template_path,
                    hours_csv_path=hours_csv,
                    output_path=output_xlsx,
                    roster_path=roster_json,
                    tips_csv_path=tips_csv,
                    tip_summary_output_path=None,
                )
            except Exception as exc:
                json_response(
                    self,
                    {"ok": False, "error": f"Workspace export failed: {exc}"},
                    status=500,
                )
                return

            file_response(
                self,
                output_xlsx.read_bytes(),
                filename=output_xlsx.name,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    def handle_workspace_save(self) -> None:
        user = self.require_auth()
        if user is None:
            return

        data = parse_json_body(self)
        week_start_raw = normalize_spaces(str(data.get("week_start", "")))
        week_start_date = parse_iso_date(week_start_raw)
        if week_start_date is None:
            json_response(self, {"ok": False, "error": "week_start must be YYYY-MM-DD"}, status=400)
            return

        week_end_date = week_start_date + timedelta(days=6)
        week_end_raw = normalize_spaces(str(data.get("week_end", "")))
        parsed_week_end = parse_iso_date(week_end_raw)
        if parsed_week_end is not None:
            week_end_date = parsed_week_end

        pay_period = normalize_spaces(str(data.get("pay_period", "")))
        if not pay_period:
            pay_period = f"{format_us_date(week_start_date)} - {format_us_date(week_end_date)}"

        period_note = str(data.get("period_note", "")).strip()
        raw_rows = data.get("employees", [])
        if not isinstance(raw_rows, list):
            json_response(self, {"ok": False, "error": "employees must be a list"}, status=400)
            return

        payload = {
            "week_start": week_start_date.isoformat(),
            "week_end": week_end_date.isoformat(),
            "pay_period": pay_period,
            "period_note": period_note,
            "employees": raw_rows,
        }

        period_id = save_payroll_week(
            user_id=user.user_id,
            week_start=payload["week_start"],
            week_end=payload["week_end"],
            pay_period=payload["pay_period"],
            period_note=payload["period_note"],
            payload_json=json.dumps(payload, separators=(",", ":")),
        )

        json_response(
            self,
            {
                "ok": True,
                "period_id": period_id,
                "week_start": payload["week_start"],
                "week_end": payload["week_end"],
                "pay_period": payload["pay_period"],
                "updated_at": now_ts(),
            },
        )

    def handle_set_template(self) -> None:
        user = self.require_auth()
        if user is None:
            return

        form = parse_multipart_form(self)
        template_file = get_file_field(form, "template_xlsx")
        if template_file is None:
            json_response(self, {"ok": False, "error": "Template XLSX is required"}, status=400)
            return

        filename, file_bytes = template_file
        template_name = safe_filename(filename, "default_template.xlsx")
        if not template_name.lower().endswith(".xlsx"):
            template_name += ".xlsx"

        udir = user_dir(user.user_id)
        target = udir / "templates" / template_name
        target.write_bytes(file_bytes)
        default_copy = udir / "templates" / "default_template.xlsx"
        shutil.copy2(target, default_copy)

        set_default_template_path(user.user_id, default_copy)
        sync = sync_employees_from_template(user.user_id, default_copy)

        json_response(
            self,
            {
                "ok": True,
                "default_template_path": str(default_copy),
                "synced_template_employees": sync["template_count"],
                "updated_employees": sync["updated_count"],
                "added_employees": sync["added_count"],
            },
        )

    def handle_preview(self) -> None:
        user = self.require_auth()
        if user is None:
            return

        form = parse_multipart_form(self)
        batch_file = get_file_field(form, "batch_csv")
        tip_file = get_file_field(form, "tip_csv")

        if batch_file is None or tip_file is None:
            json_response(self, {"ok": False, "error": "Batch CSV and Tip CSV are required"}, status=400)
            return

        exclude_weekly_overtime = parse_bool_flag(form.getfirst("exclude_weekly_overtime"), True)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            batch_name, batch_bytes = batch_file
            tip_name, tip_bytes = tip_file

            batch_path = tmp / safe_filename(batch_name, "batch.csv")
            tip_path = tmp / safe_filename(tip_name, "tips.csv")
            simple_path = tmp / "simple.csv"

            batch_path.write_bytes(batch_bytes)
            tip_path.write_bytes(tip_bytes)

            batch_names = extract_source_names_from_batch(batch_path, exclude_weekly_overtime, simple_path)
            tip_totals, _, _ = load_tips_csv(tip_path)
            source_names = sorted(set(batch_names) | set(tip_totals.keys()))

        employees = get_employees(user.user_id)
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

    def handle_submit_job(self) -> None:
        user = self.require_auth()
        if user is None:
            return

        form = parse_multipart_form(self)
        batch_file = get_file_field(form, "batch_csv")
        tip_file = get_file_field(form, "tip_csv")
        template_file = get_file_field(form, "template_xlsx")

        if batch_file is None or tip_file is None:
            json_response(self, {"ok": False, "error": "Batch CSV and Tip CSV are required"}, status=400)
            return

        exclude_weekly_overtime = parse_bool_flag(form.getfirst("exclude_weekly_overtime"), True)
        assignments_raw = form.getfirst("assignments_json", "[]")
        try:
            assignments_list = json.loads(assignments_raw)
        except Exception:
            assignments_list = []

        assignment_map: dict[str, dict[str, Any]] = {}
        if isinstance(assignments_list, list):
            for item in assignments_list:
                if not isinstance(item, dict):
                    continue
                name = normalize_spaces(str(item.get("name", "")))
                company = str(item.get("home_company", ""))
                if not name or company not in DEFAULT_BURDEN_BY_COMPANY:
                    continue
                assignment_map[name] = {
                    "home_company": company,
                    "rate": item.get("rate", ""),
                }

        job_id = create_job(user.user_id)
        job_dir = user_dir(user.user_id) / "jobs" / job_id
        job_dir.mkdir(parents=True, exist_ok=True)

        batch_name, batch_bytes = batch_file
        tip_name, tip_bytes = tip_file
        batch_path = job_dir / safe_filename(batch_name, "batch.csv")
        tip_path = job_dir / safe_filename(tip_name, "tips.csv")
        batch_path.write_bytes(batch_bytes)
        tip_path.write_bytes(tip_bytes)

        template_override_path: Path | None = None
        if template_file is not None:
            template_name, template_bytes = template_file
            template_override_path = job_dir / safe_filename(template_name, "template.xlsx")
            template_override_path.write_bytes(template_bytes)

        submit_job(
            user_id=user.user_id,
            job_id=job_id,
            batch_path=batch_path,
            tip_path=tip_path,
            template_override_path=template_override_path,
            exclude_weekly_overtime=exclude_weekly_overtime,
            assignment_map=assignment_map,
        )

        json_response(self, {"ok": True, "job_id": job_id, "status": "queued"})

    def handle_add_employee(self) -> None:
        user = self.require_auth()
        if user is None:
            return

        data = parse_json_body(self)
        name = normalize_spaces(str(data.get("name", "")))
        home_company = str(data.get("home_company", ""))
        raw_rate = data.get("rate")

        if not name:
            json_response(self, {"ok": False, "error": "Employee name is required"}, status=400)
            return
        if home_company not in DEFAULT_BURDEN_BY_COMPANY:
            json_response(self, {"ok": False, "error": "Invalid company"}, status=400)
            return
        try:
            rate = float(raw_rate)
        except (TypeError, ValueError):
            json_response(self, {"ok": False, "error": "Invalid rate"}, status=400)
            return
        if rate < 0:
            json_response(self, {"ok": False, "error": "Rate cannot be negative"}, status=400)
            return

        upsert_employee(user.user_id, name, home_company, rate)
        json_response(self, {"ok": True, "name": name})

    def handle_update_employees(self) -> None:
        user = self.require_auth()
        if user is None:
            return

        data = parse_json_body(self)
        updates = data.get("employees", [])
        if not isinstance(updates, list):
            json_response(self, {"ok": False, "error": "Invalid employees payload"}, status=400)
            return

        updated_count = 0
        for item in updates:
            if not isinstance(item, dict):
                continue
            name = normalize_spaces(str(item.get("name", "")))
            home_company = str(item.get("home_company", ""))
            try:
                rate = float(item.get("rate"))
            except (TypeError, ValueError):
                json_response(self, {"ok": False, "error": f"Invalid rate for {name}"}, status=400)
                return
            if not name:
                continue
            if home_company not in DEFAULT_BURDEN_BY_COMPANY:
                json_response(self, {"ok": False, "error": f"Invalid company for {name}"}, status=400)
                return
            if rate < 0:
                json_response(self, {"ok": False, "error": f"Rate cannot be negative for {name}"}, status=400)
                return
            upsert_employee(user.user_id, name, home_company, rate)
            updated_count += 1

        json_response(self, {"ok": True, "updated_count": updated_count})

    def handle_remove_employees(self) -> None:
        user = self.require_auth()
        if user is None:
            return

        data = parse_json_body(self)
        names = data.get("names", [])
        if not isinstance(names, list):
            json_response(self, {"ok": False, "error": "Invalid names payload"}, status=400)
            return

        removed_count = remove_employees(user.user_id, names)
        json_response(self, {"ok": True, "removed_count": removed_count})

    def handle_hide_employees(self) -> None:
        user = self.require_auth()
        if user is None:
            return

        data = parse_json_body(self)
        names = data.get("names", [])
        if not isinstance(names, list):
            json_response(self, {"ok": False, "error": "Invalid names payload"}, status=400)
            return
        hidden = bool(data.get("hidden", True))
        changed_count = set_employees_hidden(user.user_id, names, hidden=hidden)
        json_response(self, {"ok": True, "hidden": hidden, "changed_count": changed_count})

    def log_message(self, format: str, *args: Any) -> None:
        return


def run_web_app(host: str = "0.0.0.0", port: int = 8080) -> None:
    init_storage()
    server = ThreadingHTTPServer((host, port), PayrollWebRequestHandler)
    print(f"Payroll web app running on http://{host}:{port}")
    print(f"Data directory: {DATA_DIR}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    # Cloud hosts (Render, Railway, etc.) inject PORT dynamically.
    resolved_port = os.environ.get("PORT") or os.environ.get("PAYROLL_WEB_PORT") or "8080"
    run_web_app(
        host=os.environ.get("PAYROLL_WEB_HOST", "0.0.0.0"),
        port=int(resolved_port),
    )
