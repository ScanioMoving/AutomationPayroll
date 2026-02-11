#!/usr/bin/env python3
"""Push locally saved payroll weeks into the hosted site database."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any
from urllib import request
from urllib.error import HTTPError, URLError
import http.cookiejar


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync local payroll_weeks rows to a remote payroll web app."
    )
    parser.add_argument("--base-url", required=True, help="Remote app URL, e.g. https://payroll-weekly-web.onrender.com")
    parser.add_argument("--email", required=True, help="Remote app login email")
    parser.add_argument(
        "--password",
        default=os.environ.get("PAYROLL_REMOTE_PASSWORD", ""),
        help="Remote app password (or set PAYROLL_REMOTE_PASSWORD env var)",
    )
    parser.add_argument(
        "--db",
        default=".payroll_web_data/payroll_web.db",
        help="Path to local SQLite DB with payroll_weeks table",
    )
    parser.add_argument("--local-user-id", type=int, default=None, help="Optional local user_id filter")
    parser.add_argument("--since-week-start", default="", help="Only sync weeks >= YYYY-MM-DD")
    parser.add_argument("--max-weeks", type=int, default=0, help="Optional max number of weeks to sync")
    parser.add_argument("--dry-run", action="store_true", help="Print weeks that would sync without writing remote")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout seconds")
    return parser.parse_args()


def normalize_base_url(url: str) -> str:
    return str(url or "").strip().rstrip("/")


def read_local_weeks(
    db_path: Path,
    *,
    local_user_id: int | None,
    since_week_start: str,
    max_weeks: int,
) -> list[dict[str, Any]]:
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    clauses: list[str] = []
    values: list[Any] = []
    if local_user_id is not None:
        clauses.append("user_id = ?")
        values.append(local_user_id)
    if since_week_start:
        clauses.append("week_start >= ?")
        values.append(since_week_start)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    limit_sql = "LIMIT ?" if max_weeks > 0 else ""
    if max_weeks > 0:
        values.append(max_weeks)

    query = f"""
        SELECT week_start, week_end, pay_period, period_note, payload_json
        FROM payroll_weeks
        {where_sql}
        ORDER BY week_start ASC, updated_at ASC
        {limit_sql}
    """

    out: list[dict[str, Any]] = []
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        for row in con.execute(query, values).fetchall():
            try:
                payload = json.loads(str(row["payload_json"] or "{}"))
            except Exception:
                payload = {}
            if not isinstance(payload, dict):
                payload = {}

            week_start = str(payload.get("week_start") or row["week_start"] or "").strip()
            week_end = str(payload.get("week_end") or row["week_end"] or "").strip()
            pay_period = str(payload.get("pay_period") or row["pay_period"] or "").strip()
            period_note = str(payload.get("period_note") or row["period_note"] or "")
            employees = payload.get("employees")
            if not isinstance(employees, list):
                employees = []

            if not week_start or not week_end:
                continue

            out.append(
                {
                    "week_start": week_start,
                    "week_end": week_end,
                    "pay_period": pay_period,
                    "period_note": period_note,
                    "employees": employees,
                }
            )
    return out


def json_request(
    opener: request.OpenerDirector,
    *,
    method: str,
    url: str,
    payload: dict[str, Any] | None,
    timeout: float,
) -> tuple[int, dict[str, Any]]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(url=url, data=body, headers=headers, method=method)
    try:
        with opener.open(req, timeout=timeout) as resp:
            status = int(resp.status)
            data_raw = resp.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        status = int(exc.code)
        data_raw = exc.read().decode("utf-8", errors="replace")
    except URLError as exc:
        raise SystemExit(f"Network error calling {url}: {exc}") from exc

    try:
        payload_out = json.loads(data_raw) if data_raw else {}
    except Exception:
        payload_out = {"raw": data_raw}
    return status, payload_out if isinstance(payload_out, dict) else {"raw": payload_out}


def login_remote(
    opener: request.OpenerDirector, *, base_url: str, email: str, password: str, timeout: float
) -> None:
    status, payload = json_request(
        opener,
        method="POST",
        url=f"{base_url}/api/auth/login",
        payload={"email": email, "password": password},
        timeout=timeout,
    )
    if status != 200 or not payload.get("ok"):
        raise SystemExit(f"Remote login failed ({status}): {payload.get('error') or payload}")


def save_week_remote(
    opener: request.OpenerDirector, *, base_url: str, week_payload: dict[str, Any], timeout: float
) -> tuple[bool, str]:
    status, payload = json_request(
        opener,
        method="POST",
        url=f"{base_url}/api/workspace/save",
        payload=week_payload,
        timeout=timeout,
    )
    if status != 200 or not payload.get("ok"):
        return False, str(payload.get("error") or payload)
    period_id = payload.get("period_id")
    return True, f"period_id={period_id}"


def main() -> None:
    args = parse_args()
    base_url = normalize_base_url(args.base_url)
    if not base_url:
        raise SystemExit("base-url is required")
    if not args.password:
        raise SystemExit("password is required (pass --password or set PAYROLL_REMOTE_PASSWORD)")

    weeks = read_local_weeks(
        Path(args.db),
        local_user_id=args.local_user_id,
        since_week_start=str(args.since_week_start or "").strip(),
        max_weeks=int(args.max_weeks or 0),
    )

    if not weeks:
        print("No local payroll weeks found to sync.")
        return

    if args.dry_run:
        for entry in weeks:
            print(f"would sync {entry['week_start']} -> {entry['week_end']} ({len(entry['employees'])} employees)")
        return

    cookie_jar = http.cookiejar.CookieJar()
    opener = request.build_opener(request.HTTPCookieProcessor(cookie_jar))
    login_remote(opener, base_url=base_url, email=args.email, password=args.password, timeout=args.timeout)

    ok_count = 0
    fail_count = 0
    for entry in weeks:
        ok, detail = save_week_remote(opener, base_url=base_url, week_payload=entry, timeout=args.timeout)
        if ok:
            ok_count += 1
            print(f"synced {entry['week_start']} -> {entry['week_end']} ({detail})")
        else:
            fail_count += 1
            print(f"failed {entry['week_start']} -> {entry['week_end']}: {detail}", file=sys.stderr)

    print(f"done: synced={ok_count} failed={fail_count}")
    if fail_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
