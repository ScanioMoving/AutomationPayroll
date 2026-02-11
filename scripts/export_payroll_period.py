#!/usr/bin/env python3
"""Export one saved payroll week from the local SQLite DB to JSON."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export saved payroll week payload to JSON.")
    parser.add_argument(
        "--db",
        default=".payroll_web_data/payroll_web.db",
        help="Path to payroll SQLite database.",
    )
    parser.add_argument("--week-start", help="Week start in YYYY-MM-DD.")
    parser.add_argument("--week-end", help="Week end in YYYY-MM-DD.")
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Export the most recently updated payroll week.",
    )
    parser.add_argument(
        "--user-id",
        type=int,
        default=None,
        help="Optional user_id filter if database has multiple users.",
    )
    parser.add_argument(
        "--out-dir",
        default="payroll_period_exports",
        help="Folder where exported JSON will be written.",
    )
    return parser.parse_args()


def query_period(
    con: sqlite3.Connection,
    *,
    latest: bool,
    week_start: str | None,
    week_end: str | None,
    user_id: int | None,
) -> tuple[str, str, str]:
    con.row_factory = sqlite3.Row

    if latest:
        clauses = []
        values: list[object] = []
        if user_id is not None:
            clauses.append("user_id = ?")
            values.append(user_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        row = con.execute(
            f"""
            SELECT week_start, week_end, payload_json
            FROM payroll_weeks
            {where}
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            values,
        ).fetchone()
    else:
        if not week_start:
            raise SystemExit("Error: provide --week-start or use --latest")
        clauses = ["week_start = ?"]
        values = [week_start]
        if week_end:
            clauses.append("week_end = ?")
            values.append(week_end)
        if user_id is not None:
            clauses.append("user_id = ?")
            values.append(user_id)
        where = " AND ".join(clauses)
        row = con.execute(
            f"""
            SELECT week_start, week_end, payload_json
            FROM payroll_weeks
            WHERE {where}
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            values,
        ).fetchone()

    if row is None:
        raise SystemExit("Error: matching payroll week not found")
    return str(row["week_start"]), str(row["week_end"]), str(row["payload_json"])


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"Error: DB not found: {db_path}")

    with sqlite3.connect(db_path) as con:
        week_start, week_end, payload_json = query_period(
            con,
            latest=bool(args.latest),
            week_start=args.week_start,
            week_end=args.week_end,
            user_id=args.user_id,
        )

    payload = json.loads(payload_json)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{week_start}_{week_end}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n", encoding="utf-8")
    print(str(out_path))


if __name__ == "__main__":
    main()
