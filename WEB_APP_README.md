# Payroll Web App

This project now includes an online web version of the converter:

- App file: `/Users/orlandocantoni/Downloads/AutomationPayroll/payroll_web_app.py`
- Data dir (SQLite + user files): `/Users/orlandocantoni/Downloads/AutomationPayroll/.payroll_web_data`

## Features

- User registration/login (session cookie auth)
- SQLite database for users/employees/settings/jobs
- Default template per user
- Template -> employee rate/company sync
- New-name preview and assignment
- Background conversion workers (delegated agents)
- Job queue/status and XLSX download when complete

## Run locally

```bash
cd /Users/orlandocantoni/Downloads/AutomationPayroll
python payroll_web_app.py
```

Open:

- `http://127.0.0.1:8080`

## Env vars

- `PAYROLL_WEB_HOST` (default `0.0.0.0`)
- `PAYROLL_WEB_PORT` (default `8080`)
- `PORT` (fallback if `PAYROLL_WEB_PORT` is not set; useful for cloud platforms)
- `PAYROLL_DATA_DIR` (default `.payroll_web_data` under app folder)
- `PAYROLL_COOKIE_SECURE` (`1` recommended on HTTPS)
- `PAYROLL_COOKIE_SAMESITE` (`Lax`, `Strict`, or `None`)
- `PAYROLL_SESSION_COOKIE_NAME` (default `payroll_session`)
- `PAYROLL_SESSION_TTL_SECONDS` (default `604800`)
- `PAYROLL_ALLOW_REGISTRATION` (`1` default; set `0` after first admin account exists)

Example:

```bash
PAYROLL_WEB_HOST=0.0.0.0 PAYROLL_WEB_PORT=8080 python payroll_web_app.py
```

## Bring online

Use Render with `render.yaml` in this repository, or run Docker on any host with:

- HTTPS enabled
- persistent disk mapped to `PAYROLL_DATA_DIR`
- `PAYROLL_COOKIE_SECURE=1`
- `PAYROLL_ALLOW_REGISTRATION=0` after initial admin bootstrap

## Save a payroll week to GitHub (one command)

Use the helper script to export a saved week from local DB, run a smoke compile check, commit, and push:

```bash
cd /Users/orlandocantoni/Downloads/AutomationPayroll
scripts/sync_week_to_github.sh 2026-01-31 2026-02-06
```

Or push the latest saved week:

```bash
scripts/sync_week_to_github.sh --latest
```

## Populate hosted database from local saved weeks

Push local `payroll_weeks` entries into your Render site database:

```bash
cd /Users/orlandocantoni/Downloads/AutomationPayroll
export PAYROLL_REMOTE_PASSWORD='your-render-login-password'
python scripts/sync_local_weeks_to_remote.py \
  --base-url https://your-render-service.onrender.com \
  --email your-login@email.com \
  --since-week-start 2026-01-31
```

This writes each week via `/api/workspace/save` and can be rerun any time to add future weeks.

## Note

This is an MVP server using Python stdlib HTTP server + SQLite.
For high scale, migrate to FastAPI + Postgres + Redis/Celery worker queue.
