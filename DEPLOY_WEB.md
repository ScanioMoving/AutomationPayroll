# Deploy Payroll Weekly Web App

## What to sign up for first

### Required for first live version

1. GitHub account (to host this code repo).
2. Render account (to host the web app).
3. Domain provider account (Cloudflare recommended) for your office domain.

### Optional now, recommended next phase

1. Neon (managed Postgres) for multi-instance database.
2. Cloudflare R2 (or Wasabi) for storing generated PDF/XLSX files.

## API keys / secrets to gather

### Needed now

1. No external API keys are required for the current app logic.
2. Render service environment variables:
   - `PAYROLL_DATA_DIR=/var/data`
   - `PAYROLL_COOKIE_SECURE=1`
   - `PAYROLL_COOKIE_SAMESITE=Lax`
   - `PAYROLL_ALLOW_REGISTRATION=0` (after first admin account is created)
3. Domain DNS records in Cloudflare (A/CNAME) pointing to Render.

### Needed in phase 2 (DB/object storage)

1. `DATABASE_URL` from Neon project.
2. `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET` from Cloudflare R2.

## Local run

```bash
cd /Users/orlandocantoni/Downloads/AutomationPayroll
python payroll_web_app.py
```

Open `http://127.0.0.1:8080`.

## Docker run

```bash
cd /Users/orlandocantoni/Downloads/AutomationPayroll
docker build -t payroll-web .
docker run --name payroll-web \
  -e PAYROLL_DATA_DIR=/var/data \
  -e PAYROLL_COOKIE_SECURE=1 \
  -e PAYROLL_COOKIE_SAMESITE=Lax \
  -e PAYROLL_ALLOW_REGISTRATION=1 \
  -p 8080:8080 -d payroll-web
```

## Render deploy (recommended)

1. Push this folder to GitHub.
2. In Render, create a new Blueprint service from the repo.
3. Render will load `render.yaml` and create:
   - web service
   - mounted persistent disk at `/var/data`
4. Keep registration enabled only for initial setup:
   - Set `PAYROLL_ALLOW_REGISTRATION=1`
   - Register first admin user in the app
   - Set `PAYROLL_ALLOW_REGISTRATION=0`
   - redeploy
5. Add custom domain and enable HTTPS in Render + Cloudflare DNS.

## Current architecture notes

1. This release uses SQLite + persistent disk (single web instance).
2. Data stored at `PAYROLL_DATA_DIR` (DB + user templates + outputs).
3. Built-in background workers use `ThreadPoolExecutor`.
4. Next production phase should move to Postgres + object storage for stronger durability/scaling.
