# Payroll Weekly Online: Account + Key Checklist

## Create these accounts now

1. GitHub (repo host).
2. Render (app hosting).
3. Cloudflare (domain + DNS).

## Required values to prepare now

1. `Render`:
   - Web service URL (temporary domain from Render).
   - Environment variables:
     - `PAYROLL_DATA_DIR=/var/data`
     - `PAYROLL_COOKIE_SECURE=1`
     - `PAYROLL_COOKIE_SAMESITE=Lax`
     - `PAYROLL_ALLOW_REGISTRATION=1` initially, then `0`
2. `Cloudflare`:
   - Domain name
   - DNS record to point subdomain (for example `payroll.yourdomain.com`) to Render.

## First-admin bootstrap sequence

1. Deploy with `PAYROLL_ALLOW_REGISTRATION=1`.
2. Register your admin account in app login page.
3. Confirm login works.
4. Change `PAYROLL_ALLOW_REGISTRATION=0`.
5. Redeploy.

## Optional next-phase accounts (recommended)

1. Neon (managed Postgres).
2. Cloudflare R2 (object storage for generated exports).

## Next-phase secrets to collect

1. Neon:
   - `DATABASE_URL`
2. Cloudflare R2:
   - `R2_ACCOUNT_ID`
   - `R2_ACCESS_KEY_ID`
   - `R2_SECRET_ACCESS_KEY`
   - `R2_BUCKET`
