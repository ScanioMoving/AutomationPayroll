# Payroll Converter Mac App

## What this app does
- Opens to **Payroll Weekly Sheet** (`/workspace`) with a full-page spreadsheet layout:
  - pay period auto-detected from current NYC date and locked to Saturday -> Friday
  - one row per employee for weekly totals
  - collapsible daily rows (Sat-Sun-Mon-Tue-Wed-Thu-Fri) per employee
  - totals auto-calculated from daily company hours
  - optional daily `IN/OUT` time entry (30-minute intervals) with auto hour fill
  - Manage mode toggle: employee `Name` and `Payroll Company` are locked unless Manage is enabled and row is selected
  - columns for payroll company, three company-hour buckets (Scanio / Sea & Air / Flat Price), total hours, hourly rate, commissions by company
  - bulk apply tool to push the same day/company hours to multiple selected employees
  - auto-loads employees from current roster (`/api/employees`) so existing names are present
  - **Print to PDF** button builds per-employee weekly timecard pages in a report-style layout close to the original timecard format
- Drag/drop:
  - Batch CSV (raw export)
  - Tip CSV (raw export)
  - Optional template XLSX (if you do not want to use the saved default template)
- Click **Check New Names**.
- If new names are found, assign each one to one of:
  - Scanio Moving
  - Scanio Storage
  - Sea and Air Int-L
  - Flat Price
- Click **Convert and Download**.

The app exports the filled payroll workbook XLSX directly.

Current converter remains available at:
- `/converter`

## Employee management
- Use **Manage Current Employees** to add new employees (name, company, rate).
- You can edit each employee's company and hourly `Rate`, then click **Save Employee Changes**.
- Use **Remove Selected** to remove employees who left.
- New names assigned during conversion are saved to the roster for future runs.

## Default template
- The app ships with a bundled default template XLSX (if included during build).
- Users can still override or replace it in **Default Template**.
- After default is set, users can run with only the two CSVs.

## Run locally from source
```bash
./run_payroll_app.command
```

## Build a Finder double-click app (quick)
```bash
./build_finder_app.sh
```

Output:
- `PayrollConverter.app`

This `.app` can be double-clicked in Finder and shared.
Requirement on target Mac: Python 3 available at `/usr/bin/python3`, `/opt/homebrew/bin/python3`, or `/usr/local/bin/python3`.

## Build a standalone macOS app (no Python required on target Mac)
```bash
./build_mac_app.sh
```

Build requirement (on build machine only):
```bash
python -m pip install pyinstaller
```

Build output:
- `dist/PayrollConverterApp.app`

Optional: specify template explicitly when building:
```bash
./build_mac_app.sh '/path/to/PayrollTemplate.xlsx'
```

## Sharing notes
- Share the `.app` from `dist/`.
- On another Mac, first launch may require:
  - Right-click app -> **Open** (Gatekeeper prompt), or
  - Adjust security settings to allow the app.
