FROM python:3.12-slim

WORKDIR /app

COPY payroll_web_app.py /app/
COPY fill_payroll_workbook_from_hours.py /app/
COPY simplify_timecard_csv.py /app/
COPY payroll_workspace_ui.html /app/
COPY payroll_roster.json /app/
COPY ["Copy of Payroll Weekly 01.31.26- 02.06.26.xlsx", "/app/"]

ENV PAYROLL_WEB_HOST=0.0.0.0
ENV PAYROLL_WEB_PORT=8080
ENV PAYROLL_DATA_DIR=/var/data

EXPOSE 8080

CMD ["python", "payroll_web_app.py"]
