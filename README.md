# Loan Payment Reconciliation (DuckDB)

This project simulates a real-world **finance and risk reconciliation** workflow:

- Internal loan payment ledger (messy transaction records)
- External bank transaction deposit feed (high-volume data)
- Automated reconciliation, matching, and exception reporting

---

## Business Problem

Financial institutions must ensure that **loan payments recorded internally** match the
**actual deposits received in bank transaction feeds**.

Unmatched payments create operational risk, fraud exposure, and audit issues.

---

## Key Features

- Data ingestion from raw CSVs into DuckDB
- Data cleaning + standardization (dates, amounts, categories)
- Data quality metrics and missing-value flags
- Monthly reconciliation reporting
- Transaction-level matching engine:
  - ±3 day matching window  
  - Amount tolerance: $1 or 1%
- Exception queue for audit investigation
- Rule-based fraud risk tiering
- Export-ready outputs for Excel/Tableau/Power BI dashboards

---

## Project Pipeline

raw → staging (clean) → mart (reconciliation) → exception queue → dashboard exports

---

## Core Outputs

- `mart.recon_kpis` — reconciliation KPI summary
- `mart.recon_txn_matches` — matched transactions
- `mart.exception_queue` — unmatched payments requiring investigation
- `mart.recon_monthly` — monthly expected vs received totals

---

## How to Run:

Start DuckDB:

```bash
Run the full pipeline: 

duckdb db/loan_recon.duckdb

.read sql/01_ingest.sql
.read sql/02_profile.sql
.read sql/03_clean_standardize.sql
.read sql/03b_business_views.sql
.read sql/04_reconciliation.sql
.read sql/04b_txn_reconciliation.sql
.read sql/05_fraud_rules.sql
.read sql/07b_exception_queue.sql
.read sql/06_exports.sql


## Skils Demonstrated
- SQL Data Cleaning
- Financial REconciliation
- Exception Management DUCKDB Analytics Engineering
- Fraud and Risk Controls
- Dashboard Reporting

