CREATE SCHEMA IF NOT EXISTS mart;

-- dirty dataset, transaction-level issues
CREATE OR REPLACE TABLE mart.dirty_quality_summary AS
SELECT
  COUNT(*) AS total_rows,
  SUM(flag_missing_txn_id) AS missing_txn_id,
  SUM(flag_bad_date) AS bad_date_rows,
  SUM(flag_bad_price) AS bad_price_rows,
  SUM(flag_bad_quantity) AS bad_quantity_rows,
  SUM(CASE WHEN payment_method IS NULL THEN 1 ELSE 0 END) AS missing_payment_method,
  SUM(CASE WHEN transaction_status IS NULL THEN 1 ELSE 0 END) AS missing_status
FROM stg.dirty_clean;

--fraud dataset, transaction-level issues
CREATE OR REPLACE TABLE mart.fraud_quality_summary AS
SELECT
  COUNT(*) AS total_rows,
  SUM(flag_missing_txn_id) AS missing_txn_id,
  SUM(flag_bad_timestamp) AS bad_timestamp_rows,
  SUM(flag_bad_amount) AS bad_amount_rows,
  SUM(CASE WHEN is_fraud IS NULL THEN 1 ELSE 0 END) AS missing_is_fraud
FROM stg.fraud_clean;

-- reconciliation by time grain (monthly totals)
--dirty: revenue proxy = quantity*price
CREATE OR REPLACE TABLE mart.recon_monthly AS
WITH dirty_m AS (
  SELECT
    date_trunc('month', txn_date)::DATE AS month,
    COUNT(*) AS dirty_txn_count,
    SUM(CASE WHEN txn_date IS NULL THEN 1 ELSE 0 END) AS dirty_null_date_count,
    SUM(quantity * price) AS dirty_gross_sales
  FROM stg.dirty_clean
  GROUP BY 1
),
fraud_m AS (
  SELECT
    date_trunc('month', txn_ts)::DATE AS month,
    COUNT(*) AS fraud_txn_count,
    SUM(CASE WHEN txn_ts IS NULL THEN 1 ELSE 0 END) AS fraud_null_ts_count,
    SUM(amount) AS fraud_total_amount,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_flagged_count
  FROM stg.fraud_clean
  GROUP BY 1
)
SELECT
  COALESCE(dirty_m.month, fraud_m.month) AS month,
  dirty_m.dirty_txn_count,
  dirty_m.dirty_gross_sales,
  dirty_m.dirty_null_date_count,
  fraud_m.fraud_txn_count,
  fraud_m.fraud_total_amount,
  fraud_m.fraud_flagged_count,
  fraud_m.fraud_null_ts_count
FROM dirty_m
FULL OUTER JOIN fraud_m USING (month)
ORDER BY month;
