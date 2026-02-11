
CREATE OR REPLACE TABLE mart.recon_exceptions AS
SELECT
  month,
  dirty_txn_count,
  dirty_gross_sales AS internal_expected_payments,
  fraud_txn_count,
  fraud_total_amount AS bank_received_deposits,
  fraud_flagged_count,

  (fraud_total_amount - dirty_gross_sales) AS dollar_diff,
  (fraud_total_amount - dirty_gross_sales) / NULLIF(dirty_gross_sales, 0) AS pct_diff,

  CASE
    WHEN dirty_gross_sales IS NULL THEN 'Missing internal records'
    WHEN fraud_total_amount IS NULL THEN 'Missing bank deposits'
    WHEN abs(fraud_total_amount - dirty_gross_sales) <= 100 THEN 'Within tolerance'
    ELSE 'Out of tolerance'
  END AS recon_status

FROM mart.recon_monthly;
