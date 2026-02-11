
CREATE SCHEMA IF NOT EXISTS mart;

--Candidate matches using: date window: plus minus  3 days and amount tolerance: $1 OR 1% (whichever is larger)
CREATE OR REPLACE TABLE mart.recon_match_candidates AS
SELECT
  i.txn_id AS internal_txn_id,
  b.txn_id AS bank_txn_id,
  i.customer_id,
  i.txn_date AS internal_date,
  b.bank_date,
  i.expected_amount,
  b.received_amount,

  (b.received_amount - i.expected_amount) AS amount_diff,
  ABS(b.received_amount - i.expected_amount) AS abs_amount_diff,
  ABS(DATE_DIFF('day', i.txn_date, b.bank_date)) AS abs_day_diff,

  b.transaction_type,
  b.is_fraud
FROM mart.internal_expected i
JOIN mart.bank_received b
  ON ABS(DATE_DIFF('day', i.txn_date, b.bank_date)) <= 3
 AND ABS(b.received_amount - i.expected_amount) <= GREATEST(1.00, 0.01 * i.expected_amount);

-- pick best match per internal txn
CREATE OR REPLACE TABLE mart.recon_txn_matches AS
SELECT *
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY internal_txn_id
      ORDER BY abs_day_diff ASC, abs_amount_diff ASC
    ) AS rn
  FROM mart.recon_match_candidates
) t
WHERE rn = 1;

--unmatched internal payments (expected but not in bank)
CREATE OR REPLACE TABLE mart.recon_unmatched_internal AS
SELECT i.*
FROM mart.internal_expected i
LEFT JOIN mart.recon_txn_matches m
  ON i.txn_id = m.internal_txn_id
WHERE m.internal_txn_id IS NULL;

-- unmatched bank deposits (received but not in internal ledger)
CREATE OR REPLACE TABLE mart.recon_unmatched_bank AS
SELECT b.*
FROM mart.bank_received b
LEFT JOIN mart.recon_txn_matches m
  ON b.txn_id = m.bank_txn_id
WHERE m.bank_txn_id IS NULL;

-- KPI summary for dashboard
CREATE OR REPLACE TABLE mart.recon_kpis AS
SELECT
  (SELECT COUNT(*) FROM mart.internal_expected) AS internal_payment_count,
  (SELECT SUM(expected_amount) FROM mart.internal_expected) AS internal_payment_total,

  (SELECT COUNT(*) FROM mart.bank_received) AS bank_deposit_count,
  (SELECT SUM(received_amount) FROM mart.bank_received) AS bank_deposit_total,

  (SELECT COUNT(*) FROM mart.recon_txn_matches) AS matched_count,
  (SELECT SUM(expected_amount) FROM mart.recon_txn_matches) AS matched_internal_total,
  (SELECT SUM(received_amount) FROM mart.recon_txn_matches) AS matched_bank_total,

  (SELECT COUNT(*) FROM mart.recon_unmatched_internal) AS unmatched_internal_count,
  (SELECT SUM(expected_amount) FROM mart.recon_unmatched_internal) AS unmatched_internal_total,

  (SELECT COUNT(*) FROM mart.recon_unmatched_bank) AS unmatched_bank_count,
  (SELECT SUM(received_amount) FROM mart.recon_unmatched_bank) AS unmatched_bank_total;
