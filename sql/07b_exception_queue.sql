-- ============================================
-- 07b_exception_queue.sql
-- Finance-style exception queue with reason codes
-- ============================================

CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE TABLE mart.exception_queue AS

SELECT
  'INTERNAL_MISSING_IN_BANK' AS exception_type,
  txn_id,
  customer_id,
  txn_date AS event_date,
  expected_amount AS amount,
  payment_method,
  NULL::VARCHAR AS bank_txn_id,
  NULL::VARCHAR AS bank_transaction_type,
  NULL::INTEGER AS is_fraud
FROM mart.recon_unmatched_internal

UNION ALL

SELECT
  'BANK_MISSING_IN_INTERNAL' AS exception_type,
  txn_id,
  NULL::VARCHAR AS customer_id,
  bank_date AS event_date,
  received_amount AS amount,
  NULL::VARCHAR AS payment_method,
  txn_id AS bank_txn_id,
  transaction_type AS bank_transaction_type,
  is_fraud
FROM mart.recon_unmatched_bank;
