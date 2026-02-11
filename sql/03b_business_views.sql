

CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE VIEW mart.internal_expected AS
SELECT
  txn_id,
  customer_id,
  txn_date,
  gross_amount AS expected_amount,
  payment_method
FROM stg.dirty_clean
WHERE transaction_status = 'Completed'
  AND txn_date IS NOT NULL
  AND gross_amount IS NOT NULL
  AND gross_amount > 0;

-- bank feed= deposits received
CREATE OR REPLACE VIEW mart.bank_received AS
SELECT
  txn_id,
  txn_ts::DATE AS bank_date,
  sender_account,
  receiver_account,
  amount AS received_amount,
  transaction_type,
  is_fraud
FROM stg.fraud_clean
WHERE txn_ts IS NOT NULL
  AND amount IS NOT NULL
  AND amount > 0;
