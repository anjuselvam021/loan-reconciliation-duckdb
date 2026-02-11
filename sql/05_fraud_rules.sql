
CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE TABLE mart.fraud_rule_flags AS
WITH base AS (
  SELECT
    txn_id,
    txn_ts,
    sender_account,
    receiver_account,
    amount,
    transaction_type,
    merchant_category,
    location,
    device_used,
    payment_channel,
    ip_address,
    device_hash,
    is_fraud,
    fraud_type,
    time_since_last_transaction,
    spending_deviation_score,
    velocity_score,
    geo_anomaly_score
  FROM stg.fraud_clean
),

rules AS (
  SELECT
    *,

    CASE WHEN amount >= 5000 THEN 1 ELSE 0 END AS rule_high_amount,
    CASE WHEN velocity_score >= 0.80 THEN 1 ELSE 0 END AS rule_high_velocity,
    CASE WHEN geo_anomaly_score >= 0.80 THEN 1 ELSE 0 END AS rule_geo_anomaly,
    CASE WHEN spending_deviation_score >= 0.80 THEN 1 ELSE 0 END AS rule_spend_deviation,
    CASE WHEN time_since_last_transaction IS NOT NULL AND time_since_last_transaction <= 30 THEN 1 ELSE 0 END AS rule_rapid_repeat

  FROM base
),

scored AS (
  SELECT
    *,

    --Integer rule count (no boolean + boolean)
    (rule_high_amount
     + rule_high_velocity
     + rule_geo_anomaly
     + rule_spend_deviation
     + rule_rapid_repeat) AS rule_count

  FROM rules
)

SELECT
  txn_id,
  txn_ts,
  sender_account,
  receiver_account,
  amount,
  transaction_type,
  merchant_category,
  location,
  device_used,
  payment_channel,
  ip_address,
  device_hash,
  is_fraud,
  fraud_type,
  time_since_last_transaction,
  spending_deviation_score,
  velocity_score,
  geo_anomaly_score,

  rule_high_amount,
  rule_high_velocity,
  rule_geo_anomaly,
  rule_spend_deviation,
  rule_rapid_repeat,

  CASE
    WHEN rule_count >= 3 THEN 'High'
    WHEN rule_count = 2 THEN 'Medium'
    ELSE 'Low'
  END AS risk_tier

FROM scored;
