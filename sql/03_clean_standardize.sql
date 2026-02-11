

CREATE SCHEMA IF NOT EXISTS stg;

-- dirty transactions

CREATE OR REPLACE TABLE stg.dirty_clean AS
WITH base AS (
  SELECT
    Transaction_ID,
    Transaction_Date,
    Customer_ID,
    Product_Name,
    Quantity,
    Price,
    Payment_Method,
    Transaction_Status
  FROM raw.dirty
),

typed AS (
  SELECT
    NULLIF(TRIM(Transaction_ID), '') AS txn_id,
    NULLIF(TRIM(Customer_ID), '') AS customer_id,

    TRY_CAST(NULLIF(TRIM(Transaction_Date), '') AS DATE) AS txn_date,

    NULLIF(TRIM(Product_Name), '') AS product_name,

    TRY_CAST(NULLIF(TRIM(Quantity), '') AS DOUBLE) AS quantity,

    TRY_CAST(
      NULLIF(REGEXP_REPLACE(TRIM(Price), '[^0-9\\.-]', '', 'g'), '')
      AS DOUBLE
    ) AS price,

    CASE
      WHEN Payment_Method IS NULL THEN NULL
      WHEN LOWER(Payment_Method) LIKE '%paypal%' OR LOWER(Payment_Method) LIKE '%pay pal%' THEN 'PayPal'
      WHEN LOWER(Payment_Method) LIKE '%credit%' THEN 'Credit Card'
      WHEN LOWER(Payment_Method) LIKE '%debit%' THEN 'Debit Card'
      WHEN LOWER(Payment_Method) LIKE '%cash%' THEN 'Cash'
      ELSE 'Other'
    END AS payment_method,

    -- Standardize status
    CASE
      WHEN Transaction_Status IS NULL THEN NULL
      WHEN LOWER(Transaction_Status) LIKE '%complete%' THEN 'Completed'
      WHEN LOWER(Transaction_Status) LIKE '%pending%' THEN 'Pending'
      WHEN LOWER(Transaction_Status) LIKE '%fail%' THEN 'Failed'
      ELSE 'Unknown'
    END AS transaction_status

  FROM base
),

final AS (
  SELECT
    txn_id,
    customer_id,
    txn_date,
    product_name,
    quantity,
    price,
    ROUND(quantity * price, 2) AS gross_amount,
    payment_method,
    transaction_status,
-- when 1, there is problem
    CASE WHEN txn_id IS NULL THEN 1 ELSE 0 END AS flag_missing_txn_id,
    CASE WHEN txn_date IS NULL THEN 1 ELSE 0 END AS flag_bad_date,
    CASE WHEN quantity IS NULL OR quantity <= 0 THEN 1 ELSE 0 END AS flag_bad_quantity,
    CASE WHEN price IS NULL OR price <= 0 THEN 1 ELSE 0 END AS flag_bad_price,
    CASE WHEN (quantity * price) IS NULL THEN 1 ELSE 0 END AS flag_bad_amount

  FROM typed
)

SELECT *
FROM final;


--fraud bank transactions

CREATE OR REPLACE TABLE stg.fraud_clean AS
WITH base AS (
  SELECT *
  FROM raw.fraud
),

typed AS (
  SELECT
    NULLIF(TRIM(transaction_id), '') AS txn_id,

    TRY_CAST(NULLIF(TRIM(timestamp), '') AS TIMESTAMP) AS txn_ts,

    NULLIF(TRIM(sender_account), '') AS sender_account,
    NULLIF(TRIM(receiver_account), '') AS receiver_account,

    TRY_CAST(
      NULLIF(REGEXP_REPLACE(TRIM(amount), '[^0-9\\.-]', '', 'g'), '')
      AS DOUBLE
    ) AS amount,

    NULLIF(TRIM(transaction_type), '') AS transaction_type,
    NULLIF(TRIM(merchant_category), '') AS merchant_category,
    NULLIF(TRIM(location), '') AS location,
    NULLIF(TRIM(device_used), '') AS device_used,
    NULLIF(TRIM(payment_channel), '') AS payment_channel,

    NULLIF(TRIM(ip_address), '') AS ip_address,
    NULLIF(TRIM(device_hash), '') AS device_hash,

CASE
  WHEN is_fraud IS NULL THEN NULL
  WHEN LOWER(TRIM(is_fraud)) = 'true'  THEN 1
  WHEN LOWER(TRIM(is_fraud)) = 'false' THEN 0
  ELSE NULL
END AS is_fraud,
    NULLIF(TRIM(fraud_type), '') AS fraud_type,

    TRY_CAST(NULLIF(TRIM(time_since_last_transaction), '') AS DOUBLE) AS time_since_last_transaction,
    TRY_CAST(NULLIF(TRIM(spending_deviation_score), '') AS DOUBLE) AS spending_deviation_score,
    TRY_CAST(NULLIF(TRIM(velocity_score), '') AS DOUBLE) AS velocity_score,
    TRY_CAST(NULLIF(TRIM(geo_anomaly_score), '') AS DOUBLE) AS geo_anomaly_score

  FROM base
),

final AS (
  SELECT
    *,

    CASE WHEN txn_id IS NULL THEN 1 ELSE 0 END AS flag_missing_txn_id,
    CASE WHEN txn_ts IS NULL THEN 1 ELSE 0 END AS flag_bad_timestamp,
    CASE WHEN amount IS NULL OR amount <= 0 THEN 1 ELSE 0 END AS flag_bad_amount

  FROM typed
)

SELECT *
FROM final;
