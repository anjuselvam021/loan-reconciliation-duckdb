CREATE SCHEMA IF NOT EXISTS raw;

CREATE OR REPLACE TABLE raw.dirty AS
SELECT *
FROM read_csv_auto('data/raw/dirty_financial_transactions.csv', ALL_VARCHAR=TRUE);

CREATE OR REPLACE TABLE raw.fraud AS
SELECT *
FROM read_csv_auto('data/raw/financial_fraud_detection_dataset.csv', ALL_VARCHAR=TRUE);

SELECT 'raw.dirty' AS tbl, COUNT(*) AS rows FROM raw.dirty
UNION ALL
SELECT 'raw.fraud', COUNT(*) FROM raw.fraud;
