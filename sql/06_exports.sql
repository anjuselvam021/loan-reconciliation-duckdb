
COPY mart.recon_monthly
TO 'excel dashboard/recon_monthly.csv'
(HEADER, DELIMITER ',');

COPY mart.dirty_quality_summary
TO 'excel dashboard/dirty_quality_summary.csv'
(HEADER, DELIMITER ',');

COPY mart.fraud_quality_summary
TO 'excel dashboard/fraud_quality_summary.csv'
(HEADER, DELIMITER ',');

COPY stg.dirty_clean
TO 'excel dashboard/dirty_clean.csv'
(HEADER, DELIMITER ',');

--fraud flags sample 
COPY (
  SELECT *
  FROM mart.fraud_rule_flags
  WHERE txn_ts IS NOT NULL
  USING SAMPLE 200000 ROWS
)
TO 'excel dashboard/fraud_rule_flags_sample.csv'
(HEADER, DELIMITER ',');
COPY mart.recon_kpis
TO 'excel dashboard/recon_kpis.csv'
(HEADER, DELIMITER ',');

COPY mart.recon_txn_matches
TO 'excel dashboard/recon_txn_matches.csv'
(HEADER, DELIMITER ',');

COPY mart.exception_queue
TO 'excel dashboard/exception_queue.csv'
(HEADER, DELIMITER ',');
