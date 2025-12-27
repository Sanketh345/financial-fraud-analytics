SELECT
  TIMESTAMP_TRUNC(transaction_ts, MINUTE) AS minute,
  COUNT(*) AS transactions_per_minute
FROM fraud_analytics.fact_transactions
GROUP BY minute
ORDER BY minute DESC;
