SELECT
  category,
  COUNT(*) AS total_transactions,
  COUNTIF(is_flagged) AS flagged_transactions,
  SAFE_DIVIDE(COUNTIF(is_flagged), COUNT(*)) AS fraud_rate
FROM fraud_analytics.fact_transactions
GROUP BY category
ORDER BY fraud_rate DESC;
