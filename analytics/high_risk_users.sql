SELECT
  user_id,
  COUNT(*) AS total_transactions,
  COUNTIF(is_flagged) AS flagged_transactions,
  SUM(amount) AS total_amount
FROM fraud_analytics.fact_transactions
GROUP BY user_id
HAVING flagged_transactions > 0
ORDER BY flagged_transactions DESC, total_amount DESC
LIMIT 20;
