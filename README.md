# Real-Time Financial Fraud Analytics Platform

## Overview
This project implements an end-to-end **real-time fraud detection and analytics platform** that ingests streaming financial transactions, applies rule-based fraud detection in real time, and delivers analytics-ready data and dashboards for monitoring fraud behavior.

The system mirrors a **production-style data engineering pipeline**, combining streaming ingestion, stateful processing, cloud data warehousing, analytics SQL, and business-facing dashboards.

---

## Architecture

```
Synthetic Transaction Generator
        ↓
     Apache Kafka
        ↓
  Fraud Processing Engine
        ↓
      BigQuery
        ↓
  Analytics SQL & Dashboard
```

---

## Tech Stack

- **Programming**: Python  
- **Streaming**: Apache Kafka  
- **Cloud Data Warehouse**: Google BigQuery  
- **Analytics**: SQL  
- **Visualization**: Looker Studio  
- **Infrastructure**: Docker, Git/GitHub  

---

## Data Engineering Pipeline

### Transaction Generation
- Real-time synthetic financial transaction generation
- ~5000 simulated users
- ~5% of users designated as high-risk
- Fraud users exhibit:
  - High transaction velocity
  - Large transaction amounts
  - Cross-country usage

### Streaming Ingestion
- Transactions published to Kafka topic `transactions_raw`
- User ID used as partition key to preserve ordering
- Producer–consumer decoupling for scalability

### Fraud Processing
- Kafka consumer processes transactions in real time
- Stateful sliding-window fraud rules:
  - **High transaction frequency** (≥5 transactions in 5 minutes)
  - **High spend velocity** ($3,000+ in 10 minutes)
  - **Multiple countries** within short time windows
- Each transaction is enriched with:
  - Fraud flag (`is_flagged`)
  - Risk reasons
  - Processing timestamp

### Data Warehouse
- Streaming inserts into BigQuery using service account authentication
- Partitioned and analytics-ready fact table (`fact_transactions`)
- Supports low-latency querying for dashboards and analytics

---

## Analytics & KPIs

Analytics queries are stored in the `analytics/` directory and include:

- **Fraud Rate Over Time**
- **Flagged Transactions vs Total Transactions**
- **Transaction Volume per Minute**
- **High-Risk Users Leaderboard**
- **Fraud Risk by Spending Category**

These KPIs enable real-time fraud monitoring and investigation prioritization.

---

## Dashboard

A real-time dashboard was built using **Looker Studio**, directly connected to BigQuery.

### Dashboard Features
- Live KPI scorecards:
  - Total Transactions
  - Flagged Transactions
  - Fraud Rate (%)
  - Average Transaction Amount
- Time-series monitoring of fraud trends
- High-risk users table for investigation
- Category-level fraud risk breakdown

Dashboard screenshots are available in the `dashboards/` directory.

---

## Results

- Processes **thousands of streaming transactions**
- Flags approximately **8–12%** of transactions as high risk depending on time window
- Fraud rate varies naturally due to repeated violations by high-risk users
- Enables near real-time fraud monitoring and analysis

---

## Validation & Accuracy

- Metrics validated by cross-checking:
  - BigQuery SQL results
  - Looker Studio dashboard aggregations
- Fraud rate stored as ratios and displayed as percentages for clarity
- Sliding-window logic ensures explainable and deterministic detection

---

## Future Enhancements

- Spark Structured Streaming for large-scale processing
- Alerting via Slack or email when fraud thresholds are exceeded
- Machine learning-based fraud scoring
- Airflow-based batch aggregations
- Historical fraud trend analysis

---

## Key Takeaways

This project demonstrates:
- End-to-end data engineering ownership
- Real-time streaming design patterns
- Stateful event processing
- Cloud data warehousing
- Analytics engineering and BI delivery
- Clear communication of complex systems through dashboards

---

## Author

**Sanketh Reddy**  
Data Engineering & Analytics Portfolio Project
