import json
from collections import defaultdict, deque
from datetime import datetime, timedelta

from kafka import KafkaConsumer
from google.cloud import bigquery

# ---------------- CONFIG ----------------
KAFKA_TOPIC = "transactions_raw"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

PROJECT_ID = "financial-fraud-analytics"
TABLE_ID = f"{PROJECT_ID}.fraud_analytics.fact_transactions"

# ----------------------------------------

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    key_deserializer=lambda m: m.decode("utf-8"),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="fraud_processor_group"
)

bq_client = bigquery.Client(project=PROJECT_ID)

txn_windows = defaultdict(deque)
country_windows = defaultdict(deque)

TXN_COUNT_THRESHOLD = 5
TXN_COUNT_WINDOW = timedelta(minutes=5)

AMOUNT_THRESHOLD = 3000
AMOUNT_WINDOW = timedelta(minutes=10)

COUNTRY_WINDOW = timedelta(minutes=15)

def clean_old_entries(window, now, window_size):
    while window and window[0]["timestamp"] < now - window_size:
        window.popleft()

print("🚨 Starting fraud processor...\n")

for msg in consumer:
    txn = msg.value

    user_id = txn["user_id"]
    amount = float(txn["amount"])
    country = txn["country"]
    txn_time = datetime.fromisoformat(txn["timestamp"])

    txn_windows[user_id].append({
        "amount": amount,
        "timestamp": txn_time
    })

    country_windows[user_id].append({
        "country": country,
        "timestamp": txn_time
    })

    clean_old_entries(txn_windows[user_id], txn_time, TXN_COUNT_WINDOW)
    clean_old_entries(country_windows[user_id], txn_time, COUNTRY_WINDOW)

    txn_count = len(txn_windows[user_id])
    total_amount = sum(t["amount"] for t in txn_windows[user_id])
    countries = {c["country"] for c in country_windows[user_id]}

    is_flagged = False
    risk_reasons = []

    if txn_count >= TXN_COUNT_THRESHOLD:
        is_flagged = True
        risk_reasons.append("HIGH_TXN_FREQUENCY")

    if total_amount >= AMOUNT_THRESHOLD:
        is_flagged = True
        risk_reasons.append("HIGH_TOTAL_AMOUNT")

    if len(countries) > 1:
        is_flagged = True
        risk_reasons.append("MULTIPLE_COUNTRIES")

    enriched_txn = {
        **txn,
        "is_flagged": is_flagged,
        "risk_reasons": risk_reasons,
        "processed_at": datetime.utcnow().isoformat()
    }

    row = {
    "transaction_id": enriched_txn["transaction_id"],
    "user_id": enriched_txn["user_id"],
    "amount": float(enriched_txn["amount"]),
    "merchant": enriched_txn["merchant"],
    "category": enriched_txn["category"],
    "country": enriched_txn["country"],

    # ✅ FIX: keep timestamps as strings (BigQuery parses them)
    "transaction_ts": enriched_txn["timestamp"],
    "is_flagged": bool(enriched_txn["is_flagged"]),
    "risk_reasons": enriched_txn["risk_reasons"],
    "processed_at": enriched_txn["processed_at"]
}


    errors = bq_client.insert_rows_json(TABLE_ID, [row])

    if errors:
        print("❌ BigQuery insert error:")
        for error in errors:
            print(error)
    else:
        print("✅ Inserted row into BigQuery:", row["transaction_id"])
