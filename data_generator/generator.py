import json
import random
import time
from datetime import datetime
from faker import Faker
import numpy as np
from kafka import KafkaProducer

# -----------------------------
# Configuration
# -----------------------------
NUM_USERS = 500
FRAUD_USER_PERCENTAGE = 0.05  # 5% of users are fraud-prone
SLEEP_MIN = 0.1
SLEEP_MAX = 0.5
KAFKA_TOPIC = "transactions_raw"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

fake = Faker()

# -----------------------------
# Static reference data
# -----------------------------
countries = ["US", "CA", "UK", "IN", "DE"]
categories = ["groceries", "electronics", "travel", "fuel", "entertainment"]

merchants = {
    "groceries": ["Walmart", "Target", "Costco"],
    "electronics": ["Amazon", "BestBuy", "Apple"],
    "travel": ["Uber", "Lyft", "Delta"],
    "fuel": ["Shell", "Chevron", "Exxon"],
    "entertainment": ["Netflix", "Spotify", "AMC"]
}

# -----------------------------
# Users
# -----------------------------
users = [f"user_{i}" for i in range(NUM_USERS)]
fraud_users = set(random.sample(users, int(NUM_USERS * FRAUD_USER_PERCENTAGE)))

# -----------------------------
# Kafka Producer
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

# -----------------------------
# Helper functions
# -----------------------------
def generate_amount(is_fraud: bool) -> float:
    """
    Generate transaction amount.
    Fraud users have higher, more erratic amounts.
    """
    if is_fraud:
        return round(np.random.uniform(500, 5000), 2)
    return round(np.random.exponential(scale=50), 2)

def generate_transaction() -> dict:
    """
    Generate a single transaction event.
    """
    user_id = random.choice(users)
    is_fraud_user = user_id in fraud_users

    category = random.choice(categories)
    merchant = random.choice(merchants[category])

    transaction = {
        "transaction_id": fake.uuid4(),
        "user_id": user_id,
        "amount": generate_amount(is_fraud_user),
        "merchant": merchant,
        "category": category,
        "country": random.choice(countries) if is_fraud_user else "US",
        "timestamp": datetime.utcnow().isoformat()
    }

    return transaction

# -----------------------------
# Main loop
# -----------------------------
if __name__ == "__main__":
    print("🚀 Starting real-time transaction generator (Kafka producer)...\n")

    try:
        while True:
            txn = generate_transaction()

            producer.send(
                topic=KAFKA_TOPIC,
                key=txn["user_id"],
                value=txn
            )

            print(json.dumps(txn))
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    except KeyboardInterrupt:
        print("\n🛑 Generator stopped by user")

    finally:
        producer.flush()
        producer.close()
