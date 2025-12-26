import json
import random
import time
from datetime import datetime
from faker import Faker
import numpy as np

fake = Faker()

# Configuration
NUM_USERS = 500
FRAUD_USER_PERCENTAGE = 0.05  # 5% of users behave fraudulently
SLEEP_MIN = 0.1
SLEEP_MAX = 0.5

countries = ["US", "CA", "UK", "IN", "DE"]
categories = ["groceries", "electronics", "travel", "fuel", "entertainment"]

merchants = {
    "groceries": ["Walmart", "Target", "Costco"],
    "electronics": ["Amazon", "BestBuy", "Apple"],
    "travel": ["Uber", "Lyft", "Delta"],
    "fuel": ["Shell", "Chevron", "Exxon"],
    "entertainment": ["Netflix", "Spotify", "AMC"]
}

# Generate users
users = [f"user_{i}" for i in range(NUM_USERS)]
fraud_users = set(random.sample(users, int(NUM_USERS * FRAUD_USER_PERCENTAGE)))

def generate_amount(is_fraud: bool) -> float:
    if is_fraud:
        return round(np.random.uniform(500, 5000), 2)
    return round(np.random.exponential(scale=50), 2)

def generate_transaction():
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

if __name__ == "__main__":
    print("Starting transaction generator...\n")

    while True:
        txn = generate_transaction()
        print(json.dumps(txn))
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
