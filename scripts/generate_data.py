"""Generate sample data for Customer Rewards demo."""

import os
import json
import time
import random
from datetime import datetime, timedelta
from typing import Dict, Any
from confluent_kafka import Producer
from faker import Faker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

fake = Faker()

# Kafka configuration
def get_kafka_config() -> Dict[str, str]:
    """Get Kafka configuration from environment variables."""
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    api_key = os.getenv('KAFKA_API_KEY')
    api_secret = os.getenv('KAFKA_API_SECRET')

    if not all([bootstrap_servers, api_key, api_secret]):
        raise ValueError(
            "Missing Kafka configuration. Please ensure KAFKA_BOOTSTRAP_SERVERS, "
            "KAFKA_API_KEY, and KAFKA_API_SECRET are set in your .env file."
        )

    return {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': api_key,
        'sasl.password': api_secret,
    }

def create_producer() -> Producer:
    """Create a Kafka producer."""
    config = get_kafka_config()
    config.update({
        'client.id': 'amex-demo-producer',
        'acks': 'all',
        'compression.type': 'snappy',
    })
    return Producer(config)

def delivery_callback(err, msg):
    """Callback for producer delivery reports."""
    if err:
        print(f"❌ ERROR: Message delivery failed: {err}")
    else:
        print(f"✓ Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

# Sample data
LOYALTY_TIERS = ['Basic', 'Silver', 'Gold', 'Platinum']
MERCHANTS = [
    'Whole Foods', 'Starbucks', 'Amazon', 'Delta Airlines', 'Marriott Hotels',
    'Shell Gas Station', 'Apple Store', 'Best Buy', 'Target', 'Costco',
    'Home Depot', 'Nordstrom', 'Uber', 'Netflix', 'Spotify'
]
PRODUCTS_SERVICES = [
    'Groceries', 'Coffee', 'Electronics', 'Flights', 'Hotel Stays',
    'Fuel', 'iPhone', 'Laptop', 'Home Goods', 'Membership',
    'Home Improvement', 'Clothing', 'Ride Share', 'Streaming', 'Music'
]

# Major US ZIP codes for address changes
ZIP_CODES = [
    '10001',  # New York, NY
    '90210',  # Beverly Hills, CA
    '60601',  # Chicago, IL
    '77001',  # Houston, TX
    '33101',  # Miami, FL
    '02101',  # Boston, MA
    '98101',  # Seattle, WA
    '30301',  # Atlanta, GA
    '85001',  # Phoenix, AZ
    '19101',  # Philadelphia, PA
]

def generate_customer(customer_id: str) -> Dict[str, Any]:
    """Generate a customer record."""
    name = fake.name()
    current_zip = random.choice(ZIP_CODES)
    loyalty_tier = random.choice(LOYALTY_TIERS)

    # Points based on tier
    points_range = {
        'Basic': (0, 10000),
        'Silver': (10000, 30000),
        'Gold': (30000, 75000),
        'Platinum': (75000, 250000)
    }
    loyalty_points = random.randint(*points_range[loyalty_tier])

    return {
        'customer_id': customer_id,
        'name': name,
        'permanent_address': fake.address().replace('\n', ', '),
        'current_address': fake.address().replace('\n', ', '),
        'current_zip': current_zip,
        'loyalty_tier': loyalty_tier,
        'loyalty_points': loyalty_points,
        'email': fake.email(),
        'updated_at': datetime.now().isoformat()
    }

def generate_loyalty_usage(customer_id: str, count: int = 5) -> list:
    """Generate loyalty usage history for a customer."""
    usage_records = []
    for i in range(count):
        merchant = random.choice(MERCHANTS)
        product_service = random.choice(PRODUCTS_SERVICES)
        points_used = random.randint(100, 5000)
        timestamp = datetime.now() - timedelta(days=random.randint(1, 90))

        usage_records.append({
            'usage_id': f"U{customer_id[1:]}-{i+1:03d}",
            'customer_id': customer_id,
            'merchant': merchant,
            'product_service': product_service,
            'loyalty_points_used': points_used,
            'transaction_timestamp': timestamp.isoformat()
        })
    return usage_records

def generate_address_change(customer: Dict[str, Any]) -> Dict[str, Any]:
    """Generate an address change event."""
    old_zip = customer['current_zip']
    # Pick a different ZIP code
    new_zip = random.choice([z for z in ZIP_CODES if z != old_zip])

    event_id = f"E{int(time.time() * 1000)}-{customer['customer_id']}"

    return {
        'event_id': event_id,
        'customer_id': customer['customer_id'],
        'old_zip': old_zip,
        'new_zip': new_zip,
        'change_timestamp': datetime.now().isoformat()
    }

def produce_json_message(producer: Producer, topic: str, key: str, value: Dict[str, Any]):
    """Produce a JSON message to Kafka."""
    try:
        producer.produce(
            topic=topic,
            key=key.encode('utf-8'),
            value=json.dumps(value).encode('utf-8'),
            callback=delivery_callback
        )
        producer.poll(0)
    except Exception as e:
        print(f"❌ ERROR: Failed to produce message: {e}")

def main():
    """Main data generation function."""
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  Customer Rewards Demo - Data Generator")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print()

    producer = create_producer()

    # Generate 100 customers
    print("📝 Generating 100 customers...")
    customers = []
    for i in range(1, 101):
        customer_id = f"C{i:03d}"
        customer = generate_customer(customer_id)
        customers.append(customer)

        # Produce to customer_details topic
        produce_json_message(producer, 'customer_details', customer_id, customer)

    producer.flush()
    print(f"✓ Generated {len(customers)} customers\n")

    # Generate loyalty usage history
    print("📝 Generating loyalty usage history...")
    for customer in customers:
        usage_records = generate_loyalty_usage(customer['customer_id'], count=random.randint(3, 8))
        for usage in usage_records:
            produce_json_message(producer, 'loyalty_usage_history', usage['usage_id'], usage)

    producer.flush()
    print(f"✓ Generated loyalty usage history\n")

    # Generate address change events continuously
    print("📝 Generating address change events (Ctrl+C to stop)...")
    print("   Rate: ~1 event every 5 seconds\n")

    try:
        while True:
            # Pick a random customer
            customer = random.choice(customers)

            # Generate address change
            address_change = generate_address_change(customer)

            # Produce to address_changes topic
            produce_json_message(producer, 'address_changes', address_change['event_id'], address_change)

            producer.flush()

            # Wait before next event
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n\n⚠️  Stopping data generation...")
        producer.flush()
        print("✓ All messages flushed\n")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print("  Data generation complete!")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

if __name__ == '__main__':
    main()
