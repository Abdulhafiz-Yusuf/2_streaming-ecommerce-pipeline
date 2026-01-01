"""
E-Commerce Clickstream Event Producer
Simulates realistic user behavior on an e-commerce website.

Events generated:
- page_view: User lands on product page
- add_to_cart: User adds product to cart
- remove_from_cart: User removes from cart
- purchase: User completes checkout
"""

import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import uuid

# Initialize Faker for realistic fake data
fake = Faker()

# Kafka Configuration
KAFKA_BROKER = 'localhost:19092'
TOPIC_NAME = 'ecommerce-clickstream'

# Product catalog (simulate realistic e-commerce)
PRODUCT_CATALOG = [
    {"id": "P001", "name": "Wireless Earbuds", "category": "Electronics", "price": 79.99},
    {"id": "P002", "name": "Running Shoes", "category": "Sports", "price": 129.99},
    {"id": "P003", "name": "Coffee Maker", "category": "Home", "price": 89.99},
    {"id": "P004", "name": "Yoga Mat", "category": "Sports", "price": 34.99},
    {"id": "P005", "name": "Bluetooth Speaker", "category": "Electronics", "price": 59.99},
    {"id": "P006", "name": "Desk Lamp", "category": "Home", "price": 45.99},
    {"id": "P007", "name": "Protein Powder", "category": "Health", "price": 49.99},
    {"id": "P008", "name": "Laptop Stand", "category": "Electronics", "price": 39.99},
    {"id": "P009", "name": "Water Bottle", "category": "Sports", "price": 24.99},
    {"id": "P010", "name": "Backpack", "category": "Fashion", "price": 69.99},
]

DEVICE_TYPES = ["Desktop", "Mobile", "Tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]
COUNTRIES = ["Nigeria", "USA", "UK", "UAE", "Canada"]


def create_kafka_producer():
    """
    Initialize Kafka producer with JSON serialization.
    
    Returns:
        KafkaProducer: Configured producer instance
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Reliability settings
        acks='all',  # Wait for all replicas to acknowledge
        retries=3,   # Retry failed sends
        max_in_flight_requests_per_connection=1  # Ensure ordering
    )


def generate_user_session():
    """
    Simulate a single user shopping session.
    
    Returns realistic user journey:
    1. Page views (browsing)
    2. Maybe add to cart (30% chance)
    3. Maybe purchase (50% chance if cart not empty)
    
    Returns:
        list: Events for this session
    """
    user_id = f"USER_{fake.uuid4()[:8]}"
    session_id = f"SESSION_{uuid.uuid4().hex[:12]}"
    device = random.choice(DEVICE_TYPES)
    browser = random.choice(BROWSERS)
    country = random.choice(COUNTRIES)
    city = fake.city()
    
    events = []
    cart_items = []  # Track what user added to cart
    
    # Phase 1: Browsing (2-5 page views)
    num_page_views = random.randint(2, 5)
    browsed_products = random.sample(PRODUCT_CATALOG, num_page_views)
    
    for product in browsed_products:
        event = {
            "event_id": f"EVT_{uuid.uuid4().hex[:12]}",
            "event_timestamp": datetime.now().isoformat(),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": "page_view",
            "product_id": product["id"],
            "product_name": product["name"],
            "product_category": product["category"],
            "product_price": product["price"],
            "quantity": None,
            "page_url": f"https://shop.example.com/product/{product['id']}",
            "referrer_url": "https://google.com" if events == [] else events[-1]["page_url"],
            "device_type": device,
            "browser": browser,
            "country": country,
            "city": city
        }
        events.append(event)
        time.sleep(random.uniform(0.5, 2))  # Simulate reading time
        
        # Phase 2: Add to cart (30% chance per viewed product)
        if random.random() < 0.3:
            cart_event = event.copy()
            cart_event.update({
                "event_id": f"EVT_{uuid.uuid4().hex[:12]}",
                "event_timestamp": datetime.now().isoformat(),
                "event_type": "add_to_cart",
                "quantity": random.randint(1, 3)
            })
            events.append(cart_event)
            cart_items.append(cart_event)
            time.sleep(random.uniform(0.3, 1))
    
    # Phase 3: Purchase (50% chance if cart not empty)
    if cart_items and random.random() < 0.5:
        for item in cart_items:
            purchase_event = item.copy()
            purchase_event.update({
                "event_id": f"EVT_{uuid.uuid4().hex[:12]}",
                "event_timestamp": datetime.now().isoformat(),
                "event_type": "purchase",
                "page_url": "https://shop.example.com/checkout/success"
            })
            events.append(purchase_event)
            time.sleep(random.uniform(0.2, 0.5))
    
    return events


def main():
    """
    Main producer loop.
    Generates continuous stream of user sessions.
    """
    print("🚀 Starting E-Commerce Event Producer...")
    print(f"📡 Connecting to Kafka: {KAFKA_BROKER}")
    print(f"📨 Publishing to topic: {TOPIC_NAME}")
    print("-" * 60)
    
    producer = create_kafka_producer()
    event_count = 0
    
    try:
        while True:
            # Generate one user session
            session_events = generate_user_session()
            
            # Publish each event to Kafka
            for event in session_events:
                producer.send(TOPIC_NAME, value=event)
                event_count += 1
                
                # Pretty print event
                print(f"✅ [{event_count}] {event['event_type'].upper()}: "
                      f"User {event['user_id'][-8:]} | "
                      f"Product: {event['product_name']}")
            
            producer.flush()  # Ensure delivery
            
            # Pause between sessions (simulate new users arriving)
            time.sleep(random.uniform(2, 5))
            
    except KeyboardInterrupt:
        print(f"\n⏹️  Stopping producer. Total events sent: {event_count}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()