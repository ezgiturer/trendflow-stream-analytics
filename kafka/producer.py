# TrendFlow Kafka Producer for E-Commerce Streaming Project

import time
import logging
import random
import json
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from typing import Dict, List, Optional

# Extended product catalog with more realistic data
products = [
    {"product_id": "SKU_iphone_15", "product_name": "Apple iPhone 15", "category": "Smartphones", "price": 850.00},
    {"product_id": "SKU_nike_airmax", "product_name": "Nike Air Max", "category": "Shoes", "price": 149.99},
    {"product_id": "SKU_macbook_pro", "product_name": "MacBook Pro", "category": "Laptops", "price": 1899.00},
    {"product_id": "SKU_logitech_mouse", "product_name": "Logitech Wireless Mouse", "category": "Accessories", "price": 46.90},
    {"product_id": "SKU_adidas_shirt", "product_name": "Adidas Running Shirt", "category": "Clothing", "price": 29.99},
    {"product_id": "SKU_samsung_tv", "product_name": "Samsung 4K TV", "category": "Electronics", "price": 3999.00},
    {"product_id": "SKU_kindle_paperwhite", "product_name": "Kindle Paperwhite", "category": "Electronics", "price": 134.99},
    {"product_id": "SKU_sony_headphones", "product_name": "Sony Noise Cancelling Headphones", "category": "Audio", "price": 299.99},
    {"product_id": "SKU_gopro_hero", "product_name": "GoPro Hero 12", "category": "Cameras", "price": 499.99},
    {"product_id": "SKU_fitbit_charge", "product_name": "Fitbit Charge 6", "category": "Wearables", "price": 149.95},
    {"product_id": "SKU_dell_xps", "product_name": "Dell XPS 13", "category": "Laptops", "price": 1299.00},
    {"product_id": "SKU_samsung_galaxy_tab", "product_name": "Samsung Galaxy Tab S8", "category": "Tablets", "price": 649.99},
    {"product_id": "SKU_bose_soundlink", "product_name": "Bose SoundLink Revolve+", "category": "Audio", "price": 199.99},
    {"product_id": "SKU_asus_rog_laptop", "product_name": "ASUS ROG Zephyrus G14", "category": "Gaming Laptops", "price": 1799.00},
    {"product_id": "SKU_nintendo_switch", "product_name": "Nintendo Switch OLED Model", "category": "Gaming Consoles", "price": 349.99},
    {"product_id": "SKU_smartwatch_apple", "product_name": "Apple Watch Series 9", "category": "Wearables", "price": 399.99},
    {"product_id": "SKU_google_pixel_phone", "product_name": "Google Pixel 8 Pro", "category": "Smartphones", "price": 999.99},
    {"product_id": "SKU_roku_streaming_stick", "product_name": "Roku Streaming Stick 4K", "category": "Streaming Devices", "price": 49.99},
    {"product_id": "SKU_jbl_flip5", "product_name": "JBL Flip 5 Portable Speaker", "category": "Audio", "price": 89.95},
    {"product_id": "SKU_philips_air_fryer", "product_name": "Philips Air Fryer XXL", "category": "Home Appliances", "price": 249.99},
    {"product_id": "SKU_smart_light_bulb", "product_name": "Philips Hue Smart Light Bulb", "category": "Smart Home", "price": 49.99},
    {"product_id": "SKU_roomba_robot_vacuum", "product_name": "iRobot Roomba 694 Robot Vacuum", "category": "Home Appliances", "price": 299.99},
    {"product_id": "SKU_samsung_galaxy_watch", "product_name": "Samsung Galaxy Watch 6", "category": "Wearables", "price": 349.99},
    {"product_id": "SKU_anker_powerbank", "product_name": "Anker PowerCore 20100mAh Power Bank", "category": "Accessories", "price": 39.99},
    {"product_id": "SKU_logitech_keyboard", "product_name": "Logitech K380 Multi-Device Bluetooth Keyboard", "category": "Accessories", "price": 49.99},
    {"product_id": "SKU_samsung_galaxy_buds", "product_name": "Samsung Galaxy Buds2 Pro", "category": "Audio", "price": 199.99},
    {"product_id": "SKU_microsoft_surface_pro", "product_name": "Microsoft Surface Pro 9", "category": "Tablets", "price": 999.99},
    {"product_id": "SKU_apple_airpods_pro", "product_name": "Apple AirPods Pro 2nd Gen", "category": "Audio", "price": 249.99},
    {"product_id": "SKU_sony_ps5", "product_name": "Sony PlayStation 5 Console", "category": "Gaming Consoles", "price": 499.99},
    {"product_id": "SKU_xbox_series_x", "product_name": "Xbox Series X Console", "category": "Gaming Consoles", "price": 499.99},
]

# More realistic user distribution
users = [f"user_{i:03d}" for i in range(1, 101)]  # 100 users

# Product popularity weights (some products more popular than others)
product_weights = {
    "SKU_iphone_15": 0.15,
    "SKU_macbook_pro": 0.10,
    "SKU_samsung_tv": 0.08,
    "SKU_nintendo_switch": 0.12,
    "SKU_sony_ps5": 0.09,
    "SKU_xbox_series_x": 0.08,
    "SKU_apple_airpods_pro": 0.07,
    "SKU_samsung_galaxy_buds": 0.06,
    "SKU_logitech_mouse": 0.05,
    "SKU_nike_airmax": 0.04,
    "SKU_smartwatch_apple": 0.06,
    "SKU_kindle_paperwhite": 0.03,
    "SKU_bose_soundlink": 0.04,
    "SKU_fitbit_charge": 0.03,
}

def create_topic_if_not_exists(broker_address: str, topic_name: str, num_partitions: int = 3, replication_factor: int = 1):
    """Create Kafka topic if it doesn't exist"""
    admin_client = KafkaAdminClient(bootstrap_servers=broker_address)
    
    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )
    
    try:
        admin_client.create_topics([topic])
        logging.info(f"Topic '{topic_name}' created successfully")
    except TopicAlreadyExistsError:
        logging.info(f"Topic '{topic_name}' already exists")
    except Exception as e:
        logging.error(f"Error creating topic: {e}")
        raise
    finally:
        admin_client.close()

def get_weighted_product() -> Dict:
    """Select a product based on popularity weights"""
    # Create weighted choices for popular products
    weighted_products = []
    weights = []
    
    for product in products:
        weight = product_weights.get(product["product_id"], 0.01)  # Default low weight
        weighted_products.append(product)
        weights.append(weight)
    
    return random.choices(weighted_products, weights=weights, k=1)[0]

def generate_event(event_counter: int, click_prob: float = 0.75) -> Dict:
    """
    Simulates a click or purchase event on e-commerce platform
    
    Args:
        event_counter: Sequential event counter
        click_prob: Probability of click vs purchase event
    
    Returns:
        Dict containing event data
    """
    product = get_weighted_product()
    user_id = random.choice(users)
    
    # 75% chance click, 25% chance purchase (more realistic conversion rate)
    event_type = "click" if random.random() < click_prob else "purchase"
    
    # Add session information for more realistic user behavior
    session_id = f"session_{user_id}_{random.randint(1000, 9999)}"
    
    event = {
        "event_id": f"evt_{event_counter:06d}",
        "timestamp": datetime.now().isoformat(),
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "price": product["price"],
        "quantity": random.randint(1, 3) if event_type == "purchase" else 1,
        "user_agent": random.choice(["mobile", "desktop", "tablet"]),
        "source": random.choice(["organic", "paid_search", "social", "email", "direct"])
    }
    
    # Add revenue for purchase events
    if event_type == "purchase":
        event["revenue"] = round(event["price"] * event["quantity"], 2)
    
    return event

def simulate_burst_traffic(base_sleep_time: float) -> float:
    """
    Simulate realistic traffic patterns with occasional bursts
    
    Args:
        base_sleep_time: Base sleep time between events
        
    Returns:
        Adjusted sleep time
    """
    # 10% chance of burst traffic (Black Friday, flash sales, etc.)
    if random.random() < 0.1:
        return base_sleep_time * 0.1  # 10x faster during bursts
    
    # 20% chance of slow traffic (off-peak hours)
    elif random.random() < 0.2:
        return base_sleep_time * 2.0  # 2x slower during off-peak
    
    return base_sleep_time

def main():
    """Main producer function"""
    # Configuration
    BROKER_ADDRESS = "localhost:9092"
    TOPIC_NAME = "trendflow-events"
    BASE_SLEEP_TIME = 1.0  # Base time between events in seconds
    
    # Create topic if it doesn't exist
    create_topic_if_not_exists(BROKER_ADDRESS, TOPIC_NAME)
    
    # Initialize Kafka producer with proper configuration
    producer = KafkaProducer(
        bootstrap_servers=BROKER_ADDRESS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        acks='all',  # Wait for all replicas to acknowledge
        retries=5,   # Retry failed sends
        batch_size=16384,  # Batch size for efficiency
        linger_ms=10,      # Small delay to allow batching
        compression_type='snappy'  # Compress messages
    )
    
    event_counter = 0
    events_produced = 0
    
    logging.info(f"Starting TrendFlow producer...")
    logging.info(f"Broker: {BROKER_ADDRESS}")
    logging.info(f"Topic: {TOPIC_NAME}")
    logging.info(f"Products in catalog: {len(products)}")
    logging.info(f"Users: {len(users)}")
    
    try:
        while True:
            # Generate event
            event = generate_event(event_counter)
            
            # Send to Kafka
            future = producer.send(
                topic=TOPIC_NAME,
                key=event["user_id"],
                value=event
            )
            
            # Optional: Wait for delivery confirmation
            try:
                record_metadata = future.get(timeout=10)
                logging.debug(f"Message sent to partition {record_metadata.partition} at offset {record_metadata.offset}")
            except Exception as e:
                logging.error(f"Failed to send message: {e}")
                continue
            
            events_produced += 1
            event_counter += 1
            
            # Logging
            if events_produced % 10 == 0:
                logging.info(f"Produced {events_produced} events. Latest: {event['event_type']} for {event['product_name']} by {event['user_id']}")
            else:
                logging.debug(f"Event produced: {event}")
            
            # Dynamic sleep time based on traffic simulation
            sleep_time = simulate_burst_traffic(BASE_SLEEP_TIME)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        logging.info(f"Producer stopped. Total events produced: {events_produced}")
    except Exception as e:
        logging.error(f"Producer error: {e}")
        raise
    finally:
        # Ensure all messages are sent before closing
        producer.flush()
        producer.close()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    main()