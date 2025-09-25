from kafka import KafkaProducer
import json, time, random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

products = [
    {"product_id": 1, "category": "Electronics", "price": 500},
    {"product_id": 2, "category": "Clothing", "price": 300},
    {"product_id": 3, "category": "Books", "price": 100},
]

while True:
    sale = {
        "order_id": random.randint(1000, 9999),
        "customer_id": random.randint(100, 999),
        "product_id": random.choice(products)["product_id"],
        "quantity": random.randint(1, 5),
        "price": random.choice(products)["price"],
        "order_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    producer.send("sales", sale)
    print("Produced:", sale)
    time.sleep(2)  # cada 2 segundos envía una venta
