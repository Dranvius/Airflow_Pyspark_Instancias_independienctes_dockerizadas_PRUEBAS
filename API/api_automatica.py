from kafka import KafkaProducer
import json, time, random
from datetime import datetime

# Configuración del producer
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",  # Cambia a "kafka:9092" si estás en Docker
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Datos base
products = [
    {"product_id": 1, "category": "Electronics", "price": 500},
    {"product_id": 2, "category": "Clothing", "price": 300},
    {"product_id": 3, "category": "Books", "price": 100},
]

first_names = ["Juan", "Maria", "Pedro", "Luisa", "Carlos", "Ana", "Sergio", "Camila"]
last_names = ["Gomez", "Rodriguez", "Martinez", "Fernandez", "Linares", "Lopez"]

# Loop infinito
while True:
    # Generar cliente aleatorio
    cliente_id = random.randint(100, 999)
    cliente = {
        "cliente_id": cliente_id,
        "nombre": f"{random.choice(first_names)} {random.choice(last_names)}",
        "email": f"user{random.randint(1000,9999)}@example.com",
        "telefono": f"+57{random.randint(3000000000, 3999999999)}",
        "fecha_registro": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    producer.send("cliente", cliente)
    print("Produced (cliente):", cliente)

    # Generar entre 1 y 3 ventas para ese cliente
    for _ in range(random.randint(1, 3)):
        producto = random.choice(products)
        sale = {
            "order_id": random.randint(1000, 9999),
            "customer_id": cliente_id,
            "product_id": producto["product_id"],
            "quantity": random.randint(1, 5),
            "price": producto["price"],
            "order_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        producer.send("sales", sale)
        print("Produced (sales):", sale)

    time.sleep(2)  # Espera antes de generar otro cliente con sus ventas
