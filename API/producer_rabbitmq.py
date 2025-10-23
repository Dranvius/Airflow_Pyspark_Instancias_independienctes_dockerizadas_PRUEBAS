import asyncio
import aio_pika
import json
import random
from datetime import datetime

RABBITMQ_URL = "amqp://airflow:airflow123@rabbitmq:5672/%2Fapp_vhost"

products = [
    {"product_id": 1, "category": "Electronics", "price": 500},
    {"product_id": 2, "category": "Clothing", "price": 300},
    {"product_id": 3, "category": "Books", "price": 100},
]

first_names = ["Juan", "Maria", "Pedro", "Luisa", "Carlos", "Ana", "Sergio", "Camila"]
last_names = ["Gomez", "Rodriguez", "Martinez", "Fernandez", "Linares", "Lopez"]


async def get_or_declare_exchange(channel, name, type_):
    """
    Intenta obtener un exchange existente de forma pasiva.
    Si no existe, lo crea con los parámetros correctos.
    """
    try:
        return await channel.declare_exchange(name, passive=True)
    except aio_pika.exceptions.ChannelClosed:
        # Si el exchange no existe o hay conflicto, lo recreamos limpio
        print(f"⚠️ Exchange '{name}' no existe o está mal configurado. Creando nuevamente...")
        channel = await channel.connection.channel()  # Reabrir canal limpio
        return await channel.declare_exchange(name, type_, durable=True, auto_delete=False)


async def produce_messages():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()

    # 📦 Declaración de exchanges
    exchange_direct = await get_or_declare_exchange(channel, "exchange_direct_app", aio_pika.ExchangeType.DIRECT)
    exchange_topic = await get_or_declare_exchange(channel, "exchange_topic_app", aio_pika.ExchangeType.TOPIC)
    exchange_fanout = await get_or_declare_exchange(channel, "exchange_fanout_app", aio_pika.ExchangeType.FANOUT)

    print("✅ Conectado a RabbitMQ y exchanges listos.\n")

    while True:
        # ====== CLIENTES ======
        cliente_id = random.randint(100, 999)
        cliente = {
            "cliente_id": cliente_id,
            "nombre": f"{random.choice(first_names)} {random.choice(last_names)}",
            "email": f"user{random.randint(1000,9999)}@example.com",
            "telefono": f"+57{random.randint(3000000000, 3999999999)}",
            "fecha_registro": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        await exchange_direct.publish(
            aio_pika.Message(body=json.dumps(cliente).encode()),
            routing_key="cliente.created",
        )
        print(f"📤 [Direct] Sent cliente.created → {cliente}")

        # ====== ÓRDENES ======
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

            await exchange_direct.publish(
                aio_pika.Message(body=json.dumps(sale).encode()),
                routing_key="order.created",
            )
            print(f"📤 [Direct] Sent order.created → {sale}")

        # ====== AUDITORÍA (TOPIC) ======
        event = {
            "event_id": random.randint(10000, 99999),
            "event_type": random.choice(["login", "purchase", "logout"]),
            "user_id": cliente_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        await exchange_topic.publish(
            aio_pika.Message(body=json.dumps(event).encode()),
            routing_key=f"audit.{event['event_type']}",
        )
        print(f"📤 [Topic] Sent audit.{event['event_type']} → {event}")

        # ====== LOGS (FANOUT) ======
        log = {
            "log_id": random.randint(100000, 999999),
            "level": random.choice(["INFO", "WARNING", "ERROR"]),
            "message": f"Log de ejemplo generado por el cliente {cliente_id}",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        await exchange_fanout.publish(
            aio_pika.Message(body=json.dumps(log).encode()),
            routing_key=""  # <--- necesario aunque no se use
        )
        print(f"📤 [Fanout] Sent log → {log}\n")

        await asyncio.sleep(3)


if __name__ == "__main__":
    try:
        asyncio.run(produce_messages())
    except KeyboardInterrupt:
        print("🛑 Productor detenido manualmente.")
