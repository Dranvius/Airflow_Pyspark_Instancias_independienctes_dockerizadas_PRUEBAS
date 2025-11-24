from fastapi import FastAPI, WebSocket
import threading
import pika
import asyncio

app = FastAPI()
clients = []

@app.websocket("/ws/alerts")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.append(ws)
    try:
        while True:
            await ws.receive_text()  # Mantener conexión
    finally:
        clients.remove(ws)

def rabbit_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    channel.queue_declare(queue="low_stock_alerts", durable=True)

    def callback(ch, method, properties, body):
        message = body.decode()
        # Enviar mensaje a todos los websockets conectados
        for ws in clients:
            asyncio.run(ws.send_text(message))

    channel.basic_consume(queue="low_stock_alerts", on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

threading.Thread(target=rabbit_consumer, daemon=True).start()
