import pika
import json
import os
import threading
import time
import asyncio

from ..utils.email import send_email
# Importar el gestor de WebSockets
from ..websocket_manager import manager as websocket_manager

# --- RabbitMQ Connection Details ---
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
RABBITMQ_VHOST = os.environ.get("RABBITMQ_VHOST", "/app_vhost")
# --- Configuration ---
ALERT_QUEUE_NAME = 'low_stock_alerts'
ALERT_EMAIL_RECIPIENT = os.environ.get("ALERT_EMAIL_RECIPIENT") # Email para recibir las alertas

# Bucle de eventos de asyncio para ejecutar corutinas desde el hilo del consumidor
loop = None


def on_message_callback(channel, method, properties, body):
    """
    Función que se ejecuta cada vez que se recibe un mensaje.
    """
    print("--- Alerta de RabbitMQ recibida ---")
    try:
        message = json.loads(body)
        product_id = message.get("product_id")
        current_quantity = message.get("current_quantity")
        
        print(f"INFO: Alerta de bajo stock para producto {product_id}. Cantidad actual: {current_quantity}.")

        # --- Lógica para enviar a WebSocket (si hay clientes conectados) ---
        websocket_message = {
            "type": "low_stock_alert",
            "payload": message
        }
        # El gestor de websockets se encarga de convertir el dict a JSON

        # Ejecutamos la corutina de broadcast en el bucle de eventos de la aplicación principal
        if loop:
            asyncio.run_coroutine_threadsafe(
                websocket_manager.broadcast(websocket_message), loop
            )

        # --- Lógica para enviar email ---
        if ALERT_EMAIL_RECIPIENT:
            subject = f"Alerta de Bajo Stock para Producto ID: {product_id}"
            body_html = f"""
            <html>
            <body>
                <h1>Alerta de Inventario</h1>
                <p>El producto con ID <b>{product_id}</b> ha alcanzado un nivel de stock bajo.</p>
                <ul>
                    <li>Cantidad Actual: <b>{current_quantity}</b></li>
                    <li>Nivel de Alerta: <b>{message.get("warning_level", "No especificado")}</b></li>
                </ul>
                <p>Por favor, tome las acciones necesarias.</p>
            </body>
            </html>
            """
            send_email(
                recipient=ALERT_EMAIL_RECIPIENT,
                subject=subject,
                body=body_html
            )
        else:
            print("WARN: No se ha configurado la variable de entorno ALERT_EMAIL_RECIPIENT. No se enviará correo.")

        # Confirmamos que el mensaje ha sido procesado correctamente.
        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(f"INFO: Mensaje de alerta para producto {product_id} procesado y confirmado.")

    except json.JSONDecodeError:
        print("ERROR: Error al decodificar el mensaje JSON.")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"ERROR: Error procesando el mensaje: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)



def start_consuming():
    """
    Inicia el consumidor de RabbitMQ en un bucle infinito.
    Se reconectará si la conexión se pierde.
    """
    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                virtual_host=RABBITMQ_VHOST,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            # Declaramos la cola (es una operación idempotente)
            channel.queue_declare(queue=ALERT_QUEUE_NAME, durable=True)
            
            # Solo procesar un mensaje a la vez. RabbitMQ no enviará un nuevo mensaje
            # hasta que el actual sea confirmado (`basic_ack`).
            channel.basic_qos(prefetch_count=1)
            
            channel.basic_consume(
                queue=ALERT_QUEUE_NAME,
                on_message_callback=on_message_callback
            )

            print(f" consumidor de RabbitMQ iniciado. Escuchando en la cola '{ALERT_QUEUE_NAME}'...")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            print(" Conexión a RabbitMQ perdida. Reintentando en 5 segundos...")
            time.sleep(5)
        except Exception as e:
            print(f" Error inesperado en el consumidor: {e}. Reiniciando en 10 segundos...")
            time.sleep(10)

def start_consumer_in_background():
    """
    Lanza el consumidor en un hilo separado para no bloquear la aplicación principal.
    """
    global loop
    # Capturamos el bucle de eventos de asyncio del hilo principal (donde corre FastAPI)
    loop = asyncio.get_event_loop()

    consumer_thread = threading.Thread(target=start_consuming, daemon=True)
    consumer_thread.start()
    print(" Hilo del consumidor de RabbitMQ iniciado en segundo plano.")
