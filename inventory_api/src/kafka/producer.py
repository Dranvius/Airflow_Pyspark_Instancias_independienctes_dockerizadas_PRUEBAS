import os
import json
from kafka import KafkaProducer
from typing import Dict

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
producer = None

TOPIC_MAP: Dict[str, str] = {
    # Topics de Movimientos
    "entrada": os.getenv("KAFKA_TOPIC_ENTRADA", "inventory_entries"),
    "salida": os.getenv("KAFKA_TOPIC_SALIDA", "inventory_out"),
    "ajuste": os.getenv("KAFKA_TOPIC_AJUSTE", "inventory_adjustments"),
    
    # Topics de Productos
    "nuevo_producto": os.getenv("KAFKA_TOPIC_NUEVO_PRODUCTO", "inventory_new_products"),
    "producto_actualizado": os.getenv("KAFKA_TOPIC_PRODUCTO_ACTUALIZADO", "inventory_product_updates"),
    "producto_eliminado": os.getenv("KAFKA_TOPIC_PRODUCTO_ELIMINADO", "inventory_product_deletes")
}

def get_kafka_producer():
    """
    Retorna una instancia única del productor de Kafka.
    La crea si no existe.
    """
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("✅ Conectado a Kafka correctamente")
        except Exception as e:
            print(f"❌ Error conectando con Kafka: {e}")
            # En un caso real, podrías querer que la app falle si no puede conectar.
            # Por ahora, simplemente no estará disponible.
            producer = None 
    return producer

def get_topic_map() -> Dict[str, str]:
    """Retorna el diccionario de topics."""
    return TOPIC_MAP
