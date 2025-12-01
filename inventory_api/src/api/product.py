from fastapi import APIRouter, Depends, HTTPException, status
from kafka import KafkaProducer
from typing import List
import asyncio

from sqlalchemy import text
from .. import schemas
from ..kafka.producer import get_kafka_producer, TOPIC_MAP
from ..database import SessionLocal
from ..rabbitmq.producer import publish_low_stock_alert
from ..websocket_manager import manager as websocket_manager

router = APIRouter(
    prefix="/products",
    tags=["Products"]
)

# -----------------------------
# Crear producto (Publicar evento)
# -----------------------------
@router.post("/", status_code=status.HTTP_202_ACCEPTED)
def create_product(
    product: schemas.ProductCreate, 
    producer: KafkaProducer = Depends(get_kafka_producer)
):
    """
    Recibe los datos de un nuevo producto y publica un evento 'nuevo_producto' en Kafka.
    """
    if producer is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Servicio de Kafka no disponible.")

    topic = TOPIC_MAP["nuevo_producto"]
    
    try:
        producer.send(topic, product.model_dump())
        producer.flush()
        return {
            "status": "success",
            "message": "Solicitud para crear nuevo producto recibida.",
            "data": product.model_dump()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error publicando evento en Kafka: {e}")

# -----------------------------
# Actualizar producto (Publicar evento)
# -----------------------------
@router.put("/{product_id}", status_code=status.HTTP_202_ACCEPTED)
def update_product(
    product_id: int, 
    update_data: schemas.ProductCreate, 
    producer: KafkaProducer = Depends(get_kafka_producer)
):
    """
    Recibe datos de actualización y publica un evento 'producto_actualizado' en Kafka.
    """
    if producer is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Servicio de Kafka no disponible.")

    topic = TOPIC_MAP["producto_actualizado"]
    event_data = update_data.model_dump()
    event_data["id"] = product_id
    
    try:
        producer.send(topic, event_data)
        producer.flush()
        return {
            "status": "success",
            "message": f"Solicitud para actualizar producto {product_id} recibida.",
            "data": event_data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error publicando evento en Kafka: {e}")

# -----------------------------
# Eliminar producto (Publicar evento)
# -----------------------------
@router.delete("/{product_id}", status_code=status.HTTP_202_ACCEPTED)
def delete_product(
    product_id: int, 
    producer: KafkaProducer = Depends(get_kafka_producer)
):
    """
    Recibe un ID de producto y publica un evento 'producto_eliminado' en Kafka.
    """
    if producer is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Servicio de Kafka no disponible.")

    topic = TOPIC_MAP["producto_eliminado"]
    event_data = {"id": product_id}
    
    try:
        producer.send(topic, event_data)
        producer.flush()
        return {
            "status": "success",
            "message": f"Solicitud para eliminar producto {product_id} recibida."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error publicando evento en Kafka: {e}")

# -----------------------------
# Listar/Obtener productos (Deshabilitados)
# -----------------------------
@router.get("/", description="Devuelve el catálogo desde Postgres")
def list_products():
    """
    Lectura directa desde Postgres para poblar el frontend.
    """
    with SessionLocal() as db:
        rows = db.execute(
            text("""
            SELECT
              p.id,
              p.name,
              p.quantity,
              p.price,
              COALESCE(i.stock_warning_level, 10) AS stock_warning_level,
              NULL::text AS description,
              NULL::text AS sku
            FROM products p
            LEFT JOIN inventory i ON i.product_id = p.id
            ORDER BY p.id
            """)
        ).mappings().all()
        result = []
        for row in rows:
            payload = dict(row)
            result.append(payload)
            # Emitir alerta si ya está en nivel de aviso al momento de leer
            if payload["quantity"] is not None and payload["quantity"] <= payload["stock_warning_level"]:
                ok = publish_low_stock_alert(
                    product_id=payload["id"],
                    current_quantity=payload["quantity"],
                    source="read_api",
                )
                if not ok:
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            loop.create_task(
                                websocket_manager.broadcast(
                                    {
                                        "type": "low_stock_alert",
                                        "payload": {
                                            "product_id": payload["id"],
                                            "current_quantity": payload["quantity"],
                                            "source": "read_api_fallback",
                                        },
                                    }
                                )
                            )
                    except Exception as exc:
                        print(f"Fallback WS alerta producto {payload['id']} falló: {exc}")
        return result

@router.get("/{product_id}", description="Devuelve un producto por id desde Postgres")
def get_product(product_id: int):
    with SessionLocal() as db:
        row = db.execute(
            text("""
            SELECT
              id,
              name,
              quantity,
              price,
              NULL::text AS description,
              NULL::text AS sku
            FROM products
            WHERE id = :pid
            """),
            {"pid": product_id}
        ).mappings().first()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Producto no encontrado")
        return dict(row)
