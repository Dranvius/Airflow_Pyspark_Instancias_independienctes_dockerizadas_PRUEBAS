from fastapi import APIRouter, Depends, HTTPException, status
from kafka import KafkaProducer
from typing import List

from .. import schemas
from ..kafka.producer import get_kafka_producer, TOPIC_MAP

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
@router.get("/", response_model=List[schemas.Product], description="Deshabilitado en esta arquitectura.")
def list_products():
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="La lectura de datos se realiza desde el datamart consolidado, no directamente desde la API de eventos.")

@router.get("/{product_id}", response_model=schemas.Product, description="Deshabilitado en esta arquitectura.")
def get_product(product_id: int):
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="La lectura de datos se realiza desde el datamart consolidado, no directamente desde la API de eventos.")
