from fastapi import APIRouter, Depends, HTTPException, status
from kafka import KafkaProducer

from .. import schemas
from ..kafka.producer import get_kafka_producer, get_topic_map
from ..rabbitmq.producer import publish_low_stock_alert

router = APIRouter(
    prefix="/movements",
    tags=["Movements"]
)


@router.post("/", status_code=status.HTTP_202_ACCEPTED)
def record_movement(
    movement: schemas.MovementCreate,
    producer: KafkaProducer = Depends(get_kafka_producer),
    topic_map: dict = Depends(get_topic_map)
):
    """
    Recibe un movimiento de inventario y lo publica en Kafka para ser procesado de forma asíncrona.
    Además dispara una alerta simple de bajo stock para salidas/ajustes pequeños via RabbitMQ.
    """
    if producer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de Kafka no disponible."
        )

    if movement.type not in topic_map:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Tipo de movimiento '{movement.type}' no es válido."
        )

    topic = topic_map[movement.type]

    try:
        producer.send(topic, movement.model_dump())
        producer.flush()

        # Alerta de stock bajo (simplificada): salidas/ajustes con cantidad <= 10.
        if movement.type in ("salida", "ajuste") and movement.quantity <= 10:
            publish_low_stock_alert(
                product_id=movement.product_id,
                current_quantity=movement.quantity,
                source="movement_api"
            )

        return {
            "status": "success",
            "message": f"Movimiento '{movement.type}' aceptado y enviado para procesamiento.",
            "data": movement.model_dump()
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al publicar evento en Kafka: {e}"
        )
