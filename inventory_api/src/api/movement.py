from fastapi import APIRouter, Depends, HTTPException, status
from kafka import KafkaProducer
from sqlalchemy import text
import asyncio

from .. import schemas
from ..kafka.producer import get_kafka_producer, get_topic_map
from ..rabbitmq.producer import publish_low_stock_alert
from ..database import SessionLocal
from ..websocket_manager import manager as websocket_manager

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

        # Actualizar stock directo en Postgres para reflejarse en el dashboard
        with SessionLocal() as db:
            product_row = db.execute(
                text("SELECT quantity FROM products WHERE id = :pid"),
                {"pid": movement.product_id},
            ).mappings().first()
            if not product_row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Producto {movement.product_id} no encontrado.",
                )

            current_qty = product_row["quantity"] or 0
            delta = movement.quantity if movement.type in ("entrada", "ajuste") else -movement.quantity
            new_qty = current_qty + delta

            # No permitir negativos
            if new_qty < 0:
                new_qty = 0

            db.execute(
                text("UPDATE products SET quantity = :qty WHERE id = :pid"),
                {"qty": new_qty, "pid": movement.product_id},
            )

            inv_row = db.execute(
                text("SELECT id, stock_warning_level FROM inventory WHERE product_id = :pid"),
                {"pid": movement.product_id},
            ).mappings().first()

            warning_level = inv_row["stock_warning_level"] if inv_row else 10

            if inv_row:
                db.execute(
                    text("UPDATE inventory SET quantity = :qty WHERE id = :iid"),
                    {"qty": new_qty, "iid": inv_row["id"]},
                )
            else:
                db.execute(
                    text(
                        """
                        INSERT INTO inventory (product_id, quantity, stock_warning_level)
                        VALUES (:pid, :qty, :warn)
                        """
                    ),
                    {"pid": movement.product_id, "qty": new_qty, "warn": warning_level},
                )

            db.commit()

            if new_qty <= warning_level:
                ok = publish_low_stock_alert(
                    product_id=movement.product_id,
                    current_quantity=new_qty,
                    source="movement_api",
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
                                            "product_id": movement.product_id,
                                            "current_quantity": new_qty,
                                            "source": "movement_api_fallback",
                                        },
                                    }
                                )
                            )
                    except Exception as exc:
                        print(f"Fallback WS alerta movimiento {movement.product_id} fallo: {exc}")

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
