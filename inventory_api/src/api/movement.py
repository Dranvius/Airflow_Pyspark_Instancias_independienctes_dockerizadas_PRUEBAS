from fastapi import APIRouter, Depends, HTTPException, status
from kafka import KafkaProducer
from sqlalchemy import text
import asyncio
import traceback

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
@router.post("", status_code=status.HTTP_202_ACCEPTED, include_in_schema=False)
def record_movement(
    movement: schemas.MovementCreate,
    producer: KafkaProducer = Depends(get_kafka_producer),
    topic_map: dict = Depends(get_topic_map)
):
    """
    Recibe un movimiento de inventario y lo publica en Kafka para ser procesado de forma asíncrona.
    Además dispara una alerta simple de bajo stock para salidas/ajustes pequeños via RabbitMQ.
    """
    kafka_ok = True

    if movement.type not in topic_map:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Tipo de movimiento '{movement.type}' no es válido."
        )

    topic = topic_map[movement.type]

    try:
        # 1) Intentar publicar en Kafka (no crítico)
        try:
            if producer is None:
                kafka_ok = False
            else:
                producer.send(topic, movement.model_dump())
                producer.flush()
        except Exception as e:
            kafka_ok = False
            # Continuamos con la actualización de stock y alerta aunque Kafka falle
            print(f"Kafka publish failed (movement): {e}")

        # 2) Actualizar stock directo en Postgres + registrar movimiento
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

            # Registrar movimiento en tabla movements (para el historial)
            db.execute(
                text(
                    """
                    INSERT INTO movements (product_id, type, quantity, timestamp, origin)
                    VALUES (:pid, :type, :qty, NOW(), :origin)
                    """
                ),
                {
                    "pid": movement.product_id,
                    "type": movement.type,
                    "qty": movement.quantity,
                    "origin": movement.description or "frontend",
                },
            )

            db.commit()

            if new_qty <= warning_level:
                try:
                    ok = publish_low_stock_alert(
                        product_id=movement.product_id,
                        current_quantity=new_qty,
                        source="movement_api",
                    )
                except Exception as exc:
                    ok = False
                    print(f"RabbitMQ publish exception: {exc}")

                if not ok:
                    try:
                        asyncio.run(
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
                        print(f"Fallback WS alerta movimiento {movement.product_id} falló: {exc}")

        return {
            "status": "success",
            "message": f"Movimiento '{movement.type}' aceptado",
            "data": movement.model_dump(),
            "kafka_published": kafka_ok,
        }
    except HTTPException:
        raise
    except Exception as e:
        print("Error procesando movimiento:\n", traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error procesando movimiento: {e}"
        )
