from fastapi import APIRouter, Depends, HTTPException, status
from kafka import KafkaProducer
from typing import List

# Importamos los schemas y las dependencias del productor de Kafka desde la ruta correcta
from .. import schemas
from ..kafka.producer import get_kafka_producer, get_topic_map

router = APIRouter(
    prefix="/movements",
    tags=["Movements"]
)

@router.post("/", status_code=status.HTTP_202_ACCEPTED)
def record_movement(
    movement: schemas.MovementCreate, 
    # Inyectamos el productor y el mapa de topics como dependencias
    producer: KafkaProducer = Depends(get_kafka_producer),
    topic_map: dict = Depends(get_topic_map)
):
    """
    Recibe un movimiento de inventario y lo publica en Kafka para ser procesado de forma asíncrona.
    """
    # 1. Validar que el productor de Kafka esté disponible
    if producer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de Kafka no disponible."
        )

    # 2. Validar el tipo de movimiento y obtener el topic correspondiente
    if movement.type not in topic_map:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail=f"Tipo de movimiento '{movement.type}' no es válido."
        )
    
    topic = topic_map[movement.type]
    
    # 3. Enviar el evento a Kafka
    try:
        producer.send(topic, movement.model_dump())
        producer.flush() # Forzar el envío inmediato (en prod, podría ser diferente)
        
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

# @router.get("/", response_model=List[schemas.Movement])
# def read_movements(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
#     """
#     NOTA: Este endpoint se deshabilita temporalmente.
#     En una arquitectura de event sourcing, la lectura de datos no debe provenir 
#     de la misma base de datos de escritura transaccional de la API.
#     Las lecturas deben realizarse contra la base de datos consolidada que es 
#     actualizada por el consumidor de streaming (Spark).
#     Este endpoint podría pertenecer a otro servicio o leer de dicha base de datos.
#     """
#     # movements = db.query(Movement).offset(skip).limit(limit).all()
#     # return movements
#     raise HTTPException(
#         status_code=status.HTTP_501_NOT_IMPLEMENTED,
#         detail="Endpoint no implementado en esta arquitectura. Los datos se leen desde el datamart consolidado."
#     )