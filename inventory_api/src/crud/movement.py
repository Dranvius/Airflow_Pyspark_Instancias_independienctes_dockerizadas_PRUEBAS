from sqlalchemy.orm import Session
from typing import List

from .. import models, schemas

def create_movement_record(db: Session, movement: schemas.MovementCreate) -> models.Movement:
    """
    Crea un registro de un movimiento en la base de datos.
    
    Esta función sería llamada por el consumidor de eventos (Spark) después de
    procesar un evento de movimiento desde Kafka.
    """
    db_movement = models.Movement(
        product_id=movement.product_id,
        type=movement.type,
        quantity=movement.quantity,
        description=movement.description
    )
    db.add(db_movement)
    db.commit()
    db.refresh(db_movement)
    return db_movement

def get_movements_by_product_id(db: Session, product_id: int, skip: int = 0, limit: int = 100) -> List[models.Movement]:
    """
    Obtiene el historial de movimientos para un producto específico.
    """
    return db.query(models.Movement).filter(models.Movement.product_id == product_id).offset(skip).limit(limit).all()

def get_all_movements(db: Session, skip: int = 0, limit: int = 100) -> List[models.Movement]:
    """
    Obtiene una lista de todos los movimientos registrados.
    Útil para auditoría o para endpoints de lectura.
    """
    return db.query(models.Movement).offset(skip).limit(limit).all()
