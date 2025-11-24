from pydantic import BaseModel
from typing import Literal
from datetime import datetime

class MovementBase(BaseModel):
    product_id: int
    quantity: int
    # El tipo de movimiento: entrada, salida, o ajuste.
    type: Literal['entrada', 'salida', 'ajuste']
    description: str | None = None

class MovementCreate(MovementBase):
    # Este modelo se usa para la creación de un movimiento desde la API.
    # No hay campos adicionales, hereda todo de MovementBase.
    pass

class Movement(MovementBase):
    # Este modelo representa un movimiento completo, como se almacenaría en la BD.
    id: int
    timestamp: datetime

    class Config:
        # Habilita el modo ORM para que Pydantic pueda leer datos desde objetos de SQLAlchemy
        from_attributes = True
