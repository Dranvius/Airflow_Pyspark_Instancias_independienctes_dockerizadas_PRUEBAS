from pydantic import BaseModel
from datetime import datetime

class InventoryBase(BaseModel):
    product_id: int
    quantity: int
    # Nivel mínimo de stock antes de generar una alerta
    stock_warning_level: int = 10

class InventoryCreate(InventoryBase):
    # Se utiliza si se necesita crear un registro de inventario manualmente.
    pass

class Inventory(InventoryBase):
    id: int
    last_updated: datetime

    class Config:
        from_attributes = True
