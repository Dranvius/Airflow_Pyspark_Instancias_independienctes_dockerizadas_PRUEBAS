from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from .. import schemas 
from ..models import Inventory 
from ..database import get_db 

router = APIRouter(
    prefix="/inventory", 
    tags=["Inventory"]
)

@router.get("/", response_model=List[schemas.Inventory])
def read_all_inventory(db: Session = Depends(get_db)):
    """Lista el stock actual de todos los productos."""
    inventory_items = db.query(Inventory).all()
    return inventory_items

@router.get("/{product_id}", response_model=schemas.Inventory)
def read_inventory_by_product(product_id: int, db: Session = Depends(get_db)):
    """Consulta el stock actual de un producto específico."""
    inventory = db.query(Inventory).filter(
        Inventory.product_id == product_id
    ).first()
    
    if not inventory:
        raise HTTPException(status_code=404, detail="Inventario o Producto no encontrado")
        
    return inventory

# Opcional: Endpoint para obtener movimientos de un producto específico
@router.get("/product/{product_id}/movements", response_model=List[schemas.Movement])
def read_product_movements(product_id: int, db: Session = Depends(get_db)):
    """Lista los movimientos históricos de un producto específico."""
    # Accede a la relación 'movements' del modelo 'Product' a través del modelo 'Movement'
    movements = db.query(Movement).filter(Movement.product_id == product_id).all()
    return movements