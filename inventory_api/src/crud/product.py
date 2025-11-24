from sqlalchemy.orm import Session
from typing import List

from .. import models, schemas
from . import inventory as crud_inventory

def create_product(db: Session, product: schemas.ProductCreate) -> models.Product:
    """
    Crea un registro de producto en la base de datos.
    Además, inicializa su registro de inventario correspondiente.
    """
    # Crear el producto
    db_product = models.Product(**product.model_dump())
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    
    # Crear el registro de inventario inicial para este producto
    crud_inventory.create_inventory_record(db, product_id=db_product.id, quantity=0)
    
    return db_product

def get_product(db: Session, product_id: int) -> models.Product | None:
    """
    Busca un producto por su ID.
    """
    return db.query(models.Product).filter(models.Product.id == product_id).first()

def get_product_by_sku(db: Session, sku: str) -> models.Product | None:
    """
    Busca un producto por su SKU.
    """
    return db.query(models.Product).filter(models.Product.sku == sku).first()

def get_all_products(db: Session, skip: int = 0, limit: int = 100) -> List[models.Product]:
    """
    Obtiene una lista de todos los productos.
    """
    return db.query(models.Product).offset(skip).limit(limit).all()

def update_product(db: Session, product_id: int, product_update: schemas.ProductCreate) -> models.Product | None:
    """
    Actualiza los datos de un producto existente.
    """
    db_product = get_product(db, product_id=product_id)
    if not db_product:
        return None
        
    update_data = product_update.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_product, key, value)
        
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    return db_product

def delete_product(db: Session, product_id: int) -> bool:
    """
    Elimina un producto de la base de datos.
    También debería manejar la eliminación de registros relacionados si la lógica de negocio lo requiere.
    """
    db_product = get_product(db, product_id=product_id)
    if not db_product:
        return False
        
    # Opcional: Eliminar también el inventario asociado
    db_inventory = crud_inventory.get_inventory_by_product_id(db, product_id=product_id)
    if db_inventory:
        db.delete(db_inventory)
        
    db.delete(db_product)
    db.commit()
    return True
