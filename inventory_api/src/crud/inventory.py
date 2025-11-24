from sqlalchemy.orm import Session
from .. import models, schemas

def get_inventory_by_product_id(db: Session, product_id: int):
    """
    Busca un registro de inventario por el ID del producto.
    """
    return db.query(models.Inventory).filter(models.Inventory.product_id == product_id).first()

def create_inventory_record(db: Session, product_id: int, quantity: int = 0, stock_warning_level: int = 10):
    """
    Crea un nuevo registro de inventario para un producto.
    Esto se usaría, por ejemplo, cuando un 'nuevo_producto' es consumido.
    """
    db_inventory = models.Inventory(
        product_id=product_id,
        quantity=quantity,
        stock_warning_level=stock_warning_level
    )
    db.add(db_inventory)
    db.commit()
    db.refresh(db_inventory)
    return db_inventory

def update_inventory_quantity(db: Session, product_id: int, quantity_change: int):
    """
    Actualiza la cantidad de un producto en el inventario.
    Esta función sería llamada por el consumidor de Spark al procesar
    eventos de 'entrada', 'salida' o 'ajuste'.
    
    `quantity_change` puede ser positivo (entrada) o negativo (salida).
    """
    db_inventory = get_inventory_by_product_id(db, product_id=product_id)
    
    if not db_inventory:
        # Opcional: Si no existe, crearlo. Depende de la lógica de negocio.
        # Por ahora, asumimos que debe existir.
        return None

    # Realizar el ajuste de inventario
    db_inventory.quantity += quantity_change
    
    db.commit()
    db.refresh(db_inventory)
    
    # Comprobar si el stock está por debajo del nivel de alerta
    if db_inventory.quantity < db_inventory.stock_warning_level:
        # Aquí es donde se podría disparar la lógica para enviar una alerta a RabbitMQ
        print(f"ALERTA: Stock bajo para el producto {product_id}. Cantidad actual: {db_inventory.quantity}")
        # En el proyecto real, aquí iría la llamada al productor de RabbitMQ.
        
    return db_inventory

def update_stock_warning_level(db: Session, product_id: int, new_level: int):
    """
    Actualiza el nivel de alerta de stock para un producto.
    """
    db_inventory = get_inventory_by_product_id(db, product_id=product_id)
    if not db_inventory:
        return None
        
    db_inventory.stock_warning_level = new_level
    db.commit()
    db.refresh(db_inventory)
    return db_inventory
