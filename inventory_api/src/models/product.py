from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.orm import relationship

from ..database import Base

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False, index=True)
    description = Column(String(255), nullable=True)
    price = Column(Float, nullable=True)  # O Numeric para mayor precisión
    sku = Column(String(50), nullable=True, unique=True)
    category = Column(String(50), nullable=True, index=True)
    stock_min = Column(Integer, default=10)
    stock_max = Column(Integer, default=100)

    # Relaciones
    inventory = relationship("Inventory", back_populates="product", uselist=False)
    movements = relationship("Movement", back_populates="product")

