from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from datetime import datetime
from ..database import Base

class Movement(Base):
    __tablename__ = "movements"

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    type = Column(String(20))  # "entrada", "salida", "ajuste"
    quantity = Column(Integer, default=0)
    timestamp = Column(DateTime, default=datetime.utcnow)
    origin = Column(String(50), nullable=True)

    # Relación inversa
    product = relationship("Product", back_populates="movements")
