from pydantic import BaseModel

class ProductBase(BaseModel):
    name: str
    description: str | None = None
    price: float
    sku: str | None = None

class ProductCreate(ProductBase):
    # Para crear un producto, la información base es suficiente.
    pass

class Product(ProductBase):
    id: int

    class Config:
        from_attributes = True
