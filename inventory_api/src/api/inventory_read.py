from fastapi import APIRouter
from sqlalchemy import text
from ..database import SessionLocal

router = APIRouter(
    prefix="/inventory",
    tags=["Inventory"]
)

@router.get("/", description="Lectura consolidada simple desde Postgres (tabla products)")
def get_inventory():
    with SessionLocal() as db:
        rows = db.execute(
            text("""
            SELECT
              id,
              name,
              quantity,
              price,
              NULL::text AS description,
              NULL::text AS sku
            FROM products
            ORDER BY id
            """)
        ).mappings().all()
        return [dict(row) for row in rows]
