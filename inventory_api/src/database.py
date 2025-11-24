import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# --- Configuración de la Conexión a la Base de Datos ---
# Se leen las variables de entorno para construir la URL de conexión
POSTGRES_USER = os.getenv("POSTGRES_USER", "sergio")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")

SQLALCHEMY_DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# --- Creación del Engine y la Sesión de SQLAlchemy ---

# El engine es el punto de entrada a la base de datos.
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# Cada instancia de SessionLocal será una sesión de base de datos.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base declarativa que usarán nuestros modelos de SQLAlchemy.
Base = declarative_base()

# --- Dependencia para obtener la sesión de la DB ---
def get_db():
    """
    Generador de dependencia de FastAPI que provee una sesión de SQLAlchemy.
    Asegura que la sesión se cierre siempre después de cada petición.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
