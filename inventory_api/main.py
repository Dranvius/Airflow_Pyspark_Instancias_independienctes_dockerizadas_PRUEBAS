from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import os
import psycopg2

# Importar los nuevos routers
from src.api import movement, product, uploads
from src.api import inventory_read
from src.websocket_manager import manager as websocket_manager # Importamos el gestor de WebSockets
from src.rabbitmq import consumer as rabbitmq_consumer
from src.kafka.producer import get_kafka_producer
from src.database import Base, engine
from src import models

# -----------------------------
# Configuración CORS y FastAPI
# -----------------------------
app = FastAPI(
    title="Inventory API",
    description="API para gestionar eventos de inventario y publicarlos en Kafka.",
    version="1.0.0"
)

origins = [
    "http://localhost:3000",  # Puerto estándar de React
    "http://localhost:3001",  # Puerto alternativo común
    "http://localhost:3002",  # Otro puerto alternativo
    "http://react-app:3000"   # Para comunicación entre contenedores
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Incluir Routers de la API
# -----------------------------
# Se añade el router que maneja los movimientos de inventario
app.include_router(movement.router)
# Se añade el router que maneja los productos
app.include_router(product.router)
# Se añade el router que maneja la subida de archivos
app.include_router(uploads.router)
# Lectura consolidada simple
app.include_router(inventory_read.router)

# -----------------------------
# Endpoint de WebSocket para notificaciones
# -----------------------------
@app.websocket("/ws/updates")
async def websocket_endpoint(websocket: WebSocket):
    """
    Endpoint para que el frontend se conecte y reciba notificaciones en tiempo real.
    """
    await websocket_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text() # Mantiene la conexión viva
    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket)

# -----------------------------
# Eventos de Inicio de la Aplicación
# -----------------------------
@app.on_event("startup")
def startup_event():
    # Verificar conexión a PostgreSQL
    test_postgres_connection()

    # Crear tablas si no existen (productos, inventario, movimientos)
    Base.metadata.create_all(bind=engine)
    
    # Inicializar productor de Kafka para asegurar conectividad
    get_kafka_producer()

    # Iniciar consumidor de RabbitMQ en segundo plano
    rabbitmq_consumer.start_consumer_in_background()

# -----------------------------
# PostgreSQL Connection Test (Opcional, para health check)
# -----------------------------
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sergio")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123")

def test_postgres_connection():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.fetchone()
        cur.close()
        conn.close()
        print("✅ Conexión a PostgreSQL verificada con éxito!")
    except Exception as e:
        print(f"❌ Error conectando a PostgreSQL: {e}")

# -----------------------------
# Endpoint de prueba de conexión
# -----------------------------
@app.get("/api/ping")
def ping():
    return {"message": "Backend conectado correctamente"}
