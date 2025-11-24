from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from kafka import KafkaProducer
import tempfile
import os

from ..kafka.producer import get_kafka_producer
from ..utils.csv_import import process_inventory_csv

router = APIRouter(
    prefix="/uploads",
    tags=["Uploads"]
)

@router.post("/csv/inventory", status_code=status.HTTP_202_ACCEPTED)
async def upload_inventory_csv(
    file: UploadFile = File(...),
    producer: KafkaProducer = Depends(get_kafka_producer)
):
    """
    Endpoint para subir un archivo CSV con movimientos de inventario.
    El archivo es procesado y sus filas son enviadas como eventos a Kafka.
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El archivo debe tener extensión .csv")

    # --- Guardar el archivo en un directorio temporal ---
    # Usar tempfile para manejar archivos temporales de forma segura
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="wb") as tmp:
            # Escribir el contenido del archivo subido al archivo temporal
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        # --- Procesar el archivo CSV ---
        # La función de procesamiento ahora se ejecuta de forma síncrona
        # En una app de producción, esto debería ser una tarea en segundo plano (p.ej. con Celery)
        # para no bloquear el servidor si el archivo es muy grande.
        result = process_inventory_csv(tmp_path, producer)

    finally:
        # --- Limpiar el archivo temporal ---
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            os.remove(tmp_path)
            
    if result.get("status") == "error":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ocurrió un error al procesar el archivo: {result.get('message')}"
        )

    return result
