import csv
import json
from kafka import KafkaProducer
from typing import Dict, Any

# Importamos TOPIC_MAP desde un módulo centralizado de Kafka
try:
    from src.kafka_producer import TOPIC_MAP  # Ajusta la ruta si no funciona
except ImportError:
    # Definimos un TOPIC_MAP de respaldo para evitar fallos de importación
    TOPIC_MAP = {
        "entrada": "movement_entrada",
        "salida": "movement_salida",
        "ajuste": "movement_ajuste"
    }

def process_inventory_csv(file_path: str, producer: KafkaProducer) -> Dict[str, Any]:
    """
    Procesa un archivo CSV de movimientos de inventario y los publica en Kafka.
    """
    if not producer:
        return {"status": "error", "message": "Productor de Kafka no disponible."}

    processed_rows = 0
    successful_rows = 0
    failed_rows = []

    try:
        with open(file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                processed_rows += 1
                try:
                    product_id = int(row['product_id'])
                    quantity = int(row['quantity'])
                    movement_type = row['type'].lower()

                    if movement_type not in TOPIC_MAP:
                        raise ValueError(f"Tipo de movimiento '{movement_type}' no es válido.")

                    topic = TOPIC_MAP[movement_type]
                    event_data = {
                        "product_id": product_id,
                        "quantity": quantity,
                        "type": movement_type,
                        "source": "csv_import"
                    }
                    
                    producer.send(topic, json.dumps(event_data).encode('utf-8'))
                    successful_rows += 1

                except (KeyError, ValueError, TypeError) as e:
                    failed_rows.append({
                        "row_number": processed_rows,
                        "data": row,
                        "error": str(e)
                    })
                
        producer.flush()
        
        summary = {
            "status": "completed",
            "total_rows": processed_rows,
            "successful_rows": successful_rows,
            "failed_rows_count": len(failed_rows),
            "failed_rows_details": failed_rows
        }
        print(f"✅ Procesamiento de CSV completado: {summary}")
        return summary

    except FileNotFoundError:
        return {"status": "error", "message": "El archivo no fue encontrado."}
    except Exception as e:
        return {"status": "error", "message": f"Error inesperado al procesar el archivo: {e}"}
