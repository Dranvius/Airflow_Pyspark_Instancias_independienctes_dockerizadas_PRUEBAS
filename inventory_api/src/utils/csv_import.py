import csv
from kafka import KafkaProducer
from typing import Dict, Any

from ..kafka.producer import TOPIC_MAP

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
                    
                    producer.send(topic, event_data)
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
