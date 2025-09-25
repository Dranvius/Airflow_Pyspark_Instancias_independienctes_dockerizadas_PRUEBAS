import requests
import sqlite3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

DB_FILE = "/opt/airflow/dags/bitcoin.db"

# Extraccion
# Diferentes fuentes
def extract():

    #clima
    API_KEY = "0763719b1cef422a8e773521251109"
    BASE_URL = "http://api.weatherapi.com/v1/history.json"

    # Ciudades clave
    cities = ["New York", "Chicago", "Boston", "San Francisco", "Los Angeles", "Houston", "Miami"]

    # Fecha de ejemplo
    date = "2025-09-01"

    for city in cities:
        params = {
            "key": API_KEY,
            "q": city,
            "dt": date
        }
        response = requests.get(BASE_URL, params=params)
        data = response.json()

        print(f"=== {city} ===")
        print("Fecha:", data["forecast"]["forecastday"][0]["date"])
        print("Temp promedio (°C):", data["forecast"]["forecastday"][0]["day"]["avgtemp_c"])
        print("Condición:", data["forecast"]["forecastday"][0]["day"]["condition"]["text"])
        print()



    url = "https://api.coindesk.com/v1/bpi/currentprice.json"
    response = requests.get(url)
    data = response.json()

    return data  # se pasa por XCom


# Transformacion
# Limpieza
def transform( ti ):

    #Objetivo solo devolver las columas que nosotros necesitamos
    #Venta
    #Fecha (Formateada)
    #Bajo
    #Alto 
    #Nombre de accion (apple o otra)

    #Clima en las principales zonas.
    #Verificar si existen problemas relacionados al clima

    #Extraer los datos de la tarea realizada con el ID "extract"
    data = ti.xcom_pull(task_ids="extract")

    #Tomar la data
    price_usd = data["bpi"]["USD"]["rate_float"]
    time_updated = data["time"]["updatedISO"]

    #Devuelve la data
    return {"time": time_updated, "price": price_usd}

# Carga de datos en una base de datos
def load(ti):
    record = ti.xcom_pull(task_ids="transform")

    #Conectar a la base de datos y crear un cursor
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    #Crear una tabla si no existe SQL basico
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bitcoin_prices (
            time TEXT,
            price REAL
        )
    """)

    #Ingesta de datos
    cursor.execute("INSERT INTO bitcoin_prices (time, price) VALUES (?, ?)", 
                   (record["time"], record["price"]))
    conn.commit()
    conn.close()

with DAG(
    dag_id="etl_api_to_db",
    #Fecha de inicio
    start_date=datetime(2025, 1, 1),
    #Se ejecuta cada hora
    schedule_interval="@hourly",
    #Que significa esto
    catchup=False,
    #Metadata solo para este caso
    tags=["etl", "api", "sqlite"],
) as dag:

    # Definicion de tares
    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load", python_callable=load)

    #Orden de ejecucion
    t1 >> t2 >> t3
