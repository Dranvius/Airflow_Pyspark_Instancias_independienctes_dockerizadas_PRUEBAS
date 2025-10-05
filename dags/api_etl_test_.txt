import requests
import sqlite3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Conexion a la base de datos
# Instancia de docker
# Ailar la conexion en una funcion que permitya realziar inyeccion de dependencias
DB_FILE = "/opt/airflow/dags/bitcoin.db"

#
#b = requests.get("https://api.finazon.io/latest/finazon/us_stocks_essential/time_series?ticker=AAPL&country=US&interval=4mo&page=0&page_size=30&adjust=all&apikey=ef0f80d61ccf419f85723f20b62b473fdp")
# #

# Extraccion
# Diferentes fuentes
def extract():

##
import requests
import pandas as pd
from datetime import datetime, timedelta

API_KEY = "0763719b1cef422a8e773521251109"
BASE_URL = "http://api.weatherapi.com/v1/history.json"

cities = ["New York", "Chicago", "Boston", "San Francisco", "Los Angeles", "Houston", "Miami"]

# Calcular rango de 4 meses hacia atrás desde hoy
end_date = datetime.today()
start_date = end_date - timedelta(days=120)  # aprox. 4 meses
delta = timedelta(days=1)

data_list = []

for city in cities:
    current_date = start_date
    while current_date <= end_date:
        params = {
            "key": API_KEY,
            "q": city,
            "dt": current_date.strftime("%Y-%m-%d")
        }
        response = requests.get(BASE_URL, params=params)
        data = response.json()

        if "forecast" in data:
            forecast = data["forecast"]["forecastday"][0]["day"]
            data_list.append({
                "city": city,
                "date": data["forecast"]["forecastday"][0]["date"],
                "avg_temp_c": forecast["avgtemp_c"],
                "max_temp_c": forecast["maxtemp_c"],
                "min_temp_c": forecast["mintemp_c"],
                "condition": forecast["condition"]["text"]
            })
        else:
            print(f"Error con {city} en {current_date}: {data}")

        current_date += delta

# Guardar en CSV
df = pd.DataFrame(data_list)
df.to_csv("weather_history_4months.csv", index=False)
print("✅ Datos descargados de los últimos 4 meses")
print(df.head())
##

    #Api clima

    climaApi = "/alerts/active/count"

    #Api bolsa

    bolsa = "https://api.finazon.io/latest/finazon/us_stocks_essential/tickers?page_size=1000&apikey=api_key"

    #Api bolsa de valores

    url = "https://api.coindesk.com/v1/bpi/currentprice.json"
    response = requests.get(url)
    data = response.json()

    return data  # se pasa por XCom


# Transformacion
# Limpieza
def transform( ti ):




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
