# ===============================================================
# IMPORTS NECESARIOS
# ===============================================================
# DAG: estructura principal de Airflow
from airflow import DAG

# Operadores básicos
from airflow.operators.python import PythonOperator   # Para ejecutar funciones Python
from airflow.operators.bash import BashOperator       # Para ejecutar comandos de consola

# Manejo de excepciones personalizadas
from airflow.exceptions import AirflowFailException

# Utilidades de tiempo
from datetime import datetime, timedelta


# ===============================================================
# CONFIGURACIÓN DE PARÁMETROS GLOBALES DEL DAG
# ===============================================================
# Argumentos por defecto que aplican a todas las tareas del DAG
default_args = {
    "owner": "airflow",                 # Propietario del DAG
    "depends_on_past": False,           # No depende de ejecuciones previas
    "email_on_failure": False,          # No envía correo en caso de fallo
    "email_on_retry": False,            # No envía correo en caso de reintento
    "retries": 1,                       # Número de reintentos si falla
    "retry_delay": timedelta(minutes=5) # Tiempo entre reintentos
}

# ===============================================================
# DEFINICIÓN DEL DAG
# ===============================================================
dag = DAG(
    "nombre_default",                           # Nombre único del DAG
    description="mi primer_dag_etl_xoy",        # Descripción visible en la UI
    default_args=default_args,                  # Argumentos comunes definidos arriba
    schedule_interval=timedelta(days=1),        # Frecuencia: 1 vez al día
    start_date=datetime(2021, 1, 1),            # Fecha inicial de ejecución
    catchup=False,                              # No ejecuta fechas pasadas (solo actuales)
    tags=["prueba_dags"],                       # Etiquetas para organizar DAGs
    params={"commit": "0000"}                   # Parámetros de ejecución dinámicos
    # Ejemplo: puedes sobreescribir "commit" al lanzar el DAG manualmente
)


# ===============================================================
# DEFINICIÓN DE FUNCIONES PYTHON (PYTHONCALLABLES)
# ===============================================================

def tarea_0_func(**kwargs):
    """
    Función de ejemplo para la tarea 0.
    Valida parámetros recibidos por DAG Run y falla intencionalmente
    si el commit es "1".
    """
    conf = kwargs['dag_run'].conf

    # Excepción personalizada para detener el flujo si el commit es "1"
    if "commit" in conf and conf["commit"] == "1":
        raise AirflowFailException("Hoy no desplegamos porque no me gusta el commit 1")

    # Retorna un diccionario que quedará guardado en XCom
    return {"ok": 1}


def tarea_2_func(**kwargs):
    """
    Función de ejemplo para tarea 2.
    Retorna un valor para ser compartido por XCom.
    """
    return {"ok": 2}


def tarea_3_func(**kwargs):
    """
    Ejemplo de tarea que consume un valor desde XCom.
    Recupera el valor generado por otra tarea ('tarea_que_comparte').
    """
    xcom_value = kwargs['ti'].xcom_pull(task_ids='tarea_que_comparte')
    print("Valor compartido entre tareas:", xcom_value)
    return {"ok": 3}


def tarea_4_func(**kwargs):
    """
    Ejemplo de función que puede compartir un valor en XCom.
    """
    return {"ok": 4}


# ===============================================================
# DEFINICIÓN DE TAREAS (OPERADORES)
# ===============================================================

# Tarea 0: ejecuta una función Python que puede lanzar excepciones
tarea0 = PythonOperator(
    task_id="imprimir_tarea_0",
    python_callable=tarea_0_func,   # función Python a ejecutar
    dag=dag
)

# Tarea 1: imprime la fecha en consola usando un comando Bash
tarea1 = BashOperator(
    task_id="imprimir_fecha",
    bash_command='echo "La fecha es $(date)"',
    dag=dag
)

# Tarea 2: ejecuta otra función Python
tarea2 = PythonOperator(
    task_id="tarea_2",
    python_callable=tarea_2_func,
    dag=dag
)

# Tarea 3: comparte un valor (lo que retorna se guarda en XCom)
tarea3 = PythonOperator(
    task_id="tarea_que_comparte",
    python_callable=tarea_4_func,
    dag=dag
)

# Tarea 4: consume el valor compartido por 'tarea_que_comparte'
tarea4 = PythonOperator(
    task_id="tarea_que_toma",
    python_callable=tarea_3_func,
    dag=dag
)


# ===============================================================
# DEFINICIÓN DEL WORKFLOW (DEPENDENCIAS)
# ===============================================================
# Flujo:
# 1. Tarea0 se ejecuta primero
# 2. Luego de terminar, dispara en paralelo tarea1 y tarea2
# 3. tarea1 al terminar dispara tarea3
# 4. tarea3 al terminar dispara tarea4
tarea0 >> [tarea1, tarea2]
tarea1 >> tarea3 >> tarea4
