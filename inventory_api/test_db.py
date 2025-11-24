import os
import psycopg2

# Leer variables de entorno
host = os.getenv("POSTGRES_HOST", "localhost")
port = os.getenv("POSTGRES_PORT", 5432)
db = os.getenv("POSTGRES_DB", "airflow")
user = os.getenv("POSTGRES_USER", "sergio")
password = os.getenv("POSTGRES_PASSWORD", "123")

try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=db,
        user=user,
        password=password
    )
    cur = conn.cursor()

    # Crear tabla de prueba si no existe
    cur.execute("""
    CREATE TABLE IF NOT EXISTS test_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50)
    );
    """)
    conn.commit()

    # Insertar un registro de prueba
    cur.execute("INSERT INTO test_table (name) VALUES (%s) RETURNING id;", ("Sergio",))
    inserted_id = cur.fetchone()[0]
    conn.commit()

    # Leer la tabla
    cur.execute("SELECT * FROM test_table;")
    rows = cur.fetchall()
    print("Contenido de la tabla test_table:")
    for row in rows:
        print(row)

    cur.close()
    conn.close()
    print("✅ Conexión y pruebas realizadas con éxito!")

except Exception as e:
    print("❌ Error conectando a PostgreSQL:", e)
