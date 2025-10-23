# ============================================================
# Imagen base oficial de Apache Spark (sin Bitnami)
# ============================================================
FROM apache/spark:3.5.1

# ============================================================
# Cambiar a root para instalar dependencias del sistema
# ============================================================
USER root

# Instalar Python, pip, Java y utilidades básicas
RUN apt-get update && apt-get install -y \
    python3 python3-pip curl openjdk-11-jdk \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# ============================================================
# Instalar dependencias de Python (si tienes requirements.txt)
# ============================================================
COPY requirements.txt /opt/spark/requirements.txt
RUN pip install --no-cache-dir -r /opt/spark/requirements.txt || true


# 🔹 Instalar psycopg2 para conexión con PostgreSQL
RUN pip install --no-cache-dir psycopg2-binary

# ============================================================
# Descargar e incluir los conectores Spark-Kafka y dependencias
# ============================================================

# --- Dependencias Kafka + JDBC ---
WORKDIR /opt/spark/jars

# ✅ Conector correcto (Spark 3.5.1 + Scala 2.12)
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar .

# ✅ Cliente Kafka compatible con esa versión de Spark
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar .

# 🔧 (Opcional) Commons Pool — usado internamente por algunos conectores
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar .

# 🐘 (Opcional) JDBC driver de Postgres — útil si vas a escribir en bases de datos
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar .


# ============================================================
# Configurar entorno y permisos
# ============================================================
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
RUN chmod -R 777 /opt/spark

# ============================================================
# Crear directorio de trabajo para tu aplicación
# ============================================================
WORKDIR /opt/spark/app
COPY . /opt/spark/app

# ============================================================
# Usuario root (puedes cambiarlo si quieres más seguridad)
# ============================================================
USER root

# ============================================================
# Comando por defecto
# ============================================================
CMD ["bash"]
