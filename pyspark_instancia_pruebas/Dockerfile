# Imagen base de Spark (compatible y disponible en Docker Hub)
FROM spark:3.5.1

# Instalar Python y utilidades
USER root
RUN apt-get update && apt-get install -y \
    python3 python3-pip curl \
    && rm -rf /var/lib/apt/lists/*

# Configurar Python como predeterminado
RUN ln -s /usr/bin/python3 /usr/bin/python

# Copiar requirements si los tienes
COPY requirements.txt /opt/bitnami/spark/requirements.txt
RUN pip install --no-cache-dir -r /opt/bitnami/spark/requirements.txt || true

# === Instalar JARs necesarios para Kafka ===
WORKDIR /opt/bitnami/spark/jars

# Spark-Kafka connector + dependencias
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.5.1/spark-sql-kafka-0-10_2.13-3.5.1.jar .
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar .
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar .

# Directorio de trabajo
WORKDIR /opt/bitnami/spark/app

# Usuario airflow (bitnami/spark trabaja con este user)
USER 1001
