FROM apache/airflow:3.0.6

USER root

# Instalar Java (solo cliente, no Spark completo)
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    iputils-ping \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# Variables de entorno (para cliente Spark)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow

# Instalar provider de Spark + extras
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    psycopg2-binary \
    cryptography \
    kafka-python \
    pika

