# Imagen base con Java
FROM openjdk:11-slim

# Instalar Python y utilidades
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Configurar Python como predeterminado
RUN ln -s /usr/bin/python3 /usr/bin/python

# Instalar dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Configurar directorio de trabajo
WORKDIR /app

# Copiar tu aplicación
COPY app/ /app/

# Variable de entorno para Spark local
ENV SPARK_MASTER=local[*]

# Comando por defecto (puedes sobrescribirlo con docker run)
CMD ["python", "main.py"]
