#!/bin/bash

# -------------------------------
# Esperar a que Kafka esté listo
# -------------------------------
echo "Esperando a que Kafka inicie..."
RETRIES=30
SLEEP_TIME=5

for i in $(seq 1 $RETRIES); do
  # Revisar si el broker responde
  kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "Kafka está listo!"
    break
  fi
  echo "Intento $i/$RETRIES: Kafka aún no está listo, esperando $SLEEP_TIME s..."
  sleep $SLEEP_TIME
done

# Si no se pudo conectar
if [ $i -eq $RETRIES ]; then
  echo "Error: Kafka no respondió después de $RETRIES intentos."
  exit 1
fi

# -------------------------------
# Crear tópicos
# -------------------------------
echo "Creando tópicos..."
TOPICS=("sales" "cliente")

for TOPIC in "${TOPICS[@]}"; do
  kafka-topics.sh --create \
    --topic $TOPIC \
    --bootstrap-server kafka:9092 \
    --replication-factor 1 \
    --partitions 1 2>/dev/null || echo "El tópico $TOPIC ya existe"
done

echo "Tópicos creados correctamente"
