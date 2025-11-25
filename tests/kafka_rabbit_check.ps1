$ErrorActionPreference = "Stop"
$testQueue = "low_stock_alerts_smoke"

Write-Host "=== Kafka: producir mensaje de prueba en inventory_entries ==="
docker exec -it kafka bash -lc "printf '{\"product_id\":999,\"quantity\":1,\"type\":\"entrada\",\"source\":\"smoke\"}\n' > /tmp/kmsg.json && /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic inventory_entries < /tmp/kmsg.json"

Write-Host "=== Kafka: consumir hasta 5 mensajes de inventory_entries ==="
docker exec -it kafka bash -lc "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic inventory_entries --from-beginning --max-messages 5 --timeout-ms 15000"

Write-Host "=== RabbitMQ: esperar arranque y publicar alerta de prueba en cola aislada ==="
docker exec -it rabbitmq rabbitmqctl await_startup
docker exec -it rabbitmq bash -lc "rabbitmqadmin --host=127.0.0.1 --port=15672 --username=guest --password=guest --vhost=/app_vhost declare queue name=$testQueue durable=true"
docker exec -it rabbitmq bash -lc "rabbitmqadmin --host=127.0.0.1 --port=15672 --username=guest --password=guest --vhost=/app_vhost publish exchange=amq.default routing_key=$testQueue payload='{\"product_id\":999,\"current_quantity\":3,\"source\":\"smoke\"}'"

Write-Host "=== RabbitMQ: leer 1 mensaje de la cola aislada (no consumida por backend) ==="
docker exec -it rabbitmq bash -lc "rabbitmqadmin --host=127.0.0.1 --port=15672 --username=guest --password=guest --vhost=/app_vhost get queue=$testQueue count=1 ackmode=ack_requeue_true"

Write-Host "=== Fin de pruebas Kafka/Rabbit ==="
