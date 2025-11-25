$ErrorActionPreference = "Stop"

Write-Host "=== Prueba: ping backend ==="
Invoke-WebRequest -Uri "http://localhost:9000/api/ping"

Write-Host "=== Prueba: crear producto ==="
Invoke-WebRequest -Method POST -Uri "http://localhost:9000/products" `
  -Headers @{ "Content-Type" = "application/json" } `
  -Body '{"name":"Producto smoke","price":10.5,"description":"demo","sku":"SMK-1"}'

Write-Host "=== Prueba: movimiento entrada ==="
Invoke-WebRequest -Method POST -Uri "http://localhost:9000/movements" `
  -Headers @{ "Content-Type" = "application/json" } `
  -Body '{"product_id":1,"quantity":5,"type":"entrada","description":"smoke"}'

Write-Host "=== Prueba: importar CSV ==="
# Usamos curl.exe para compatibilidad con PowerShell 5 (multipart)
curl.exe -X POST "http://localhost:9000/uploads/csv/inventory" -F "file=@movimientos_inventario_ejemplo.csv"

Write-Host "=== Prueba: consumir Kafka inventory_entries (muestra 5 mensajes) ==="
docker exec -it kafka sh -c "kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic inventory_entries --from-beginning --max-messages 5 --timeout-ms 4000"

Write-Host "=== Prueba: publicar alerta RabbitMQ y leer 1 mensaje ==="
docker exec -it rabbitmq rabbitmqadmin --vhost=/app_vhost publish exchange=amq.default routing_key=low_stock_alerts payload='{\"product_id\":999,\"current_quantity\":3}'
docker exec -it rabbitmq rabbitmqadmin --vhost=/app_vhost get queue=low_stock_alerts count=1

Write-Host "=== Fin de smoke ==="
