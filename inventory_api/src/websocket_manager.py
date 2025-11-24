from typing import List
from fastapi import WebSocket
import json

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        """Acepta una nueva conexión WebSocket y la añade a la lista."""
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"Nueva conexión WebSocket. Total de conexiones: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        """Elimina una conexión WebSocket de la lista."""
        self.active_connections.remove(websocket)
        print(f"Conexión WebSocket cerrada. Total de conexiones: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        """Envía un mensaje JSON a todas las conexiones activas."""
        # Hacemos una copia de la lista por si cambia durante la iteración
        disconnected_connections = []
        for connection in self.active_connections:
            try:
                await connection.send_text(json.dumps(message))
            except Exception as e:
                # Si el cliente se ha desconectado de forma abrupta, no podremos enviarle el mensaje.
                # Lo marcaremos para eliminarlo después.
                print(f"Error enviando mensaje a un WebSocket (probablemente desconectado): {e}")
                disconnected_connections.append(connection)
        
        # Limpiar conexiones rotas
        for connection in disconnected_connections:
            self.active_connections.remove(connection)

# Creamos una instancia única del gestor que será usada en toda la aplicación.
manager = ConnectionManager()
