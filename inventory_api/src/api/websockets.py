from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from ..websocket_manager import manager

router = APIRouter(
    tags=["WebSockets"]
)

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Endpoint WebSocket para la comunicación en tiempo real con el frontend.
    - Acepta conexiones de clientes.
    - Las mantiene vivas para que el servidor pueda enviarles mensajes.
    - Maneja la desconexión de forma limpia.
    """
    await manager.connect(websocket)
    try:
        while True:
            # El servidor espera aquí, manteniendo la conexión abierta.
            # Podríamos recibir mensajes del cliente si fuera necesario.
            # Por ejemplo, para que un cliente se suscriba a un "canal" específico.
            data = await websocket.receive_text()
            # Opcional: Lógica para manejar mensajes del cliente.
            # await manager.broadcast(f"Cliente envió: {data}")
    except WebSocketDisconnect:
        # Esto se activa cuando el cliente cierra la conexión.
        manager.disconnect(websocket)
    except Exception as e:
        # Manejar otras excepciones y asegurar la desconexión.
        print(f"Error inesperado en el WebSocket: {e}")
        manager.disconnect(websocket)
