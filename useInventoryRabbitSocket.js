import { useState, useEffect, useRef } from 'react';

// Lee la URL desde las variables de entorno de React, con un valor por defecto para desarrollo.
// Este endpoint debe ser implementado en tu backend para leer de RabbitMQ y enviar a través del WebSocket.
const WEBSOCKET_URL = process.env.REACT_APP_RABBIT_WEBSOCKET_URL || 'ws://localhost:9000/ws/rabbit-updates';

const useInventoryRabbitSocket = () => {
  const [lastMessage, setLastMessage] = useState(null);
  const [isConnected, setIsConnected] = useState(false);
  const socketRef = useRef(null);

  useEffect(() => {
    // Evita reconexiones en desarrollo con React.StrictMode
    if (socketRef.current) return;

    console.log('Intentando conectar al WebSocket de RabbitMQ...');
    const socket = new WebSocket(WEBSOCKET_URL);
    socketRef.current = socket;

    socket.onopen = () => {
      console.log('WebSocket (RabbitMQ) conectado');
      setIsConnected(true);
    };

    socket.onmessage = (event) => {
      console.log('Mensaje (RabbitMQ) recibido:', event.data);
      const data = JSON.parse(event.data);
      setLastMessage(data);
    };

    socket.onclose = () => {
      console.log('WebSocket (RabbitMQ) desconectado');
      setIsConnected(false);
    };

    socket.onerror = (error) => console.error('Error en WebSocket (RabbitMQ):', error);

    return () => socket.close();
  }, []);

  return { lastMessage, isConnected };
};

export default useInventoryRabbitSocket;