import { useState, useEffect, useRef } from 'react';

// Lee la URL desde las variables de entorno de React, con un valor por defecto para desarrollo.
const WEBSOCKET_URL = process.env.REACT_APP_WEBSOCKET_URL || 'ws://localhost:9000/ws/updates';

const useInventorySocket = () => {
  const [lastMessage, setLastMessage] = useState(null);
  const [isConnected, setIsConnected] = useState(false);
  const socketRef = useRef(null);

  useEffect(() => {
    // Evita reconexiones en desarrollo con React.StrictMode
    if (socketRef.current) return;

    console.log('Intentando conectar al WebSocket...');
    const socket = new WebSocket(WEBSOCKET_URL);
    socketRef.current = socket;

    socket.onopen = () => {
      console.log('WebSocket conectado');
      setIsConnected(true);
    };

    socket.onmessage = (event) => {
      console.log('Mensaje recibido:', event.data);
      const data = JSON.parse(event.data);
      setLastMessage(data);
    };

    socket.onclose = () => {
      console.log('WebSocket desconectado');
      setIsConnected(false);
    };

    socket.onerror = (error) => {
      console.error('Error en WebSocket:', error);
    };

    return () => socket.close();
  }, []);

  return { lastMessage, isConnected };
};

export default useInventorySocket;