-- Este script se ejecuta automáticamente la primera vez que el contenedor de PostgreSQL se inicia.

-- Crear la tabla 'products' si no existe.
-- Esta tabla almacenará la información del inventario.
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Insertar algunos datos de ejemplo para que la aplicación tenga un estado inicial.
-- La cláusula ON CONFLICT (name) DO NOTHING evita errores si el script se ejecuta
-- accidentalmente más de una vez con los mismos datos.
INSERT INTO products (name, quantity, price) VALUES
('Laptop Pro', 50, 1200.00),
('Mouse Inalámbrico', 200, 25.50),
('Teclado Mecánico', 75, 89.99),
('Monitor 4K', 30, 450.00)
ON CONFLICT (name) DO NOTHING;