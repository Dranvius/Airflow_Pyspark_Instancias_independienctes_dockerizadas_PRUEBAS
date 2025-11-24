# Usar una imagen oficial de Python
FROM python:3.11-slim

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Añadir el directorio de trabajo al PYTHONPATH para asegurar que las importaciones funcionen
ENV PYTHONPATH "${PYTHONPATH}:/app"

# Copiar el archivo de requerimientos primero para aprovechar el cache de Docker
COPY requirements.txt .

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del código de la aplicación al directorio de trabajo
COPY . .

# Comando para ejecutar la aplicación
# Le decimos a Uvicorn que busque el objeto 'app' en el archivo 'main.py'
# --host 0.0.0.0 es crucial para que sea accesible desde fuera del contenedor
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9000", "--reload"]