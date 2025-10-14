# Usa Python base
FROM python:3.11-slim

# Crea el directorio de trabajo
WORKDIR /app

# Copia dependencias e instálalas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código
COPY . .

# Comando por defecto (para FastAPI)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
