# ┌───────────────────────────────────────────────────────────────────────────┐
# │ Imagen base ligera con Python 3.11                                          │
# └───────────────────────────────────────────────────────────────────────────┘
FROM python:3.11-slim

# Evitar mensajes de pip
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1

# ┌───────────────────────────────────────────────────────────────────────────┐
# │ Instalar dependencias del sistema (para geopandas, psycopg2, etc.)         │
# └───────────────────────────────────────────────────────────────────────────┘
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        gdal-bin \
        libgdal-dev \
        && \
    rm -rf /var/lib/apt/lists/*

# ┌───────────────────────────────────────────────────────────────────────────┐
# │ Fijar la ruta de trabajo y copiar archivos                                 │
# └───────────────────────────────────────────────────────────────────────────┘
WORKDIR /app
COPY . /app

# ┌───────────────────────────────────────────────────────────────────────────┐
# │ Instalar paquetes Python                                                   │
# └───────────────────────────────────────────────────────────────────────────┘
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# ┌───────────────────────────────────────────────────────────────────────────┐
# │ Exponer el puerto por defecto de Streamlit                                  │
# └───────────────────────────────────────────────────────────────────────────┘
EXPOSE 8501

# ┌───────────────────────────────────────────────────────────────────────────┐
# │ Arrancar el Streamlit App                                                   │
# └───────────────────────────────────────────────────────────────────────────┘
ENTRYPOINT ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
