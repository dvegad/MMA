#!/bin/bash
set -e

# 1) Instalar dependencias (en CI or local env fresco)
pip install --upgrade pip
pip install -r requirements.txt

# 2) Arrancar Streamlit en modo headless en el puerto 8501
streamlit run app.py --server.port=8501 --server.address=0.0.0.0
