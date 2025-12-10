#!/usr/bin/env bash
# Configurações do Python para PySpark
export PYSPARK_PYTHON=/usr/local/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

# Adiciona diretório apps ao PYTHONPATH para imports do brid_python
export PYTHONPATH=/apps:$PYTHONPATH

# Checa se a variavel existe antes de exportar
[[ -n "${SPARK_MASTER_HOST}" ]] && export SPARK_MASTER_HOST="${SPARK_MASTER_HOST}"
[[ -n "${SPARK_MASTER_PORT}" ]] && export SPARK_MASTER_PORT="${SPARK_MASTER_PORT}"
[[ -n "${SPARK_MASTER_WEBUI_PORT}" ]] && export SPARK_MASTER_WEBUI_PORT="${SPARK_MASTER_WEBUI_PORT}"
[[ -n "${SPARK_LOG_DIR}" ]] || export SPARK_LOG_DIR=/opt/spark/logs