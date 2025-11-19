#!/usr/bin/env bash
# Configurações do Python para PySpark
export PYSPARK_PYTHON=/usr/local/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

export SPARK_MASTER_HOST=${SPARK_MASTER_HOST}
export SPARK_MASTER_PORT=${SPARK_MASTER_PORT}
export SPARK_MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI_PORT}
export SPARK_LOG_DIR=/opt/spark/logs
