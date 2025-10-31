#!/usr/bin/env bash (Checa se a variavel existe antes de exportar)
[[ -n "${SPARK_WORKER_CORES}" ]] && export SPARK_WORKER_CORES
[[ -n "${SPARK_WORKER_MEMORY}" ]] && export SPARK_WORKER_MEMORY
[[ -n "${SPARK_WORKER_PORT}" ]] && export SPARK_WORKER_PORT
[[ -n "${SPARK_WORKER_WEBUI_PORT}" ]] && export SPARK_WORKER_WEBUI_PORT
[[ -n "${SPARK_MASTER_URL}" ]] && export SPARK_MASTER="${SPARK_MASTER_URL}"

