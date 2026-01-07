#!/usr/bin/env bash
# Configurações do Python para PySpark
export PYSPARK_PYTHON=/usr/local/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

# Adiciona diretório apps ao PYTHONPATH para imports do brid_python
export PYTHONPATH=/apps:$PYTHONPATH

# Configura IP local do worker para comunicação entre workers
if [[ -n "${SPARK_LOCAL_IP}" ]]; then
    export SPARK_LOCAL_IP="${SPARK_LOCAL_IP}"
    # Força o worker a usar o IP real da máquina
    export SPARK_LOCAL_HOSTNAME="${SPARK_LOCAL_IP}"
fi

# Checa se a variavel existe antes de exportar
[[ -n "${SPARK_WORKER_PORT}" ]] && export SPARK_WORKER_PORT
[[ -n "${SPARK_WORKER_WEBUI_PORT}" ]] && export SPARK_WORKER_WEBUI_PORT
[[ -n "${SPARK_MASTER_URL}" ]] && export SPARK_MASTER="${SPARK_MASTER_URL}"

# Todos os núcleos
export SPARK_WORKER_CORES=$(nproc)

# Toda a memória física da máquina
TOTAL_MEM_MB=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)
export SPARK_WORKER_MEMORY="${TOTAL_MEM_MB}m"