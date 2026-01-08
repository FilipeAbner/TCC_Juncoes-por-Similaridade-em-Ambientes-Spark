#!/bin/bash
set -e

# Adiciona hostname ao /etc/hosts para resolver o problema de DNS com network_mode: host
HOSTNAME=$(hostname)
HOST_IP=${SPARK_LOCAL_IP:-$(hostname -I | awk '{print $1}')}

# Adiciona entrada ao /etc/hosts se não existir (requer permissões root)
if ! grep -q "$HOSTNAME" /etc/hosts 2>/dev/null; then
    echo "$HOST_IP $HOSTNAME" | tee -a /etc/hosts > /dev/null
fi

# Adiciona spark-worker ao /etc/hosts
if ! grep -q "spark-worker" /etc/hosts 2>/dev/null; then
    echo "127.0.0.1 spark-worker" | tee -a /etc/hosts > /dev/null
fi

# Muda para o diretório de trabalho do Spark
cd /opt/spark

# Substitui variáveis de ambiente no spark-defaults.conf
envsubst < ${SPARK_HOME}/conf/spark-defaults.conf > ${SPARK_HOME}/conf/spark-defaults.conf.tmp
mv ${SPARK_HOME}/conf/spark-defaults.conf.tmp ${SPARK_HOME}/conf/spark-defaults.conf

# Inicia Worker como root
exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker ${SPARK_MASTER_URL}
