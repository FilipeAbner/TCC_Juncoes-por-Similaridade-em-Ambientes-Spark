# Dockerfile - Spark Worker

FROM python:3.11-slim AS base

ENV PYSPARK_VERSION=4.0.1 \
    SPARK_VERSION=spark-4.0.1-bin-hadoop3 \
    SPARK_TGZ=spark-4.0.1-bin-hadoop3.tgz \
    SPARK_DOWNLOAD_URL=https://dlcdn.apache.org/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    PYTHONPATH=/apps:$PYTHONPATH \
    PYSPARK_PYTHON=/usr/local/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

# Instala dependências necessárias
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        wget \
        ca-certificates \
        procps \
        gettext-base && \
    rm -rf /var/lib/apt/lists/*

# Cria usuário não-root
ARG SPARK_USER=spark
RUN useradd -m -s /bin/bash ${SPARK_USER}

# Define diretório de trabalho
WORKDIR /opt

# Baixa e instala o Apache Spark
RUN wget -qO /tmp/${SPARK_TGZ} ${SPARK_DOWNLOAD_URL} && \
    tar -xzf /tmp/${SPARK_TGZ} && \
    mv ${SPARK_VERSION} /opt/spark && \
    rm /tmp/${SPARK_TGZ}

# Cria o diretório de logs
RUN mkdir -p /opt/spark/logs

# Instala dependências Python
COPY ../requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Copia arquivos de configuração
COPY spark-env.sh ${SPARK_HOME}/conf/spark-env.sh
COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY log4j2.properties ${SPARK_HOME}/conf/log4j2.properties
COPY entrypoint-worker.sh /usr/local/bin/entrypoint.sh

# Torna scripts executáveis
RUN chmod +x ${SPARK_HOME}/conf/spark-env.sh && \
    chmod +x /usr/local/bin/entrypoint.sh

# Container executa como root para evitar problemas de permissão
WORKDIR /opt/spark

# Porta padrão do worker (UI)
EXPOSE 8081

# Usa script de entrypoint que resolve hostname para network_mode: host
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
