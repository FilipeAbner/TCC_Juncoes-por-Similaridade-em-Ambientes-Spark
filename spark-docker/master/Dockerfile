# Dockerfile - Spark Master

FROM python:3.11-slim AS base

ENV PYSPARK_VERSION=4.0.1 \
    SPARK_VERSION=spark-4.0.1-bin-hadoop3 \
    SPARK_TGZ=spark-4.0.1-bin-hadoop3.tgz \
    SPARK_DOWNLOAD_URL=https://dlcdn.apache.org/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
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

# Cria o diretório de logs e dá permissão ao usuário spark
RUN mkdir -p /opt/spark/logs && chown -R spark:spark /opt/spark

# Instala dependências Python
COPY ../requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Copia arquivos de configuração
COPY spark-env.sh ${SPARK_HOME}/conf/spark-env.sh
COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY log4j2.properties ${SPARK_HOME}/conf/log4j2.properties

# Torna spark-env.sh executável e ajusta permissões
RUN chmod +x ${SPARK_HOME}/conf/spark-env.sh && \
    chown -R spark:spark ${SPARK_HOME}/conf

COPY ../apps/ /apps/

USER ${SPARK_USER}
WORKDIR /home/${SPARK_USER}

# Porta padrão do worker (UI)
EXPOSE 7077 8080

ENTRYPOINT ["/opt/spark/bin/spark-class"]
CMD ["org.apache.spark.deploy.master.Master"]

