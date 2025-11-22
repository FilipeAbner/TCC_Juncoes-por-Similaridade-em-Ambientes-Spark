# Configuração do Cluster Spark Multi-Máquina

Este guia explica como configurar um cluster Spark com Master e Workers em máquinas diferentes utilizando docker.

## Topologia do Cluster em máquinas diferentes

```
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│   Máquina 1     │         │   Máquina 2     │         │   Máquina 3     │
│                 │         │                 │         │                 │
│  Spark Master   │◄────────┤  Spark Worker   │         │  Spark Worker   │
│  IP: 192.168... │         │                 │         │                 │
│  Porta: 7077    │◄────────┴─────────────────┴─────────┤                 │
└─────────────────┘                                     └─────────────────┘
```

## Configuração do Master (Máquina 1)

### Passo 1: Descobrir o IP da máquina Master

```bash
# Linux/Mac
ip addr show | grep inet

# Ou
hostname -I

```

Anote o IP (exemplo: `192.168.1.100`)

### Passo 2: Configurar .env para o Master e Worker

**ATENÇÃO:** Esta é a configuração mais crítica!

No arquivo `master/.env`, **SUBSTITUA** `0.0.0.0` pelo **IP REAL** da máquina Master:

```env
SPARK_MODE=master
SPARK_MASTER_HOST=192.168.1.100  # ← SEU IP REAL DA MÁQUINA MASTER AQUI
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8080
```

No arquivo `master/spark-defaults.conf`, **SUBSTITUA** o hostname pelo **IP REAL**:

```properties
spark.master                     spark://192.168.1.100:7077
spark.driver.bindAddress         0.0.0.0
spark.driver.host                192.168.1.100  # ← SEU IP REAL AQUI
spark.driver.port                35000
```

**💡 DICA:** Use o arquivo `master/spark-defaults-multi-maquina.conf` como template!

**⚠️ IMPORTANTE:** O Master e o Driver precisam anunciar o IP real para que os executores dos Workers consigam conectar de volta. Se usar `0.0.0.0` ou `spark-master`, os Workers registram mas os jobs falham!

No arquivo `worker/.env`, use o **MESMO IP** do Master:

```env
SPARK_MODE=worker
SPARK_MASTER_URL=spark://192.168.1.100:7077  # ← MESMO IP DO MASTER
SPARK_WORKER_WEBUI_PORT=8081
```


### Passo 3: Verificar o Firewall

**IMPORTANTE:** As portas precisam estar abertas no firewall do Master!

```bash
# Ubuntu/Debian
sudo ufw allow 7077/tcp    # Master RPC
sudo ufw allow 8080/tcp    # Master UI
sudo ufw allow 35000/tcp   # Driver (comunicação com executores)

# CentOS/RHEL
sudo firewall-cmd --permanent --add-port=7077/tcp
sudo firewall-cmd --permanent --add-port=8080/tcp
sudo firewall-cmd --permanent --add-port=35000/tcp
sudo firewall-cmd --reload
```

### Passo 4: Iniciar o Master

```bash
cd master
sudo docker compose up --build
```

Verifique se está rodando:
- Web UI: `http://IP_DO_MASTER:8080`
- Porta RPC: `IP_DO_MASTER:7077`

---

## Configuração do Worker (Máquina 2, 3, ...)

### Passo 2: Testar conectividade com o Master

Antes de subir o worker, teste se consegue alcançar o Master:

```bash
# Teste de ping
ping 192.168.1.100

# Teste de porta (deve retornar algo, não "Connection refused")
telnet 192.168.1.100 7077
# ou
nc -zv 192.168.1.100 7077
```

### Passo 3: Iniciar o Worker

```bash
cd worker
sudo docker compose up --build
```

### Passo 4: Verificar se o Worker conectou

Veja os logs do worker:
```bash
sudo docker logs -f spark-worker
```

Se conectou com sucesso, você verá algo como:
```
INFO Worker: Successfully registered with master spark://192.168.1.100:7077
```

Verifique também na Web UI do Master (`http://IP_DO_MASTER:8080`), o worker deve aparecer na lista.

### Passo 5: Testar com um job

Execute um job de teste:
```bash
# Na máquina do Master
sudo docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://192.168.1.7:7077 \
  --executor-memory 512m \
  --executor-cores 1 \
  --total-executor-cores 2 \
  /apps/test_spark_basic.py
```

**⚠️ ATENÇÃO:** Use o IP real do Master no `--master`, não o hostname `spark-master`!

Se os executores não conectarem, verifique:
1. `SPARK_MASTER_HOST` no Master está com o IP real (não `0.0.0.0`)
2. Firewall permite tráfego bidirecional na porta 7077
3. Workers conseguem fazer ping no IP do Master

---

## Configuração Adicionais

### Limitar recursos do Worker

Edite `worker/.env`:

```env
SPARK_WORKER_CORES=4        # Número de CPUs para o Worker usar
SPARK_WORKER_MEMORY=8g      # Memória RAM para o Worker
SPARK_WORKER_PORT=7078      # Porta de comunicação do Worker
```

## Topologia do cluster na mesma máquina

```
┌─────────────────┐ 
│   Máquina 1     │
│                 │  
│  Spark Master   │
│  Porta: 7077    │
│   Spark Worker  │
└─────────────────┘                           
```

## Configuração do Master e Worker na mesma máquina

#### 1. Criar rede Docker externa
```bash
sudo docker network create spark-network \
  --driver bridge \
  --subnet 172.20.0.0/16 \
  --gateway 172.20.0.1
```

#### 2. Configurar .env para mesma máquina

No arquivo `master/.env`:
```env
SPARK_MODE=master
SPARK_MASTER_HOST=0.0.0.0  # ← 0.0.0.0 funciona para mesma máquina
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8080
```

No arquivo `worker/.env`:
```env
SPARK_MODE=worker
SPARK_MASTER_URL=spark://spark-master:7077  # ← hostname funciona na mesma máquina
SPARK_WORKER_WEBUI_PORT=8081
```

**Importante:** O arquivo `master/spark-defaults.conf` já está configurado para mesma máquina com `spark.driver.host=spark-master`.

#### 3. Iniciar os containers
```bash
# Master
cd master
sudo docker compose up --build -d

# Worker (em outro terminal)
cd worker
sudo docker compose up --build -d
```


### Executar o Script

#### **Opção A: Via spark-submit no Master (Recomendado)**

```bash
# Copiar script para o Master (se ainda não estiver)
sudo docker cp master/apps/test_spark_basic.py spark-master:/apps/test_spark_basic.py

# Executar com spark-submit
sudo docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m \
  --executor-cores 1 \
  --total-executor-cores 2 \
  /apps/test_spark_basic.py
```

#### **Opção B: Via container interativo**

```bash
# Entrar no container Master
sudo docker exec -it spark-master /bin/bash

# Dentro do container, executar:
spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m \
  --executor-cores 1 \
  /apps/test_spark_basic.py
```

## 📊 Consultar Logs Completos

### Ver logs em tempo real:
```bash
sudo docker exec -it spark-master tail -f /opt/spark/logs/$(sudo docker exec spark-master ls -t /opt/spark/logs/ | grep spark- | head -1)
```