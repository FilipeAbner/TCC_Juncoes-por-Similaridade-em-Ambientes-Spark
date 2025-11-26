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

No arquivo `master/.env`, **SUBSTITUA** pelos valores reais:

```env
SPARK_MODE=master
SPARK_MASTER_HOST=0.0.0.0  # Sempre 0.0.0.0 para aceitar conexões
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8080
SPARK_DRIVER_HOST=192.168.1.100  # ← SEU IP REAL DA MÁQUINA MASTER
SPARK_DRIVER_PORT=35000
SPARK_DRIVER_BLOCKMANAGER_PORT=35100
```

**⚠️ IMPORTANTE:** As variáveis `SPARK_DRIVER_*` são automaticamente substituídas no `spark-defaults.conf` durante a inicialização do container.

No arquivo `worker/.env`, use o **IP REAL do Master**:

```env
SPARK_MODE=worker
SPARK_MASTER_URL=spark://192.168.1.100:7077  # ← IP REAL DO MASTER
SPARK_MASTER_HOST=192.168.1.100  # ← MESMO IP
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

### Passo 5: Configurar NFS para Compartilhamento de Arquivos (Recomendado)

Se você precisa compartilhar arquivos de entrada entre Master e Workers (ex: datasets CSV, Parquet), configure NFS:

#### **No Master (192.168.1.7 - Linux):**

```bash
# 1. Instalar servidor NFS
sudo apt-get update
sudo apt-get install -y nfs-kernel-server

# 2. Criar diretório compartilhado
sudo mkdir -p /shared/spark-data
sudo chmod 777 /shared/spark-data

# 3. Configurar exportação NFS (permitir toda a rede 192.168.1.x)
echo "/shared/spark-data 192.168.1.0/24(rw,sync,no_subtree_check,no_root_squash,insecure)" | sudo tee -a /etc/exports

# 4. Aplicar configurações
sudo exportfs -ra
sudo systemctl restart nfs-kernel-server

# 5. Verificar se está funcionando
sudo exportfs -v
sudo systemctl status nfs-kernel-server

# 6. Liberar firewall (se ativo)
sudo ufw allow from 192.168.1.0/24 to any port nfs
sudo ufw allow from 192.168.1.0/24 to any port 2049
```

#### **Adicionar volume ao docker-compose do Master:**

Edite `master/docker-compose.yml` e adicione o volume:
```yaml
volumes:
  - ./apps:/apps
  - /shared/spark-data:/data  # ← Volume NFS compartilhado
```

Agora você pode acessar arquivos em `/data` dentro do container Master.

---

## Configuração do Worker (Máquina 2, 3, ...)

### Passo 1: Montar NFS (se configurado no Master)

#### **No Worker Linux:**

```bash
# 1. Instalar cliente NFS
sudo apt-get update
sudo apt-get install -y nfs-common

# 2. Criar diretório de montagem
sudo mkdir -p /shared/spark-data

# 3. Testar se o NFS está acessível
showmount -e 192.168.1.7

# 4. Montar o volume NFS
sudo mount -t nfs 192.168.1.7:/shared/spark-data /shared/spark-data

# 5. Verificar montagem
df -h | grep spark-data

# 6. Testar acesso
sudo touch /shared/spark-data/teste_worker.txt
ls -la /shared/spark-data/

# 7. (Opcional) Tornar montagem permanente após reboot
echo "192.168.1.7:/shared/spark-data /shared/spark-data nfs defaults,_netdev 0 0" | sudo tee -a /etc/fstab
```

#### **No Worker Windows com WSL:**

```bash
# 1. Instalar cliente NFS
sudo apt-get update
sudo apt-get install -y nfs-common

# 2. Criar diretório de montagem
sudo mkdir -p /shared/spark-data

# 3. Testar se o NFS está acessível
showmount -e 192.168.1.7

# 4. Montar o volume NFS
sudo mount -t nfs 192.168.1.7:/shared/spark-data /shared/spark-data

# 5. Verificar montagem
df -h | grep spark-data

# 6. Testar acesso
sudo touch /shared/spark-data/teste_wsl.txt
ls -la /shared/spark-data/

# 7. (Opcional) Se der erro "access denied", tente com opções adicionais:
sudo mount -t nfs -o vers=3,proto=tcp 192.168.1.7:/shared/spark-data /shared/spark-data
```

**⚠️ Troubleshooting - Se der erro "access denied":**

1. **Verificar no Master** se a configuração permite a rede:
   ```bash
   # No Master
   cat /etc/exports
   # Deve conter: /shared/spark-data 192.168.1.0/24(rw,sync,no_subtree_check,no_root_squash,insecure)
   ```

2. **Recarregar configuração do NFS** no Master:
   ```bash
   sudo exportfs -ra
   sudo systemctl restart nfs-kernel-server
   ```

3. **Verificar conectividade de rede**:
   ```bash
   # No Worker
   ping 192.168.1.7
   nc -zv 192.168.1.7 2049
   ```

#### **Adicionar volume ao docker-compose do Worker:**

Edite `worker/docker-compose.yml` e adicione o volume:
```yaml
volumes:
  - /shared/spark-data:/data  # ← Mesmo volume NFS do Master
```

Agora todos os Workers podem acessar os mesmos arquivos em `/data`.

**Exemplo de uso:**
```python
# No código PySpark
df = spark.read.csv("/data/vendas.csv", header=True)
# Todos os executors conseguem acessar o arquivo!
```

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
SPARK_MASTER_HOST=0.0.0.0
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8080
SPARK_DRIVER_HOST=192.168.1.7  # Use seu IP local (não afeta mesma máquina)
SPARK_DRIVER_PORT=35000
SPARK_DRIVER_BLOCKMANAGER_PORT=35100
```

No arquivo `worker/.env`:
```env
SPARK_MODE=worker
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_MASTER_HOST=spark-master
SPARK_WORKER_WEBUI_PORT=8081
```

**Importante:** Para mesma máquina, o Worker usa o hostname `spark-master` que é resolvido automaticamente pelo Docker.

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