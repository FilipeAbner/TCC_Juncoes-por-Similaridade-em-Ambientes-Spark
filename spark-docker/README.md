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

**IMPORTANTE:** Para que os Workers acessem os mesmos arquivos que o Master, vamos exportar o diretório `apps` via NFS.

 Criar network docker externa:
```bash 
    sudo docker network create spark-network --driver bridge --subnet 172.20.0.0/16 --gateway 172.20.0.1
```


#### **Método Recomendado: Exportar diretório `apps` do Master**

Este método garante que Master e Workers vejam exatamente os mesmos arquivos.

```bash
# 1. Parar Master (se estiver rodando)
cd ~/TCC_Juncoes-por-Similaridade-em-Ambientes-Spark/spark-docker/master
sudo docker-compose down

# 2. Instalar servidor NFS
sudo apt-get update
sudo apt-get install -y nfs-kernel-server

# 3. Obter caminho absoluto do diretório apps
APPS_DIR=$(pwd)/apps
echo "Exportando: $APPS_DIR"

# 4. Configurar NFS para exportar apps (substitua pelo caminho real)
# Exemplo: /home/filipe-abner/TCC_Juncoes-por-Similaridade-em-Ambientes-Spark/spark-docker/master/apps
echo "$APPS_DIR *(rw,sync,no_subtree_check,no_root_squash,insecure,all_squash,anonuid=1000,anongid=1000)" | sudo tee -a /etc/exports

# 5. Aplicar configurações
sudo exportfs -ra
sudo systemctl restart nfs-kernel-server

# 6. Verificar se está exportando
sudo exportfs -v | grep apps

# 7. Liberar firewall (se ativo)
sudo ufw allow from 192.168.1.0/24 to any port nfs
sudo ufw allow from 192.168.1.0/24 to any port 2049

# 8. Subir Master novamente
sudo docker-compose up -d
```

**Verificar no Master:**
```bash
# Listar arquivos compartilhados
ls -la ~/TCC_Juncoes-por-Similaridade-em-Ambientes-Spark/spark-docker/master/apps/
# Deve mostrar: test_nfs_data.py, vendas_exemplo.csv, etc.
```

---

## Configuração do Worker (Máquina 2, 3, ...)

#### **No Worker Linux:**

```bash
# 1. Instalar cliente NFS
sudo apt-get update
sudo apt-get install -y nfs-common

# 2. Criar ponto de montagem
sudo mkdir -p /mnt/master-apps

# 3. Testar se consegue ver o compartilhamento NFS do Master
showmount -e 192.168.1.6
# Deve mostrar: /home/filipe-abner/.../master/apps *

# 4. Montar o diretório apps do Master
# SUBSTITUA o caminho pelo caminho real retornado por showmount
sudo mount -t nfs 192.168.1.6:/home/filipe-abner/TCC_Juncoes-por-Similaridade-em-Ambientes-Spark/spark-docker/master/apps /mnt/master-apps

# 5. Verificar se montou corretamente
ls -la /mnt/master-apps/
# Deve mostrar: test_nfs_data.py, vendas_exemplo.csv, etc.

# 6. Testar acesso
cat /mnt/master-apps/vendas_exemplo.csv | head -5

# 7. (Opcional) Tornar montagem permanente após reboot
echo "192.168.1.7:/home/filipe-abner/TCC_Juncoes-por-Similaridade-em-Ambientes-Spark/spark-docker/master/apps /mnt/master-apps nfs defaults,_netdev 0 0" | sudo tee -a /etc/fstab
```

#### **No Worker Windows com WSL:**

```bash
# 1. Instalar cliente NFS
sudo apt-get update
sudo apt-get install -y nfs-common

# 2. Criar ponto de montagem
sudo mkdir -p /mnt/master-apps

# 3. Testar se o NFS está acessível
showmount -e 192.168.1.7

# 4. Montar o diretório apps do Master (substitua pelo caminho real)
sudo mount -t nfs 192.168.1.7:/home/filipe-abner/TCC_Juncoes-por-Similaridade-em-Ambientes-Spark/spark-docker/master/apps /mnt/master-apps

# 5. Verificar montagem
ls -la /mnt/master-apps/

# 6. Se der erro "access denied", tente com opções adicionais:
sudo mount -t nfs -o vers=3,proto=tcp 192.168.1.7:/home/filipe-abner/TCC_Juncoes-por-Similaridade-em-Ambientes-Spark/spark-docker/master/apps /mnt/master-apps
```

**⚠️ Troubleshooting - Se der erro "access denied":**

1. **Verificar no Master** se o diretório apps está sendo exportado:
   ```bash
   # No Master
   sudo exportfs -v | grep apps
   # Deve mostrar o caminho completo do diretório apps
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
  - /mnt/master-apps:/apps  # ← Monta o diretório apps do Master
```

Agora Master e Workers compartilham o mesmo diretório `/apps` com todos os arquivos!
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

### Passo 5: Testar com dados compartilhados via NFS

Execute o teste de leitura de dados compartilhados:

```bash
# Na máquina do Master
sudo docker exec -it spark-master \
  spark-submit \
  --master spark://192.168.1.7:7077 \
  --executor-memory 512m \
  --executor-cores 1 \
  --total-executor-cores 2 \
  /apps/test_nfs_data.py
```

**Resultado esperado:**
```
======================================================================
TESTE: Leitura de Dados do NFS Compartilhado
======================================================================

✓ Arquivo lido com sucesso!
✓ Total de registros: 60
✓ Análises estatísticas executadas
✓ Resultado salvo em: /apps/resultado_analise_vendas
✅ TESTE CONCLUÍDO COM SUCESSO!
```

**Verificar resultado:**
```bash
# Verificar arquivos gerados
ls -la ~/TCC_Juncoes-por-Similaridade-em-Ambientes-Spark/spark-docker/master/apps/resultado_analise_vendas/

# Deve mostrar:
# _SUCCESS
# part-xxxxx.snappy.parquet
```

### Passo 6: Teste básico de conectividade

Execute um teste simples sem dados externos:
```bash
# Na máquina do Master
sudo docker exec -it spark-master \
  spark-submit \
  --master spark://192.168.1.7:7077 \
  --executor-memory 512m \
  --executor-cores 1 \
  --total-executor-cores 2 \
  /apps/test_spark_basic.py
```

**⚠️ ATENÇÃO:** Use o IP real do Master no `--master`, não o hostname `spark-master`!

**✅ Validação do Compartilhamento NFS:**

Este teste comprova que:
- ✅ Master e Workers acessam o mesmo diretório `/apps`
- ✅ Arquivos são compartilhados via NFS
- ✅ Executors conseguem ler e processar dados compartilhados
- ✅ Resultados são salvos no diretório compartilhado
- ✅ Processamento distribuído funciona corretamente

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

### Spark Events

Os logs de eventos do Spark são salvos em `/opt/spark/events` dentro do container Master. Junto da inicialização do master 
os eventos tambem são montados em um volume local `./master/spark-events` para facilitar o acesso, e analisar o ciclo da aplicação sendo possivel acessa-los diretamente na máquina host através da url:

```bash
  http://localhost:18080
```