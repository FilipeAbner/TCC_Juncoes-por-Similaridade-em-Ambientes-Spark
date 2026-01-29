#!/usr/bin/env python3
"""
Junção por Similaridade usando BRIDk com Datasets Reais

Este script realiza junção por similaridade diversificada usando o algoritmo
BRIDk entre dois datasets. Cada elemento do Dataset A é usado como consulta
para encontrar k elementos similares e diversos do Dataset B.

Operação: A ⋈ B (similarity join)
- Para cada tupla em A, encontra k tuplas similares em B
- Garante diversidade nos resultados usando BRIDk
- Execução distribuída com PySpark

ALTERE O NUMERO DE PARTIÇÕES (d) CONFORME O NÚMERO DE WORKERS DISPONÍVEIS NO SEU CLUSTER.
"""

import os
import sys
import random
import numpy as np
import re
import time
import argparse
from pyspark.sql import SparkSession
from brid_python.spark.brid_spark import BridSpark
from brid_python.types.tuple import Tuple
from relatorio_juncao import gerar_estatisticas_juncao, gerar_relatorio_juncao, gerar_relatorio_debug
from selecao_similaridade_pivos import selecionar_pivos_kmeans_plus_plus
from utilidades_juncao import selecionar_amostra_dataset_a

def processar_argumentos():
    """
    Processa argumentos de linha de comando.
    
    Returns:
        argparse.Namespace: Argumentos parseados
    """
    parser = argparse.ArgumentParser(
        description='Junção por Similaridade usando BRIDk com Datasets Reais'
    )
    parser.add_argument(
        '--debug',
        type=int,
        default=None,
        help='ID da consulta para gerar relatório de debug detalhado'
    )
    parser.add_argument(
        '--d1',
        type=str,
        default='/apps/Datasets/Sintético/Sint2D-Gaussiana/sint2g.dataset.txt',
        help='Caminho para o Dataset A (consultas).'
    )
    parser.add_argument(
        '--d2',
        type=str,
        default='/apps/Datasets/Sintético/Sint2D-Gaussiana/sint2g.dataset.txt',
        help='Caminho para o Dataset B (busca).'
    )
    parser.add_argument(
        '--k',
        type=int,
        default=5,
        help='Número de vizinhos diversificados por consulta. Padrão: 5'
    )
    parser.add_argument(
        '--p',
        type=int,
        default=1,
        help='Porcentagem do Dataset B para selecionar como pivôs. Se não informado, usa 1 pivô.'
    )
    return parser.parse_args()

def calcular_distancia(tupla1, tupla2):
    """Calcula distância Euclidiana entre duas tuplas."""
    coords1 = np.array(tupla1.getAttributes())
    coords2 = np.array(tupla2.getAttributes())
    return np.linalg.norm(coords1 - coords2)

def calcular_distancias_aos_pivos(spark_session, dataset, pivos, num_particoes):
    """
    Pré-calcula as distâncias de cada objeto ao pivô da sua partição.
    
    ESTRATÉGIA DISTRIBUÍDA:
    - Cada partição tem 1 pivô
    - Objetos em cada partição calculam distância apenas ao pivô local
    - Processamento paralelo no Spark
    
    Estrutura de retorno:
    {
        tupla_id: dist_ao_pivo_da_particao
    }
    
    Args:
        spark_session: Sessão Spark
        dataset: Lista de tuplas
        pivos: Lista de pivôs (1 por partição)
        num_particoes: Número de partições
    
    Returns:
        dict: Dicionário com distâncias pré-calculadas {tupla_id: distancia_ao_pivo}
    """
    
    inicio = time.time()
    
    # Criar RDD particionado
    dataset_rdd = spark_session.sparkContext.parallelize(dataset, num_particoes)
    
    # Broadcast dos pivôs para todos os workers
    pivos_broadcast = spark_session.sparkContext.broadcast(pivos)
    
    def calcular_distancias_na_particao(partition_index, iterator):
        """
        Função executada em cada partição.
        Calcula distância de cada objeto ao pivô da partição.
        """
        pivos_locais = pivos_broadcast.value
        
        # Cada partição usa seu próprio pivô
        if partition_index < len(pivos_locais):
            pivo_local = pivos_locais[partition_index]
        else:
            # Fallback se houver mais partições que pivôs
            pivo_local = pivos_locais[partition_index % len(pivos_locais)]
        
        resultados = []
        for tupla in iterator:
            coords1 = np.array(tupla.getAttributes())
            coords2 = np.array(pivo_local.getAttributes())
            dist = np.linalg.norm(coords1 - coords2)
            resultados.append((tupla.getId(), dist))
        
        return iter(resultados)
    
    # Processar em paralelo
    distancias_rdd = dataset_rdd.mapPartitionsWithIndex(calcular_distancias_na_particao)
    
    # Coletar resultados
    distancias_pivos = dict(distancias_rdd.collect())
    
    tempo_decorrido = time.time() - inicio
    
    print(f"  ✓ Distâncias calculadas em {tempo_decorrido:.2f}s (distribuído)")
    print(f"  Total de distâncias: {len(distancias_pivos)}")
    print(f"  Taxa: {len(distancias_pivos) / tempo_decorrido:.0f} cálculos/s")
    
    return distancias_pivos

def criar_diretorio_resultado():
    """Cria o diretório de resultados se não existir."""
    result_dir = "/apps/result"
    os.makedirs(result_dir, exist_ok=True)
    return result_dir


def gerar_relatorio_simplificado(resultados, output_file):
    """
    Gera relatório simplificado no formato:
    ID_consulta
    ID_viz1 ID_viz2 ... ID_vizk
    
    Exemplo com k=3:
    1
    2 17 34
    2
    90 189 56
    ...
    
    Este formato permite fácil comparação entre execução distribuída e centralizada.
    
    Args:
        resultados: Dicionário com resultados da junção
        output_file: Caminho do arquivo de saída
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        for consulta_id in sorted(resultados.keys()):
            info = resultados[consulta_id]
            resultado_b = info['resultado_b']
            
            # Escrever ID da consulta
            f.write(f"{consulta_id}\n")
            
            # Escrever IDs dos vizinhos encontrados
            ids_vizinhos = [str(tupla.getId()) for tupla in resultado_b]
            f.write(' '.join(ids_vizinhos) + '\n')
    
    print(f"  ✓ Relatório simplificado gerado: {output_file}")


def ler_dataset(caminho_arquivo):
    """
    Lê um dataset no formato padrão.
    
    Formato do arquivo:
    - Linha 1: quantidade de tuplas
    - Linha 2: se tem ID (1 = sim, 0 = não)
    - Linha 3: número de dimensões
    - Linha 4: se tem informação textual ao final (1 = sim, 0 = não)
    - Linha 5: tamanho do bloco (ignorado)
    - Linha 6: nome do dataset
    - Demais linhas: dados no formato: ID dim1 dim2 ... dimN [texto_descritivo]
    
    Returns:
        tuple: (dataset, metadados, descricoes)
    """
    # Tentar abrir o arquivo com diferentes codificações
    codificacoes = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    linhas = None
    
    for encoding in codificacoes:
        try:
            with open(caminho_arquivo, 'r', encoding=encoding) as f:
                linhas = f.readlines()
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    if linhas is None:
        raise UnicodeDecodeError(f"Não foi possível ler o arquivo {caminho_arquivo} com nenhuma codificação conhecida")
    
    # Ler metadados (primeiras 6 linhas)
    num_tuplas = int(linhas[0].strip())
    tem_id = int(linhas[1].strip()) == 1
    num_dimensoes = int(linhas[2].strip())
    tem_texto = int(linhas[3].strip()) == 1
    tamanho_bloco = int(linhas[4].strip())
    nome_dataset = linhas[5].strip()
    
    metadados = {
        'num_tuplas': num_tuplas,
        'tem_id': tem_id,
        'num_dimensoes': num_dimensoes,
        'tem_texto': tem_texto,
        'tamanho_bloco': tamanho_bloco,
        'nome_dataset': nome_dataset,
        'caminho_arquivo': caminho_arquivo
    }
    
    # Ler dados
    dataset = []
    descricoes = {}
    
    for i in range(6, len(linhas)):
        linha = linhas[i].strip()
        if not linha:
            continue
            
        partes = re.split(r'\s+', linha.strip())
        
        # Extrair ID
        id_obj = int(partes[0]) if tem_id else i - 5
        
        # Extrair dimensões
        inicio_dim = 1 if tem_id else 0
        fim_dim = inicio_dim + num_dimensoes
        
        dimensoes = []
        for j in range(inicio_dim, fim_dim):
            try:
                valor = float(partes[j]) if partes[j] else 0.0
                dimensoes.append(valor)
            except (ValueError, IndexError):
                dimensoes.append(0.0)
        
        # Extrair descrição textual se existir
        if tem_texto and len(partes) > fim_dim:
            descricao = '\t'.join(partes[fim_dim:])
            descricoes[id_obj] = descricao
        
        # Criar Tuple
        tupla = Tuple(dimensoes)
        tupla.setId(id_obj)
        if id_obj in descricoes:
            tupla.setDescription(descricoes[id_obj])
        dataset.append(tupla)
    
    return dataset, metadados, descricoes


def executar_juncao_similaridade(spark_session, dataset_a_rdd_particionado, dataset_b_rdd_particionado, k=10, d=1):
    """
    Executa a junção por similaridade: A ⋈ B
    
    Para cada tupla em A, encontra k tuplas similares e diversas em B usando BRIDk.
    Usa método dos pivôs, com dataset B já particionado por pivôs.
    
    Args:
        spark_session: Sessão Spark ativa
        dataset_a_rdd_particionado: RDD do dataset A particionado
        dataset_b_rdd_particionado: RDD do dataset B particionado por pivôs
        k: Número de vizinhos diversificados a selecionar
        d: Número de partições (diversidade)
    
    Returns:
        dict: Dicionário com resultados para cada tupla de A
    """

    brid = BridSpark(spark_session)
    resultados = {}
    
    # Estatísticas globais
    estatisticas_globais = {
        'tempo_bridk': 0,
        'total_distance_calculations': 0,
        'total_cache_hits': 0,
        'total_cache_hit_rate': 0
    }
    
    print(f"\nProcessando consultas...")
    progresso_anterior = 0
    
    # Coletar o RDD em uma lista para iteração local
    dataset_a_consultas = dataset_a_rdd_particionado.collect()
    
    # Executa BRIDk para cada consulta em A
    for i, consulta_a in enumerate(dataset_a_consultas, 1):
        # Mostrar progresso a cada 10%
        progresso = int((i / len(dataset_a_consultas)) * 100)
        if progresso >= progresso_anterior + 10:
            print(f"  Progresso: {progresso}% ({i}/{len(dataset_a_consultas)} consultas processadas)")
            progresso_anterior = progresso
        
        # Executar BRIDk no RDD completo
        inicio_bridk = time.time()
        
        resultado_b, debug_info = brid.execute_from_rdd(
            dataset_rdd=dataset_b_rdd_particionado,
            query=consulta_a,
            k=k,
            num_partitions=d,
            return_debug_info=False,
            custom_partitioner='pivot_based'
        )

        tempo_bridk = time.time() - inicio_bridk
        estatisticas_globais['tempo_bridk'] += tempo_bridk
        
        # Acumular estatísticas de cache
        if debug_info:
            estatisticas_globais['total_distance_calculations'] += debug_info.get('distance_calculations', 0)
            estatisticas_globais['total_cache_hits'] += debug_info.get('cache_hits', 0)
        
        # Armazenar resultados
        resultado_info = {
            'consulta_a': consulta_a,
            'resultado_b': resultado_b,
            'debug_info': debug_info,
            'k': k,
            'd': d
        }
        
        resultados[consulta_a.getId()] = resultado_info
    
    # Calcular taxa média de acerto do cache
    total_ops = estatisticas_globais['total_distance_calculations'] + estatisticas_globais['total_cache_hits']
    if total_ops > 0:
        estatisticas_globais['total_cache_hit_rate'] = (estatisticas_globais['total_cache_hits'] / total_ops) * 100
    
    print(f"\n  Progresso: 100% ({len(dataset_a_consultas)}/{len(dataset_a_consultas)} consultas processadas)")
    print(f"Tempo Execução Bridk: {estatisticas_globais['tempo_bridk']}s")
    print(f"Total de cálculos de distância: {estatisticas_globais['total_distance_calculations']}")
    print(f"Total de acertos no cache: {estatisticas_globais['total_cache_hits']}")
    print(f"Taxa de acerto do cache: {estatisticas_globais['total_cache_hit_rate']:.2f}%")

    return resultados, estatisticas_globais

# Particionar os objetos do dataset para o pivô mais próximo
def particionar_por_pivo_mais_proximo(obj, pivos_bc):
    """Atribui objeto ao índice do pivô mais próximo."""
    pivos = pivos_bc.value
    min_dist = float('inf')
    pivo_idx = 0
    for i, pivo in enumerate(pivos):
        coords_obj = np.array(obj.getAttributes())
        coords_pivo = np.array(pivo.getAttributes())
        dist = np.linalg.norm(coords_obj - coords_pivo)
        if dist < min_dist:
            min_dist = dist
            pivo_idx = i
    return pivo_idx

def main():
    """Função principal."""
    
    # Processar argumentos de linha de comando
    args = processar_argumentos()
    
    # CONFIGURAÇÃO: As seguintes configurações sao usadas como True apenas para pequenos testes,
    # para produção em datasets maiores, recomenda-se False

    GERAR_RELATORIO = True  # Altere para False para desabilitar relatórios
    GERAR_RELATORIO_SIMPLIFICADO = True  # Relatório simplificado (IDs apenas)
    GERAR_GRAFICO = False   # Altere para False para desabilitar geração grafica
    LISTAR_OBJETOS_POR_PIVO = False  # True: exibe objetos associados a cada pivô
    # Criar diretório de resultados
    result_dir = criar_diretorio_resultado()
    
    # Definir caminhos dos datasets usando argumentos ou padrões
    caminho_dataset_a = args.d1
    caminho_dataset_b = args.d2
    
    print(f"\nDatasets:")
    print(f"  Dataset A (--d1): {caminho_dataset_a}")
    print(f"  Dataset B (--d2): {caminho_dataset_b}")

    # Ler Dataset A (consultas)
    dataset_a, metadados_a, descricoes_a = ler_dataset(caminho_dataset_a)
    
    # Ler Dataset B (busca)
    dataset_b, metadados_b, descricoes_b = ler_dataset(caminho_dataset_b)
    
    # Verificar compatibilidade de dimensões: TO DO: Permitir comparação entre datasets com diferentes dimensões
    # Agora permitimos dimensões diferentes, padronizando para a dimensão máxima
    dim_max = max(metadados_a['num_dimensoes'], metadados_b['num_dimensoes'])
    
    def pad_attributes(tupla, dim_max):
        attrs = tupla.getAttributes()
        if len(attrs) < dim_max:
            padded = attrs + [0.0] * (dim_max - len(attrs))
            tupla.setAttributes(padded)
        return tupla
    
    # Padronizar todos os objetos para dim_max
    dataset_a = [pad_attributes(t, dim_max) for t in dataset_a]
    dataset_b = [pad_attributes(t, dim_max) for t in dataset_b]
    
    # Atualizar metadados
    metadados_a['num_dimensoes'] = dim_max
    metadados_b['num_dimensoes'] = dim_max
        
    # print(f"\nDataset A:" )
    # for tupla in dataset_a[:5]:
    #     print(tupla)
    # print(f"\nDataset B:" )
    # for tupla in dataset_b[:5]:
    #     print(tupla)
    spark = SparkSession.builder \
        .appName("Juncao por Similaridade - BRIDk") \
        .getOrCreate()
    
    print(f"\nSparkSession criada com sucesso!")

    
    # Selecionar amostra do Dataset A (ou usar tudo)
    # Para testes utilizar um numero limitado de consultas
    # Usar None para todas as tuplas
    # max_consultas = 100  
    max_consultas = 100
    percent= False
    dataset_a_consultas = selecionar_amostra_dataset_a(dataset_a, max_consultas,percent)
    
    # Parâmetros da junção
    k = args.k  # Número de vizinhos diversificados por consulta (via argumento --k)
    # d = 2   # Número de partições == numero de pivos
    # O numero de pivos é igual ao numero de partições

    # Configuração para seleção de pivôs
    use_percent_pivos = False  # Se True, usa porcentagem; se False, usa d diretamente
    qtd_pivos = args.p  # Porcentagem do dataset B para selecionar como pivôs (usado se use_percent_pivos=True)

    if use_percent_pivos:
        num_pivos = max(1, int(len(dataset_b) * qtd_pivos / 100))
        print(f"Selecionando {qtd_pivos}% do Dataset B como pivôs: {num_pivos} pivôs")
    else:
        num_pivos = qtd_pivos
        print(f"Selecionando {num_pivos} pivôs conforme parâmetro d")

    pivos = selecionar_pivos_kmeans_plus_plus(dataset_b, num_pivos, seed=42)
    
    # Atualizar d para ser igual ao número de pivôs selecionados
    d = len(pivos)
    print(f'Quantiodade de pivos igual a {d}')

    # Broadcast dos pivôs
    pivos_bc = spark.sparkContext.broadcast(pivos)
    
    # Particionar dataset B usando pivôs (antes da junção)
    dataset_b_com_particao = spark.sparkContext.parallelize(dataset_b) \
        .map(lambda obj: (particionar_por_pivo_mais_proximo(obj, pivos_bc), obj))

    # Criar RDD particionado por pivô
    dataset_b_rdd_particionado = dataset_b_com_particao.partitionBy(d).map(lambda x: x[1])

    # Particionar dataset A de acordo com os pivos
    dataset_a_com_particao = spark.sparkContext.parallelize(dataset_a_consultas) \
        .map(lambda obj: (particionar_por_pivo_mais_proximo(obj, pivos_bc), obj))

    # Criar RDD particionado por pivô
    dataset_a_rdd_particionado = dataset_a_com_particao.partitionBy(d).map(lambda x: x[1])

    # na função executar_juncao_similaridade utiliza o dataset como um RDD particionado:
    resultados, estatisticas_filtragem = executar_juncao_similaridade(
        spark_session=spark,
        dataset_a_rdd_particionado=dataset_a_rdd_particionado,
        dataset_b_rdd_particionado=dataset_b_rdd_particionado,  
        k=k,
        d=d
    )

# ----------------Relatorio ----------------------- 
    # Coletar contagens e objetos por pivô para relatórios 
    # Dataset B
    counts_b_list = dataset_b_com_particao \
        .aggregateByKey(0, lambda acc, _: acc + 1, lambda acc1, acc2: acc1 + acc2) \
        .collect()
    
    counts_b = dict(counts_b_list)

    objetos_por_pivo_b = {}
    if LISTAR_OBJETOS_POR_PIVO:
        objetos_agrupados_b = dataset_b_com_particao.groupByKey().collect()
        for pivo_idx, objetos in objetos_agrupados_b:
            lista = []
            for obj in objetos:
                lista.append({
                    'id': obj.getId(),
                    'coords': obj.getAttributes(),
                    'descricao': descricoes_b.get(obj.getId(), '')
                })
            objetos_por_pivo_b[int(pivo_idx)] = lista

    # Preencher partições vazias
    for i in range(d):
        counts_b.setdefault(i, 0)
        objetos_por_pivo_b.setdefault(i, [])


    # Dataset A
    counts_a_list = dataset_a_com_particao \
        .aggregateByKey(0, lambda acc, _: acc + 1, lambda acc1, acc2: acc1 + acc2) \
        .collect()
    counts_a = dict(counts_a_list)

    objetos_por_pivo_a = {}
    if LISTAR_OBJETOS_POR_PIVO:
        objetos_agrupados_a = dataset_a_com_particao.groupByKey().collect()
        for pivo_idx, objetos in objetos_agrupados_a:
            lista = []
            for obj in objetos:
                lista.append({
                    'id': obj.getId(),
                    'coords': obj.getAttributes(),
                    'descricao': descricoes_a.get(obj.getId(), '')
                })
            objetos_por_pivo_a[int(pivo_idx)] = lista

    for i in range(d):
        counts_a.setdefault(i, 0)
        objetos_por_pivo_a.setdefault(i, [])

# ----------------Relatorio -----------------------

    
    partition_counts = {
        'A': counts_a,
        'B': counts_b,
        'num_partitions': d
    }
    
    # Preparar informações dos pivôs para o relatório (se solicitado)
    pivos_info = None
    if LISTAR_OBJETOS_POR_PIVO:
        pivos_info = {
            'pivos': pivos,
            'objetos_a': objetos_por_pivo_a,
            'objetos_b': objetos_por_pivo_b,
            'descricoes_a': descricoes_a,
            'descricoes_b': descricoes_b
        }
    
    estatisticas = gerar_estatisticas_juncao(resultados)

    # Gerar relatórios (se habilitado)
    if GERAR_RELATORIO:
        nome_arquivo_base = f"juncao_{metadados_a['nome_dataset']}_{metadados_b['nome_dataset']}_p{d}"
        output_report = os.path.join(result_dir, f"{nome_arquivo_base}_{k}_relatorio.txt")
        
        # Relatório textual detalhado
        gerar_relatorio_juncao(resultados, descricoes_a, descricoes_b,
                              metadados_a, metadados_b, estatisticas,
                              output_report, max_exemplos=20,
                              partition_counts=partition_counts,
                              pivos_info=pivos_info,
                              filtering_stats=estatisticas_filtragem)
        
        # Relatório simplificado (IDs apenas)
        if GERAR_RELATORIO_SIMPLIFICADO:
            output_simplificado = os.path.join(result_dir, f"{nome_arquivo_base}_{k}_p{d}_simplificado.txt")
            gerar_relatorio_simplificado(resultados, output_simplificado)
        
        # Gerar relatório de debug para consulta específica (se solicitado)
        if args.debug is not None:
            if args.debug in resultados:
                debug_file = os.path.join(result_dir, f"debug_consulta_{args.debug}.txt")
                gerar_relatorio_debug(
                    consulta_id=args.debug,
                    resultados=resultados,
                    descricoes_a=descricoes_a,
                    descricoes_b=descricoes_b,
                    dataset_b=dataset_b,
                    pivos=pivos,
                    output_file=debug_file
                )
                print(f"\nRelatório de debug gerado: {debug_file} \n")
            else:
                print(f"\nAVISO: Consulta ID {args.debug} não encontrada nos resultados.")
                print(f"IDs disponíveis: {sorted(list(resultados.keys())[:10])}...")

    print("EXECUÇÃO CONCLUÍDA COM SUCESSO!")
    print("=" * 70)
    print(f"\nResumo da Junção:")
    print(f"  Dataset A: {metadados_a['nome_dataset']} ({metadados_a['num_tuplas']} tuplas)")
    print(f"  Dataset B: {metadados_b['nome_dataset']} ({metadados_b['num_tuplas']} tuplas)")
    print(f"  Consultas executadas: {len(resultados)}")
    print(f"  Parâmetros: k={k}, d={d}")
    
    # Gerar gráfico visual (opcional)
    if GERAR_GRAFICO:
        # import lazy para evitar carregar matplotlib se não for necessário
        try:
            from relatorio_visual import gerar_grafico_juncao
        except Exception as e:
            print(f"Aviso: não foi possível importar relatorio_visual: {e}")
        else:
            # opcional: limitar nº de consultas a plotar para não lotar o gráfico
            def _filter_results(resultados, max_queries=5):
                keys = list(resultados.keys())[:max_queries]
                return {k: resultados[k] for k in keys}

            max_queries_plot = 5  # ajuste conforme necessidade
            resultados_plot = _filter_results(resultados, max_queries_plot)

            if resultados_plot:
                output_img = os.path.join(result_dir, f"{nome_arquivo_base}_visual.png")
                # Passar dataset_b e dataset_a (consultas) para o plot; pivos=None se não houver
                gerar_grafico_juncao(resultados_plot, dataset_b=dataset_b,
                     dataset_a=dataset_a_consultas, pivos=pivos,
                     output_file=output_img)

            else:
                print("Aviso: nenhum resultado disponível para plotar.")

    if GERAR_RELATORIO:
        print(f"\nArquivos gerados:")
        print(f"  Relatório: {output_report}")
        
    # Encerrar Spark
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nERRO: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
