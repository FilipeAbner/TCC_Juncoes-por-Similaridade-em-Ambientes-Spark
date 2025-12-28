#!/usr/bin/env python3
"""
Seleção por Similaridade usando BRIDk com Método dos Pivôs

Este script implementa seleção por similaridade diversificada usando o algoritmo
BRIDk otimizado com o método dos pivôs (pivot-based indexing).

Método dos Pivôs:
- Seleciona p pivôs representativos do dataset
- Pré-calcula distâncias de todos os objetos aos pivôs
- Usa desigualdade triangular para podar buscas
- Reduz drasticamente o número de cálculos de distância

Dataset: Autos (1046 tuplas, 9 dimensões)
- Características técnicas de veículos
- Execução distribuída com PySpark
- Indexação por pivôs para otimização

ALTERE O NUMERO DE PARTIÇÕES (d) E PIVÔS (p) CONFORME SEU CLUSTER.
"""

import os
import sys
import random
import time
from pyspark.sql import SparkSession
from brid_python.spark.brid_spark import BridSpark
from brid_python.types.tuple import Tuple

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np


def criar_diretorio_resultado():
    """Cria o diretório de resultados se não existir."""
    result_dir = "/apps/result"
    os.makedirs(result_dir, exist_ok=True)
    return result_dir


def ler_dataset_generico(caminho_arquivo):
    """
    Lê um dataset no formato padrão.
    
    Returns:
        tuple: (dataset, metadados, descricoes)
    """
    print(f"\nLendo dataset: {caminho_arquivo}")
    print("-" * 70)
    
    with open(caminho_arquivo, 'r') as f:
        linhas = f.readlines()
    
    # Ler metadados
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
    
    print(f"  Nome: {nome_dataset}")
    print(f"  Tuplas: {num_tuplas}")
    print(f"  Dimensões: {num_dimensoes}")
    
    # Ler dados
    dataset = []
    descricoes = {}
    
    for i in range(6, len(linhas)):
        linha = linhas[i].strip()
        if not linha:
            continue
            
        partes = linha.split('\t')
        
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
    
    print(f"  Carregadas: {len(dataset)} tuplas")
    
    return dataset, metadados, descricoes


def calcular_distancia(tupla1, tupla2):
    """Calcula distância Euclidiana entre duas tuplas."""
    coords1 = np.array(tupla1.getAttributes())
    coords2 = np.array(tupla2.getAttributes())
    return np.linalg.norm(coords1 - coords2)


def selecionar_pivos_kmeans_plus_plus(dataset, num_pivos, seed=42):
    """
    Seleciona pivôs usando estratégia k-means++.
    
    Algoritmo k-means++:
    1. Escolhe primeiro pivô aleatoriamente
    2. Para cada próximo pivô:
       - Calcula distância de cada ponto ao pivô mais próximo
       - Escolhe novo pivô com probabilidade proporcional à distância²
    3. Isso espalha os pivôs pelo espaço
    
    Args:
        dataset: Lista de tuplas
        num_pivos: Número de pivôs a selecionar
        seed: Semente para reprodutibilidade
    
    Returns:
        list: Lista de tuplas selecionadas como pivôs
    """
    
    random.seed(seed)
    np.random.seed(seed)
    
    if num_pivos >= len(dataset):
        print(f"  AVISO: Número de pivôs ({num_pivos}) >= tamanho dataset ({len(dataset)})")
        return random.sample(dataset, len(dataset))
    
    pivos = []
    
    # 1. Primeiro pivô: escolher aleatoriamente
    primeiro_pivo = random.choice(dataset)
    pivos.append(primeiro_pivo)
    pivos_ids = set([primeiro_pivo.getId()])
    
    # 2. Pivôs seguintes: k-means++
    for i in range(1, num_pivos):
        # Calcular distância mínima de cada ponto aos pivôs existentes
        distancias_min = []
        
        for tupla in dataset:
            # Pula se já é pivô
            if tupla.getId() in pivos_ids:
                distancias_min.append(0)
                continue
            
            # Distância ao pivô mais próximo
            dist_min = min([calcular_distancia(tupla, pivo) for pivo in pivos])
            distancias_min.append(dist_min)
        
        # Converter para array numpy
        distancias_min = np.array(distancias_min)
        
        # Calcular probabilidades proporcionais a distância²
        probabilidades = distancias_min ** 2
        soma_prob = probabilidades.sum()
        
        if soma_prob > 0:
            probabilidades = probabilidades / soma_prob
        else:
            # Se todas as distâncias são zero, usar probabilidade uniforme
            probabilidades = np.ones(len(dataset)) / len(dataset)
        
        # Selecionar próximo pivô
        idx_proximo_pivo = np.random.choice(len(dataset), p=probabilidades)
        proximo_pivo = dataset[idx_proximo_pivo]
        pivos.append(proximo_pivo)
        pivos_ids.add(proximo_pivo.getId())  # Adicionar ao conjunto
            
    return pivos


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
    print(f"\nPré-calculando distâncias distribuídas (1 pivô por partição)...")
    print(f"  Partições: {num_particoes}")
    print(f"  Pivôs: {len(pivos)}")
    print(f"  Dataset: {len(dataset)} tuplas")
    print(f"  Cálculos por partição: ~{len(dataset) // num_particoes} distâncias")
    
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


def filtrar_com_desigualdade_triangular(consulta, dataset, pivos, 
                                       distancias_pivos_dataset, raio):
    """
    Filtra candidatos usando desigualdade triangular.
    
    ESTRATÉGIA COM 1 PIVÔ POR PARTIÇÃO:
    - Cada objeto tem distância pré-calculada ao pivô da sua partição
    - Consulta calcula distância a todos os pivôs
    - Usa desigualdade triangular: |d(q,p) - d(x,p)| ≤ d(q,x)
    
    Desigualdade Triangular:
    Para qualquer três pontos q (consulta), p (pivô), x (objeto):
    |d(q,p) - d(x,p)| ≤ d(q,x) ≤ d(q,p) + d(x,p)
    
    Lower Bound (limite inferior):
    d(q,x) ≥ |d(q,p) - d(x,p)|
    
    Se |d(q,p) - d(x,p)| > raio, então d(q,x) > raio
    Logo, x pode ser descartado sem calcular d(q,x)!
    
    Args:
        consulta: Tupla de consulta
        dataset: Lista de todas as tuplas
        pivos: Lista de pivôs
        distancias_pivos_dataset: Dict {tupla_id: distancia_ao_pivo_da_particao}
        raio: Raio de busca
    
    Returns:
        tuple: (candidatos_filtrados, estatisticas)
    """
    # Calcular distâncias da consulta a todos os pivôs
    distancias_consulta_pivos = []
    for pivo in pivos:
        dist = calcular_distancia(consulta, pivo)
        distancias_consulta_pivos.append(dist)
    
    candidatos = []
    descartados_total = 0
    
    for tupla in dataset:
        tupla_id = tupla.getId()
        
        # Não considerar a própria consulta
        if tupla_id == consulta.getId():
            candidatos.append(tupla)
            continue
        
        # Pegar distância pré-calculada desta tupla ao seu pivô local
        d_xp = distancias_pivos_dataset.get(tupla_id, float('inf'))
        
        # Aplicar desigualdade triangular usando o pivô mais próximo da consulta
        # Como não sabemos qual partição o objeto está, testamos com todos os pivôs
        pode_descartar = False
        
        for d_qp in distancias_consulta_pivos:
            # Lower bound: d(q,x) ≥ |d(q,p) - d(x,p)|
            lower_bound = abs(d_qp - d_xp)
            
            # Se lower_bound > raio, então d(q,x) > raio com certeza
            if lower_bound > raio:
                pode_descartar = True
                break
        
        if pode_descartar:
            descartados_total += 1
        else:
            candidatos.append(tupla)
    
    estatisticas = {
        'total_dataset': len(dataset),
        'candidatos': len(candidatos),
        'descartados': descartados_total,
        'taxa_filtragem': (descartados_total / len(dataset)) * 100 if len(dataset) > 0 else 0
    }
    
    return candidatos, estatisticas


def selecionar_consultas_aleatorias(dataset, num_consultas=10, seed=42, pivos=None):
    """
    Seleciona consultas aleatórias do dataset.
    Garante que consultas não sejam pivôs.
    
    Args:
        dataset: Lista de tuplas
        num_consultas: Número de consultas
        seed: Semente de aleatoriedade
        pivos: Lista de pivôs (opcional) - consultas não podem ser pivôs
    
    Returns:
        list: Lista de consultas selecionadas
    """
    random.seed(seed)
    
    # Se há pivôs, remover da lista de candidatos
    if pivos:
        ids_pivos = {p.getId() for p in pivos}
        candidatos = [t for t in dataset if t.getId() not in ids_pivos]
        print(f"  Candidatos para consulta: {len(candidatos)} (excluindo {len(pivos)} pivôs)")
    else:
        candidatos = dataset
    
    return random.sample(candidatos, min(num_consultas, len(candidatos)))


def executar_selecao_similaridade_com_pivos(spark_session, dataset, consultas, 
                                            pivos, distancias_pivos_dataset,
                                            k=10, d=1):
    """
    Executa seleção por similaridade usando BRIDk + método dos pivôs.
    
    Fluxo:
    1. Para cada consulta:
       a. Calcula distâncias da consulta aos pivôs
       b. Estima raio inicial (heurística)
       c. Filtra candidatos usando desigualdade triangular
       d. Aplica BRIDk nos candidatos filtrados
    
    Args:
        spark_session: Sessão Spark
        dataset: Dataset completo
        consultas: Lista de consultas
        pivos: Lista de pivôs
        distancias_pivos_dataset: Distâncias pré-calculadas
        k: Número de vizinhos
        d: Número de partições
    
    Returns:
        dict: Resultados com estatísticas de filtragem
    """
    print("\n" + "=" * 70)
    print(f"EXECUÇÃO COM MÉTODO DOS PIVÔS")
    print("=" * 70)
    print(f"\nParâmetros:")
    print(f"  k (vizinhos): {k}")
    print(f"  d (partições): {d}")
    print(f"  p (pivôs): {len(pivos)}")
    print(f"  Consultas: {len(consultas)}")
    print(f"  Tamanho dataset: {len(dataset)}")
    
    brid = BridSpark(spark_session)
    resultados = {}
    
    estatisticas_globais = {
        'total_candidatos': 0,
        'total_descartados': 0,
        'tempo_filtragem': 0,
        'tempo_bridk': 0,
        'calculos_distancia_economizados': 0
    }
    
    for idx, consulta in enumerate(consultas, 1):
        print(f"\n{'─' * 70}")
        print(f"Consulta {idx}/{len(consultas)} - ID: {consulta.getId()}")
        print(f"{'─' * 70}")
        
        # Estimar raio inicial baseado em k e número de pivôs
        # Heurística: usar distância média aos pivôs como referência
        distancias_consulta_pivos = [calcular_distancia(consulta, p) for p in pivos]
        raio_estimado = np.mean(distancias_consulta_pivos) * 1.5
        
        print(f"  Raio estimado para filtragem: {raio_estimado:.4f}")
        
        # Filtrar candidatos usando desigualdade triangular
        inicio_filtragem = time.time()
        
        candidatos, stats_filtragem = filtrar_com_desigualdade_triangular(
            consulta, dataset, pivos, distancias_pivos_dataset, raio_estimado
        )
        
        tempo_filtragem = time.time() - inicio_filtragem
        
        print(f"  Filtragem completada em {tempo_filtragem:.4f}s:")
        print(f"    Candidatos: {stats_filtragem['candidatos']}/{stats_filtragem['total_dataset']}")
        print(f"    Descartados: {stats_filtragem['descartados']}")
        print(f"    Taxa de filtragem: {stats_filtragem['taxa_filtragem']:.2f}%")
        
        # Aplicar BRIDk nos candidatos filtrados
        print(f"  Executando BRIDk em {len(candidatos)} candidatos...")
        
        inicio_bridk = time.time()
        
        candidatos_rdd = spark_session.sparkContext.parallelize(candidatos)
        resultado, debug_info = brid.execute_from_rdd(
            dataset_rdd=candidatos_rdd,
            query=consulta,
            k=k,
            num_partitions=d,
            return_debug_info=True,
            custom_partitioner='x_sign'  # Particionar por sinal de X
        )
        
        tempo_bridk = time.time() - inicio_bridk
        
        print(f"  BRIDk completado em {tempo_bridk:.4f}s")
        print(f"  Resultado: {len(resultado)} elementos selecionados")
        
        # Log de debug das partições
        print(f"\n  DEBUG - Candidatos por partição:")
        for partition_idx in sorted(debug_info['partition_candidates'].keys()):
            candidates = debug_info['partition_candidates'][partition_idx]
            all_elements = debug_info['partition_all_elements'][partition_idx]
            
            print(f"\n    Partição {partition_idx}:")
            print(f"      Total de elementos na partição: {len(all_elements)}")
            print(f"      Candidatos selecionados pelo BRIDk local: {len(candidates)}")
            
            # Mostrar alguns elementos da partição
            print(f"      Primeiros 5 elementos da partição (ordenados por distância):")
            sorted_elements = sorted(all_elements, key=lambda e: e[2])
            for elem_id, x_coord, dist in sorted_elements[:5]:
                print(f"        ID {elem_id}: X={x_coord:.4f}, dist={dist:.4f}")
            
            print(f"      Candidatos selecionados:")
            for cand in candidates:
                dist = calcular_distancia(consulta, cand)
                x_coord = cand.getAttributes()[0]
                print(f"        ID {cand.getId()}: X={x_coord:.4f}, dist={dist:.4f}")
        
        # Armazenar resultados com informações de debug
        resultados[consulta.getId()] = {
            'consulta': consulta,
            'resultado': resultado,
            'k': k,
            'd': d,
            'p': len(pivos),
            'estatisticas_filtragem': stats_filtragem,
            'tempo_filtragem': tempo_filtragem,
            'tempo_bridk': tempo_bridk,
            'raio_estimado': raio_estimado,
            'debug_info': debug_info
        }
        
        # Atualizar estatísticas globais
        estatisticas_globais['total_candidatos'] += stats_filtragem['candidatos']
        estatisticas_globais['total_descartados'] += stats_filtragem['descartados']
        estatisticas_globais['tempo_filtragem'] += tempo_filtragem
        estatisticas_globais['tempo_bridk'] += tempo_bridk
        
        # Calcular economia de cálculos de distância
        # Cada objeto descartado economiza 1 cálculo de distância
        estatisticas_globais['calculos_distancia_economizados'] += stats_filtragem['descartados']
    
    # Resumo final
    print(f"\n{'=' * 70}")
    print(f"RESUMO DA EXECUÇÃO COM PIVÔS")
    print(f"{'=' * 70}")
    print(f"\nEstatísticas Globais:")
    print(f"  Total de candidatos processados: {estatisticas_globais['total_candidatos']}")
    print(f"  Total de objetos descartados: {estatisticas_globais['total_descartados']}")
    print(f"  Cálculos de distância economizados: {estatisticas_globais['calculos_distancia_economizados']}")
    
    taxa_reducao = (estatisticas_globais['total_descartados'] / 
                   (estatisticas_globais['total_candidatos'] + estatisticas_globais['total_descartados'])) * 100
    print(f"  Taxa de redução: {taxa_reducao:.2f}%")
    
    print(f"\nTempo total:")
    print(f"  Filtragem: {estatisticas_globais['tempo_filtragem']:.2f}s")
    print(f"  BRIDk: {estatisticas_globais['tempo_bridk']:.2f}s")
    print(f"  Total: {estatisticas_globais['tempo_filtragem'] + estatisticas_globais['tempo_bridk']:.2f}s")
    
    resultados['_estatisticas_globais'] = estatisticas_globais
    
    return resultados


def visualizar_pivos_e_resultados(dataset, pivos, resultados, descricoes, output_file):
    """
    Visualiza os pivôs e resultados.
    Se dados são 2D: usa coordenadas diretas
    Se dados são >2D: aplica PCA para redução
    """
    print(f"\nGerando visualização com pivôs: {output_file}")
    
    # Extrair coordenadas
    dataset_coords = np.array([t.getAttributes() for t in dataset])
    pivos_coords = np.array([p.getAttributes() for p in pivos])
    
    num_dimensoes = dataset_coords.shape[1]
    print(f"  Dimensões do dataset: {num_dimensoes}")
    
    # Se já é 2D, usar coordenadas diretas
    if num_dimensoes == 2:
        print(f"  Dataset já é 2D - usando coordenadas originais")
        dataset_2d = dataset_coords
        pivos_2d = pivos_coords
        variancia_explicada = 100.0
    else:
        # Aplicar PCA para redução dimensional
        try:
            from sklearn.decomposition import PCA
        except ImportError:
            print("\nAVISO: scikit-learn não instalado. Visualização não será gerada.")
            return
        
        print(f"  Aplicando PCA para redução {num_dimensoes}D → 2D")
        pca = PCA(n_components=2)
        dataset_2d = pca.fit_transform(dataset_coords)
        pivos_2d = pca.transform(pivos_coords)
        variancia_explicada = sum(pca.explained_variance_ratio_) * 100
        print(f"  Variância explicada: {variancia_explicada:.2f}%")
    
    # Remover estatísticas globais do dicionário de resultados
    resultados_consultas = {k: v for k, v in resultados.items() if k != '_estatisticas_globais'}
    
    # Criar figura
    num_consultas = len(resultados_consultas)
    cols = min(3, num_consultas)
    rows = (num_consultas + cols - 1) // cols
    
    fig, axes = plt.subplots(rows, cols, figsize=(6*cols, 5*rows))
    if num_consultas == 1:
        axes = [axes]
    else:
        axes = axes.flatten() if num_consultas > 1 else [axes]
    
    for idx, (consulta_id, info) in enumerate(resultados_consultas.items()):
        ax = axes[idx]
        
        consulta = info['consulta']
        resultado = info['resultado']
        stats = info['estatisticas_filtragem']
        
        # Transformar para 2D
        consulta_coords = np.array([consulta.getAttributes()])
        resultado_coords = np.array([t.getAttributes() for t in resultado])
        
        if num_dimensoes == 2:
            # Usar coordenadas diretas
            consulta_2d = consulta_coords
            resultado_2d = resultado_coords
        else:
            # Aplicar PCA
            consulta_2d = pca.transform(consulta_coords)
            resultado_2d = pca.transform(resultado_coords)
        
        # Plotar dataset (preto, pequeno)
        ax.scatter(dataset_2d[:, 0], dataset_2d[:, 1], 
                  c='black', s=10, alpha=0.2, label='Dataset')
        
        # Plotar pivôs (azul, diamante)
        ax.scatter(pivos_2d[:, 0], pivos_2d[:, 1],
                  c='blue', s=200, marker='D', alpha=0.8, 
                  edgecolors='darkblue', linewidths=2, label=f'Pivôs (p={info["p"]})')
        
        # Plotar selecionados (verde)
        ax.scatter(resultado_2d[:, 0], resultado_2d[:, 1],
                  c='green', s=100, alpha=0.7, edgecolors='darkgreen',
                  linewidths=2, label=f'Selecionados (k={info["k"]})')
        
        # Plotar consulta (vermelho, estrela)
        ax.scatter(consulta_2d[:, 0], consulta_2d[:, 1],
                  c='red', s=300, marker='*', edgecolors='darkred',
                  linewidths=2, label='Consulta', zorder=5)
        
        # Título
        titulo = f"Consulta ID {consulta_id}"
        if consulta_id in descricoes:
            desc_curta = descricoes[consulta_id][:30] + "..." if len(descricoes[consulta_id]) > 30 else descricoes[consulta_id]
            titulo += f"\n{desc_curta}"
        titulo += f"\nFiltragem: {stats['taxa_filtragem']:.1f}% ({stats['descartados']}/{stats['total_dataset']})"
        
        ax.set_title(titulo, fontsize=9, fontweight='bold')
        
        # Eixos
        if num_dimensoes == 2:
            ax.set_xlabel('X')
            ax.set_ylabel('Y')
        else:
            ax.set_xlabel(f'PC1 ({variancia_explicada * pca.explained_variance_ratio_[0]:.1f}%)')
            ax.set_ylabel(f'PC2 ({variancia_explicada * pca.explained_variance_ratio_[1]:.1f}%)')
        ax.legend(loc='best', fontsize=7)
        ax.grid(True, alpha=0.3)
    
    # Remover subplots vazios
    for idx in range(num_consultas, len(axes)):
        fig.delaxes(axes[idx])
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"Visualização salva: {output_file}")


def gerar_relatorio_com_pivos(resultados, descricoes, metadados, pivos, output_file):
    """Gera relatório detalhado incluindo estatísticas de pivôs."""
    print(f"\nGerando relatório: {output_file}")
    
    # Remover estatísticas globais
    estatisticas_globais = resultados.pop('_estatisticas_globais', None)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("RELATÓRIO DE SELEÇÃO POR SIMILARIDADE - BRIDk + MÉTODO DOS PIVÔS\n")
        f.write("=" * 80 + "\n\n")
        
        f.write("INFORMAÇÕES DO DATASET\n")
        f.write("-" * 80 + "\n")
        f.write(f"Nome: {metadados['nome_dataset']}\n")
        f.write(f"Total de tuplas: {metadados['num_tuplas']}\n")
        f.write(f"Dimensões: {metadados['num_dimensoes']}\n\n")
        
        f.write("MÉTODO DOS PIVÔS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Número de pivôs: {len(pivos)}\n")
        f.write(f"Estratégia de seleção: k-means++\n\n")
        f.write("IDs dos Pivôs:\n")
        for i, pivo in enumerate(pivos, 1):
            pivo_id = pivo.getId()
            desc = descricoes.get(pivo_id, "Sem descrição")
            f.write(f"  {i}. ID {pivo_id}: {desc}\n")
        f.write("\n")
        
        if estatisticas_globais:
            f.write("ESTATÍSTICAS GLOBAIS\n")
            f.write("-" * 80 + "\n")
            f.write(f"Total de candidatos processados: {estatisticas_globais['total_candidatos']}\n")
            f.write(f"Total de objetos descartados: {estatisticas_globais['total_descartados']}\n")
            f.write(f"Cálculos de distância economizados: {estatisticas_globais['calculos_distancia_economizados']}\n")
            
            taxa = (estatisticas_globais['total_descartados'] / 
                   (estatisticas_globais['total_candidatos'] + estatisticas_globais['total_descartados'])) * 100
            f.write(f"Taxa de redução: {taxa:.2f}%\n")
            f.write(f"Tempo total filtragem: {estatisticas_globais['tempo_filtragem']:.4f}s\n")
            f.write(f"Tempo total BRIDk: {estatisticas_globais['tempo_bridk']:.4f}s\n\n")
        
        f.write("RESULTADOS DAS CONSULTAS\n")
        f.write("=" * 80 + "\n\n")
        
        for i, (consulta_id, info) in enumerate(resultados.items(), 1):
            consulta = info['consulta']
            resultado = info['resultado']
            stats = info['estatisticas_filtragem']
            
            f.write(f"\nCONSULTA {i}\n")
            f.write("-" * 80 + "\n")
            f.write(f"ID da Consulta: {consulta_id}\n")
            
            if consulta_id in descricoes:
                f.write(f"Descrição: {descricoes[consulta_id]}\n")
            
            f.write(f"Coordenadas: {consulta.getAttributes()}\n")
            f.write(f"\nParâmetros: k={info['k']}, d={info['d']}, p={info['p']}\n")
            f.write(f"Raio estimado: {info['raio_estimado']:.4f}\n\n")
            
            f.write("Estatísticas de Filtragem:\n")
            f.write(f"  Candidatos após filtragem: {stats['candidatos']}/{stats['total_dataset']}\n")
            f.write(f"  Objetos descartados: {stats['descartados']}\n")
            f.write(f"  Taxa de filtragem: {stats['taxa_filtragem']:.2f}%\n")
            f.write(f"  Tempo de filtragem: {info['tempo_filtragem']:.4f}s\n")
            f.write(f"  Tempo BRIDk: {info['tempo_bridk']:.4f}s\n\n")
            
            f.write(f"Total de elementos selecionados: {len(resultado)}\n")
            
            # Seção de DEBUG: Candidatos por partição
            if 'debug_info' in info:
                debug_info = info['debug_info']
                f.write(f"\nDEBUG - FASE 1 (MAP): CANDIDATOS POR PARTIÇÃO\n")
                f.write(f"Particionamento: X negativo → Partição 0, X positivo → Partição 1\n")
                f.write("=" * 80 + "\n")
                
                for partition_idx in sorted(debug_info['partition_candidates'].keys()):
                    candidates = debug_info['partition_candidates'][partition_idx]
                    all_elements = debug_info['partition_all_elements'][partition_idx]
                    debug_log = debug_info['partition_debug_log'][partition_idx]
                    
                    f.write(f"\nPARTIÇÃO {partition_idx}:\n")
                    f.write("-" * 80 + "\n")
                    f.write(f"Total de elementos: {len(all_elements)}\n")
                    f.write(f"Candidatos selecionados: {len(candidates)}\n\n")
                    
                    # Mostrar decisões do BRIDk
                    f.write("DECISÕES DO ALGORITMO BRIDk:\n")
                    f.write("-" * 80 + "\n")
                    
                    for entry in debug_log:
                        cand = entry['candidate']
                        action = entry['action']
                        dist = entry['distance']
                        x_coord = cand.getAttributes()[0]
                        desc = descricoes.get(cand.getId(), "Sem descrição")
                        
                        if action == 'accepted':
                            f.write(f"\n✓ ACEITO: ID {cand.getId()} ({desc})\n")
                            f.write(f"  Coordenada X: {x_coord:.4f}\n")
                            f.write(f"  Distância da consulta: {dist:.4f}\n")
                            
                            # Mostrar todos os testes de influência para elementos aceitos
                            if 'checks' in entry and entry['checks']:
                                f.write(f"  Testes de influência realizados:\n")
                                for check in entry['checks']:
                                    elem = check['element']
                                    elem_desc = descricoes.get(elem.getId(), "Sem descrição")
                                    f.write(f"    Teste com ID {elem.getId()} ({elem_desc}):\n")
                                    f.write(f"      dist(ID{elem.getId()}→ID{cand.getId()}) = {check['dist_s_to_t']:.4f}\n")
                                    f.write(f"      influence(ID{elem.getId()}→ID{cand.getId()}) = {check['inf_s_to_t']:.4f}\n")
                                    f.write(f"      influence(ID{elem.getId()}→Consulta) = {check['inf_s_to_q']:.4f}\n")
                                    f.write(f"      influence(Consulta→ID{cand.getId()}) = {check['inf_q_to_t']:.4f}\n")
                                    f.write(f"      Condição 1: {check['inf_s_to_t']:.4f} >= {check['inf_s_to_q']:.4f}? {check['cond1']}\n")
                                    f.write(f"      Condição 2: {check['inf_s_to_t']:.4f} >= {check['inf_q_to_t']:.4f}? {check['cond2']}\n")
                                    f.write(f"      Influência forte? {check['is_strong']}\n")
                        else:
                            f.write(f"\n✗ REJEITADO: ID {cand.getId()} ({desc})\n")
                            f.write(f"  Coordenada X: {x_coord:.4f}\n")
                            f.write(f"  Distância da consulta: {dist:.4f}\n")
                            
                            if 'influenced_by' in entry and entry['influenced_by']:
                                inf_data = entry['influenced_by']
                                influencer = inf_data['influencer']
                                inf_desc = descricoes.get(influencer.getId(), "Sem descrição")
                                
                                f.write(f"  Motivo: Fortemente influenciado por ID {influencer.getId()} ({inf_desc})\n")
                                f.write(f"  Cálculos de influência:\n")
                                f.write(f"    dist(ID{influencer.getId()}→ID{cand.getId()}) = {inf_data['dist_s_to_t']:.4f}\n")
                                f.write(f"    influence(ID{influencer.getId()}→ID{cand.getId()}) = {inf_data['inf_s_to_t']:.4f}\n")
                                f.write(f"    influence(ID{influencer.getId()}→Consulta) = {inf_data['inf_s_to_q']:.4f}\n")
                                f.write(f"    influence(Consulta→ID{cand.getId()}) = {inf_data['inf_q_to_t']:.4f}\n")
                                f.write(f"  Condições de influência forte:\n")
                                f.write(f"    {inf_data['inf_s_to_t']:.4f} >= {inf_data['inf_s_to_q']:.4f}? {inf_data['inf_s_to_t'] >= inf_data['inf_s_to_q']}\n")
                                f.write(f"    {inf_data['inf_s_to_t']:.4f} >= {inf_data['inf_q_to_t']:.4f}? {inf_data['inf_s_to_t'] >= inf_data['inf_q_to_t']}\n")
                    
                f.write(f"\n" + "=" * 80 + "\n")
                f.write(f"\nTotal de candidatos (todas as partições): {debug_info['total_candidates']}\n")
                f.write(f"\nFASE 2 (REDUCE): Refinamento global aplicou BRIDk nos {debug_info['total_candidates']} candidatos\n")
                f.write(f"              → Resultado final: {len(resultado)} elementos\n")
            
            f.write(f"\n" + "=" * 80 + "\n")
            f.write(f"\nELEMENTOS SELECIONADOS (RESULTADO FINAL):\n")
            f.write("-" * 80 + "\n")
            
            for j, tupla in enumerate(resultado, 1):
                tupla_id = tupla.getId()
                f.write(f"\n{j}. ID: {tupla_id}\n")
                
                if tupla_id in descricoes:
                    f.write(f"   Descrição: {descricoes[tupla_id]}\n")
                
                dist = calcular_distancia(tupla, consulta)
                f.write(f"   Distância da consulta: {dist:.4f}\n")
            
            f.write("\n" + "=" * 80 + "\n")
    
    # Restaurar estatísticas globais
    if estatisticas_globais:
        resultados['_estatisticas_globais'] = estatisticas_globais
    
    print(f"Relatório salvo: {output_file}")


def main():
    """Função principal."""
    print("\n" + "=" * 70)
    print("SELEÇÃO POR SIMILARIDADE COM MÉTODO DOS PIVÔS")
    print("=" * 70)
    
    # Criar diretório de resultados
    result_dir = criar_diretorio_resultado()
    
    # Definir caminho do dataset
    # ALTERE AQUI PARA O SEU DATASET
    caminho_dataset = "/apps/tests/Datasets/Reais/Autos/autos.txt"
    
    print("\n" + "=" * 70)
    print("LEITURA DO DATASET")
    print("=" * 70)
    
    # Ler dataset
    dataset, metadados, descricoes = ler_dataset_generico(caminho_dataset)
    
    # Criar SparkSession
    print("\n" + "=" * 70)
    print("CONFIGURAÇÃO DO SPARK")
    print("=" * 70)
    
    spark = SparkSession.builder \
        .appName("Selecao Similaridade - Metodo dos Pivos") \
        .getOrCreate()
    
    print(f"\nSparkSession criada!")
    print(f"  Master: {spark.sparkContext.master}")
    print(f"  Spark Version: {spark.version}")
    
    # Parâmetros do método dos pivôs
    print("\n" + "=" * 70)
    print("CONFIGURAÇÃO DO MÉTODO DOS PIVÔS")
    print("=" * 70)
    
    # IMPORTANTE: número de partições (d) define número de pivôs (p)
    # Um pivô por partição para distribuição balanceada
    d = 4   # Número de partições (ajuste conforme workers disponíveis)
    num_pivos = d  # Um pivô por partição
    
    print(f"\nParâmetros:")
    print(f"  Número de partições (d): {d}")
    print(f"  Número de pivôs (p): {num_pivos} (1 pivô por partição)")
    print(f"  Tamanho do dataset: {len(dataset)}")
    print(f"  Razão p/n: {num_pivos/len(dataset)*100:.2f}%")
    
    # Selecionar pivôs usando k-means++
    pivos = selecionar_pivos_kmeans_plus_plus(dataset, num_pivos)
    
    # Pré-calcular distâncias aos pivôs (distribuído no Spark)
    distancias_pivos_dataset = calcular_distancias_aos_pivos(spark, dataset, pivos, d)
    
    # Selecionar consultas
    print("\n" + "=" * 70)
    print("SELEÇÃO DE CONSULTAS")
    print("=" * 70)
    
    # Selecionar consultas aleatórias que não são pivôs
    num_consultas = 5
    consultas = selecionar_consultas_aleatorias(dataset, num_consultas, pivos=pivos)
    print(f"\n{num_consultas} consulta(s) selecionada(s):")
    for i, consulta in enumerate(consultas, 1):
        consulta_id = consulta.getId()
        desc = descricoes.get(consulta_id, "Sem descrição")
        print(f"  {i}. ID {consulta_id}: {desc}")
    
    # Executar seleção por similaridade com pivôs
    k = 10  # Número de vizinhos
    
    resultados = executar_selecao_similaridade_com_pivos(
        spark_session=spark,
        dataset=dataset,
        consultas=consultas,
        pivos=pivos,
        distancias_pivos_dataset=distancias_pivos_dataset,
        k=k,
        d=d
    )
    
    # Gerar visualizações e relatórios
    print("\n" + "=" * 70)
    print("GERAÇÃO DE VISUALIZAÇÕES E RELATÓRIOS")
    print("=" * 70)
    
    nome_base = f"selecao_pivos_{metadados['nome_dataset']}_p{num_pivos}"
    output_image = os.path.join(result_dir, f"{nome_base}.png")
    output_report = os.path.join(result_dir, f"{nome_base}_relatorio.txt")
    
    # Visualização
    visualizar_pivos_e_resultados(dataset, pivos, resultados, descricoes, output_image)
    
    # Relatório
    gerar_relatorio_com_pivos(resultados, descricoes, metadados, pivos, output_report)
    
    # Resumo final
    print("\n" + "=" * 70)
    print("EXECUÇÃO CONCLUÍDA COM SUCESSO!")
    print("=" * 70)
    
    estatisticas_globais = resultados.get('_estatisticas_globais', {})
    
    print(f"\nResumo:")
    print(f"  Dataset: {metadados['nome_dataset']}")
    print(f"  Tuplas: {metadados['num_tuplas']}")
    print(f"  Dimensões: {metadados['num_dimensoes']}")
    print(f"  Pivôs: {num_pivos}")
    print(f"  Consultas: {num_consultas}")
    print(f"  Parâmetros: k={k}, d={d}")
    
    if estatisticas_globais:
        taxa = (estatisticas_globais['total_descartados'] / 
               (estatisticas_globais['total_candidatos'] + estatisticas_globais['total_descartados'])) * 100
        print(f"\n  Eficiência do Método dos Pivôs:")
        print(f"    Objetos descartados: {estatisticas_globais['total_descartados']}")
        print(f"    Taxa de redução: {taxa:.2f}%")
        print(f"    Cálculos economizados: {estatisticas_globais['calculos_distancia_economizados']}")
    
    print(f"\nArquivos gerados:")
    print(f"  Visualização: {output_image}")
    print(f"  Relatório: {output_report}")
    print("\nPara visualizar:")
    print(f"  docker cp spark-master:{output_image} .")
    print(f"  docker cp spark-master:{output_report} .")
    print("=" * 70 + "\n")
    
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
