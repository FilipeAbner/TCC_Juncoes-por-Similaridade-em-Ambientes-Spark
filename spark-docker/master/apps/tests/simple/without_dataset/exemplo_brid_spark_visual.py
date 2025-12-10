"""
================================================================================
EXEMPLO BRIDk COM PYSPARK + VISUALIZAÇÃO GRÁFICA
================================================================================

DESCRIÇÃO:
    Este script demonstra a execução do algoritmo BRIDk em ambiente distribuído usando 
    PySpark, com geração de visualização gráfica 2D dos resultados.

O QUE FAZ:
    1. Cria um RDD de Tuples diretamente em memória com 5 pontos em 2D
    2. Define uma query (ponto de consulta) na origem (0,0)
    3. Executa o algoritmo BRIDk distribuído usando Spark RDDs
    4. Seleciona os k=3 vizinhos mais similares aplicando o conceito de diversidade
    5. Gera visualização gráfica 2D dos resultados, disponivel em no diretório /apps/result/brid_visual_test_resultado_2d.png

VISUALIZAÇÃO GERADA:
    O gráfico mostra:
    • Dataset completo (pontos pretos, tamanho menor, transparentes)
    • Query (estrela vermelha grande)
    • Pontos selecionados (círculos verdes grandes com borda destacada)
    • Linhas tracejadas conectando query aos pontos selecionados
    • Labels identificando cada ponto selecionado

DATASET DE EXEMPLO:
    Sq = (0, 0)   - Query (ponto de consulta)
    A  = (3, 0)   - Ponto horizontal à direita
    B  = (3, 3)   - Ponto diagonal superior direita
    C  = (5, -3)  - Ponto inferior direita
    D  = (4, -5)  - Ponto inferior afastado

EXECUÇÃO:
    spark-submit /apps/tests/exemplo_brid_spark_visual.py

ARQUIVOS GERADOS:
    • /apps/result/brid_visual_test_resultado_2d.png - Visualização 2D

PARÂMETROS CONFIGURÁVEIS:
    k = 5              - Número de vizinhos diversificados a retornar
    num_partitions = 2 - Número de partições Spark para processamento
    dimension = 2      - Dimensionalidade do espaço (2D)
"""

import os
from pyspark.sql import SparkSession
from brid_python.spark.brid_spark import BridSpark
from brid_python.types.tuple import Tuple

import matplotlib
matplotlib.use('Agg')  # Backend sem interface gráfica
import matplotlib.pyplot as plt
import numpy as np


def visualizar_resultados_2d(dataset, query, results, output_file="/apps/result/brid_visual_test_resultado_2d.png"):
    """Gera visualização 2D (projeção XY) dos resultados do BRIDk."""
    # Garantir que o diretório existe
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    print(f"\nGerando visualização 2D: {output_file}")
    
    # Extrair coordenadas (apenas X e Y)
    dataset_coords = np.array([t.getAttributes()[:2] for t in dataset])
    query_coords = np.array(query.getAttributes()[:2])
    results_coords = np.array([t.getAttributes()[:2] for t in results])
    results_ids = set(t.getId() for t in results)
    
    # Separar
    not_selected = [t for t in dataset if t.getId() not in results_ids]
    not_selected_coords = np.array([t.getAttributes()[:2] for t in not_selected]) if not_selected else np.array([]).reshape(0, 2)
    
    # Criar figura 2D
    fig, ax = plt.subplots(figsize=(12, 10))
    
    # Plotar pontos não selecionados
    if len(not_selected_coords) > 0:
        ax.scatter(not_selected_coords[:, 0], not_selected_coords[:, 1],
                   c='black', marker='o', s=40, alpha=0.7, label='Dataset')
    
    # Plotar pontos selecionados
    if len(results_coords) > 0:
        ax.scatter(results_coords[:, 0], results_coords[:, 1],
                   c='limegreen', marker='o', s=150, alpha=0.9,
                   edgecolors='darkgreen', linewidths=2.5, label=f'Selecionados (k={len(results)})')
        
        # Adicionar labels dos IDs
        for result in results:
            coords = result.getAttributes()[:2]
            ax.annotate(f" {result.getDescription()}", xy=(coords[0], coords[1]), 
                       fontsize=9, color='darkgreen', fontweight='bold',
                       xytext=(5, 5), textcoords='offset points')
    
    # Plotar query
    ax.scatter(query_coords[0], query_coords[1],
               c='red', marker='*', s=600, alpha=1.0,
               edgecolors='darkred', linewidths=3, label='Query', zorder=10)
    ax.annotate(' Query', xy=(query_coords[0], query_coords[1]), 
               fontsize=10, color='darkred', fontweight='bold',
               xytext=(8, 8), textcoords='offset points')
    
    # Conectar query aos selecionados
    for result in results:
        coords = result.getAttributes()[:2]
        ax.plot([query_coords[0], coords[0]],
                [query_coords[1], coords[1]],
                'r--', alpha=0.4, linewidth=1.0)
    
    # Configurações
    ax.set_xlabel('Dimensão X', fontsize=12, fontweight='bold')
    ax.set_ylabel('Dimensão Y', fontsize=12, fontweight='bold')
    ax.set_title('BRIDk - Visualização 2D dos Resultados (Projeção XY)', 
                fontsize=15, fontweight='bold', pad=15)
    ax.legend(loc='upper right', fontsize=11, framealpha=0.9)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_aspect('equal', adjustable='box')
    
    # Salvar
    plt.tight_layout()
    plt.savefig(output_file, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"   Gráfico 2D salvo: {output_file}")


def exemplo_com_visualizacao():
    """Exemplo BRIDk com visualização gráfica usando RDD direto."""
    print("="*80)
    print("BRIDk DISTRIBUÍDO COM PYSPARK + VISUALIZAÇÃO GRÁFICA")
    print("="*80)
    
    # Criar SparkSession
    spark = SparkSession.builder \
        .appName("BRIDk-Visualization") \
        .getOrCreate()
    
    try:
        # Criar dataset diretamente em memória
        print("\n1. Criando dataset de exemplo em memória...")
        dataset_tuples = []
        
        pontos = [
            (0, 0.0, 0.0, "Sq"),
            (1, 3.0, 0.0, "A"),
            (2, 3.0, 3.00000000001, "B"),
            (3, 5.0, -3.0, "C"),
            (4, 4.0, -5.0, "D")
        ]
        
        for pid, x, y, nome in pontos:
            tupla = Tuple([x, y])
            tupla.setId(pid)
            tupla.setDescription(nome)
            dataset_tuples.append(tupla)
        
        print(f"   Dataset criado: {len(pontos)} tuplas")
        
        # Criar RDD a partir da lista de tuplas
        print("\n2. Criando RDD de Tuples...")
        tuples_rdd = spark.sparkContext.parallelize(dataset_tuples, numSlices=2)
        print(f"   RDD criado com {tuples_rdd.count()} tuplas em {tuples_rdd.getNumPartitions()} partições")
        
        # Definir query (Sq = ponto 0)
        print("\n3. Definindo query no ponto Sq=(0, 0)...")
        query = Tuple([0.0, 0.0])
        query.setId(0)
        query.setDescription("Sq")
        print(f"   Query: {query}")
        
        # Executar BRIDk usando RDD direto
        print("\n4. Executando BRIDk distribuído sobre RDD (k=3)...")
        brid_spark = BridSpark(spark)
        
        results = brid_spark.execute_from_rdd(
            dataset_rdd=tuples_rdd,
            query=query,
            k=3,
            num_partitions=2
        )
        
        # Exibir resultados
        print("\n5. Resultados (k-vizinhos diversificados):")
        print("   " + "="*60)
        for i, tupla in enumerate(results, 1):
            coords = tupla.getAttributes()
            desc = tupla.getDescription() or f"Tupla_{tupla.getId()}"
            print(f"   {i}. {desc} = ({coords[0]:.1f}, {coords[1]:.1f})")
        print("   " + "="*60)
        
        # Gerar visualização 2D
        print("\n6. Gerando visualização gráfica...")
        result_dir = "/apps/result"
        os.makedirs(result_dir, exist_ok=True)
        output_path = f"{result_dir}/brid_visual_test_resultado_2d.png"
        visualizar_resultados_2d(dataset_tuples, query, results, output_path)
        
        print("\n" + "="*80)
        print("EXECUÇÃO CONCLUÍDA COM SUCESSO!")
        print("="*80)
        print(f"\nArquivo gerado:")
        print(f"   • {output_path}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    print("\n" + "="*70)
    print("BRIDk COM VISUALIZAÇÃO GRÁFICA")
    print("="*70 + "\n")
    
    try:
        exemplo_com_visualizacao()
    except Exception as e:
        print(f"\nErro: {e}")
        import traceback
        traceback.print_exc()
