#!/usr/bin/env python3
"""
================================================================================
TESTE GEOGEBRA - BRIDk COM MÉTODO DOS PIVÔS
================================================================================

DESCRIÇÃO:
    Script para testar o algoritmo BRIDk usando datasets com referencia visual implementada
    no GeoGebra com particionamento baseado em pivôs pré-definidos. Permite
    validação visual dos resultados comparando com visualizações do GeoGebra.

O QUE FAZ:
    1. Carrega dataset 2D exportado do GeoGebra (formato: ID \t X \t Y \t Nome)
    2. Seleciona pivôs manualmente por coordenadas exatas
    3. Define ponto de consulta manualmente por coordenadas
    4. Executa BRIDk com particionamento baseado em pivôs
    5. Gera relatório detalhado com resultados e distâncias disponivel em /apps/result/teste_geogebra_<dataset>_p2_relatorio.txt

MÉTODO DOS PIVÔS:
    - Usa p pivôs estratégicos para dividir o espaço em d=p partições
    - Cada ponto do dataset é atribuído à partição do pivô mais próximo
    - Pré-calcula distâncias aos pivôs para otimizar particionamento
    - Reduz cálculos de distância comparado ao particionamento aleatório

DATASETS SUPORTADOS:
    • points_geogebra.txt   - Dataset com pontos J, D1, G1
      Pivôs: J (1.46, 1.89) e D1 (-1.33, 1.72)
      Consulta: G1 (-1.00, 2.36)
    
    • points_geogebra_2.txt - Dataset com pontos V1, V2, Q
      Pivôs: V1 (-5.54, 4.82) e V2 (6.53, 4.54)
      Consulta: Q (-0.98, 4.88)

CONFIGURAÇÃO:
    Edite a seção "CONFIGURAÇÃO DO TESTE" no código para:
    - Escolher o dataset (comente/descomente caminho_dataset)
    - Definir k (número de vizinhos) e d (número de partições)

EXECUÇÃO:
    spark-submit /apps/tests/simple/with_dataset/teste_geogebra_pivos.py

ARQUIVOS GERADOS:
    • /apps/result/teste_geogebra_<dataset>_p2_relatorio.txt
      Relatório com:
      - Informações dos pivôs selecionados
      - Distâncias da consulta aos pivôs
      - Partições processadas
      - k vizinhos mais diversos encontrados
      - Distâncias de cada vizinho à consulta

VALIDAÇÃO:
    Compare os resultados com a visualização no GeoGebra:
    - IDs dos vizinhos selecionados
    - Ordem de seleção (por diversidade)
    - Distribuição entre partições

PARÂMETROS PADRAO:
    k = 5  - Retorna 5 vizinhos mais diversos
    d = 2  - Usa 2 partições (uma para cada pivô)

"""

import os
import sys
from pyspark.sql import SparkSession

# Adicionar o diretório pai ao path
import sys
import os

# Adiciona o diretório /apps/tests ao path para permitir imports relativos
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Agora pode importar do módulo algorithms
from algorithms.selecao_similaridade_pivos import (
    criar_diretorio_resultado,
    ler_dataset_generico,
    calcular_distancias_aos_pivos,
    executar_selecao_similaridade_com_pivos,
    gerar_relatorio_com_pivos
)


def main():
    """Função principal para testes com GeoGebra."""
    print("\n" + "=" * 70)
    print("TESTE GEOGEBRA - BRIDk COM MÉTODO DOS PIVÔS")
    print("=" * 70)
    
    # Criar diretório de resultados
    result_dir = criar_diretorio_resultado()
    
    # =========================================================================
    # CONFIGURAÇÃO DO TESTE - AJUSTE AQUI PARA DIFERENTES DATASETS
    # =========================================================================
    
    # Escolha o dataset (descomente o que deseja usar)
    # caminho_dataset = "/apps/tests/Datasets/points_geogebra.txt"
    caminho_dataset = "/apps/Datasets/Simples/points_geogebra_2.txt"
    
    # Dataset: points_geogebra.txt
    # Pivôs: J (ID 10) e D1 (ID 28)
    # Consulta: G1 (ID 31)
    if "points_geogebra.txt" in caminho_dataset and "2" not in caminho_dataset:
        pivo1_coords = [1.4635713124746, 1.8888200988007]     # J (ID 10)
        pivo2_coords = [-1.3295606183634, 1.7217667536548]   # D1 (ID 28)
        consulta_coords = [-1.0021360618776, 2.356569465209] # G1 (ID 31)
        pivo1_nome = "J"
        pivo2_nome = "D1"
        consulta_nome = "G1"
    
    # Dataset: points_geogebra_2.txt
    # Pivôs: V1 (ID 7) e V2 (ID 14)
    # Consulta: Q (ID 1)
    elif "points_geogebra_2" in caminho_dataset:
        pivo1_coords = [-5.5372682926829,	4.8159512195122]  # V1 (ID 7)
        pivo2_coords = [6.5251707317073,	4.5369268292683]   # V2 (ID 14)
        consulta_coords = [-0.98234156,	4.87654321]  # Q (ID 1)
        pivo1_nome = "V1"
        pivo2_nome = "V2"
        consulta_nome = "Q"
    
    # Parâmetros
    k = 5  # Número de vizinhos
    d = 2  # Número de partições
    
    # =========================================================================
    # LEITURA DO DATASET
    # =========================================================================
    
    print("\n" + "=" * 70)
    print("LEITURA DO DATASET")
    print("=" * 70)
    
    dataset, metadados, descricoes = ler_dataset_generico(caminho_dataset)
    
    # =========================================================================
    # CONFIGURAÇÃO DO SPARK
    # =========================================================================
    
    print("\n" + "=" * 70)
    print("CONFIGURAÇÃO DO SPARK")
    print("=" * 70)
    
    spark = SparkSession.builder \
        .appName("Teste GeoGebra - Metodo dos Pivos") \
        .getOrCreate()
    
    print(f"\nSparkSession criada!")
    print(f"  Master: {spark.sparkContext.master}")
    print(f"  Spark Version: {spark.version}")
    
    # =========================================================================
    # SELEÇÃO DOS PIVÔS (MANUAL)
    # =========================================================================
    
    print("\n" + "=" * 70)
    print("SELEÇÃO DOS PIVÔS (MANUAL)")
    print("=" * 70)
    
    print(f"\nBuscando pivôs por coordenadas:")
    print(f"  Pivô 1 ({pivo1_nome}): {pivo1_coords}")
    print(f"  Pivô 2 ({pivo2_nome}): {pivo2_coords}")
    
    pivos = []
    for tupla in dataset:
        coords = tupla.getAttributes()
        if abs(coords[0] - pivo1_coords[0]) < 0.0001 and abs(coords[1] - pivo1_coords[1]) < 0.0001:
            pivos.append(tupla)
            print(f"  ✓ Pivô 1 ({pivo1_nome}) encontrado: ID {tupla.getId()}")
        elif abs(coords[0] - pivo2_coords[0]) < 0.0001 and abs(coords[1] - pivo2_coords[1]) < 0.0001:
            pivos.append(tupla)
            print(f"  ✓ Pivô 2 ({pivo2_nome}) encontrado: ID {tupla.getId()}")
    
    if len(pivos) != 2:
        print(f"\n  ✗ ERRO: Encontrados {len(pivos)} pivôs ao invés de 2")
        print(f"  Verifique as coordenadas no dataset!")
        spark.stop()
        return
    
    # Pré-calcular distâncias aos pivôs
    distancias_pivos_dataset = calcular_distancias_aos_pivos(spark, dataset, pivos, d)
    
    # =========================================================================
    # SELEÇÃO DA CONSULTA (MANUAL)
    # =========================================================================
    
    print("\n" + "=" * 70)
    print("SELEÇÃO DA CONSULTA (MANUAL)")
    print("=" * 70)
    
    print(f"\nBuscando consulta por coordenadas:")
    print(f"  Consulta ({consulta_nome}): {consulta_coords}")
    
    consultas = []
    for tupla in dataset:
        coords = tupla.getAttributes()
        if abs(coords[0] - consulta_coords[0]) < 0.0001 and abs(coords[1] - consulta_coords[1]) < 0.0001:
            consultas.append(tupla)
            print(f"  ✓ Consulta ({consulta_nome}) encontrada: ID {tupla.getId()}")
            break
    
    if len(consultas) == 0:
        print(f"\n  ✗ ERRO: Consulta não encontrada!")
        print(f"  Verifique as coordenadas no dataset!")
        spark.stop()
        return
    
    # =========================================================================
    # EXECUÇÃO DO BRIDk
    # =========================================================================
    
    print("\n" + "=" * 70)
    print("EXECUÇÃO DO BRIDk COM PIVÔS")
    print("=" * 70)
    print(f"\nParâmetros:")
    print(f"  k = {k} vizinhos")
    print(f"  d = {d} partições")
    
    resultados = executar_selecao_similaridade_com_pivos(
        spark_session=spark,
        dataset=dataset,
        consultas=consultas,
        pivos=pivos,
        distancias_pivos_dataset=distancias_pivos_dataset,
        k=k,
        d=d
    )
    
    # =========================================================================
    # GERAÇÃO DO RELATÓRIO
    # =========================================================================
    
    print("\n" + "=" * 70)
    print("GERAÇÃO DO RELATÓRIO")
    print("=" * 70)
    
    nome_base = metadados['nome_dataset']
    output_file = f"{result_dir}/teste_geogebra_{nome_base}_p{len(pivos)}_relatorio.txt"
    
    gerar_relatorio_com_pivos(
        resultados=resultados,
        descricoes=descricoes,
        metadados=metadados,
        pivos=pivos,
        output_file=output_file
    )
    
    print(f"\n✓ Relatório gerado: {output_file}")
    
    # Finalizar Spark
    spark.stop()
    
    print("\n" + "=" * 70)
    print("TESTE CONCLUÍDO!")
    print("=" * 70)


if __name__ == "__main__":
    main()
