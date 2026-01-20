#!/usr/bin/env python3
"""
Funções Compartilhadas para Juncoes por Similaridade

Este módulo contém funções utilitárias que são usadas por ambos os scripts:
- juncao_similaridade.py (distribuído com Spark)
- brid_brute_force.py (centralizado)

As funções aqui NÃO dependem de PySpark ou outras bibliotecas de distribuição,
garantindo compatibilidade com ambos os scripts.
"""

import random


def selecionar_amostra_dataset_a(dataset_a, max_consultas=None, use_percent=False, seed=42, tem_id=True):
    """
    Seleciona uma amostra do dataset A para usar como consultas.
    Se max_consultas for None, usa todo o dataset A.
    
    IMPORTANTE: Esta função é usada por AMBOS os scripts:
    - juncao_similaridade.py (distribuído)
    - brid_brute_force.py (centralizado)
    
    Usando a mesma função com a mesma seed garante que ambos
    os scripts processem EXATAMENTE as mesmas consultas.
    
    Args:
        dataset_a: Dataset completo A (lista de tuplas)
        max_consultas: Número máximo de consultas (None = todas)
        use_percent: Se True, max_consultas é uma porcentagem do total
        seed: Semente para reprodutibilidade (padrão: 42)
        tem_id: Se True, o dataset já tem IDs (usar existentes).
                Se False, atribuir IDs sequenciais a TODO o dataset antes de amostrar
    
    Returns:
        list: Lista de tuplas que serão usadas como consultas
    """
    # Se o dataset não tinha IDs, atribuir IDs sequenciais a TODAS as tuplas antes de amostrar
    if not tem_id:
        print(f"  Atribuindo IDs sequenciais ao dataset completo (1 a {len(dataset_a)})")
        for idx, tupla in enumerate(dataset_a, start=1):
            tupla.setId(idx)

    amostra = None

    if use_percent:
        percent = max_consultas
        max_consultas = int(len(dataset_a) * percent / 100)
        random.seed(seed)
        amostra = random.sample(dataset_a, max_consultas)
        print(f"\nSelecionando {percent}% do Dataset A: {max_consultas} tuplas como consultas")
    elif max_consultas is None or max_consultas >= len(dataset_a):
        print(f"\nUsando TODAS as {len(dataset_a)} tuplas do Dataset A como consultas")
        amostra = dataset_a
    else:
        random.seed(seed)
        amostra = random.sample(dataset_a, max_consultas)
        print(f"\nSelecionadas {max_consultas} tuplas aleatórias do Dataset A como consultas")

    return amostra
