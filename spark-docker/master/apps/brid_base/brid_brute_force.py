#!/usr/bin/env python3
"""
Junção por Similaridade usando BRIDk - Força Bruta Centralizada

Este script realiza junção por similaridade diversificada usando o algoritmo
BRIDk de forma centralizada (sem distribuição), para comparação de desempenho
com a versão distribuída em Spark.

Operação: A ⋈ B (similarity join) - Centralizado
- Para cada tupla em A, encontra k tuplas similares em B
- Garante diversidade nos resultados usando BRIDk
- Execução centralizada (sem Spark)
- Não utiliza pivôs (força bruta - processa todo o dataset B para cada consulta)

Objetivo: Avaliar impacto do ambiente distribuído vs centralizado

COMPORTAMENTO IMPORTANTE DO BRIDk:
====================================
O algoritmo BRIDk PRIORIZA DIVERSIDADE sobre quantidade de resultados.
Isso significa que ele pode retornar MENOS de k vizinhos se:

1. Critério de Diversificação Restritivo:
   - Um candidato é rejeitado se for "fortemente influenciado" por outro já selecionado
   - Influência forte ocorre quando um vizinho já selecionado está mais próximo
     do candidato do que a consulta está

2. Dataset Pequeno ou com Agrupamentos:
   - Em datasets com pontos agrupados, o BRIDk seleciona representantes de cada grupo
   - Outros pontos do mesmo grupo são considerados redundantes (não diversos)

3. Filtro de Auto-Exclusão:
   - A própria consulta é excluída se estiver no dataset de busca

Exemplo Prático:
Se temos 5 pontos muito próximos entre si formando um cluster, o BRIDk pode
selecionar apenas 1 ou 2 deles, pois os demais seriam considerados redundantes
(não diversificados) segundo o critério de influência.

Este comportamento é CORRETO e ESPERADO do algoritmo BRIDk, que busca
DIVERSIDADE nos resultados, não apenas proximidade.
"""

import os
import sys
import time
import argparse
import numpy as np

# Adicionar o diretório pai ao path para importações
# Detectar se está rodando dentro do container ou localmente
if os.path.exists('/apps'):
    # Dentro do container Docker
    sys.path.insert(0, '/apps')
else:
    # Fora do container - usar caminho relativo
    script_dir = os.path.dirname(os.path.abspath(__file__))
    apps_dir = os.path.join(script_dir, '..')
    sys.path.insert(0, apps_dir)

from brid_python.types.tuple import Tuple
from tests.algorithms.relatorio_juncao import gerar_estatisticas_juncao, gerar_relatorio_juncao, gerar_relatorio_debug

class EstatisticasExecucao:
    """Classe para armazenar estatísticas de execução."""
    def __init__(self):
        self.calculos_distancia = 0
    
    def reset(self):
        """Reseta as estatísticas."""
        self.calculos_distancia = 0


def calcular_distancia_euclidiana(tupla1, tupla2):
    """
    Calcula a distância Euclidiana entre duas tuplas.
    
    Args:
        tupla1: Primeira tupla
        tupla2: Segunda tupla
    
    Returns:
        float: Distância Euclidiana
    """
    coords1 = np.array(tupla1.getAttributes())
    coords2 = np.array(tupla2.getAttributes())
    return np.linalg.norm(coords1 - coords2)


def calcular_distancia(tupla1, tupla2, estatisticas):
    """
    Calcula a distância entre duas tuplas (sem cache para evitar estouro de memória).
    
    Args:
        tupla1: Primeira tupla
        tupla2: Segunda tupla
        estatisticas: Objeto de estatísticas para tracking
    
    Returns:
        float: Distância entre as tuplas
    """
    distancia = calcular_distancia_euclidiana(tupla1, tupla2)
    estatisticas.calculos_distancia += 1
    return distancia


def calcular_nivel_influencia(tupla_s, tupla_t, estatisticas):
    """
    Calcula o nível de influência que 's' exerce sobre 't'.
    
    Influência = 1 / distância
    Quanto menor a distância, maior a influência.
    
    Args:
        tupla_s: Tupla que exerce influência
        tupla_t: Tupla que sofre influência
        estatisticas: Objeto de estatísticas
    
    Returns:
        float: Nível de influência (infinito se distância = 0)
    """
    dist = calcular_distancia(tupla_s, tupla_t, estatisticas)
    if dist == 0:
        return float('inf')
    return 1.0 / dist


def eh_influencia_forte(tupla_s, tupla_candidato, tupla_consulta, estatisticas):
    """
    Verifica se 's' exerce uma influência FORTE sobre o candidato em relação à consulta.
    
    Uma influência é considerada FORTE quando ambas as condições são verdadeiras:
    1. s está mais próximo do candidato do que s está da consulta
    2. s está mais próximo do candidato do que a consulta está do candidato
    
    Em termos de influência (1/distância):
    - influencia(s→candidato) >= influencia(s→consulta)  AND
    - influencia(s→candidato) >= influencia(consulta→candidato)
    
    Args:
        tupla_s: Tupla já selecionada (que pode influenciar)
        tupla_candidato: Candidato sendo avaliado
        tupla_consulta: Tupla de consulta
        estatisticas: Objeto de estatísticas
    
    Returns:
        bool: True se a influência é forte
    """
    inf_s_para_consulta = calcular_nivel_influencia(tupla_s, tupla_consulta, estatisticas)
    inf_s_para_candidato = calcular_nivel_influencia(tupla_s, tupla_candidato, estatisticas)
    inf_consulta_para_candidato = calcular_nivel_influencia(tupla_consulta, tupla_candidato, estatisticas)
    
    # s exerce influência forte sobre candidato se:
    # - s está mais perto do candidato do que de consulta E
    # - s está mais perto do candidato do que consulta está
    return (inf_s_para_candidato >= inf_s_para_consulta and 
            inf_s_para_candidato >= inf_consulta_para_candidato)


def candidato_nao_eh_influenciado(candidato, consulta, resultado_atual, estatisticas):
    """
    Verifica se o candidato NÃO é influenciado por nenhum elemento já selecionado.
    
    Percorre o conjunto de resultados em ordem REVERSA (os mais recentemente adicionados primeiro)
    e verifica se algum exerce influência forte sobre o candidato.
    
    Args:
        candidato: Tupla candidata a ser adicionada
        consulta: Tupla de consulta
        resultado_atual: Lista de tuplas já selecionadas
        estatisticas: Objeto de estatísticas
    
    Returns:
        bool: True se o candidato NÃO é influenciado (pode ser adicionado)
    """
    # Iterar em ordem reversa (como no algoritmo original)
    for elemento_resultado in reversed(resultado_atual):
        if eh_influencia_forte(elemento_resultado, candidato, consulta, estatisticas):
            # Candidato é fortemente influenciado por este elemento
            return False
    
    # Candidato não é influenciado por nenhum elemento
    return True


def bridk_centralizado(consulta, dataset_busca, k, estatisticas, debug_consulta_id=None):
    """
    Implementação centralizada do algoritmo BRIDk.
    
    Algoritmo:
    1. Ordena todo o dataset de busca por distância à consulta (ordem crescente)
    2. Itera pelos candidatos em ordem:
       a. Filtra a própria consulta (se estiver no dataset)
       b. Verifica se o candidato não é influenciado pelos já selecionados
       c. Se não for influenciado, adiciona ao resultado
       d. Para quando atingir k vizinhos OU esgotar os candidatos
    3. Retorna os k vizinhos diversificados
    
    Args:
        consulta: Tupla de consulta
        dataset_busca: Lista de tuplas do dataset de busca
        k: Número de vizinhos diversificados desejados
        estatisticas: Objeto para tracking de estatísticas
        debug_consulta_id: ID da consulta para debug detalhado (opcional)
    
    Returns:
        list: Lista com até k vizinhos diversificados
    """
    # Debug mode
    debug_mode = (debug_consulta_id is not None and consulta.getId() == debug_consulta_id)
    
    # Fase 1: Ordenar dataset por distância à consulta (CRESCENTE)
    # Isso garante que processamos candidatos do mais próximo ao mais distante
    dataset_ordenado = sorted(
        dataset_busca,
        key=lambda elem: calcular_distancia(elem, consulta, estatisticas)
    )
    
    if debug_mode:
        print(f"\n{'='*80}")
        print(f"DEBUG - Consulta ID {consulta.getId()}")
        print(f"{'='*80}")
        print(f"Total de candidatos: {len(dataset_ordenado)}")
        print(f"k desejado: {k}")
        # Removed top 10 candidates print
    
    resultado = []
    candidatos_rejeitados = []
    
    # Fase 2: Iterar pelos candidatos e aplicar critério de diversificação
    for idx, candidato in enumerate(dataset_ordenado):
        # Verificar se já temos k vizinhos
        if len(resultado) >= k:
            break
        
        # FILTRO 1: Não incluir a própria consulta como vizinho
        # Verifica se é o mesmo objeto (distância muito próxima de 0)
        dist_para_consulta = calcular_distancia(candidato, consulta, estatisticas)
        if dist_para_consulta < 1e-10:  # Praticamente zero (mesma posição)
            if debug_mode:
                print(f"\n[FILTRADO] ID={candidato.getId()} - É a própria consulta (dist={dist_para_consulta:.10f})")
            continue
        
        # FILTRO 2: Critério de diversificação
        # Verificar se o candidato NÃO é fortemente influenciado por elementos já selecionados
        if candidato_nao_eh_influenciado(candidato, consulta, resultado, estatisticas):
            resultado.append(candidato)
            # Removed acceptance print
        else:
            candidatos_rejeitados.append((candidato, dist_para_consulta))
            # Removed rejection print with details
    
    # Removed final result print
    
    return resultado


# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================



def processar_argumentos():
    """
    Processa argumentos de linha de comando.
    
    Returns:
        argparse.Namespace: Argumentos parseados
    """
    parser = argparse.ArgumentParser(
        description='Junção por Similaridade usando BRIDk - Força Bruta Centralizada'
    )
    parser.add_argument(
        '--debug',
        type=int,
        default=None,
        help='ID da consulta para gerar relatório de debug detalhado'
    )
    return parser.parse_args()


def criar_diretorio_resultado():
    """Cria o diretório de resultados se não existir."""
    if os.path.exists('/apps'):
        # Dentro do container Docker
        result_dir = "/apps/result"
    else:
        # Fora do container - usar caminho relativo
        script_dir = os.path.dirname(os.path.abspath(__file__))
        result_dir = os.path.join(script_dir, '..', 'result')
    
    os.makedirs(result_dir, exist_ok=True)
    return result_dir


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
    - Demais linhas: dados no formato: [ID] dim1 dim2 ... dimN [texto_descritivo]
    
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
        
        partes = linha.split()
        
        if tem_id:
            tupla_id = int(partes[0])
            inicio_coords = 1
        else:
            # Se não tem ID, usar ID sequencial baseado na linha
            tupla_id = i - 5  # ID sequencial começando em 1
            inicio_coords = 0
        
        # Extrair coordenadas
        if tem_texto:
            # Tentar extrair exatamente num_dimensoes valores numéricos
            coords = []
            texto_parts = []
            
            for j in range(inicio_coords, len(partes)):
                try:
                    valor = float(partes[j])
                    if len(coords) < num_dimensoes:
                        coords.append(valor)
                    else:
                        # Se já temos num_dimensoes, o resto é texto
                        texto_parts.append(partes[j])
                except ValueError:
                    # Não é um número, deve ser parte do texto descritivo
                    texto_parts.extend(partes[j:])
                    break
            
            # Verificar se conseguimos extrair o número correto de dimensões
            if len(coords) != num_dimensoes:
                print(f"Aviso: Tupla {tupla_id} tem {len(coords)} dimensões em vez de {num_dimensoes} esperadas")
                # Preencher com zeros se necessário
                while len(coords) < num_dimensoes:
                    coords.append(0.0)
            
            # Texto descritivo
            if texto_parts:
                descricoes[tupla_id] = ' '.join(texto_parts)
        else:
            # Todas as partes após o ID são coordenadas
            try:
                coords = [float(partes[j]) for j in range(inicio_coords, len(partes))]
                # Verificar se temos o número correto de dimensões
                if len(coords) != num_dimensoes:
                    print(f"Aviso: Tupla {tupla_id} linha {i} tem {len(coords)} dimensões em vez de {num_dimensoes} esperadas. Pulando...")
                    continue
            except ValueError as e:
                print(f"Aviso: Erro ao converter coordenadas na linha {i} (tupla {tupla_id}): {e}. Pulando linha...")
                continue
        
        # Criar tupla
        tupla = Tuple(coords)
        tupla.setId(tupla_id)
        dataset.append(tupla)
    
    return dataset, metadados, descricoes


def selecionar_amostra_dataset_a(dataset_a, max_consultas=None, use_percent=False, seed=42):
    """
    Seleciona uma amostra do dataset A para usar como consultas.
    Se max_consultas for None, usa todo o dataset A.
    
    Args:
        dataset_a: Dataset completo A
        max_consultas: Número máximo de consultas (None = todas)
        use_percent: Se True, max_consultas é uma porcentagem
        seed: Semente para reprodutibilidade
    
    Returns:
        list: Lista de tuplas que serão usadas como consultas
    """
    if use_percent:
        max_consultas = int(len(dataset_a) * max_consultas / 100)
        print(f"  Usando {max_consultas} consultas ({max_consultas / len(dataset_a) * 100:.1f}% do dataset A)")
    
    if max_consultas is None or max_consultas >= len(dataset_a):
        print(f"  Usando todas as {len(dataset_a)} tuplas do Dataset A como consultas")
        # Reatribuir IDs sequenciais para consultas
        consultas_com_ids_sequenciais = []
        for i, tupla in enumerate(dataset_a, 1):
            nova_tupla = Tuple(tupla.getAttributes())
            nova_tupla.setId(i)
            consultas_com_ids_sequenciais.append(nova_tupla)
        return consultas_com_ids_sequenciais
    else:
        np.random.seed(seed)
        indices = np.random.choice(len(dataset_a), max_consultas, replace=False)
        amostra = [dataset_a[i] for i in sorted(indices)]
        print(f"  Amostra selecionada: {len(amostra)} consultas de {len(dataset_a)} tuplas")
        
        # Reatribuir IDs sequenciais para as consultas selecionadas
        consultas_com_ids_sequenciais = []
        for i, tupla in enumerate(amostra, 1):
            nova_tupla = Tuple(tupla.getAttributes())
            nova_tupla.setId(i)
            consultas_com_ids_sequenciais.append(nova_tupla)
        return consultas_com_ids_sequenciais


def executar_juncao_brute_force(dataset_a_consultas, dataset_b, k=10, debug_consulta_id=None):
    """
    Executa a junção por similaridade usando força bruta centralizada.
    
    Para cada tupla em A, encontra k tuplas similares e diversas em B usando BRIDk
    de forma centralizada (sem distribuição, sem pivôs).
    
    IMPORTANTE: Este é o algoritmo BRIDk PURO em ambiente centralizado:
    - Não usa partições ou pivôs
    - Processa TODO o dataset B para cada consulta em A
    - Garante diversidade usando os critérios de influência do BRIDk
    - Pode retornar menos de k vizinhos se o critério de diversificação for muito restritivo
    
    Args:
        dataset_a_consultas: Lista de tuplas do dataset A (consultas)
        dataset_b: Lista de tuplas do dataset B (busca)
        k: Número de vizinhos diversificados a selecionar
        debug_consulta_id: ID da consulta para debug detalhado (opcional)
    
    Returns:
        tuple: (resultados, tempo_total, estatisticas)
            - resultados: dict com resultados para cada tupla de A
            - tempo_total: tempo total de execução em segundos
            - estatisticas: estatísticas de execução
    """
    print(f"\n{'=' * 80}")
    print(f"EXECUTANDO JUNÇÃO FORÇA BRUTA CENTRALIZADA (BRIDk PURO)")
    print(f"{'=' * 80}")
    print(f"Dataset A (consultas): {len(dataset_a_consultas)} tuplas")
    print(f"Dataset B (busca): {len(dataset_b)} tuplas")
    print(f"k (vizinhos diversificados desejados): {k}")
    print(f"Método: BRIDk centralizado (sem partições, sem pivôs)")
    if debug_consulta_id:
        print(f"Debug habilitado para consulta ID: {debug_consulta_id}")
    print(f"{'=' * 80}\n")
    
    # Criar objeto de estatísticas global (compartilhado entre todas as consultas)
    estatisticas_global = EstatisticasExecucao()
    
    resultados = {}
    tempo_inicio_total = time.time()
    
    # Estatísticas detalhadas
    total_vizinhos_encontrados = 0
    consultas_completas = 0  # Consultas que encontraram exatamente k vizinhos
    consultas_incompletas = 0  # Consultas que encontraram menos de k vizinhos
    
    print(f"Processando consultas...")
    print(f"Nota: O BRIDk pode retornar menos de k vizinhos quando o critério de diversificação é muito restritivo.\n")
    progresso_anterior = 0
    
    for i, consulta_a in enumerate(dataset_a_consultas, 1):
        # Executar BRIDk centralizado para encontrar k vizinhos diversificados
        tempo_inicio = time.time()
        
        # Buscar k vizinhos diversificados usando a implementação centralizada pura
        # O algoritmo bridk_centralizado:
        # 1. Ordena todo o dataset B por distância à consulta
        # 2. Itera pelos candidatos em ordem crescente de distância
        # 3. Aplica o critério de diversificação (não influenciado)
        # 4. Para quando encontra k vizinhos OU esgota os candidatos
        vizinhos_b = bridk_centralizado(consulta_a, dataset_b, k, estatisticas_global, debug_consulta_id)
        
        tempo_fim = time.time()
        tempo_busca = tempo_fim - tempo_inicio
        
        # Estatísticas
        total_vizinhos_encontrados += len(vizinhos_b)
        if len(vizinhos_b) == k:
            consultas_completas += 1
        else:
            consultas_incompletas += 1
        
        # Armazenar resultados
        resultados[consulta_a.getId()] = {
            'consulta_a': consulta_a,
            'resultado_b': vizinhos_b,
            'k': k,
            'k_encontrado': len(vizinhos_b),
            'd': 1,  # Força bruta não usa partições
            'tempo_busca': tempo_busca
        }
        
        # Mostrar progresso (não mostrar se estiver em modo debug para não poluir)
        if not debug_consulta_id or consulta_a.getId() != debug_consulta_id:
            progresso = int((i / len(dataset_a_consultas)) * 100)
            if progresso % 10 == 0 and progresso != progresso_anterior:
                print(f"  Progresso: {progresso}% ({i}/{len(dataset_a_consultas)} consultas processadas)")
                progresso_anterior = progresso
    
    tempo_fim_total = time.time()
    tempo_total = tempo_fim_total - tempo_inicio_total
    
    print(f"\n  Progresso: 100% ({len(dataset_a_consultas)}/{len(dataset_a_consultas)} consultas processadas)")
    print(f"\n{'=' * 80}")
    print(f"ESTATÍSTICAS DE EXECUÇÃO - BRIDk CENTRALIZADO")
    print(f"{'=' * 80}")
    print(f"Tempo total de execução: {tempo_total:.2f}s")
    print(f"Tempo médio por consulta: {tempo_total / len(dataset_a_consultas):.4f}s")
    print(f"")
    print(f"CÁLCULOS DE DISTÂNCIA:")
    print(f"  Total de cálculos: {estatisticas_global.calculos_distancia}")
    print(f"  Taxa de cálculos/s: {estatisticas_global.calculos_distancia / tempo_total:.0f}")
    print(f"")
    print(f"VIZINHOS ENCONTRADOS:")
    print(f"  Total de vizinhos: {total_vizinhos_encontrados}")
    print(f"  Média por consulta: {total_vizinhos_encontrados / len(dataset_a_consultas):.2f}")
    print(f"  Consultas com k={k} vizinhos: {consultas_completas} ({consultas_completas / len(dataset_a_consultas) * 100:.1f}%)")
    print(f"  Consultas com <k vizinhos: {consultas_incompletas} ({consultas_incompletas / len(dataset_a_consultas) * 100:.1f}%)")
    print(f"")
    print(f"OBSERVAÇÃO: O BRIDk prioriza DIVERSIDADE sobre quantidade.")
    print(f"Vizinhos são rejeitados se forem fortemente influenciados por outros já selecionados.")
    print(f"{'=' * 80}\n")
    
    estatisticas = {
        'tempo_total': tempo_total,
        'distance_calculations': estatisticas_global.calculos_distancia,
        'total_vizinhos': total_vizinhos_encontrados,
        'consultas_completas': consultas_completas,
        'consultas_incompletas': consultas_incompletas
    }
    
    return resultados, tempo_total, estatisticas


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


def pad_attributes(tupla, dim_max):
    """Padroniza atributos de uma tupla para dim_max dimensões."""
    attrs = list(tupla.getAttributes())
    while len(attrs) < dim_max:
        attrs.append(0.0)
    nova_tupla = Tuple(attrs)
    nova_tupla.setId(tupla.getId())
    return nova_tupla


def main():
    """Função principal."""
    
    # Processar argumentos de linha de comando
    args = processar_argumentos()
    
    # CONFIGURAÇÃO
    GERAR_RELATORIO_DETALHADO = True  # Relatório detalhado (compatível com versão distribuída)
    GERAR_RELATORIO_SIMPLIFICADO = True  # Novo relatório simplificado
    
    # Criar diretório de resultados
    result_dir = criar_diretorio_resultado()
    
    # Definir caminhos dos datasets

        # Fora do container - usar caminho relativo
    script_dir = os.path.dirname(os.path.abspath(__file__))
    caminho_dataset_a = os.path.join(script_dir, '..', 'Datasets', 'Reais', 'USCities','us.cities.txt')
    caminho_dataset_b = os.path.join(script_dir, '..', 'Datasets', 'Reais', 'USCities','us.cities.txt')
    
    print(f"\n{'=' * 80}")
    print(f"JUNÇÃO POR SIMILARIDADE - FORÇA BRUTA CENTRALIZADA")
    print(f"{'=' * 80}\n")
    
    # Ler Dataset A (consultas)
    print(f"Lendo Dataset A (consultas)...") 
    dataset_a, metadados_a, descricoes_a = ler_dataset(caminho_dataset_a)
    print(f"  ✓ {len(dataset_a)} tuplas carregadas de {metadados_a['nome_dataset']}")
    
    # Ler Dataset B (busca)
    print(f"\nLendo Dataset B (busca)...")
    dataset_b, metadados_b, descricoes_b = ler_dataset(caminho_dataset_b)
    print(f"  ✓ {len(dataset_b)} tuplas carregadas de {metadados_b['nome_dataset']}")
    
    # Padronizar dimensões (se necessário)
    dim_max = max(metadados_a['num_dimensoes'], metadados_b['num_dimensoes'])
    if metadados_a['num_dimensoes'] != dim_max or metadados_b['num_dimensoes'] != dim_max:
        print(f"\nPadronizando dimensões para {dim_max}...")
        dataset_a = [pad_attributes(t, dim_max) for t in dataset_a]
        dataset_b = [pad_attributes(t, dim_max) for t in dataset_b]
        metadados_a['num_dimensoes'] = dim_max
        metadados_b['num_dimensoes'] = dim_max
        print(f"  ✓ Dimensões padronizadas")
    
    # Selecionar amostra do Dataset A (ou usar tudo)
    print(f"\nSelecionando consultas do Dataset A...")
    max_consultas = 5  # None = todas as tuplas
    # max_consultas = 100  # Para testes, descomente e ajuste
    percent = True
    dataset_a_consultas = selecionar_amostra_dataset_a(dataset_a, max_consultas, percent)
    
    # Parâmetros da junção
    k = 5  # Número de vizinhos diversificados por consulta
    
    print(f"\nParâmetros da junção:")
    print(f"  k (vizinhos diversificados desejados): {k}")
    print(f"  Modo: BRIDk Centralizado (Força Bruta)")
    print(f"  Estratégia: Processa TODO o dataset B para cada consulta")
    
    # Executar junção (com debug se solicitado)
    resultados, tempo_total, estatisticas_exec = executar_juncao_brute_force(
        dataset_a_consultas, dataset_b, k, debug_consulta_id=args.debug
    )
    
    # Gerar relatórios
    print(f"\n{'=' * 80}")
    print(f"GERANDO RELATÓRIOS")
    print(f"{'=' * 80}\n")
    
    # Relatório Simplificado
    if GERAR_RELATORIO_SIMPLIFICADO:
        nome_arquivo = f"{metadados_a['nome_dataset']}_{k}_simplificado.txt"
        arquivo_simplificado = os.path.join(result_dir, nome_arquivo)
        gerar_relatorio_simplificado(resultados, arquivo_simplificado)
    
    # Relatório Detalhado (compatível com versão distribuída)
    if GERAR_RELATORIO_DETALHADO:
        print(f"\nGerando relatório detalhado...")
        estatisticas = gerar_estatisticas_juncao(resultados)
        
        # Adicionar estatísticas de execução ao relatório
        estatisticas['execucao'] = estatisticas_exec
        
        nome_arquivo = f"juncao_{metadados_a['nome_dataset']}_{k}_detalhado.txt"
        arquivo_detalhado = os.path.join(result_dir, "relatorio_juncao_detalhado_brute_force.txt")
        gerar_relatorio_juncao(
            resultados=resultados,
            descricoes_a=descricoes_a,
            descricoes_b=descricoes_b,
            metadados_a=metadados_a,
            metadados_b=metadados_b,
            estatisticas=estatisticas,
            output_file=arquivo_detalhado,
            max_exemplos=5,
            partition_counts=None,  # Força bruta não usa partições
            pivos_info=None,  # Força bruta não usa pivôs
            filtering_stats=estatisticas_exec  # Passar estatísticas de execução
        )
        print(f"  ✓ Relatório detalhado gerado: {arquivo_detalhado}")
    
    # Relatório de Debug (se solicitado)
    # Nota: O relatório de debug padrão não funciona para força bruta (requer pivôs)
    # O debug detalhado já foi exibido durante a execução se --debug foi especificado
    if args.debug is not None:
        print(f"\n✓ Debug detalhado foi exibido durante a execução para consulta {args.debug}")
        print(f"  (O relatório de debug em arquivo requer pivôs e não é aplicável ao modo força bruta)")
    
    print(f"\n{'=' * 80}")
    print(f"EXECUÇÃO CONCLUÍDA")
    print(f"{'=' * 80}")
    print(f"Tempo total: {tempo_total:.2f}s")
    print(f"Resultados salvos em: {result_dir}")
    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    main()
