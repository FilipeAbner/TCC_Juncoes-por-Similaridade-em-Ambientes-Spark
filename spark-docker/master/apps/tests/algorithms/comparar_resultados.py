#!/usr/bin/env python3
"""
Comparador de Resultados: Distribuído vs Centralizado

Este script compara os resultados da junção por similaridade entre:
- Execução distribuída (Spark com pivôs)
- Execução centralizada (Força bruta)

Usa os relatórios simplificados gerados por ambas as versões para
comparar os vizinhos encontrados para cada consulta.
"""

import os
import argparse
from collections import defaultdict


def ler_relatorio_simplificado(arquivo):
    """
    Lê relatório simplificado no formato:
    ID_consulta
    ID_viz1 ID_viz2 ... ID_vizk
    
    Returns:
        dict: {consulta_id: set(vizinhos_ids)}
    """
    resultados = {}
    
    with open(arquivo, 'r') as f:
        linhas = f.readlines()
    
    i = 0
    while i < len(linhas):
        # Linha com ID da consulta
        consulta_id = int(linhas[i].strip())
        i += 1
        
        # Linha com IDs dos vizinhos
        if i < len(linhas):
            vizinhos_str = linhas[i].strip()
            if vizinhos_str:
                vizinhos_ids = set(map(int, vizinhos_str.split()))
            else:
                vizinhos_ids = set()
            i += 1
        else:
            vizinhos_ids = set()
        
        resultados[consulta_id] = vizinhos_ids
    
    return resultados


def comparar_resultados(dist_result, central_result, output_file=None):
    """
    Compara resultados distribuídos e centralizados.
    
    Args:
        dist_result: Resultados da versão distribuída
        central_result: Resultados da versão centralizada
        output_file: Arquivo de saída para relatório (opcional)
    """
    # Estatísticas
    total_consultas = 0
    consultas_identicas = 0
    consultas_diferentes = 0
    
    # Métricas de similaridade
    jaccard_scores = []
    precisao_scores = []
    recall_scores = []
    
    # Detalhes das diferenças
    diferencas = []
    
    # Consultas em ambos os resultados
    consultas_comuns = set(dist_result.keys()) & set(central_result.keys())
    
    # Verificar se há consultas em comum
    if not consultas_comuns:
        print("AVISO: Nenhuma consulta em comum encontrada entre os resultados!")
        print(f"  IDs no Distribuído (primeiros 10): {sorted(list(dist_result.keys()))[:10]}")
        print(f"  IDs no Centralizado (primeiros 10): {sorted(list(central_result.keys()))[:10]}")
    
    for consulta_id in sorted(consultas_comuns):
        total_consultas += 1
        
        vizinhos_dist = dist_result[consulta_id]
        vizinhos_central = central_result[consulta_id]
        
        # Calcular métricas
        intersecao = vizinhos_dist & vizinhos_central
        uniao = vizinhos_dist | vizinhos_central
        
        # Jaccard Similarity
        jaccard = len(intersecao) / len(uniao) if len(uniao) > 0 else 0
        jaccard_scores.append(jaccard)
        
        # Precisão (quantos dos resultados distribuídos estão no centralizado)
        precisao = len(intersecao) / len(vizinhos_dist) if len(vizinhos_dist) > 0 else 0
        precisao_scores.append(precisao)
        
        # Recall (quantos dos resultados centralizados estão no distribuído)
        recall = len(intersecao) / len(vizinhos_central) if len(vizinhos_central) > 0 else 0
        recall_scores.append(recall)
        
        # Verificar se são idênticos
        if vizinhos_dist == vizinhos_central:
            consultas_identicas += 1
        else:
            consultas_diferentes += 1
            
            # Vizinhos únicos em cada versão
            apenas_dist = vizinhos_dist - vizinhos_central
            apenas_central = vizinhos_central - vizinhos_dist
            
            diferencas.append({
                'consulta_id': consulta_id,
                'jaccard': jaccard,
                'precisao': precisao,
                'recall': recall,
                'total_dist': len(vizinhos_dist),
                'total_central': len(vizinhos_central),
                'intersecao': len(intersecao),
                'apenas_dist': apenas_dist,
                'apenas_central': apenas_central
            })
    
    # Calcular médias
    jaccard_medio = sum(jaccard_scores) / len(jaccard_scores) if jaccard_scores else 0
    precisao_media = sum(precisao_scores) / len(precisao_scores) if precisao_scores else 0
    recall_medio = sum(recall_scores) / len(recall_scores) if recall_scores else 0
    f1_score = 2 * (precisao_media * recall_medio) / (precisao_media + recall_medio) if (precisao_media + recall_medio) > 0 else 0
    
    # Gerar relatório
    relatorio = []
    relatorio.append("=" * 80)
    relatorio.append("COMPARAÇÃO: DISTRIBUÍDO (SPARK) vs CENTRALIZADO (FORÇA BRUTA)")
    relatorio.append("=" * 80)
    relatorio.append("")
    
    relatorio.append("RESUMO GERAL")
    relatorio.append("-" * 80)
    relatorio.append(f"Total de consultas no Distribuído: {len(dist_result)}")
    relatorio.append(f"Total de consultas no Centralizado: {len(central_result)}")
    relatorio.append(f"Total de consultas comparadas: {total_consultas}")
    
    if total_consultas > 0:
        relatorio.append(f"Consultas com resultados idênticos: {consultas_identicas} ({consultas_identicas/total_consultas*100:.1f}%)")
        relatorio.append(f"Consultas com resultados diferentes: {consultas_diferentes} ({consultas_diferentes/total_consultas*100:.1f}%)")
    else:
        relatorio.append("ERRO: Nenhuma consulta em comum para comparar!")
        relatorio.append("Verifique se os IDs das consultas correspondem entre os arquivos.")
    relatorio.append("")
    
    relatorio.append("MÉTRICAS DE SIMILARIDADE (Médias)")
    relatorio.append("-" * 80)
    relatorio.append(f"Jaccard Similarity: {jaccard_medio:.4f}")
    relatorio.append(f"Precisão: {precisao_media:.4f}")
    relatorio.append(f"Recall: {recall_medio:.4f}")
    relatorio.append(f"F1-Score: {f1_score:.4f}")
    relatorio.append("")
    
    if diferencas:
        relatorio.append("DIFERENÇAS DETALHADAS")
        relatorio.append("-" * 80)
        relatorio.append(f"Total de consultas com diferenças: {len(diferencas)}")
        relatorio.append("")
        
        # Mostrar as 10 primeiras diferenças
        max_mostrar = min(10, len(diferencas))
        relatorio.append(f"Mostrando as primeiras {max_mostrar} diferenças:")
        relatorio.append("")
        
        for i, diff in enumerate(diferencas[:max_mostrar], 1):
            relatorio.append(f"\n{i}. Consulta ID: {diff['consulta_id']}")
            relatorio.append(f"   Jaccard: {diff['jaccard']:.4f}")
            relatorio.append(f"   Precisão: {diff['precisao']:.4f}")
            relatorio.append(f"   Recall: {diff['recall']:.4f}")
            relatorio.append(f"   Total Distribuído: {diff['total_dist']}")
            relatorio.append(f"   Total Centralizado: {diff['total_central']}")
            relatorio.append(f"   Interseção: {diff['intersecao']}")
            
            if diff['apenas_dist']:
                relatorio.append(f"   Apenas no Distribuído: {sorted(diff['apenas_dist'])}")
            if diff['apenas_central']:
                relatorio.append(f"   Apenas no Centralizado: {sorted(diff['apenas_central'])}")
    
    relatorio.append("")
    relatorio.append("=" * 80)
    
    # Imprimir no console
    relatorio_texto = '\n'.join(relatorio)
    print(relatorio_texto)
    
    # Salvar em arquivo se especificado
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(relatorio_texto)
        print(f"\nRelatório de comparação salvo em: {output_file}")
    
    return {
        'total_consultas': total_consultas,
        'consultas_identicas': consultas_identicas,
        'consultas_diferentes': consultas_diferentes,
        'jaccard_medio': jaccard_medio,
        'precisao_media': precisao_media,
        'recall_medio': recall_medio,
        'f1_score': f1_score,
        'diferencas': diferencas
    }


def main():
    """Função principal."""
    parser = argparse.ArgumentParser(
        description='Compara resultados de junção distribuída vs centralizada'
    )
    parser.add_argument(
        '--distribuido',
        type=str,
        required=True,
        help='Caminho do relatório simplificado da execução distribuída'
    )
    parser.add_argument(
        '--centralizado',
        type=str,
        required=True,
        help='Caminho do relatório simplificado da execução centralizada'
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Arquivo de saída para o relatório de comparação (opcional)'
    )
    
    args = parser.parse_args()
    
    # Verificar se arquivos existem
    if not os.path.exists(args.distribuido):
        print(f"ERRO: Arquivo não encontrado: {args.distribuido}")
        return
    
    if not os.path.exists(args.centralizado):
        print(f"ERRO: Arquivo não encontrado: {args.centralizado}")
        return
    
    print("Lendo resultados...")
    print(f"  Distribuído: {args.distribuido}")
    print(f"  Centralizado: {args.centralizado}")
    print()
    
    # Ler resultados
    dist_result = ler_relatorio_simplificado(args.distribuido)
    central_result = ler_relatorio_simplificado(args.centralizado)
    
    print(f"  ✓ Distribuído: {len(dist_result)} consultas")
    print(f"  ✓ Centralizado: {len(central_result)} consultas")
    print()
    
    # Comparar resultados
    comparar_resultados(dist_result, central_result, args.output)


if __name__ == "__main__":
    main()
