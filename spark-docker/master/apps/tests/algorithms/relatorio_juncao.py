#!/usr/bin/env python3
"""
Módulo de Geração de Relatórios para Junção por Similaridade

Este módulo contém todas as funcionalidades para gerar relatórios
detalhados sobre a junção por similaridade usando BRIDk.

Funções:
- gerar_estatisticas_juncao: Calcula estatísticas da junção
- gerar_relatorio_juncao: Gera relatório textual completo
- gerar_relatorio_debug: Gera relatório detalhado de debug para uma consulta específica
"""

import numpy as np


def gerar_estatisticas_juncao(resultados, metadados_a, metadados_b):
    """
    Calcula estatísticas sobre a junção realizada.
    
    Args:
        resultados: Dicionário com resultados da junção
        metadados_a: Metadados do dataset A
        metadados_b: Metadados do dataset B
    
    Returns:
        dict: Estatísticas da junção
    """
    total_pares = sum(len(r['resultado_b']) for r in resultados.values())
    
    distribuicao_tamanhos = {}
    for info in resultados.values():
        tamanho = len(info['resultado_b'])
        distribuicao_tamanhos[tamanho] = distribuicao_tamanhos.get(tamanho, 0) + 1
    
    # Calcular distâncias médias
    distancias_todas = []
    for info in resultados.values():
        consulta = info['consulta_a']
        for tupla_b in info['resultado_b']:
            dist = np.linalg.norm(
                np.array(tupla_b.getAttributes()) - np.array(consulta.getAttributes())
            )
            distancias_todas.append(dist)
    
    estatisticas = {
        'total_consultas': len(resultados),
        'total_pares': total_pares,
        'pares_por_consulta_medio': total_pares / len(resultados) if resultados else 0,
        'distribuicao_tamanhos': distribuicao_tamanhos,
        'distancia_media': np.mean(distancias_todas) if distancias_todas else 0,
        'distancia_min': np.min(distancias_todas) if distancias_todas else 0,
        'distancia_max': np.max(distancias_todas) if distancias_todas else 0,
        'distancia_std': np.std(distancias_todas) if distancias_todas else 0
    }
    
    return estatisticas


def gerar_relatorio_debug(consulta_id, resultados, descricoes_a, descricoes_b, 
                         dataset_b, pivos, output_file):
    """
    Gera relatório de debug detalhado para uma consulta específica.
    
    Mostra todas as etapas do processo BRIDk para identificar inconsistências.
    
    Args:
        consulta_id: ID da consulta a debugar
        resultados: Dicionário com resultados da junção
        descricoes_a: Descrições do dataset A
        descricoes_b: Descrições do dataset B
        dataset_b: Lista completa do dataset B
        pivos: Lista de pivôs
        distancias_pivos_dataset: Dict com distâncias pré-calculadas
        output_file: Caminho do arquivo de saída
    """
    
    if consulta_id not in resultados:
        print(f"Consulta ID {consulta_id} não encontrada nos resultados.")
        return
    
    info = resultados[consulta_id]
    consulta = info['consulta_a']
    resultado_b = info['resultado_b']
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=" * 100 + "\n")
        f.write(f"RELATÓRIO DE DEBUG - CONSULTA {consulta_id}\n")
        f.write("=" * 100 + "\n\n")
        
        # Informações da consulta
        f.write("CONSULTA ANALISADA\n")
        f.write("-" * 50 + "\n")
        f.write(f"ID: {consulta_id}\n")
        if consulta_id in descricoes_a:
            coords = consulta.getAttributes()
            f.write(f"{descricoes_a[consulta_id]} = ({coords[0]}, {coords[1]})\n")
        else:
            f.write(f"Coordenadas: {consulta.getAttributes()}\n")
        f.write("\n")
        
        # Parâmetros BRIDk
        f.write("PARÂMETROS BRIDk\n")
        f.write("-" * 50 + "\n")
        f.write(f"k (vizinhos): {info['k']}\n")
        f.write(f"d (partições/diversidade): {info['d']}\n")
        f.write("\n")
        
        # Pivôs utilizados
        f.write("PIVÔS UTILIZADOS\n")
        f.write("-" * 50 + "\n")
        for i, pivo in enumerate(pivos):
            f.write(f"Pivô {i}: ID {pivo.getId()}, Coordenadas {pivo.getAttributes()}\n")
        f.write("\n")
        
        # Distâncias da consulta aos pivôs
        f.write("DISTÂNCIAS DA CONSULTA AOS PIVÔS\n")
        f.write("-" * 50 + "\n")
        for i, pivo in enumerate(pivos):
            dist = np.linalg.norm(np.array(consulta.getAttributes()) - np.array(pivo.getAttributes()))
            f.write(f"Consulta -> Pivô {i}: {dist:.4f}\n")
        f.write("\n")
        
        # Todos os candidatos do dataset B com distâncias
        f.write("TODOS OS CANDIDATOS DO DATASET B\n")
        f.write("-" * 50 + "\n")
        f.write("Ordenados por distância à consulta:\n\n")
        
        candidatos_com_distancia = []
        for tupla_b in dataset_b:
            dist = np.linalg.norm(np.array(tupla_b.getAttributes()) - np.array(consulta.getAttributes()))
            desc = descricoes_b.get(tupla_b.getId(), '')
            candidatos_com_distancia.append({
                'id': tupla_b.getId(),
                'coords': tupla_b.getAttributes(),
                'desc': desc,
                'distancia': dist
            })
        
        # Ordenar por distância
        candidatos_com_distancia.sort(key=lambda x: x['distancia'])
        
        for cand in candidatos_com_distancia:
            desc_str = f" '{cand['desc']}'" if cand['desc'] else ""
            f.write(f"ID {cand['id']}{desc_str}: Dist={cand['distancia']:.4f}, Coords={cand['coords']}\n")
        f.write("\n")
        
        # Filtragem com desigualdade triangular (se aplicável)
        if 'estatisticas_filtragem' in info:
            f.write("FILTRAGEM COM DESIGUALDADE TRIANGULAR\n")
            f.write("-" * 50 + "\n")
            stats = info['estatisticas_filtragem']
            f.write(f"Candidatos antes da filtragem: {stats['total_dataset']}\n")
            f.write(f"Candidatos após filtragem: {stats['candidatos']}\n")
            f.write(f"Descartados: {stats['descartados']}\n")
            f.write(f"Taxa de filtragem: {stats['taxa_filtragem']:.2f}%\n")
            f.write(f"Tempo de filtragem: {info.get('tempo_filtragem', 0):.4f}s\n")
            f.write("\n")
        
        # Resultados finais do BRIDk
        f.write("RESULTADOS FINAIS DO BRIDk\n")
        f.write("-" * 50 + "\n")
        f.write(f"Total de vizinhos selecionados: {len(resultado_b)}\n\n")
        
        for i, tupla_b in enumerate(resultado_b, 1):
            tupla_b_id = tupla_b.getId()
            dist = np.linalg.norm(np.array(tupla_b.getAttributes()) - np.array(consulta.getAttributes()))
            desc = descricoes_b.get(tupla_b_id, '')
            desc_str = f" '{desc}'" if desc else ""
            f.write(f"{i}. ID {tupla_b_id}{desc_str}: Dist={dist:.4f}, Coords={tupla_b.getAttributes()}\n")
        
        f.write("\n")
        
        # Candidatos por partição/pivô
        f.write("CANDIDATOS POR PARTIÇÃO/PIVÔ\n")
        f.write("-" * 50 + "\n")
        candidatos_por_pivo = {i: [] for i in range(len(pivos))}
        for cand in candidatos_com_distancia:
            # Encontrar o pivô mais próximo para este candidato
            pivo_mais_proximo = min(range(len(pivos)), 
                                   key=lambda i: np.linalg.norm(np.array(cand['coords']) - np.array(pivos[i].getAttributes())))
            candidatos_por_pivo[pivo_mais_proximo].append(cand)
        
        for p_idx in range(len(pivos)):
            f.write(f"Pivô {p_idx} (ID {pivos[p_idx].getId()}):\n")
            if candidatos_por_pivo[p_idx]:
                # Ordenar candidatos desta partição por distância
                candidatos_por_pivo[p_idx].sort(key=lambda x: x['distancia'])
                for cand in candidatos_por_pivo[p_idx][:5]:  # Mostrar top 5 por partição
                    desc_str = f" '{cand['desc']}'" if cand['desc'] else ""
                    marker = " <-- SELECIONADO" if any(t.getId() == cand['id'] for t in resultado_b) else ""
                    f.write(f"  ID {cand['id']}{desc_str}: Dist={cand['distancia']:.4f}{marker}\n")
                if len(candidatos_por_pivo[p_idx]) > 5:
                    f.write(f"  ... e mais {len(candidatos_por_pivo[p_idx]) - 5} candidatos\n")
            else:
                f.write("  (nenhum candidato nesta partição)\n")
            f.write("\n")
        
        # Análise de possíveis problemas
        f.write("ANÁLISE\n")
        f.write("-" * 50 + "\n")
        
        # Verificar se os k primeiros por distância estão nos resultados
        k = info['k']
        top_k_por_distancia = candidatos_com_distancia[:k]
        top_k_ids = {cand['id'] for cand in top_k_por_distancia}
        resultado_ids = {tupla.getId() for tupla in resultado_b}
        
        f.write(f"Top {k} candidatos por distância (sem diversidade):\n")
        for cand in top_k_por_distancia:
            desc_str = f" '{cand['desc']}'" if cand['desc'] else ""
            marker = " <-- SELECIONADO" if cand['id'] in resultado_ids else ""
            f.write(f"  ID {cand['id']}{desc_str}: Dist={cand['distancia']:.4f}{marker}\n")
        
        f.write("\n")
        
        nao_selecionados_top = top_k_ids - resultado_ids
        if nao_selecionados_top:
            f.write("Candidatos do top-k por distância NÃO selecionados pelo BRIDk:\n")
            for cid in nao_selecionados_top:
                cand = next(c for c in top_k_por_distancia if c['id'] == cid)
                desc_str = f" '{cand['desc']}'" if cand['desc'] else ""
                f.write(f"  ID {cid}{desc_str}: Dist={cand['distancia']:.4f}\n")
            f.write("Isso indica que o BRIDk priorizou diversidade sobre proximidade.\n")
            f.write("Verifique a seção 'CANDIDATOS POR PARTIÇÃO/PIVÔ' para entender por quê.\n")
        else:
            f.write("Todos os top-k por distância foram selecionados.\n")
        
        f.write("\n")
        
        # Verificar diversidade (distribuição por partições/pivôs)
        if pivos:
            f.write("DISTRIBUIÇÃO DOS RESULTADOS POR PIVÔ MAIS PRÓXIMO\n")
            f.write("-" * 50 + "\n")
            distribuicao = {}
            for tupla_b in resultado_b:
                pivo_mais_proximo = min(range(len(pivos)), 
                                       key=lambda i: np.linalg.norm(np.array(tupla_b.getAttributes()) - np.array(pivos[i].getAttributes())))
                distribuicao[pivo_mais_proximo] = distribuicao.get(pivo_mais_proximo, 0) + 1
            
            for p_idx, count in sorted(distribuicao.items()):
                pivo_desc = descricoes_b.get(pivos[p_idx].getId(), '')
                pivo_desc_str = f" '{pivo_desc}'" if pivo_desc else ""
                f.write(f"Pivô {p_idx} (ID {pivos[p_idx].getId()}{pivo_desc_str}): {count} seleções\n")
            f.write("\n")
        

def gerar_relatorio_juncao(resultados, descricoes_a, descricoes_b, 
                           metadados_a, metadados_b, estatisticas, 
                           output_file, max_exemplos=5, partition_counts=None, 
                           pivos_info=None, filtering_stats=None):  # Novo parâmetro
    """
    Gera relatório textual detalhado da junção.
    
    Args:
        resultados: Dicionário com resultados da junção
        descricoes_a: Descrições do dataset A
        descricoes_b: Descrições do dataset B
        metadados_a: Metadados do dataset A
        metadados_b: Metadados do dataset B
        estatisticas: Estatísticas calculadas da junção
        output_file: Caminho do arquivo de saída
        max_exemplos: Número máximo de exemplos a incluir no relatório
        partition_counts: Dicionário com contagens por partição
        pivos_info: Dicionário com informações dos pivôs e objetos associados
    """

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("RELATÓRIO DE JUNÇÃO POR SIMILARIDADE - ALGORITMO BRIDk\n")
        f.write("=" * 80 + "\n\n")
        
        # Informações dos datasets
        f.write("INFORMAÇÕES DOS DATASETS\n")
        f.write("-" * 80 + "\n\n")
        
        f.write(f"Dataset A (Consultas):\n")
        f.write(f"  Nome: {metadados_a['nome_dataset']}\n")
        f.write(f"  Arquivo: {metadados_a['caminho_arquivo']}\n")
        f.write(f"  Total de tuplas: {metadados_a['num_tuplas']}\n")
        f.write(f"  Dimensões: {metadados_a['num_dimensoes']}\n")
        f.write(f"  Consultas executadas: {estatisticas['total_consultas']}\n\n")
        
        f.write(f"Dataset B (Busca):\n")
        f.write(f"  Nome: {metadados_b['nome_dataset']}\n")
        f.write(f"  Arquivo: {metadados_b['caminho_arquivo']}\n")
        f.write(f"  Total de tuplas: {metadados_b['num_tuplas']}\n")
        f.write(f"  Dimensões: {metadados_b['num_dimensoes']}\n\n")
        
        # Estatísticas gerais
        f.write("ESTATÍSTICAS DA JUNÇÃO\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total de pares (A,B) gerados: {estatisticas['total_pares']}\n")
        f.write(f"Média de pares por consulta: {estatisticas['pares_por_consulta_medio']:.2f}\n")
        f.write(f"Distância média: {estatisticas['distancia_media']:.4f}\n")
        f.write(f"Distância mínima: {estatisticas['distancia_min']:.4f}\n")
        f.write(f"Distância máxima: {estatisticas['distancia_max']:.4f}\n")
        f.write(f"Desvio padrão: {estatisticas['distancia_std']:.4f}\n\n")
        
        # Adicionar seção de Estatísticas de Filtragem (se disponível)
        if filtering_stats and filtering_stats.get('total_candidatos', 0) > 0:
            f.write("ESTATÍSTICAS DE FILTRAGEM COM PIVÔS\n")
            f.write("-" * 80 + "\n")
            f.write(f"Total de candidatos processados: {filtering_stats['total_candidatos']}\n")
            f.write(f"Total de objetos descartados: {filtering_stats['total_descartados']}\n")
            taxa_filtragem = (filtering_stats['total_descartados'] / 
                              (filtering_stats['total_candidatos'] + filtering_stats['total_descartados'])) * 100
            f.write(f"Taxa de filtragem: {taxa_filtragem:.2f}%\n")
            f.write(f"Tempo total de filtragem: {filtering_stats['tempo_filtragem']:.2f}s\n")
            f.write(f"Tempo total BRIDk: {filtering_stats['tempo_bridk']:.2f}s\n\n")
        
                # Distribuição por partição (se disponível)
        if partition_counts:
            f.write("\nDISTRIBUIÇÃO POR PARTIÇÃO\n")
            f.write("-" * 80 + "\n")
            num_p = partition_counts.get('num_partitions', max(
                max(partition_counts.get('A', {}).keys(), default=[-1]) + 1,
                max(partition_counts.get('B', {}).keys(), default=[-1]) + 1
            ))
            f.write("Partição\tTuplas A\tTuplas B\n")
            for pid in range(num_p):
                a_count = partition_counts.get('A', {}).get(pid, 0)
                b_count = partition_counts.get('B', {}).get(pid, 0)
                f.write(f"  Partição {pid+1:>2}: {a_count:>5} tuplas A\t{b_count:>5} tuplas B\n")
            f.write("\n")
        
        # Parâmetros
        if resultados:
            primeiro = list(resultados.values())[0]
            f.write(f"Parâmetros: k={primeiro['k']}, d={primeiro['d']}\n\n")
        
        # Objetos associados a cada pivô (se disponível)
        if pivos_info:
            f.write("=" * 80 + "\n")
            f.write("OBJETOS ASSOCIADOS A CADA PIVÔ\n")
            f.write("=" * 80 + "\n\n")
            
            pivos = pivos_info['pivos']
            objetos_a = pivos_info['objetos_a']
            objetos_b = pivos_info['objetos_b']
            desc_a = pivos_info['descricoes_a']
            desc_b = pivos_info['descricoes_b']
            
            for i in range(len(pivos)):
                pivo = pivos[i]
                f.write(f"\nPIVÔ {i} (ID {pivo.getId()})\n")
                f.write("-" * 80 + "\n")
                f.write(f"Coordenadas: {pivo.getAttributes()}\n")
                
                if hasattr(pivo, 'getDescription') and pivo.getDescription():
                    f.write(f"Descrição: {pivo.getDescription()}\n")
                
                # Objetos do Dataset A
                objs_a = objetos_a.get(i, [])
                f.write(f"\nObjetos do Dataset A associados ({len(objs_a)}):\n")
                if objs_a:
                    for obj_info in objs_a:
                        desc = desc_a.get(obj_info['id'], '')
                        f.write(f"  - ID {obj_info['id']}: {obj_info['coords']}")
                        if desc:
                            f.write(f" ({desc})")
                        f.write("\n")
                else:
                    f.write("  (nenhum)\n")
                
                # Objetos do Dataset B
                objs_b = objetos_b.get(i, [])
                f.write(f"\nObjetos do Dataset B associados ({len(objs_b)}):\n")
                if objs_b:
                    # Limitar a 20 objetos por pivô no relatório
                    max_exibir = 20
                    for idx, obj_info in enumerate(objs_b[:max_exibir]):
                        desc = desc_b.get(obj_info['id'], '')
                        f.write(f"  - ID {obj_info['id']}: {obj_info['coords']}")
                        if desc:
                            f.write(f" ({desc})")
                        f.write("\n")
                    if len(objs_b) > max_exibir:
                        f.write(f"  ... e mais {len(objs_b) - max_exibir} objetos\n")
                else:
                    f.write("  (nenhum)\n")
                
                f.write("\n")
        
        # Exemplos de junções
        f.write("=" * 80 + "\n")
        f.write(f"EXEMPLOS DE JUNÇÕES (primeiras {max_exemplos} consultas)\n")
        f.write("=" * 80 + "\n\n")
        
        for idx, (consulta_id, info) in enumerate(sorted(resultados.items())[:max_exemplos], 1):
            consulta = info['consulta_a']
            resultado_b = info['resultado_b']
            
            f.write(f"\nCONSULTA {idx}\n")
            f.write("-" * 80 + "\n")
            f.write(f"ID do Dataset A: {consulta_id}\n")
            
            if consulta_id in descricoes_a:
                f.write(f"Descrição: {descricoes_a[consulta_id]}\n")
            
            f.write(f"Coordenadas: {consulta.getAttributes()}\n")
            f.write(f"\nPares encontrados no Dataset B: {len(resultado_b)}\n\n")
            
            f.write("PARES (A,B):\n")
            f.write("-" * 80 + "\n")
            
            for j, tupla_b in enumerate(resultado_b, 1):
                tupla_b_id = tupla_b.getId()
                
                # Calcular distância
                dist = np.linalg.norm(
                    np.array(tupla_b.getAttributes()) - np.array(consulta.getAttributes())
                )
                
                f.write(f"\n{j}. Par: (A:{consulta_id}, B:{tupla_b_id})\n")
                f.write(f"   ID B: {tupla_b_id}\n")
                
                if tupla_b_id in descricoes_b:
                    f.write(f"   Descrição B: {descricoes_b[tupla_b_id]}\n")
                
                f.write(f"   Coordenadas B: {tupla_b.getAttributes()}\n")
                f.write(f"   Distância: {dist:.4f}\n")
            
            f.write("\n" + "=" * 80 + "\n")


def gerar_estatisticas_juncao(resultados):
    """
    Calcula estatísticas sobre a junção realizada.
    
    Args:
        resultados: Dicionário com resultados da junção
    
    Returns:
        dict: Estatísticas da junção
    """
    total_pares = sum(len(r['resultado_b']) for r in resultados.values())
    
    distribuicao_tamanhos = {}
    for info in resultados.values():
        tamanho = len(info['resultado_b'])
        distribuicao_tamanhos[tamanho] = distribuicao_tamanhos.get(tamanho, 0) + 1
    
    # Calcular distâncias médias
    distancias_todas = []
    for info in resultados.values():
        consulta = info['consulta_a']
        for tupla_b in info['resultado_b']:
            dist = np.linalg.norm(
                np.array(tupla_b.getAttributes()) - np.array(consulta.getAttributes())
            )
            distancias_todas.append(dist)
    
    estatisticas = {
        'total_consultas': len(resultados),
        'total_pares': total_pares,
        'pares_por_consulta_medio': total_pares / len(resultados) if resultados else 0,
        'distribuicao_tamanhos': distribuicao_tamanhos,
        'distancia_media': np.mean(distancias_todas) if distancias_todas else 0,
        'distancia_min': np.min(distancias_todas) if distancias_todas else 0,
        'distancia_max': np.max(distancias_todas) if distancias_todas else 0,
        'distancia_std': np.std(distancias_todas) if distancias_todas else 0
    }
    
    return estatisticas


def gerar_relatorio_juncao(resultados, descricoes_a, descricoes_b, 
                           metadados_a, metadados_b, estatisticas, 
                           output_file, max_exemplos=5, partition_counts=None, 
                           pivos_info=None, filtering_stats=None):  # Novo parâmetro
    """
    Gera relatório textual detalhado da junção.
    
    Args:
        resultados: Dicionário com resultados da junção
        descricoes_a: Descrições do dataset A
        descricoes_b: Descrições do dataset B
        metadados_a: Metadados do dataset A
        metadados_b: Metadados do dataset B
        estatisticas: Estatísticas calculadas da junção
        output_file: Caminho do arquivo de saída
        max_exemplos: Número máximo de exemplos a incluir no relatório
        partition_counts: Dicionário com contagens por partição
        pivos_info: Dicionário com informações dos pivôs e objetos associados
    """

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("RELATÓRIO DE JUNÇÃO POR SIMILARIDADE - ALGORITMO BRIDk\n")
        f.write("=" * 80 + "\n\n")
        
        # Informações dos datasets
        f.write("INFORMAÇÕES DOS DATASETS\n")
        f.write("-" * 80 + "\n\n")
        
        f.write(f"Dataset A (Consultas):\n")
        f.write(f"  Nome: {metadados_a['nome_dataset']}\n")
        f.write(f"  Arquivo: {metadados_a['caminho_arquivo']}\n")
        f.write(f"  Total de tuplas: {metadados_a['num_tuplas']}\n")
        f.write(f"  Dimensões: {metadados_a['num_dimensoes']}\n")
        f.write(f"  Consultas executadas: {estatisticas['total_consultas']}\n\n")
        
        f.write(f"Dataset B (Busca):\n")
        f.write(f"  Nome: {metadados_b['nome_dataset']}\n")
        f.write(f"  Arquivo: {metadados_b['caminho_arquivo']}\n")
        f.write(f"  Total de tuplas: {metadados_b['num_tuplas']}\n")
        f.write(f"  Dimensões: {metadados_b['num_dimensoes']}\n\n")
        
        # Estatísticas gerais
        f.write("ESTATÍSTICAS DA JUNÇÃO\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total de pares (A,B) gerados: {estatisticas['total_pares']}\n")
        f.write(f"Média de pares por consulta: {estatisticas['pares_por_consulta_medio']:.2f}\n")
        f.write(f"Distância média: {estatisticas['distancia_media']:.4f}\n")
        f.write(f"Distância mínima: {estatisticas['distancia_min']:.4f}\n")
        f.write(f"Distância máxima: {estatisticas['distancia_max']:.4f}\n")
        f.write(f"Desvio padrão: {estatisticas['distancia_std']:.4f}\n\n")
        
        # Adicionar seção de Estatísticas de Filtragem (se disponível)
        if filtering_stats and filtering_stats.get('total_candidatos', 0) > 0:
            f.write("ESTATÍSTICAS DE FILTRAGEM COM PIVÔS\n")
            f.write("-" * 80 + "\n")
            f.write(f"Total de candidatos processados: {filtering_stats['total_candidatos']}\n")
            f.write(f"Total de objetos descartados: {filtering_stats['total_descartados']}\n")
            taxa_filtragem = (filtering_stats['total_descartados'] / 
                              (filtering_stats['total_candidatos'] + filtering_stats['total_descartados'])) * 100
            f.write(f"Taxa de filtragem: {taxa_filtragem:.2f}%\n")
            f.write(f"Tempo total de filtragem: {filtering_stats['tempo_filtragem']:.2f}s\n")
            f.write(f"Tempo total BRIDk: {filtering_stats['tempo_bridk']:.2f}s\n\n")
        
                # Distribuição por partição (se disponível)
        if partition_counts:
            f.write("\nDISTRIBUIÇÃO POR PARTIÇÃO\n")
            f.write("-" * 80 + "\n")
            num_p = partition_counts.get('num_partitions', max(
                max(partition_counts.get('A', {}).keys(), default=[-1]) + 1,
                max(partition_counts.get('B', {}).keys(), default=[-1]) + 1
            ))
            f.write("Partição\tTuplas A\tTuplas B\n")
            for pid in range(num_p):
                a_count = partition_counts.get('A', {}).get(pid, 0)
                b_count = partition_counts.get('B', {}).get(pid, 0)
                f.write(f"  Partição {pid+1:>2}: {a_count:>5} tuplas A\t{b_count:>5} tuplas B\n")
            f.write("\n")
        
        # Parâmetros
        if resultados:
            primeiro = list(resultados.values())[0]
            f.write(f"Parâmetros: k={primeiro['k']}, d={primeiro['d']}\n\n")
        
        # Objetos associados a cada pivô (se disponível)
        if pivos_info:
            f.write("=" * 80 + "\n")
            f.write("OBJETOS ASSOCIADOS A CADA PIVÔ\n")
            f.write("=" * 80 + "\n\n")
            
            pivos = pivos_info['pivos']
            objetos_a = pivos_info['objetos_a']
            objetos_b = pivos_info['objetos_b']
            desc_a = pivos_info['descricoes_a']
            desc_b = pivos_info['descricoes_b']
            
            for i in range(len(pivos)):
                pivo = pivos[i]
                f.write(f"\nPIVÔ {i} (ID {pivo.getId()})\n")
                f.write("-" * 80 + "\n")
                f.write(f"Coordenadas: {pivo.getAttributes()}\n")
                
                if hasattr(pivo, 'getDescription') and pivo.getDescription():
                    f.write(f"Descrição: {pivo.getDescription()}\n")
                
                # Objetos do Dataset A
                objs_a = objetos_a.get(i, [])
                f.write(f"\nObjetos do Dataset A associados ({len(objs_a)}):\n")
                if objs_a:
                    for obj_info in objs_a:
                        desc = desc_a.get(obj_info['id'], '')
                        f.write(f"  - ID {obj_info['id']}: {obj_info['coords']}")
                        if desc:
                            f.write(f" ({desc})")
                        f.write("\n")
                else:
                    f.write("  (nenhum)\n")
                
                # Objetos do Dataset B
                objs_b = objetos_b.get(i, [])
                f.write(f"\nObjetos do Dataset B associados ({len(objs_b)}):\n")
                if objs_b:
                    # Limitar a 20 objetos por pivô no relatório
                    max_exibir = 20
                    for idx, obj_info in enumerate(objs_b[:max_exibir]):
                        desc = desc_b.get(obj_info['id'], '')
                        f.write(f"  - ID {obj_info['id']}: {obj_info['coords']}")
                        if desc:
                            f.write(f" ({desc})")
                        f.write("\n")
                    if len(objs_b) > max_exibir:
                        f.write(f"  ... e mais {len(objs_b) - max_exibir} objetos\n")
                else:
                    f.write("  (nenhum)\n")
                
                f.write("\n")
        
        # Exemplos de junções
        f.write("=" * 80 + "\n")
        f.write(f"EXEMPLOS DE JUNÇÕES (primeiras {max_exemplos} consultas)\n")
        f.write("=" * 80 + "\n\n")
        
        for idx, (consulta_id, info) in enumerate(sorted(resultados.items())[:max_exemplos], 1):
            consulta = info['consulta_a']
            resultado_b = info['resultado_b']
            
            f.write(f"\nCONSULTA {idx}\n")
            f.write("-" * 80 + "\n")
            f.write(f"ID do Dataset A: {consulta_id}\n")
            
            if consulta_id in descricoes_a:
                f.write(f"Descrição: {descricoes_a[consulta_id]}\n")
            
            f.write(f"Coordenadas: {consulta.getAttributes()}\n")
            f.write(f"\nPares encontrados no Dataset B: {len(resultado_b)}\n\n")
            
            f.write("PARES (A,B):\n")
            f.write("-" * 80 + "\n")
            
            for j, tupla_b in enumerate(resultado_b, 1):
                tupla_b_id = tupla_b.getId()
                
                # Calcular distância
                dist = np.linalg.norm(
                    np.array(tupla_b.getAttributes()) - np.array(consulta.getAttributes())
                )
                
                f.write(f"\n{j}. Par: (A:{consulta_id}, B:{tupla_b_id})\n")
                f.write(f"   ID B: {tupla_b_id}\n")
                
                if tupla_b_id in descricoes_b:
                    f.write(f"   Descrição B: {descricoes_b[tupla_b_id]}\n")
                
                f.write(f"   Coordenadas B: {tupla_b.getAttributes()}\n")
                f.write(f"   Distância: {dist:.4f}\n")
            
            f.write("\n" + "=" * 80 + "\n")
    
