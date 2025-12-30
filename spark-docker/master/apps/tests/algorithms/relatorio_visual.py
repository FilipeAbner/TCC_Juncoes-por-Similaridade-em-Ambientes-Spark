#!/usr/bin/env python3
"""
Geração de visualizações 2D para resultados de junção por similaridade (BRIDk).

Função principal:
- gerar_grafico_juncao(resultados, dataset_b=None, dataset_a=None, pivos=None, output_file=...)

O gráfico mostra:
- todos os pontos de B (pontos pequenos, cor clara)
- consultas (marcador estrela, cor distinta)
- objetos escolhidos (marcador maior por consulta)
- circunferência ao redor de cada objeto escolhido com raio = distância objeto-consulta
- pivôs (se informados) com marcador específico

Retorna: caminho do arquivo salvo.
"""

import math
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib import cm
import numpy as np
from typing import List, Dict, Any, Optional


def _to_coords(obj) -> (float, float):
    """Extrai coordenadas de um objeto Tuple/Point ou de um tuple/list."""
    # Caso objeto já seja (x,y)
    if isinstance(obj, (tuple, list)) and len(obj) >= 2:
        return float(obj[0]), float(obj[1])
    # Caso objeto seja o tipo Tuple do projeto (possui getAttributes)
    if hasattr(obj, "getAttributes"):
        attrs = obj.getAttributes()
        return float(attrs[0]), float(attrs[1])
    # Caso objeto seja Point (possui x/y)
    if hasattr(obj, "x") and hasattr(obj, "y"):
        return float(obj.x), float(obj.y)
    raise ValueError("Não foi possível extrair coordenadas do objeto: " + str(type(obj)))


def gerar_grafico_juncao(
    resultados: Dict[Any, dict],
    dataset_b: Optional[List[Any]] = None,
    dataset_a: Optional[List[Any]] = None,
    pivos: Optional[List[Any]] = None,
    output_file: str = "/apps/result/juncao_visual.png",
    figsize=(8, 8),
    dpi=150
) -> str:
    """
    Gera e salva o gráfico 2D.

    Args:
        resultados: dicionário de resultados (mesmo formato retornado por juncao_similaridade).
        dataset_b: lista de objetos do dataset B (opcional, para plotar todos os pontos).
        dataset_a: lista de objetos do dataset A (opcional, para plotar todos os pontos de consulta).
        pivos: lista de pivôs (cada pivô pode ser Tuple, Point ou (x,y)).
        output_file: caminho do arquivo de saída (PNG).
    Returns:
        output_file
    """
    plt.figure(figsize=figsize)
    ax = plt.gca()
    ax.set_aspect('equal', adjustable='box')

    # Plot dataset B (todos os pontos) como pontos cinza claros
    if dataset_b:
        coords_b = [_to_coords(t) for t in dataset_b]
        xb = [c[0] for c in coords_b]
        yb = [c[1] for c in coords_b]
        ax.scatter(xb, yb, s=10, color="#cccccc", label="Dataset B (todos)")

    # Optionally plot dataset A points (all queries)
    if dataset_a:
        coords_a = [_to_coords(t) for t in dataset_a]
        xa = [c[0] for c in coords_a]
        ya = [c[1] for c in coords_a]
        ax.scatter(xa, ya, s=30, color="magenta", marker="^", label="Dataset A (consultas)")

    # Plot pivots
    if pivos:
        coords_p = [_to_coords(p) for p in pivos]
        xp = [c[0] for c in coords_p]
        yp = [c[1] for c in coords_p]
        ax.scatter(xp, yp, s=120, color="black", marker="P", label="Pivôs")

    # Colors: one color per query (cycled)
    cmap = cm.get_cmap("tab10")
    queries = list(resultados.items())
    nqueries = len(queries)
    if nqueries == 0:
        raise ValueError("resultados vazio: nenhum ponto de consulta encontrado para plotar.")

    # Limitar às primeiras 5 consultas para clareza
    queries = queries[:5]

    for qi, (consulta_id, info) in enumerate(queries):
        consulta = info['consulta_a']
        qx, qy = _to_coords(consulta)
        color = cmap(qi % 10)

        # Plot consulta
        ax.scatter([qx], [qy], s=120, marker="*", color=color, edgecolor='black', zorder=5,
                   label=f"Consulta ID {consulta_id}")

        # Plot escolhidos para essa consulta
        chosen = info['resultado_b']
        for chosen_obj in chosen:
            cx, cy = _to_coords(chosen_obj)
            # distância para consulta
            dist = math.dist((qx, qy), (cx, cy))
            # marcador do chosen object
            ax.scatter([cx], [cy], s=80, marker="o", color=color, edgecolor='black', zorder=6)
            # circunferência com raio = distância
            circ = patches.Circle((cx, cy), radius=dist, fill=False, edgecolor=color, linewidth=1.2, alpha=0.8, linestyle='--')
            ax.add_patch(circ)

    # Legenda e layout
    ax.set_xlabel("X")
    ax.set_ylabel("Y")
    # Construir legenda manual evitando duplicatas
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ax.legend(by_label.values(), by_label.keys(), loc='best', fontsize='small')

    plt.title("Visualização: consultas, pivôs e objetos escolhidos")
    plt.tight_layout()
    plt.savefig(output_file, dpi=dpi, bbox_inches='tight')
    plt.close()

    return output_file


# Exemplo de uso (para rodar manualmente)
if __name__ == "__main__":
    # Este bloco não executa em produção; é apenas exemplificativo.
    print("Módulo `relatorio_visual` carregado - use a função gerar_grafico_juncao() a partir do seu script.")