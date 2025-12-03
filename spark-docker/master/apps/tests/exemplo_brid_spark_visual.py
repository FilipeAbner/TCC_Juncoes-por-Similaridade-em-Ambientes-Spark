"""
Exemplo BRIDk com PySpark + Visualização Gráfica

Gera gráficos 2D e 3D mostrando:
- Dataset completo (cinza)
- Query (vermelho, estrela)
- Pontos selecionados (verde)
"""

from pyspark.sql import SparkSession
from brid_python.spark.brid_spark import BridSpark
from brid_python.types.tuple import Tuple
from brid_python.types.point import Point
from brid_python.helpers.parser_tuple import ParserTuple

import matplotlib
matplotlib.use('Agg')  # Backend sem interface gráfica
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np


def visualizar_resultados_3d(dataset, query, results, output_file="/apps/brid_resultado_3d.png"):
    """Gera visualização 3D dos resultados do BRIDk."""
    print(f"\n📊 Gerando visualização 3D: {output_file}")
    
    # Extrair coordenadas
    dataset_coords = np.array([t.getAttributes() for t in dataset])
    query_coords = np.array(query.getAttributes())
    results_coords = np.array([t.getAttributes() for t in results])
    results_ids = set(t.getId() for t in results)
    
    # Separar selecionados e não selecionados
    not_selected = [t for t in dataset if t.getId() not in results_ids]
    not_selected_coords = np.array([t.getAttributes() for t in not_selected]) if not_selected else np.array([]).reshape(0, 3)
    
    # Criar figura 3D
    fig = plt.figure(figsize=(14, 10))
    ax = fig.add_subplot(111, projection='3d')
    
    # Plotar pontos não selecionados (cinza)
    if len(not_selected_coords) > 0:
        ax.scatter(not_selected_coords[:, 0], 
                   not_selected_coords[:, 1], 
                   not_selected_coords[:, 2],
                   c='lightgray', marker='o', s=40, alpha=0.5, label='Dataset')
    
    # Plotar pontos selecionados (verde)
    if len(results_coords) > 0:
        ax.scatter(results_coords[:, 0], 
                   results_coords[:, 1], 
                   results_coords[:, 2],
                   c='limegreen', marker='o', s=150, alpha=0.9, 
                   edgecolors='darkgreen', linewidths=2.5, label=f'Selecionados (k={len(results)})')
        
        # Adicionar labels dos IDs
        for result in results:
            coords = result.getAttributes()
            ax.text(coords[0], coords[1], coords[2], f"  {result.getId()}", 
                   fontsize=8, color='darkgreen', fontweight='bold')
    
    # Plotar query (vermelho estrela)
    ax.scatter(query_coords[0], query_coords[1], query_coords[2],
               c='red', marker='*', s=600, alpha=1.0,
               edgecolors='darkred', linewidths=3, label='Query', zorder=10)
    
    # Conectar query aos selecionados com linhas
    for result in results:
        coords = result.getAttributes()
        ax.plot([query_coords[0], coords[0]],
                [query_coords[1], coords[1]],
                [query_coords[2], coords[2]],
                'r--', alpha=0.4, linewidth=1.0)
    
    # Configurações do gráfico
    ax.set_xlabel('Dimensão X', fontsize=11, fontweight='bold')
    ax.set_ylabel('Dimensão Y', fontsize=11, fontweight='bold')
    ax.set_zlabel('Dimensão Z', fontsize=11, fontweight='bold')
    ax.set_title('BRIDk - Visualização 3D dos Resultados', fontsize=15, fontweight='bold', pad=20)
    ax.legend(loc='upper left', fontsize=11, framealpha=0.9)
    ax.grid(True, alpha=0.3)
    
    # Ajustar ângulo de visualização
    ax.view_init(elev=20, azim=45)
    
    # Salvar
    plt.tight_layout()
    plt.savefig(output_file, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"   ✓ Gráfico 3D salvo: {output_file}")


def visualizar_resultados_2d(dataset, query, results, output_file="/apps/brid_resultado_2d.png"):
    """Gera visualização 2D (projeção XY) dos resultados do BRIDk."""
    print(f"\n📊 Gerando visualização 2D: {output_file}")
    
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
                   c='lightgray', marker='o', s=40, alpha=0.5, label='Dataset')
    
    # Plotar pontos selecionados
    if len(results_coords) > 0:
        ax.scatter(results_coords[:, 0], results_coords[:, 1],
                   c='limegreen', marker='o', s=150, alpha=0.9,
                   edgecolors='darkgreen', linewidths=2.5, label=f'Selecionados (k={len(results)})')
        
        # Adicionar labels dos IDs
        for result in results:
            coords = result.getAttributes()[:2]
            ax.annotate(f" {result.getId()}", xy=(coords[0], coords[1]), 
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
    
    print(f"   ✓ Gráfico 2D salvo: {output_file}")


def exemplo_com_visualizacao():
    """Exemplo BRIDk com visualização gráfica."""
    print("="*80)
    print("BRIDk DISTRIBUÍDO COM PYSPARK + VISUALIZAÇÃO GRÁFICA")
    print("="*80)
    
    # Criar SparkSession
    spark = SparkSession.builder \
        .appName("BRIDk-Visualization") \
        .getOrCreate()
    
    try:
        # Criar dataset com pontos específicos
        print("\n1. Criando dataset de exemplo...")
        data = []
        dataset_tuples = []
        
        # Dataset: Sq=(0,0), A=(3,0), B=(3,2), C=(5,-3), D=(4,-5)
        pontos = [
            (0, 0.0, 0.0, "Sq"),
            (1, 3.0, 0.0, "A"),
            (2, 3.0, 2.0, "B"),
            (3, 5.0, -3.0, "C"),
            (4, 4.0, -5.0, "D")
        ]
        
        for pid, x, y, nome in pontos:
            # Formato arquivo: id \t x \t y \t descricao (2D, sem Z)
            data.append(f"{pid}\t{x}\t{y}\t{nome}")
            
            # Guardar tuplas para visualização
            tupla = Tuple([x, y])
            tupla.setId(pid)
            tupla.setDescription(nome)
            dataset_tuples.append(tupla)
        
        # Salvar dataset
        dataset_path = "/apps/datasets/tests/brid_dataset_visual.txt"
        with open(dataset_path, 'w') as f:
            f.write('\n'.join(data))
        
        print(f"   ✓ Dataset criado: {len(pontos)} tuplas")
        print(f"      Sq=(0,0), A=(3,0), B=(3,2), C=(5,-3), D=(4,-5)")
        
        # Definir query (Sq = ponto 0)
        print("\n2. Definindo query no ponto Sq=(0, 0)...")
        query = Tuple([0.0, 0.0])
        query.setId(0)
        query.setDescription("Sq")
        print(f"   Query: {query}")
        
        # Configurar parser para 2D
        parser = ParserTuple(dimension=2, hasId=True, hasDesc=True)
        
        # Executar BRIDk
        print("\n3. Executando BRIDk distribuído (k=4)...")
        brid_spark = BridSpark(spark)
        
        results = brid_spark.execute_random_partitioning(
            dataset_path=dataset_path,
            query=query,
            k=4,  # Buscar os 4 pontos mais diversos (excluindo query)
            num_partitions=2,
            parser=parser
        )
        
        # Exibir resultados
        print("\n4. Resultados (k-vizinhos diversificados):")
        print("   " + "="*60)
        for i, tupla in enumerate(results, 1):
            coords = tupla.getAttributes()
            print(f"   {i}. {tupla.getDescription()} = ({coords[0]:.1f}, {coords[1]:.1f})")
        print("   " + "="*60)
        
        # Gerar visualização 2D (apenas 2D pois os pontos são planares)
        print("\n5. Gerando visualização gráfica...")
        visualizar_resultados_2d(dataset_tuples, query, results,
                                "/apps/brid_resultado_2d.png")
        
        print("\n" + "="*80)
        print("✓ EXECUÇÃO CONCLUÍDA COM SUCESSO!")
        print("="*80)
        print("\n📁 Arquivo gerado:")
        print("   • /apps/brid_resultado_2d.png  - Visualização 2D")
        print("\n💡 Dica: Copie o arquivo PNG para visualizar:")
        print("   docker cp spark-master:/apps/brid_resultado_2d.png .")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    print("\n" + "🎨 "*35)
    print("BRIDk COM VISUALIZAÇÃO GRÁFICA")
    print("🎨 "*35 + "\n")
    
    try:
        exemplo_com_visualizacao()
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        import traceback
        traceback.print_exc()
