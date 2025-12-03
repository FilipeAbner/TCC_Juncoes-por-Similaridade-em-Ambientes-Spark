#!/usr/bin/env python3
"""
Teste Avançado PySpark - Análise de Dados
Simulação de análise de vendas usando DataFrames
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import sys

def test_dataframe_operations():
    """Teste avançado com operações de DataFrame"""
    
    print("=" * 70)
    print("TESTE AVANÇADO - ANÁLISE DE DADOS DE VENDAS")
    print("=" * 70)
    
    # Criar SparkSession
    spark = SparkSession.builder \
        .appName("Analise de Vendas") \
        .master("spark://192.168.1.7:7077") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "2") \
        .getOrCreate()
    
    print(f"\n✓ Conectado ao cluster Spark")
    print(f"✓ Versão: {spark.version}")
    
    # Definir schema dos dados
    schema = StructType([
        StructField("id_venda", IntegerType(), False),
        StructField("produto", StringType(), False),
        StructField("categoria", StringType(), False),
        StructField("quantidade", IntegerType(), False),
        StructField("preco_unitario", DoubleType(), False),
        StructField("regiao", StringType(), False),
    ])
    
    # Dados de exemplo: vendas de uma loja
    dados_vendas = [
        (1, "Notebook", "Eletrônicos", 2, 3500.00, "Sul"),
        (2, "Mouse", "Periféricos", 5, 50.00, "Norte"),
        (3, "Teclado", "Periféricos", 3, 150.00, "Sul"),
        (4, "Monitor", "Eletrônicos", 1, 1200.00, "Centro"),
        (5, "Cadeira", "Móveis", 4, 800.00, "Norte"),
        (6, "Mesa", "Móveis", 2, 1500.00, "Sul"),
        (7, "Webcam", "Periféricos", 3, 300.00, "Centro"),
        (8, "Headset", "Periféricos", 6, 200.00, "Norte"),
        (9, "Notebook", "Eletrônicos", 1, 4000.00, "Centro"),
        (10, "Impressora", "Eletrônicos", 2, 900.00, "Sul"),
        (11, "Mouse", "Periféricos", 10, 45.00, "Centro"),
        (12, "Cadeira", "Móveis", 3, 750.00, "Sul"),
        (13, "Monitor", "Eletrônicos", 2, 1100.00, "Norte"),
        (14, "Mesa", "Móveis", 1, 1600.00, "Centro"),
        (15, "Teclado", "Periféricos", 4, 140.00, "Norte"),
    ]
    
    # Criar DataFrame
    df_vendas = spark.createDataFrame(dados_vendas, schema)
    
    print("\n" + "-" * 70)
    print("📊 DADOS ORIGINAIS")
    print("-" * 70)
    df_vendas.show()
    
    # Análise 1: Calcular valor total de cada venda
    print("\n" + "-" * 70)
    print("📊 ANÁLISE 1: Valor Total por Venda")
    print("-" * 70)
    
    df_com_total = df_vendas.withColumn(
        "valor_total", 
        col("quantidade") * col("preco_unitario")
    )
    df_com_total.select("id_venda", "produto", "quantidade", "preco_unitario", "valor_total").show()
    
    # Análise 2: Vendas por Categoria
    print("\n" + "-" * 70)
    print("📊 ANÁLISE 2: Resumo por Categoria")
    print("-" * 70)
    
    vendas_por_categoria = df_com_total.groupBy("categoria").agg(
        spark_sum("valor_total").alias("faturamento_total"),
        spark_sum("quantidade").alias("qtd_total_vendida"),
        count("id_venda").alias("num_vendas"),
        avg("valor_total").alias("ticket_medio")
    ).orderBy(col("faturamento_total").desc())
    
    vendas_por_categoria.show()
    
    # Análise 3: Vendas por Região
    print("\n" + "-" * 70)
    print("📊 ANÁLISE 3: Resumo por Região")
    print("-" * 70)
    
    vendas_por_regiao = df_com_total.groupBy("regiao").agg(
        spark_sum("valor_total").alias("faturamento_total"),
        count("id_venda").alias("num_vendas"),
        avg("valor_total").alias("ticket_medio")
    ).orderBy(col("faturamento_total").desc())
    
    vendas_por_regiao.show()
    
    # Análise 4: Top 5 Produtos
    print("\n" + "-" * 70)
    print("📊 ANÁLISE 4: Top 5 Produtos Mais Vendidos (em valor)")
    print("-" * 70)
    
    top_produtos = df_com_total.groupBy("produto").agg(
        spark_sum("valor_total").alias("faturamento"),
        spark_sum("quantidade").alias("qtd_vendida")
    ).orderBy(col("faturamento").desc()).limit(5)
    
    top_produtos.show()
    
    # Análise 5: Classificação de vendas
    print("\n" + "-" * 70)
    print("📊 ANÁLISE 5: Classificação de Vendas (Alto/Médio/Baixo Valor)")
    print("-" * 70)
    
    df_classificado = df_com_total.withColumn(
        "classificacao",
        when(col("valor_total") >= 3000, "Alto Valor")
        .when(col("valor_total") >= 1000, "Médio Valor")
        .otherwise("Baixo Valor")
    )
    
    df_classificado.select("id_venda", "produto", "valor_total", "classificacao").show()
    
    # Análise 6: Estatísticas Gerais
    print("\n" + "-" * 70)
    print("📊 ANÁLISE 6: Estatísticas Gerais")
    print("-" * 70)
    
    from pyspark.sql.functions import max as spark_max, min as spark_min
    
    stats = df_com_total.agg(
        spark_sum("valor_total").alias("faturamento_total"),
        avg("valor_total").alias("ticket_medio"),
        spark_max("valor_total").alias("maior_venda"),
        spark_min("valor_total").alias("menor_venda"),
        count("id_venda").alias("total_vendas")
    )
    
    stats.show()
    
    # Análise 7: Join - Criar tabela de metas e comparar
    print("\n" + "-" * 70)
    print("📊 ANÁLISE 7: Comparação com Metas por Região")
    print("-" * 70)
    
    # Criar DataFrame de metas
    dados_metas = [
        ("Norte", 8000.00),
        ("Sul", 12000.00),
        ("Centro", 10000.00),
    ]
    
    df_metas = spark.createDataFrame(dados_metas, ["regiao", "meta"])
    
    # Join com vendas por região
    resultado_vs_meta = vendas_por_regiao.join(df_metas, "regiao")
    
    resultado_vs_meta = resultado_vs_meta.withColumn(
        "percentual_atingido",
        (col("faturamento_total") / col("meta") * 100).cast("decimal(10,2)")
    ).withColumn(
        "status",
        when(col("percentual_atingido") >= 100, "✓ Meta Atingida")
        .otherwise("✗ Abaixo da Meta")
    )
    
    resultado_vs_meta.select("regiao", "faturamento_total", "meta", "percentual_atingido", "status").show()
    
    # Resumo Final
    print("\n" + "=" * 70)
    print("✅ ANÁLISE CONCLUÍDA COM SUCESSO!")
    print("=" * 70)
    
    # Obter totalizadores
    total_row = stats.collect()[0]
    print(f"\n📈 RESUMO EXECUTIVO:")
    print(f"   • Faturamento Total: R$ {total_row['faturamento_total']:,.2f}")
    print(f"   • Ticket Médio: R$ {total_row['ticket_medio']:,.2f}")
    print(f"   • Maior Venda: R$ {total_row['maior_venda']:,.2f}")
    print(f"   • Menor Venda: R$ {total_row['menor_venda']:,.2f}")
    print(f"   • Total de Vendas: {total_row['total_vendas']}")
    
    # Encerrar
    spark.stop()
    print("\n✓ SparkSession encerrada")
    print("\n" + "=" * 70 + "\n")

if __name__ == "__main__":
    try:
        test_dataframe_operations()
    except Exception as e:
        print(f"\n❌ ERRO: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
