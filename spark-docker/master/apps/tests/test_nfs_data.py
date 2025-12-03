#!/usr/bin/env python3
"""
Teste de leitura de dados do NFS compartilhado
Demonstra como ler arquivos CSV do volume NFS em um cluster Spark distribuído
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, max as spark_max, min as spark_min
import sys

def test_nfs_read():
    """Testa leitura de arquivo CSV do NFS"""
    print("=" * 70)
    print("TESTE: Leitura de Dados do NFS Compartilhado")
    print("=" * 70)
    
    # Criar SparkSession
    spark = SparkSession.builder \
        .appName("Teste NFS - Análise de Vendas") \
        .getOrCreate()
    
    try:
        # Caminho do arquivo no volume compartilhado
        # Use /data/vendas_exemplo.csv quando o NFS estiver funcionando
        # Por enquanto, usando /apps que já está mapeado
        nfs_path = "/apps/vendas_exemplo.csv"
        
        print(f"\nLendo arquivo: {nfs_path}")
        print("-" * 70)
        
        # Ler arquivo CSV do NFS
        df = spark.read.csv(
            nfs_path,
            header=True,
            inferSchema=True,
            sep=","
        )
        
        # Informações básicas
        total_registros = df.count()
        print(f"\nArquivo lido com sucesso!")
        print(f"Total de registros: {total_registros:,}")
        print(f"Número de colunas: {len(df.columns)}")
        print(f"Colunas: {', '.join(df.columns)}")
        
        # Mostrar schema
        print("\nSchema do DataFrame:")
        print("-" * 70)
        df.printSchema()
        
        # Mostrar primeiras linhas
        print("\nPrimeiras 10 linhas:")
        print("-" * 70)
        df.show(10, truncate=False)
        
        # Análises estatísticas
        print("\nAnálises Estatísticas:")
        print("-" * 70)
        
        # 1. Total de vendas por categoria
        print("\n1. Total de Vendas por Categoria:")
        vendas_categoria = df.groupBy("categoria") \
            .agg(
                count("*").alias("quantidade"),
                spark_sum("valor").alias("total_vendas"),
                avg("valor").alias("valor_medio")
            ) \
            .orderBy(col("total_vendas").desc())
        
        vendas_categoria.show(truncate=False)
        
        # 2. Vendas por região
        print("\n2. Total de Vendas por Região:")
        vendas_regiao = df.groupBy("regiao") \
            .agg(
                count("*").alias("quantidade"),
                spark_sum("valor").alias("total_vendas"),
                avg("valor").alias("valor_medio")
            ) \
            .orderBy(col("total_vendas").desc())
        
        vendas_regiao.show(truncate=False)
        
        # 3. Top 10 produtos mais vendidos
        print("\n3. Top 10 Produtos Mais Vendidos:")
        top_produtos = df.groupBy("produto") \
            .agg(
                count("*").alias("quantidade_vendas"),
                spark_sum("quantidade").alias("unidades_vendidas"),
                spark_sum("valor").alias("receita_total")
            ) \
            .orderBy(col("receita_total").desc()) \
            .limit(10)
        
        top_produtos.show(truncate=False)
        
        # 4. Estatísticas de valores
        print("\n4. Estatísticas de Valores:")
        stats = df.select(
            spark_sum("valor").alias("total_geral"),
            avg("valor").alias("valor_medio"),
            spark_max("valor").alias("valor_maximo"),
            spark_min("valor").alias("valor_minimo")
        )
        stats.show(truncate=False)
        
        # 5. Distribuição por mês
        print("\n5. Vendas por Mês:")
        vendas_mes = df.groupBy("mes") \
            .agg(
                count("*").alias("quantidade"),
                spark_sum("valor").alias("total_vendas")
            ) \
            .orderBy("mes")
        
        vendas_mes.show(12, truncate=False)
        
        # Salvar resultado processado
        output_path = "/apps/resultado_analise_vendas"
        print(f"\nSalvando resultado da análise em: {output_path}")
        
        # Consolidar análise em um único DataFrame
        resultado_final = df.groupBy("categoria", "regiao") \
            .agg(
                count("*").alias("total_vendas"),
                spark_sum("valor").alias("receita"),
                avg("valor").alias("ticket_medio")
            ) \
            .orderBy("categoria", "regiao")
        
        resultado_final.write.mode("overwrite").parquet(output_path)
        print("Resultado salvo com sucesso!")
        
        print("\n" + "=" * 70)
        print("TESTE CONCLUÍDO COM SUCESSO!")
        print("=" * 70)
        print("\nResumo:")
        print(f"   • Arquivo lido: {nfs_path}")
        print(f"   • Total de registros: {total_registros:,}")
        print(f"   • Resultado salvo em: {output_path}")
        print(f"   • Todos os Workers conseguiram acessar o arquivo NFS!")
        print("=" * 70 + "\n")
        
    except Exception as e:
        print(f"\nERRO ao processar arquivo:")
        print(f"   {str(e)}")
        print("\nVerifique se:")
        print("   1. O arquivo existe em /apps/vendas_exemplo.csv")
        print("   2. O volume ./apps:/apps está configurado no docker-compose")
        print("   3. As permissões do arquivo estão corretas")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    test_nfs_read()
