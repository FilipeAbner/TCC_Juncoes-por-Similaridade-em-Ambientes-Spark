#!/usr/bin/env python3
"""
Teste Básico PySpark - Contagem de Palavras
Este script testa a conectividade com o cluster Spark
"""

from pyspark.sql import SparkSession
import sys
import os

def print_header(text):
    """Imprime cabeçalho formatado"""
    print("\n" + "="*60)
    print(text)
    print("="*60 + "\n")

def print_section(text):
    """Imprime seção formatada"""
    print("\n" + "-"*60)
    print(text)
    print("-"*60)

def main():
    print_header("INICIANDO TESTE BÁSICO DO PYSPARK")
    
    # Criar SparkSession
    spark = SparkSession.builder \
        .appName("Teste Basico PySpark") \
        .getOrCreate()
    
    print(f"SparkSession criada com sucesso!")
    print(f"Master: {spark.sparkContext.master}")
    print(f"App Name: {spark.sparkContext.appName}")
    print(f"Spark Version: {spark.version}\n")
    
    # Teste 1: RDD básico
    print_header("TESTE 1: Criando RDD e contando elementos")
    
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    rdd = spark.sparkContext.parallelize(data, numSlices=4)
    count = rdd.count()
    soma = rdd.sum()
    
    print(f"RDD criado com {count} elementos")
    print(f"Soma dos elementos: {soma}")
    
    # Teste 2: Criar DataFrame
    print_section("TESTE 2: Criando DataFrame e realizando operações")
    
    data_df = [
        ("Alice", 34, "Engenheira"),
        ("Bob", 45, "Médico"),
        ("Charlie", 28, "Designer"),
        ("Diana", 32, "Professora"),
        ("Eve", 29, "Desenvolvedor")
    ]
    
    df = spark.createDataFrame(data_df, ["nome", "idade", "profissao"])
    
    print(f"DataFrame criado com {df.count()} registros")
    print("\nPrimeiras linhas do DataFrame:")
    df.show()
    
    print("\nSchema do DataFrame:")
    df.printSchema()
    
    # Teste 3: Transformações e ações
    print_section("TESTE 3: Aplicando transformações")
    
    # Filtrar pessoas com mais de 30 anos
    df_filtrado = df.filter(df.idade > 30)
    print(f"\nPessoas com mais de 30 anos:")
    df_filtrado.show()
    
    # Calcular média de idade
    from pyspark.sql.functions import avg, max, min, count
    
    stats = df.agg(
        avg("idade").alias("idade_media"),
        max("idade").alias("idade_maxima"),
        min("idade").alias("idade_minima"),
        count("nome").alias("total_pessoas")
    )
    
    print("\nEstatísticas:")
    stats.show()
    
    # Teste 4: Contagem de palavras (clássico MapReduce)
    print("\n" + "-" * 60)
    print("TESTE 4: Word Count (exemplo clássico)")
    print("-" * 60)
    
    texto = [
        "Apache Spark é um framework de processamento distribuído",
        "Spark é rápido e escalável",
        "PySpark é a API Python do Apache Spark",
        "Clusters Spark processam big data de forma eficiente"
    ]
    
    rdd_texto = spark.sparkContext.parallelize(texto)
    
    # MapReduce: dividir em palavras, contar e ordenar
    word_counts = rdd_texto \
        .flatMap(lambda linha: linha.lower().split()) \
        .map(lambda palavra: (palavra, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1], ascending=False)
    
    print("\nTop 10 palavras mais frequentes:")
    for palavra, count in word_counts.take(10):
        print(f"   {palavra}: {count}")
    
    # Teste 5: Operações em paralelo
    print("\n" + "-" * 60)
    print("TESTE 5: Processamento em paralelo")
    print("-" * 60)
    
    # Criar um RDD maior para testar paralelização
    numeros = spark.sparkContext.parallelize(range(1, 1001), numSlices=10)
    
    # Aplicar transformação: elevar ao quadrado
    quadrados = numeros.map(lambda x: x ** 2)
    
    # Filtrar apenas números pares
    pares = quadrados.filter(lambda x: x % 2 == 0)
    
    total_pares = pares.count()
    soma_pares = pares.sum()
    
    print(f"Total de quadrados pares: {total_pares}")
    print(f"Soma dos quadrados pares: {soma_pares}")
    print(f"Número de partições: {pares.getNumPartitions()}")
    
    # Informações finais
    print("\n" + "=" * 60)
    print("TODOS OS TESTES CONCLUÍDOS COM SUCESSO!")
    print("=" * 60)
    print(f"\nInformações do Cluster:")
    print(f"   - Master: {spark.sparkContext.master}")
    print(f"   - Default Parallelism: {spark.sparkContext.defaultParallelism}")
    
    # Encerrar sessão
    spark.stop()
    print("\nSparkSession encerrada")
    print_header("FIM DOS TESTES")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nERRO: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
