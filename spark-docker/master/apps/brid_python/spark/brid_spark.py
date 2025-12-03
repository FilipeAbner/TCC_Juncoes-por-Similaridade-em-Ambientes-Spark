"""
Driver PySpark para executar o algoritmo BRIDk distribuído.

Adaptação do BridDriver Java (Hadoop MapReduce) para PySpark com RDDs.
Implementa a arquitetura de 2 fases do BRIDk:
- Fase 1: Particionamento e busca local
- Fase 2: Refinamento e busca global
"""

from typing import List, Tuple as TupleType, Iterator, Optional
from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

from ..algorithms.brid import Brid
from ..metrics.eucledian import Eucledian
from ..metrics.metric import Metric
from ..types.tuple import Tuple
from ..types.point import Point
from ..types.partition_distance_pair import PartitionDistancePair
from ..helpers.parser_tuple import ParserTuple


class BridSpark:
    """
    Driver PySpark para executar o algoritmo BRIDk distribuído usando RDDs.
    
    Arquitetura equivalente ao BridDriver Java:
    - Job 1 (Particionamento): Divide dataset em partições e executa Brid local
    - Job 2 (Refinamento): Consolida resultados parciais e executa Brid global
    """
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa o driver BRIDk Spark.
        
        Args:
            spark: SparkSession ativa.
        """
        self.spark = spark
        self.sc: SparkContext = spark.sparkContext
        self.metric: Metric = Eucledian()
    
    def execute_pivot_based(
        self,
        dataset_path: str,
        query: Tuple,
        k: int,
        pivots: List[Point],
        num_partitions: int,
        parser: Optional[ParserTuple] = None
    ) -> List[Tuple]:
        """
        Executa BRIDk usando particionamento baseado em pivôs.
        
        Equivalente ao método PivotBasedMapper do Java.
        
        Args:
            dataset_path: Caminho para o arquivo de dataset.
            query: Objeto de consulta.
            k: Número de vizinhos diversificados desejados.
            pivots: Lista de pivôs para particionamento.
            num_partitions: Número de partições (deve ser = len(pivots)).
            parser: Parser para converter strings em Tuple. 
                   Se None, usa ParserTuple padrão.
            
        Returns:
            Lista com k vizinhos diversificados.
        """
        if parser is None:
            parser = ParserTuple()
        
        # Broadcast de dados compartilhados
        query_bc = self.sc.broadcast(query)
        k_bc = self.sc.broadcast(k)
        pivots_bc = self.sc.broadcast(pivots)
        metric_class = type(self.metric)
        
        # FASE 1: PARTICIONAMENTO
        print("\n" + "="*60)
        print("FASE 1: PARTICIONAMENTO")
        print("="*60)
        
        # Ler dataset
        dataset_rdd: RDD[str] = self.sc.textFile(dataset_path)
        
        # Map: Parsear e atribuir a partições
        def map_to_partition(line: str) -> TupleType[int, Tuple]:
            """
            Equivalente ao PivotBasedMapper.map()
            Atribui cada tupla à partição do pivô mais próximo.
            """
            tuple_obj = parser.parse(line)
            
            # Encontrar pivô mais próximo
            metric = metric_class()
            min_distance = float('inf')
            closest_pivot_idx = 0
            
            for i, pivot in enumerate(pivots_bc.value):
                dist = metric.distance(tuple_obj, pivot)
                if dist < min_distance:
                    min_distance = dist
                    closest_pivot_idx = i
            
            # Retorna (partition_id, tuple)
            return (closest_pivot_idx, tuple_obj)
        
        partitioned_rdd = dataset_rdd.map(map_to_partition)
        
        # Particionar por chave e ordenar por distância à query
        def add_distance_key(item: TupleType[int, Tuple]) -> TupleType[TupleType[int, float], Tuple]:
            """Adiciona distância à query como parte da chave para ordenação."""
            partition_id, tuple_obj = item
            metric = metric_class()
            dist_to_query = metric.distance(tuple_obj, query_bc.value)
            return ((partition_id, dist_to_query), tuple_obj)
        
        keyed_rdd = partitioned_rdd.map(add_distance_key)
        
        # Ordenar por chave composta (partition, distance)
        sorted_rdd = keyed_rdd.sortByKey()
        
        # Agrupar por partição
        def extract_partition_id(item: TupleType[TupleType[int, float], Tuple]) -> int:
            """Extrai partition_id da chave composta."""
            return item[0][0]
        
        grouped_by_partition = sorted_rdd.groupBy(extract_partition_id)
        
        # Reduce: Executar Brid local em cada partição
        def brid_local_reduce(partition_data: TupleType[int, Iterator[TupleType[TupleType[int, float], Tuple]]]) -> List[Tuple]:
            """
            Equivalente ao BridIntermediaryReducer.reduce()
            Executa Brid em cada partição localmente.
            """
            partition_id, tuples_iter = partition_data
            
            # Converter iterator para lista
            dataset_local = [tuple_obj for ((pid, dist), tuple_obj) in tuples_iter]
            
            # Executar Brid local
            metric = metric_class()
            brid = Brid(dataset_local, metric)
            local_results = brid.search(query_bc.value, k_bc.value)
            
            print(f"Partição {partition_id}: {len(dataset_local)} tuplas -> {len(local_results)} resultados locais")
            
            return local_results
        
        # Executar Brid em cada partição e coletar resultados parciais
        partial_results_rdd = grouped_by_partition.flatMap(
            lambda x: brid_local_reduce(x)
        )
        
        # Coletar resultados parciais
        all_candidates = partial_results_rdd.collect()
        
        print(f"\nTotal de candidatos das partições: {len(all_candidates)}")
        
        # FASE 2: REFINAMENTO
        print("\n" + "="*60)
        print("FASE 2: REFINAMENTO")
        print("="*60)
        
        # Executar Brid final sobre todos os candidatos
        metric_final = metric_class()
        brid_final = Brid(all_candidates, metric_final)
        final_results = brid_final.search(query, k)
        
        print(f"\nResultados finais: {len(final_results)} tuplas")
        print(f"Total de cálculos de distância: {metric_final.numberOfCalculations}")
        
        return final_results
    
    def execute_random_partitioning(
        self,
        dataset_path: str,
        query: Tuple,
        k: int,
        num_partitions: int,
        parser: Optional[ParserTuple] = None
    ) -> List[Tuple]:
        """
        Executa BRIDk usando particionamento aleatório.
        
        Equivalente ao RandomPartitioning do Java.
        
        Args:
            dataset_path: Caminho para o arquivo de dataset.
            query: Objeto de consulta.
            k: Número de vizinhos diversificados desejados.
            num_partitions: Número de partições.
            parser: Parser para converter strings em Tuple.
            
        Returns:
            Lista com k vizinhos diversificados.
        """
        if parser is None:
            parser = ParserTuple()
        
        # Broadcast
        query_bc = self.sc.broadcast(query)
        k_bc = self.sc.broadcast(k)
        metric_class = type(self.metric)
        
        # FASE 1: PARTICIONAMENTO ALEATÓRIO
        print("\n" + "="*60)
        print("FASE 1: PARTICIONAMENTO ALEATÓRIO")
        print("="*60)
        
        # Ler e parsear dataset
        dataset_rdd: RDD[Tuple] = self.sc.textFile(dataset_path).map(parser.parse)
        
        # Reparticionar aleatoriamente
        repartitioned_rdd = dataset_rdd.repartition(num_partitions)
        
        # Aplicar Brid em cada partição
        def brid_partition(iterator: Iterator[Tuple]) -> Iterator[Tuple]:
            """Executa Brid em cada partição."""
            dataset_local = list(iterator)
            if not dataset_local:
                return iter([])
            
            metric = metric_class()
            brid = Brid(dataset_local, metric)
            local_results = brid.search(query_bc.value, k_bc.value)
            
            print(f"Partição: {len(dataset_local)} tuplas -> {len(local_results)} resultados")
            
            return iter(local_results)
        
        partial_results_rdd = repartitioned_rdd.mapPartitions(brid_partition)
        
        # Coletar candidatos
        all_candidates = partial_results_rdd.collect()
        
        print(f"\nTotal de candidatos: {len(all_candidates)}")
        
        # FASE 2: REFINAMENTO
        print("\n" + "="*60)
        print("FASE 2: REFINAMENTO")
        print("="*60)
        
        metric_final = metric_class()
        brid_final = Brid(all_candidates, metric_final)
        final_results = brid_final.search(query, k)
        
        print(f"\nResultados finais: {len(final_results)}")
        
        return final_results
    
    def execute_from_rdd(
        self,
        dataset_rdd: RDD[Tuple],
        query: Tuple,
        k: int,
        num_partitions: int
    ) -> List[Tuple]:
        """
        Executa BRIDk diretamente sobre um RDD de Tuples.
        
        Útil quando o dataset já está carregado como RDD.
        
        Args:
            dataset_rdd: RDD contendo objetos Tuple.
            query: Objeto de consulta.
            k: Número de vizinhos diversificados desejados.
            num_partitions: Número de partições.
            
        Returns:
            Lista com k vizinhos diversificados.
        """
        query_bc = self.sc.broadcast(query)
        k_bc = self.sc.broadcast(k)
        metric_class = type(self.metric)
        
        # Reparticionar
        repartitioned_rdd = dataset_rdd.repartition(num_partitions)
        
        # Fase 1: Brid local
        def brid_partition(iterator: Iterator[Tuple]) -> Iterator[Tuple]:
            dataset_local = list(iterator)
            if not dataset_local:
                return iter([])
            
            metric = metric_class()
            brid = Brid(dataset_local, metric)
            return iter(brid.search(query_bc.value, k_bc.value))
        
        partial_results = repartitioned_rdd.mapPartitions(brid_partition).collect()
        
        # Fase 2: Brid global
        metric_final = metric_class()
        brid_final = Brid(partial_results, metric_final)
        return brid_final.search(query, k)
