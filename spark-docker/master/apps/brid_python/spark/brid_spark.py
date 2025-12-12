"""
Driver PySpark para executar o algoritmo BRIDk distribuído.

Adaptação do BridDriver Java (Hadoop MapReduce) para PySpark com RDDs.
Implementa a arquitetura de 2 fases do BRIDk:
- Fase 1: Particionamento e busca local
- Fase 2: Refinamento e busca global
"""

from typing import List, Tuple as TupleType, Iterator, Optional, Union
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
            """Executa Brid em cada partição.
            
            IMPORTANTE: O BRIDk já faz a ordenação internamente por distância à consulta.
            Cada partição processa seus elementos localmente e retorna até k candidatos.
            """
            dataset_local = list(iterator)
            if not dataset_local:
                return iter([])
            
            metric = metric_class()
            brid = Brid(dataset_local, metric)
            # O método search() do Brid já ordena os dados internamente
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
        num_partitions: int,
        return_debug_info: bool = False,
        custom_partitioner: Optional[str] = None
    ) -> Union[List[Tuple], TupleType[List[Tuple], dict]]:
        """
        Executa BRIDk diretamente sobre um RDD de Tuples.
        
        Útil quando o dataset já está carregado como RDD.
        
        Args:
            dataset_rdd: RDD contendo objetos Tuple.
            query: Objeto de consulta.
            k: Número de vizinhos diversificados desejados.
            num_partitions: Número de partições.
            return_debug_info: Se True, retorna informações de debug.
            custom_partitioner: Tipo de particionador ('x_sign' para particionar por sinal de X).
            
        Returns:
            Lista com k vizinhos diversificados, ou tupla (resultados, debug_info).
        """
        query_bc = self.sc.broadcast(query)
        k_bc = self.sc.broadcast(k)
        metric_class = type(self.metric)
        
        # Reparticionar com particionador customizado se especificado
        if custom_partitioner == 'x_sign':
            # Particionador baseado no sinal da coordenada X
            # X negativo -> partição 0, X positivo -> partição 1
            def x_sign_partitioner(tupla):
                x_coord = tupla.getAttributes()[0]
                partition_id = 0 if x_coord < 0 else 1
                return (partition_id, tupla)
            
            # Criar RDD com chave (partition_id, tupla) e particionar por chave
            keyed_rdd = dataset_rdd.map(x_sign_partitioner)
            repartitioned_rdd = keyed_rdd.partitionBy(2).map(lambda x: x[1])
        else:
            # Particionamento padrão (aleatório)
            repartitioned_rdd = dataset_rdd.repartition(num_partitions)
        
        # Fase 1: Brid local
        def brid_partition_with_index(partition_index: int, iterator: Iterator[Tuple]) -> Iterator[TupleType[int, Tuple, List, List]]:
            """Executa Brid localmente em cada partição com informações de debug.
            
            O método search() do Brid ordena internamente os elementos
            por distância à consulta antes de aplicar a diversificação.
            """
            dataset_local = list(iterator)
            if not dataset_local:
                return iter([])
            
            # DEBUG: Log dos elementos recebidos nesta partição
            import sys
            query = query_bc.value
            metric = metric_class()
            
            print(f"\n[PARTITION {partition_index}] Recebeu {len(dataset_local)} elementos:", file=sys.stderr)
            for elem in sorted(dataset_local, key=lambda e: metric.distance(e, query))[:5]:
                x_coord = elem.getAttributes()[0]
                dist = metric.distance(elem, query)
                print(f"  ID {elem.getId()}: X={x_coord:.4f}, dist={dist:.4f}", file=sys.stderr)
            if len(dataset_local) > 5:
                print(f"  ... e mais {len(dataset_local)-5} elementos", file=sys.stderr)
            
            brid = Brid(dataset_local, metric)
            # search() já ordena os dados internamente por distância à consulta
            local_results = brid.search(query, k_bc.value, return_debug=True)
            debug_log = getattr(brid, '_debug_log', [])
            
            print(f"[PARTITION {partition_index}] BRIDk retornou {len(local_results)} candidatos:", file=sys.stderr)
            for elem in local_results:
                x_coord = elem.getAttributes()[0]
                dist = metric.distance(elem, query)
                print(f"  ID {elem.getId()}: X={x_coord:.4f}, dist={dist:.4f}", file=sys.stderr)
            
            # Retornar tuplas (partition_index, candidato, todos_elementos_da_particao, debug_log) para rastreamento
            all_elements_info = [(e.getId(), e.getAttributes()[0], metric.distance(e, query)) for e in dataset_local]
            return iter((partition_index, candidate, all_elements_info, debug_log) for candidate in local_results)
        
        # Coletar com informações de partição
        partial_results_with_partition = repartitioned_rdd.mapPartitionsWithIndex(brid_partition_with_index).collect()
        
        # Separar resultados e informações de debug
        partition_info = {}
        partition_all_elements = {}
        partition_debug_log = {}
        partial_results = []
        
        for partition_idx, candidate, all_elements_info, debug_log in partial_results_with_partition:
            if partition_idx not in partition_info:
                partition_info[partition_idx] = []
                partition_all_elements[partition_idx] = all_elements_info
                partition_debug_log[partition_idx] = debug_log
            partition_info[partition_idx].append(candidate)
            partial_results.append(candidate)
        
        # Fase 2: Brid global
        metric_final = metric_class()
        brid_final = Brid(partial_results, metric_final)
        final_results = brid_final.search(query, k)
        
        if return_debug_info:
            debug_info = {
                'partition_candidates': partition_info,
                'partition_all_elements': partition_all_elements,
                'partition_debug_log': partition_debug_log,
                'total_candidates': len(partial_results),
                'num_partitions': num_partitions
            }
            return final_results, debug_info
        else:
            return final_results
