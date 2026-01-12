"""
Driver PySpark para executar o algoritmo BRIDk distribuído.

Adaptação do BridDriver Java (Hadoop MapReduce) para PySpark com RDDs.
Implementa a arquitetura de 2 fases do BRIDk:
- Fase 1: Particionamento e busca local
- Fase 2: Refinamento e busca global
"""

from typing import List, Tuple as TupleType, Iterator, Optional, Union
import sys
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

    
    def execute_from_rdd(self, dataset_rdd: RDD[Tuple], query: Tuple, k: int, num_partitions: int, 
                         return_debug_info: bool = False, custom_partitioner: Optional[str] = None
                        ) -> Union[List[Tuple], TupleType[List[Tuple], dict]]:
        """
        Executa BRIDk diretamente sobre um um dataset já trnasformado em RDD de Tuples.
        
        Args:
            dataset_rdd: RDD contendo objetos Tuple.
            query: Objeto de consulta.
            k: Número de vizinhos diversificados desejados.
            num_partitions: Número de partições.
            return_debug_info: Se True, retorna informações de debug.
            custom_partitioner: Tipo de particionador:
                                -> 'x_sign' para particionar por sinal de X (X negativo em uma partição e X positivo em outra).
                                -> 'pivot_based' para assumir que o RDD já está particionado por pivôs.
                                -> None para particionamento aleatório padrão (Não Suportado).
        Returns:
            Lista com k vizinhos diversificados, ou tupla (resultados, debug_info).
        """
        
        query_bc = self.sc.broadcast(query)
        k_bc = self.sc.broadcast(k)

        # Atualmente usando métrica Euclidiana
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
        elif custom_partitioner == 'pivot_based':
            # Assumir que o RDD já está particionado por pivôs (chaves 0 a num_partitions-1)
            repartitioned_rdd = dataset_rdd
        else:
            # Particionamento padrão (aleatório)
            repartitioned_rdd = dataset_rdd.repartition(num_partitions)
        
        # Fase 1: Brid local
        def brid_partition_with_index(partition_index: int, iterator: Iterator[Tuple]) -> Iterator[TupleType[int, Tuple]]:
            """Executa Brid localmente em cada partição.
            
            O método search() do Brid ordena internamente os elementos
            por distância à consulta antes de aplicar a diversificação.
            """
            dataset_local = list(iterator)
            if not dataset_local:
                return iter([])
            
            query = query_bc.value
            metric = metric_class()
            
            brid = Brid(dataset_local, metric)
            # search() já ordena os dados internamente por distância à consulta
            local_results = brid.search(query, k_bc.value)
            
            # Retornar tuplas (partition_index, candidato) para rastreamento
            return iter((partition_index, candidate) for candidate in local_results)
        
        # Coletar com informações de partição
        partial_results_with_partition = repartitioned_rdd.mapPartitionsWithIndex(brid_partition_with_index).collect()
        
        # Separar resultados e informações de debug
        partition_info = {}
        partial_results = []
        
        for partition_idx, candidate in partial_results_with_partition:
            if partition_idx not in partition_info:
                partition_info[partition_idx] = []
            partition_info[partition_idx].append(candidate)
            partial_results.append(candidate)
        
        all_candidates = partial_results
        
        # # Fase 2: Refinamento com BRIDk global
        # Aplicar algoritmo BRIDk completo nos candidatos consolidados
        # Isso garante diversidade entre os resultados finais, não apenas dentro de cada partição
        metric_final = metric_class()
        brid_final = Brid(all_candidates, metric_final)
        final_results = brid_final.search(query, k)

        # Sempre calcular estatísticas de cache
        total_ops = metric_final.numberOfCalculations + metric_final.cacheHits
        cache_hit_rate = (metric_final.cacheHits / total_ops * 100) if total_ops > 0 else 0
        
        # Sempre retornar estatísticas básicas de cache
        debug_info = {
            'distance_calculations': metric_final.numberOfCalculations,
            'cache_hits': metric_final.cacheHits,
            'cache_hit_rate': cache_hit_rate
        }
        
        if (return_debug_info):
            # Adicionar informações detalhadas de debug
            print(f"\n Total de candidatos locais: {len(all_candidates)} \n", file=sys.stderr)            
            sorted_all = sorted(all_candidates, key=lambda c: brid_final.get_cached_distance(c, query))
            print(f"Candidatos ordenados (top 10): {[f'{c.getId()}:{brid_final.get_cached_distance(c, query):.4f}' for c in sorted_all[:10]]}", file=sys.stderr)
            print(f"Resultados BRIDk selecionados: {[f'{c.getId()}:{brid_final.get_cached_distance(c, query):.4f}' for c in final_results]}", file=sys.stderr)
            print(f"\nResultados finais: {len(final_results)} tuplas")
            
            debug_info.update({
                'partition_candidates': partition_info,
                'total_candidates': len(all_candidates),
                'num_partitions': num_partitions
            })
        
        return final_results, debug_info
