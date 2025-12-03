"""
Implementação do algoritmo BRIDk.

Conversão fiel do arquivo Java: com/algorithms/Brid.java
"""

from typing import Generic, TypeVar, List, Optional, Union
import sys

from ..metrics.metric import Metric
from ..types.point import Point

T = TypeVar('T', bound=Point)


class Brid(Generic[T]):
    """
    Implementação do algoritmo BRIDk.
    Dado um conjunto de dados de entrada e um objeto de consulta sq, esse algoritmo
    retorna o k vizinhos diversificados do objeto de consulta.
    """
    
    metric: Metric
    dataset: List[T]
    
    def __init__(self, dataset_or_metric: Union[List[T], Metric], metric: Optional[Metric] = None):
        """
        Construtor do algoritmo BRIDk.
        Corresponde aos dois construtores Java:
        - Brid(List<T> dataset, Metric metric)
        - Brid(Metric metric)
        
        Args:
            dataset_or_metric: Se metric é None, este é um Metric (segundo construtor). 
                              Se metric é fornecido, este é um List[T] dataset (primeiro construtor).
            metric: Métrica de distância (opcional, usado no primeiro construtor).
        """
        if metric is not None:
            # Construtor: Brid(List<T> dataset, Metric metric)
            self.dataset = dataset_or_metric  # type: ignore
            self.metric = metric
        else:
            # Construtor: Brid(Metric metric)
            self.metric = dataset_or_metric  # type: ignore
            self.dataset = []
    
    def search(self, query: T, k: int) -> List[T]:
        """
        Search for k diversified nearest neighbors.
        
        Busca pelos k vizinhos diversificados mais próximos do objeto de consulta.
        
        Args:
            query: Objeto de consulta.
            k: Número de vizinhos diversificados desejados.
            
        Returns:
            Lista com os k vizinhos diversificados.
        """
        result: List[T] = []
        pos = 0
        while len(result) < k and pos < len(self.dataset):
            candidate = self.dataset[pos]
            pos += 1
            if self.notInfluenced(candidate, query, result):
                result.append(candidate)
        return result
    
    def influenceLevel(self, s: T, t: T) -> float:
        """
        Compute the level of influence "s" exerts on "t".
        
        Calcula o nível de influência que "s" exerce sobre "t".
        
        Args:
            s: Primeiro ponto.
            t: Segundo ponto.
            
        Returns:
            Nível de influência (1/distância).
        """
        dist = self.metric.distance(s, t)
        return (sys.float_info.max if dist == 0 else (1 / dist))
    
    def notInfluenced(self, candidate: T, query: T, resultSet: List[T]) -> bool:
        """
        Check that the candidate object is not influenced
        by any other object in the response set.
        
        Verifica se o candidato não é influenciado por nenhum
        outro objeto no conjunto de resposta.
        
        Args:
            candidate: Objeto candidato.
            query: Objeto de consulta.
            resultSet: Conjunto de resultados atual.
            
        Returns:
            True se não for influenciado, False caso contrário.
        """
        # Iterate over list resultSet in the reverse order
        ans = True
        for i in range(len(resultSet) - 1, -1, -1):
            resultElement = resultSet[i]
            if self.isStrongInfluence(resultElement, candidate, query):
                ans = False
                break
        return ans
    
    def isStrongInfluence(self, s: T, t: T, query: T) -> bool:
        """
        Computes whether "s" is a strong influence on "t" with respect to "query".
        
        Calcula se "s" é uma forte influência sobre "t" em relação à "query".
        
        Args:
            s: Ponto que pode influenciar.
            t: Ponto que pode ser influenciado.
            query: Objeto de consulta de referência.
            
        Returns:
            True se "s" for uma forte influência, False caso contrário.
        """
        influence_s_to_query = self.influenceLevel(s, query)
        influence_s_to_t = self.influenceLevel(s, t)
        influence_query_to_t = self.influenceLevel(query, t)
        
        return (
            influence_s_to_t >= influence_s_to_query
            and influence_s_to_t >= influence_query_to_t
        )
