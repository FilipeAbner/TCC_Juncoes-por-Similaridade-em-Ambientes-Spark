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
    
    def search(self, query: T, k: int, return_debug: bool = False) -> List[T]:
        """
        Search for k diversified nearest neighbors.
        
        Busca pelos k vizinhos diversificados mais próximos do objeto de consulta.
        
        IMPORTANTE: O algoritmo BRIDk requer que os dados sejam processados em ordem
        crescente de distância à consulta para garantir que os k elementos
        selecionados sejam os mais próximos E diversificados.
        
        Args:
            query: Objeto de consulta.
            k: Número de vizinhos diversificados desejados.
            return_debug: Se True, armazena informações de debug.
            
        Returns:
            Lista com os k vizinhos diversificados (excluindo a própria consulta).
        """
        # CORREÇÃO: Ordenar dataset por distância à consulta
        # O BRIDk assume que os elementos são processados em ordem crescente de distância
        # Fase Bridge: seleciona o mais próximo (primeiro após ordenação)
        # Fase Incremental Ranking: itera pelos seguintes aplicando diversificação
        sorted_dataset = sorted(
            self.dataset,
            key=lambda elem: self.metric.distance(elem, query)
        )
        
        result: List[T] = []
        debug_log = [] if return_debug else None
        pos = 0
        while len(result) < k and pos < len(sorted_dataset):
            candidate = sorted_dataset[pos]
            pos += 1
            
            # FILTRO: Não incluir a própria consulta como vizinho
            # Verifica se é o mesmo objeto (distância ~0 ou mesmo ID)
            dist_to_query = self.metric.distance(candidate, query)
            if dist_to_query < 1e-10:  # Praticamente zero (mesma posição)
                continue
            
            # Verificar também por ID se disponível
            if hasattr(candidate, 'getId') and hasattr(query, 'getId'):
                if candidate.getId() == query.getId():
                    continue
            
            influenced_by = self.notInfluenced(candidate, query, result, debug_log if return_debug else None)
            if influenced_by is True or (isinstance(influenced_by, tuple) and influenced_by[0]):
                result.append(candidate)
                if return_debug:
                    debug_info = influenced_by[1] if isinstance(influenced_by, tuple) else None
                    debug_log.append({
                        'candidate': candidate,
                        'action': 'accepted',
                        'distance': dist_to_query,
                        'checks': debug_info.get('all_checks', []) if debug_info else []
                    })
            elif return_debug and isinstance(influenced_by, tuple):
                debug_log.append({
                    'candidate': candidate,
                    'action': 'rejected',
                    'distance': dist_to_query,
                    'influenced_by': influenced_by[1]
                })
        
        if return_debug:
            self._debug_log = debug_log
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
    
    def notInfluenced(self, candidate: T, query: T, resultSet: List[T], debug_log: List = None) -> bool:
        """
        Check that the candidate object is not influenced
        by any other object in the response set.
        
        Verifica se o candidato não é influenciado por nenhum
        outro objeto no conjunto de resposta.
        
        Args:
            candidate: Objeto candidato.
            query: Objeto de consulta.
            resultSet: Conjunto de resultados atual.
            debug_log: Lista para armazenar informações de debug.
            
        Returns:
            True se não for influenciado, ou tupla (False, influencer_info).
        """
        # Iterate over list resultSet in the reverse order
        ans = True
        all_checks = [] if debug_log is not None else None
        
        for i in range(len(resultSet) - 1, -1, -1):
            resultElement = resultSet[i]
            inf_s_to_q = self.influenceLevel(resultElement, query)
            inf_s_to_t = self.influenceLevel(resultElement, candidate)
            inf_q_to_t = self.influenceLevel(query, candidate)
            dist_s_to_t = self.metric.distance(resultElement, candidate)
            
            is_strong = self.isStrongInfluence(resultElement, candidate, query)
            
            if debug_log is not None:
                all_checks.append({
                    'element': resultElement,
                    'inf_s_to_t': inf_s_to_t,
                    'inf_s_to_q': inf_s_to_q,
                    'inf_q_to_t': inf_q_to_t,
                    'dist_s_to_t': dist_s_to_t,
                    'is_strong': is_strong,
                    'cond1': inf_s_to_t >= inf_s_to_q,
                    'cond2': inf_s_to_t >= inf_q_to_t
                })
            
            if is_strong:
                if debug_log is not None:
                    return (False, {
                        'influencer': resultElement,
                        'inf_s_to_t': inf_s_to_t,
                        'inf_s_to_q': inf_s_to_q,
                        'inf_q_to_t': inf_q_to_t,
                        'dist_s_to_t': dist_s_to_t,
                        'all_checks': all_checks
                    })
                ans = False
                break
        
        if debug_log is not None and ans:
            return (True, {'all_checks': all_checks})
        return ans if debug_log is None else (True, None)
    
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
