"""
Classe abstrata para métricas de distância.

Conversão fiel do arquivo Java: com/metrics/Metric.java
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

# Evita import circular
if TYPE_CHECKING:
    from ..types.point import Point


class Metric(ABC):
    """
    Classe abstrata para métricas de distância em espaços métricos.
    """
    
    def __init__(self):
        """
        Construtor da classe Metric.
        """
        self.numberOfCalculations: int = 0
    
    @abstractmethod
    def distance(self, s: 'Point', t: 'Point') -> float:
        """
        Calcula a distância entre o elemento s e o elemento t.
        
        Args:
            s: Primeiro ponto.
            t: Segundo ponto.
            
        Returns:
            Distância entre s e t.
        """
        pass
    
    @abstractmethod
    def distHyperplane(self, p0: 'Point', p1: 'Point', t: 'Point') -> float:
        """
        Calcula a distância entre o elemento "t" e o hiperplano 
        generalizado formado entre p0 e p1.
        
        Args:
            p0: Primeiro ponto que define o hiperplano.
            p1: Segundo ponto que define o hiperplano.
            t: Ponto para calcular distância ao hiperplano.
            
        Returns:
            Distância de t ao hiperplano.
        """
        pass
