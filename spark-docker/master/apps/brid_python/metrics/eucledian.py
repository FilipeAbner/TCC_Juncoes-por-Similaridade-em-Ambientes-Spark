"""
Métrica Euclidiana.

Conversão fiel do arquivo Java: com/metrics/Eucledian.java
"""

import math,sys
from typing import TYPE_CHECKING
from .metric import Metric

if TYPE_CHECKING:
    from ..types.point import Point


class Eucledian(Metric):
    """
    Implementação da métrica de distância Euclidiana.
    """
    
    def distance(self, s: 'Point', t: 'Point') -> float:
        """
        Calcula a distância euclidiana entre os pontos s e t.
        
        Args:
            s: Primeiro ponto.
            t: Segundo ponto.
            
        Returns:
            Distância euclidiana entre s e t.
        """
        distanceSquare = 0.0
        for i in range(len(s.getAttributes())):
            diff = s.getAttributes()[i] - t.getAttributes()[i]
            distanceSquare += diff * diff
        
        self.numberOfCalculations += 1
        return math.sqrt(distanceSquare)
    
    def distHyperplane(self, p0: 'Point', p1: 'Point', t: 'Point') -> float:
        """
        Calcula a distância entre o ponto t e o hiperplano 
        formado entre p0 e p1.
        
        Args:
            p0: Primeiro ponto que define o hiperplano.
            p1: Segundo ponto que define o hiperplano.
            t: Ponto para calcular distância ao hiperplano.
            
        Returns:
            Distância de t ao hiperplano.
        """
        return abs(self.eucledianSquare(t, p0) - self.eucledianSquare(t, p1)) / (2 * self.distance(p0, p1))
    
    def eucledianSquare(self, s: 'Point', t: 'Point') -> float:
        """
        Calcula a distância euclidiana ao quadrado entre s e t.
        
        Args:
            s: Primeiro ponto.
            t: Segundo ponto.
            
        Returns:
            Distância euclidiana ao quadrado.
        """
        distanceSquare = 0.0
        for i in range(len(s.getAttributes())):
            diff = s.getAttributes()[i] - t.getAttributes()[i]
            distanceSquare += diff * diff
        
        return distanceSquare
