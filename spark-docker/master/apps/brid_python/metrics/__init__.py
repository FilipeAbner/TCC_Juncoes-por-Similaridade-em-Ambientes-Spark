"""
Módulo de métricas de distância.

Exporta classes de métricas para cálculo de distâncias entre pontos.
"""

from .metric import Metric
from .eucledian import Eucledian

__all__ = ['Metric', 'Eucledian']
