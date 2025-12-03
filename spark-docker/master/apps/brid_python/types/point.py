"""
Representa um ponto qualquer em um espaço métrico.

Conversão fiel do arquivo Java: com/types/Point.java
"""

from typing import List


class Point:
    """
    Representa um ponto qualquer em um espaço métrico.
    """
    
    def __init__(self, attributes: List[float] = None):
        """
        Construtor do Point.
        
        Args:
            attributes: Lista de atributos (coordenadas) do ponto. 
                       Se None, inicializa lista vazia.
        """
        if attributes is None:
            self.attributes: List[float] = []
        else:
            self.attributes: List[float] = attributes
    
    def setAttributes(self, attributes: List[float]) -> None:
        """
        Define os atributos do ponto.
        
        Args:
            attributes: Lista de atributos (coordenadas) do ponto.
        """
        self.attributes = attributes
    
    def getAttributes(self) -> List[float]:
        """
        Retorna os atributos do ponto.
        
        Returns:
            Lista de atributos (coordenadas) do ponto.
        """
        return self.attributes
