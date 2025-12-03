"""
Realizar o parser de uma string em um objeto do tipo Point.

Conversão fiel do arquivo Java: com/helpers/ParserPoint.java
"""

from typing import List
from ..types.point import Point


class ParserPoint:
    """
    Realizar o parser de uma string em um objeto do tipo Ponto.
    """
    
    @staticmethod
    def parse(string: str) -> Point:
        """
        Converte uma string em um objeto Point.
        A string deve conter valores separados por espaço em branco.
        
        Args:
            string: String com valores separados por espaço.
            
        Returns:
            Objeto Point com os atributos parseados.
            
        Example:
            >>> ParserPoint.parse("1.5 2.0 3.5")
            Point([1.5, 2.0, 3.5])
        """
        point = Point()
        tokens = string.split()
        for token in tokens:
            point.getAttributes().append(float(token))
        return point
