"""
Representa uma tupla com identificador e descrição.

Conversão fiel do arquivo Java: com/types/Tuple.java
"""

from typing import List, Optional
from .point import Point


class Tuple(Point):
    """
    Tupla que estende Point, adicionando id, descrição e distância.
    """
    
    # Atributos de classe para corresponder aos protected fields do Java
    id: Optional[int]
    description: Optional[str]
    distance: float
    
    def __init__(self, attributes: List[float] = None):
        """
        Construtor da Tuple.
        Corresponde aos dois construtores Java:
        - Tuple() - sem argumentos
        - Tuple(List<Double> attributes) - com lista de atributos
        
        Args:
            attributes: Lista de atributos. Se None, inicializa lista vazia (construtor sem args).
        """
        # Chama construtor da classe pai
        if attributes is None:
            # Construtor sem argumentos: attributes = new ArrayList<>();
            super().__init__([])
        else:
            # Construtor com argumentos: this.attributes = attributes;
            super().__init__(attributes)
        
        # Inicializa atributos específicos da Tuple (não inicializados no Java)
        self.id = None
        self.description = None
        self.distance = 0.0
    
    def asString(self) -> str:
        """
        Retorna o id como string.
        
        Returns:
            String representando o id.
        """
        return str(self.id)
    
    def getId(self) -> Optional[int]:
        """
        Retorna o identificador da tupla.
        
        Returns:
            Identificador da tupla (pode ser None).
        """
        return self.id
    
    def setId(self, id: int) -> None:
        """
        Define o identificador da tupla.
        
        Args:
            id: Identificador da tupla.
        """
        self.id = id
    
    def getDescription(self) -> Optional[str]:
        """
        Retorna a descrição da tupla.
        
        Returns:
            Descrição da tupla (pode ser None).
        """
        return self.description
    
    def setDescription(self, description: str) -> None:
        """
        Define a descrição da tupla.
        
        Args:
            description: Descrição da tupla.
        """
        self.description = description
    
    def getDistance(self) -> float:
        """
        Retorna a distância entre a tupla e o pivô mais próximo.
        
        Returns:
            Distância.
        """
        return self.distance
    
    def setDistance(self, distance: float) -> None:
        """
        Define a distância entre a tupla e o pivô mais próximo.
        
        Args:
            distance: Distância.
        """
        self.distance = distance
    
    def addAttribute(self, atr: float) -> None:
        """
        Adiciona um atributo à lista de atributos.
        
        Args:
            atr: Atributo a ser adicionado.
        """
        self.attributes.append(atr)
    
    def __str__(self) -> str:
        """
        Retorna representação em string da tupla.
        
        Returns:
            String formatada com id, atributos e descrição.
        """
        result = []
        
        if self.id is not None:
            result.append(str(self.id))
        
        for value in self.attributes:
            result.append(str(value))
        
        if self.description is not None and self.description != "":
            result.append(self.description)
        
        return "\t".join(result).strip()
