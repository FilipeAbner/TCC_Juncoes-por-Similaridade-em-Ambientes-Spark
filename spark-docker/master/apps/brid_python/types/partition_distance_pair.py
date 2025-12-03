"""
Par (partição, distância) usado como chave composta no processamento distribuído.

Conversão adaptada do arquivo Java: com/types/PartitionDistancePair.java
"""

from typing import Tuple as TupleType


class PartitionDistancePair:
    """
    O PartitionDistancePair nos permite implementar uma chave 
    composta que pode ser usada para aplicar ordenação e agrupamento
    no processamento distribuído (equivalente ao secondary sort do MapReduce).
    
    No Spark, usamos esta classe para particionar e ordenar dados.
    """
    
    partition: int
    distance: float
    
    def __init__(self, partition: int = 0, distance: float = 0.0):
        """
        Construtor do PartitionDistancePair.
        
        Args:
            partition: Índice da partição.
            distance: Distância associada.
        """
        self.partition = partition
        self.distance = distance
    
    def getPartition(self) -> int:
        """
        Retorna o índice da partição.
        
        Returns:
            Índice da partição.
        """
        return self.partition
    
    def setPartition(self, partition: int) -> None:
        """
        Define o índice da partição.
        
        Args:
            partition: Índice da partição.
        """
        self.partition = partition
    
    def getDistance(self) -> float:
        """
        Retorna a distância.
        
        Returns:
            Distância.
        """
        return self.distance
    
    def setDistance(self, distance: float) -> None:
        """
        Define a distância.
        
        Args:
            distance: Distância.
        """
        self.distance = distance
    
    def __lt__(self, other: 'PartitionDistancePair') -> bool:
        """
        Comparação menor que (para ordenação).
        Primeiro compara partição, depois distância.
        
        Args:
            other: Outro PartitionDistancePair.
            
        Returns:
            True se self < other.
        """
        if self.partition != other.partition:
            return self.partition < other.partition
        return self.distance < other.distance
    
    def __le__(self, other: 'PartitionDistancePair') -> bool:
        """Menor ou igual."""
        return self < other or self == other
    
    def __gt__(self, other: 'PartitionDistancePair') -> bool:
        """Maior que."""
        return not self <= other
    
    def __ge__(self, other: 'PartitionDistancePair') -> bool:
        """Maior ou igual."""
        return not self < other
    
    def __eq__(self, other: object) -> bool:
        """
        Igualdade.
        
        Args:
            other: Outro objeto.
            
        Returns:
            True se são iguais.
        """
        if not isinstance(other, PartitionDistancePair):
            return False
        
        return (self.partition == other.partition and 
                self.distance == other.distance)
    
    def __ne__(self, other: object) -> bool:
        """Diferente."""
        return not self == other
    
    def __hash__(self) -> int:
        """
        Hash code para uso em dicionários e sets.
        
        Returns:
            Hash code.
        """
        result = hash(self.partition)
        result = 31 * result + hash(self.distance)
        return result
    
    def __repr__(self) -> str:
        """Representação string."""
        return f"PartitionDistancePair(partition={self.partition}, distance={self.distance})"
    
    def to_tuple(self) -> TupleType[int, float]:
        """
        Converte para tupla Python (útil para Spark).
        
        Returns:
            Tupla (partition, distance).
        """
        return (self.partition, self.distance)
    
    @staticmethod
    def from_tuple(t: TupleType[int, float]) -> 'PartitionDistancePair':
        """
        Cria PartitionDistancePair a partir de tupla.
        
        Args:
            t: Tupla (partition, distance).
            
        Returns:
            PartitionDistancePair.
        """
        return PartitionDistancePair(t[0], t[1])
