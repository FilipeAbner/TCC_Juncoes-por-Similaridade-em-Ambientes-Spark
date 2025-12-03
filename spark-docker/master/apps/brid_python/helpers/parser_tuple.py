"""
Parser para converter strings em objetos Tuple.

Conversão fiel do arquivo Java: com/helpers/ParserTuple.java
"""

from typing import List, Optional
from ..types.tuple import Tuple


class ParserTuple:
    """
    O parser recebe uma string como entrada e constrói uma Tupla
    seguindo o formato definido.
    """
    
    dimension: int
    hasId: bool
    hasDesc: bool
    
    def __init__(self, dimension: int = -1, hasId: bool = False, hasDesc: bool = False):
        """
        Construtor do ParserTuple.
        Corresponde aos dois construtores Java:
        - ParserTuple() - trata todos os valores como atributos
        - ParserTuple(int dimension, boolean hasId, boolean hasDesc)
        
        Args:
            dimension: Número de atributos do registro. 
                      Se -1, usa todos os tokens disponíveis.
            hasId: Determina se o primeiro valor é um identificador.
            hasDesc: Determina se os últimos valores são uma descrição do registro.
        """
        self.dimension = dimension
        self.hasId = hasId
        self.hasDesc = hasDesc
    
    def parse(self, string: str) -> Tuple:
        """
        Converte string em Tuple seguindo o formato configurado.
        
        Args:
            string: String a ser parseada.
            
        Returns:
            Objeto Tuple com os valores parseados.
            
        Raises:
            ValueError: Se a string não está no formato esperado.
            
        Example:
            >>> parser = ParserTuple(dimension=3, hasId=True, hasDesc=True)
            >>> parser.parse("1\\t1.5\\t2.0\\t3.5\\tDescricao")
            Tuple(id=1, attributes=[1.5, 2.0, 3.5], description="Descricao")
        """
        try:
            tokens = string.split()
            tuple_obj = Tuple()
            
            token_index = 0
            
            # Parse ID se configurado
            if self.hasId:
                tuple_obj.setId(int(tokens[token_index]))
                token_index += 1
            
            # Parse atributos
            attributes = self._parseAttributes(tokens, token_index)
            tuple_obj.setAttributes(attributes)
            token_index += len(attributes)
            
            # Parse descrição se configurado
            if self.hasDesc and token_index < len(tokens):
                # Pega todos os tokens restantes como descrição
                description = ' '.join(tokens[token_index:])
                tuple_obj.setDescription(description)
            
            return tuple_obj
            
        except (IndexError, ValueError) as e:
            print(f"Erro: A string passada como parâmetro não está no formato esperado.")
            raise ValueError("A string passada como parâmetro não está no formato esperado.") from e
    
    def _parseAttributes(self, tokens: List[str], start_index: int) -> List[float]:
        """
        Parse dos atributos numéricos da tupla.
        
        Args:
            tokens: Lista de tokens.
            start_index: Índice inicial para começar o parse.
            
        Returns:
            Lista de atributos float.
        """
        attrs: List[float] = []
        
        # Se dimension é -1, usa todos os tokens restantes (exceto descrição)
        dimension = self.dimension
        if dimension == -1:
            dimension = len(tokens) - start_index
            if self.hasDesc:
                dimension -= 1  # Reserva último token para descrição
        
        for i in range(dimension):
            if start_index + i < len(tokens):
                attrs.append(float(tokens[start_index + i]))
        
        return attrs
