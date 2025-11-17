"""
Gate Dimension Validators Module

Validadores para verificar existência (ou não existência) de valores em dimensões.
Dimensões podem ser DataFrames, tabelas externas, APIs, etc.

A arquitetura é modular para permitir diferentes implementações futuras.
"""

from abc import abstractmethod
from typing import List, Optional, Union
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col
from .core import Validator


class DimensionValidator(Validator):
    """
    Classe base abstrata para validadores de dimensão.

    Validadores de dimensão verificam se valores existem (ou não existem)
    em uma fonte de dados externa (dimensão).

    O parâmetro 'exists' controla a lógica:
    - exists=True: valida que o valor EXISTE na dimensão (padrão)
    - exists=False: valida que o valor NÃO EXISTE na dimensão (validação reversa)
    """

    def __init__(self, exists: bool = True, name: Optional[str] = None):
        """
        Args:
            exists: Se True, valida que o valor EXISTE na dimensão.
                   Se False, valida que o valor NÃO EXISTE na dimensão.
            name: Nome do validador
        """
        super().__init__(name)
        self.exists = exists

    @abstractmethod
    def get_valid_values(self) -> list:
        """
        Retorna lista de valores válidos da dimensão.

        Returns:
            list: Lista de valores válidos
        """
        pass

    def validate(self, df: DataFrame) -> Column:
        """
        Valida usando a lista de valores válidos.

        Este método é implementado na classe base e usa get_valid_values()
        para obter os valores da dimensão.

        Returns:
            Column: True se passou na validação
        """
        raise NotImplementedError("Subclasses devem implementar validate()")


class DataFrameDimensionValidator(DimensionValidator):
    """
    Valida se valores existem (ou não) em um DataFrame de dimensão.

    Usa o método isin() do PySpark para validação eficiente.
    Ideal para dimensões de tamanho pequeno a médio.

    Para dimensões muito grandes, considere implementar um validador
    que use joins distribuídos em vez de coletar valores.
    """

    def __init__(
        self,
        column: str,
        dimension_df: DataFrame,
        dimension_column: Optional[str] = None,
        exists: bool = True,
        name: Optional[str] = None
    ):
        """
        Args:
            column: Nome da coluna no DataFrame fonte para validar
            dimension_df: DataFrame da dimensão (tabela de referência)
            dimension_column: Nome da coluna na dimensão para comparar.
                            Se None, usa o mesmo nome de 'column'
            exists: Se True, valida que existe na dimensão.
                   Se False, valida que NÃO existe na dimensão.
            name: Nome do validador
        """
        super().__init__(exists, name)

        self.column = column
        self.dimension_df = dimension_df
        self.dimension_column = dimension_column or column

        # Cache dos valores válidos (será preenchido na primeira validação)
        self._cached_values = None

    def get_valid_values(self) -> list:
        """
        Retorna lista de valores únicos da coluna de dimensão.

        Os valores são coletados do DataFrame e cacheados para performance.

        Returns:
            list: Lista de valores únicos da dimensão
        """
        if self._cached_values is None:
            # Coleta valores únicos da dimensão
            self._cached_values = [
                row[self.dimension_column]
                for row in self.dimension_df.select(self.dimension_column).distinct().collect()
            ]
        return self._cached_values

    def validate(self, df: DataFrame) -> Column:
        """
        Valida se valores da coluna existem (ou não) na dimensão.

        Returns:
            Column: True se passou na validação
        """
        valid_values = self.get_valid_values()

        if self.exists:
            # Deve existir na dimensão
            return col(self.column).isin(valid_values)
        else:
            # NÃO deve existir na dimensão (validação reversa)
            return ~col(self.column).isin(valid_values)

    def __repr__(self) -> str:
        return (
            f"DataFrameDimensionValidator("
            f"column='{self.column}', "
            f"dimension_column='{self.dimension_column}', "
            f"exists={self.exists}, "
            f"name='{self.name}')"
        )


class ListDimensionValidator(DimensionValidator):
    """
    Valida se valores existem (ou não) em uma lista pré-definida.

    Similar ao InListValidator, mas usa a interface de DimensionValidator
    para consistência e suporte à lógica reversa (not exists).
    """

    def __init__(
        self,
        column: str,
        valid_values: list,
        exists: bool = True,
        name: Optional[str] = None
    ):
        """
        Args:
            column: Nome da coluna para validar
            valid_values: Lista de valores válidos
            exists: Se True, valida que existe na lista.
                   Se False, valida que NÃO existe na lista.
            name: Nome do validador
        """
        super().__init__(exists, name)
        self.column = column
        self.valid_values = valid_values

    def get_valid_values(self) -> list:
        """Retorna a lista de valores válidos."""
        return self.valid_values

    def validate(self, df: DataFrame) -> Column:
        """
        Valida se valores existem (ou não) na lista.

        Returns:
            Column: True se passou na validação
        """
        if self.exists:
            return col(self.column).isin(self.valid_values)
        else:
            return ~col(self.column).isin(self.valid_values)

    def __repr__(self) -> str:
        values_preview = str(self.valid_values[:3]) + "..." if len(self.valid_values) > 3 else str(self.valid_values)
        return (
            f"ListDimensionValidator("
            f"column='{self.column}', "
            f"values={values_preview}, "
            f"exists={self.exists}, "
            f"name='{self.name}')"
        )


# Placeholder para futuras implementações de dimensões externas
#
# class ExternalTableDimensionValidator(DimensionValidator):
#     """
#     Valida contra uma tabela externa do Databricks.
#
#     Esta é uma implementação futura que usará joins distribuídos
#     em vez de coletar valores, permitindo validação contra
#     dimensões muito grandes.
#     """
#     pass
#
#
# class APIDimensionValidator(DimensionValidator):
#     """
#     Valida contra uma API externa.
#
#     Permite validar valores consultando uma API REST.
#     Útil para validações contra sistemas externos.
#     """
#     pass
