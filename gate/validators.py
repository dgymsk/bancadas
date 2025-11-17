"""
Gate Validators Module

Este módulo contém implementações de validadores comuns para uso com PySpark.
"""

from typing import List, Optional, Any
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, length, trim, lit
from .core import Validator


class NotNullValidator(Validator):
    """
    Valida que as colunas especificadas não são nulas.
    """

    def __init__(self, columns: List[str], name: Optional[str] = None):
        """
        Args:
            columns: Lista de nomes de colunas que não podem ser nulas
            name: Nome do validador (opcional)
        """
        super().__init__(name)
        self.columns = columns

    def validate(self, df: DataFrame) -> Column:
        """
        Retorna True se todas as colunas especificadas não são nulas.
        """
        condition = lit(True)
        for column in self.columns:
            condition = condition & col(column).isNotNull()
        return condition

    def __repr__(self) -> str:
        return f"NotNullValidator(columns={self.columns}, name='{self.name}')"


class NotEmptyValidator(Validator):
    """
    Valida que colunas de string não são vazias (após trim).
    """

    def __init__(self, columns: List[str], name: Optional[str] = None):
        """
        Args:
            columns: Lista de nomes de colunas que não podem ser vazias
            name: Nome do validador (opcional)
        """
        super().__init__(name)
        self.columns = columns

    def validate(self, df: DataFrame) -> Column:
        """
        Retorna True se todas as colunas especificadas não são vazias.
        """
        condition = lit(True)
        for column in self.columns:
            condition = condition & (length(trim(col(column))) > 0)
        return condition

    def __repr__(self) -> str:
        return f"NotEmptyValidator(columns={self.columns}, name='{self.name}')"


class RangeValidator(Validator):
    """
    Valida que valores numéricos estão dentro de um intervalo.
    """

    def __init__(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        inclusive: bool = True,
        name: Optional[str] = None
    ):
        """
        Args:
            column: Nome da coluna a ser validada
            min_value: Valor mínimo (opcional)
            max_value: Valor máximo (opcional)
            inclusive: Se True, usa >= e <=; se False, usa > e <
            name: Nome do validador (opcional)
        """
        super().__init__(name)
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
        self.inclusive = inclusive

    def validate(self, df: DataFrame) -> Column:
        """
        Retorna True se o valor está dentro do intervalo especificado.
        """
        column_ref = col(self.column)
        condition = lit(True)

        if self.min_value is not None:
            if self.inclusive:
                condition = condition & (column_ref >= self.min_value)
            else:
                condition = condition & (column_ref > self.min_value)

        if self.max_value is not None:
            if self.inclusive:
                condition = condition & (column_ref <= self.max_value)
            else:
                condition = condition & (column_ref < self.max_value)

        return condition

    def __repr__(self) -> str:
        return (f"RangeValidator(column='{self.column}', "
                f"min_value={self.min_value}, max_value={self.max_value}, "
                f"inclusive={self.inclusive}, name='{self.name}')")


class RegexValidator(Validator):
    """
    Valida que uma coluna de string corresponde a um padrão regex.
    """

    def __init__(self, column: str, pattern: str, name: Optional[str] = None):
        """
        Args:
            column: Nome da coluna a ser validada
            pattern: Padrão regex
            name: Nome do validador (opcional)
        """
        super().__init__(name)
        self.column = column
        self.pattern = pattern

    def validate(self, df: DataFrame) -> Column:
        """
        Retorna True se a string corresponde ao padrão regex.
        """
        return col(self.column).rlike(self.pattern)

    def __repr__(self) -> str:
        return f"RegexValidator(column='{self.column}', pattern='{self.pattern}', name='{self.name}')"


class InListValidator(Validator):
    """
    Valida que o valor de uma coluna está em uma lista de valores permitidos.
    """

    def __init__(self, column: str, allowed_values: List[Any], name: Optional[str] = None):
        """
        Args:
            column: Nome da coluna a ser validada
            allowed_values: Lista de valores permitidos
            name: Nome do validador (opcional)
        """
        super().__init__(name)
        self.column = column
        self.allowed_values = allowed_values

    def validate(self, df: DataFrame) -> Column:
        """
        Retorna True se o valor está na lista de valores permitidos.
        """
        return col(self.column).isin(self.allowed_values)

    def __repr__(self) -> str:
        return f"InListValidator(column='{self.column}', allowed_values={self.allowed_values}, name='{self.name}')"
