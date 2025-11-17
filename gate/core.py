"""
Gate Core Module

Este módulo contém as classes base para o sistema de validação Gate.
Inspirado no padrão Chain of Responsibility, mas onde todas as validações
devem passar para que uma linha seja aprovada.

A principal funcionalidade é manter um histórico das validações que falharam
em uma coluna tipo array, permitindo rastreabilidade completa.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import (
    col, lit, array, when, size, array_contains
)
from .exceptions import ValidationError, ConfigurationError


class Validator(ABC):
    """
    Classe base abstrata para validadores.

    Cada validador deve implementar o método validate() que retorna
    uma coluna booleana do PySpark indicando se a linha passou na validação.
    """

    def __init__(self, name: Optional[str] = None):
        """
        Inicializa o validador.

        Args:
            name: Nome do validador (opcional). Se não fornecido, usa o nome da classe.
        """
        self.name = name or self.__class__.__name__

    @abstractmethod
    def validate(self, df: DataFrame) -> Column:
        """
        Executa a validação e retorna uma coluna booleana.

        Args:
            df: DataFrame a ser validado

        Returns:
            Column: Coluna booleana onde True indica que a linha passou na validação
        """
        pass

    def get_validation_column_name(self) -> str:
        """
        Retorna o nome da coluna de validação que será adicionada ao DataFrame.

        Returns:
            str: Nome da coluna de validação
        """
        return f"_gate_valid_{self.name}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}')"


class Gate:
    """
    Classe principal que gerencia a execução de múltiplos validadores.

    O Gate executa todos os validadores na ordem fornecida e mantém um histórico
    das validações que falharam em uma coluna array. Uma linha só é considerada
    válida se passar em TODAS as validações (array de falhas vazio).
    """

    def __init__(self, validators: List[Validator]):
        """
        Inicializa o Gate com uma lista de validadores.

        Args:
            validators: Lista de validadores a serem executados
        """
        if not validators:
            raise ConfigurationError("A lista de validadores não pode estar vazia")

        self.validators = validators

    def process(self, df: DataFrame, history_column: str = "_gate_failures") -> DataFrame:
        """
        Processa o DataFrame aplicando todos os validadores.

        Cria uma coluna array contendo os nomes dos validadores que falharam.
        Uma linha é válida se o array estiver vazio.

        Args:
            df: DataFrame de entrada
            history_column: Nome da coluna que conterá o histórico de falhas (array)

        Returns:
            DataFrame: DataFrame com a coluna de histórico adicionada
        """
        from pyspark.sql.functions import expr

        result_df = df
        validation_columns = []

        # Aplica cada validador criando colunas booleanas temporárias
        for validator in self.validators:
            col_name = validator.get_validation_column_name()
            validation_col = validator.validate(result_df)
            result_df = result_df.withColumn(col_name, validation_col)
            validation_columns.append((col_name, validator.name))

        # Constrói array com nomes dos validadores que falharam
        # Para cada validador: se falhou (False), adiciona o nome; senão, adiciona None
        failure_conditions = []
        for col_name, validator_name in validation_columns:
            failure_conditions.append(
                when(col(col_name) == False, lit(validator_name)).otherwise(lit(None))
            )

        # Cria array com todas as condições e filtra valores None usando filter SQL
        failures_array = array(*failure_conditions)
        result_df = result_df.withColumn("_temp_failures", failures_array)
        result_df = result_df.withColumn(
            history_column,
            expr("filter(_temp_failures, x -> x is not null)")
        )

        # Remove colunas temporárias
        cols_to_drop = [col_name for col_name, _ in validation_columns] + ["_temp_failures"]
        result_df = result_df.drop(*cols_to_drop)

        return result_df

    def filter_valid(self, df: DataFrame, history_column: str = "_gate_failures") -> DataFrame:
        """
        Processa o DataFrame e retorna apenas as linhas que passaram em todas as validações.

        Uma linha é válida se o array de falhas estiver vazio.

        Args:
            df: DataFrame de entrada
            history_column: Nome da coluna de histórico

        Returns:
            DataFrame: DataFrame filtrado apenas com linhas válidas (sem coluna de histórico)
        """
        result_df = self.process(df, history_column)
        valid_df = result_df.filter(size(col(history_column)) == 0)

        # Remove a coluna de histórico
        valid_df = valid_df.drop(history_column)

        return valid_df

    def filter_invalid(self, df: DataFrame, history_column: str = "_gate_failures") -> DataFrame:
        """
        Processa o DataFrame e retorna apenas as linhas que falharam em alguma validação.

        Mantém a coluna de histórico mostrando quais validações falharam.

        Args:
            df: DataFrame de entrada
            history_column: Nome da coluna de histórico

        Returns:
            DataFrame: DataFrame filtrado apenas com linhas inválidas (com coluna de histórico)
        """
        result_df = self.process(df, history_column)
        invalid_df = result_df.filter(size(col(history_column)) > 0)

        return invalid_df

    def split(self, df: DataFrame, history_column: str = "_gate_failures") -> tuple:
        """
        Processa o DataFrame e retorna dois DataFrames: válidos e inválidos.

        Args:
            df: DataFrame de entrada
            history_column: Nome da coluna de histórico

        Returns:
            tuple: (DataFrame com linhas válidas, DataFrame com linhas inválidas)
        """
        result_df = self.process(df, history_column)

        valid_df = result_df.filter(size(col(history_column)) == 0).drop(history_column)
        invalid_df = result_df.filter(size(col(history_column)) > 0)

        return valid_df, invalid_df

    def __repr__(self) -> str:
        validators_repr = ", ".join([repr(v) for v in self.validators])
        return f"Gate(validators=[{validators_repr}])"
