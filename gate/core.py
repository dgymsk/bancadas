"""
Gate Core Module

Este módulo contém as classes base para o sistema de validação Gate.
Inspirado no padrão Chain of Responsibility, mas onde todas as validações
devem passar para que uma linha seja aprovada.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
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

    O Gate executa todos os validadores na ordem fornecida e apenas
    as linhas que passarem em TODAS as validações são consideradas válidas.
    """

    def __init__(self, validators: List[Validator], keep_validation_columns: bool = False):
        """
        Inicializa o Gate com uma lista de validadores.

        Args:
            validators: Lista de validadores a serem executados
            keep_validation_columns: Se True, mantém as colunas de validação individuais
                                    no DataFrame final (útil para debugging)
        """
        if not validators:
            raise ConfigurationError("A lista de validadores não pode estar vazia")

        self.validators = validators
        self.keep_validation_columns = keep_validation_columns
        self._validation_stats: Dict[str, Any] = {}

    def process(self, df: DataFrame, final_column_name: str = "_gate_passed") -> DataFrame:
        """
        Processa o DataFrame aplicando todos os validadores.

        Args:
            df: DataFrame de entrada
            final_column_name: Nome da coluna final que indica se a linha passou em todas as validações

        Returns:
            DataFrame: DataFrame com a coluna de validação adicionada
        """
        result_df = df
        validation_columns = []

        # Aplica cada validador
        for validator in self.validators:
            col_name = validator.get_validation_column_name()
            validation_col = validator.validate(result_df)
            result_df = result_df.withColumn(col_name, validation_col)
            validation_columns.append(col_name)

        # Combina todas as validações (AND lógico)
        # Uma linha só passa se todas as validações retornarem True
        final_validation = F.lit(True)
        for col_name in validation_columns:
            final_validation = final_validation & F.col(col_name)

        result_df = result_df.withColumn(final_column_name, final_validation)

        # Remove colunas intermediárias se não for para mantê-las
        if not self.keep_validation_columns:
            result_df = result_df.drop(*validation_columns)

        return result_df

    def filter_valid(self, df: DataFrame, final_column_name: str = "_gate_passed") -> DataFrame:
        """
        Processa o DataFrame e retorna apenas as linhas que passaram em todas as validações.

        Args:
            df: DataFrame de entrada
            final_column_name: Nome da coluna de validação

        Returns:
            DataFrame: DataFrame filtrado apenas com linhas válidas
        """
        result_df = self.process(df, final_column_name)
        valid_df = result_df.filter(F.col(final_column_name) == True)

        # Remove a coluna de validação final
        valid_df = valid_df.drop(final_column_name)

        return valid_df

    def filter_invalid(self, df: DataFrame, final_column_name: str = "_gate_passed") -> DataFrame:
        """
        Processa o DataFrame e retorna apenas as linhas que falharam em alguma validação.

        Args:
            df: DataFrame de entrada
            final_column_name: Nome da coluna de validação

        Returns:
            DataFrame: DataFrame filtrado apenas com linhas inválidas
        """
        result_df = self.process(df, final_column_name)
        invalid_df = result_df.filter(F.col(final_column_name) == False)

        return invalid_df

    def split(self, df: DataFrame, final_column_name: str = "_gate_passed") -> tuple:
        """
        Processa o DataFrame e retorna dois DataFrames: válidos e inválidos.

        Args:
            df: DataFrame de entrada
            final_column_name: Nome da coluna de validação

        Returns:
            tuple: (DataFrame com linhas válidas, DataFrame com linhas inválidas)
        """
        result_df = self.process(df, final_column_name)

        valid_df = result_df.filter(F.col(final_column_name) == True).drop(final_column_name)
        invalid_df = result_df.filter(F.col(final_column_name) == False)

        return valid_df, invalid_df

    def get_validation_stats(self, df: DataFrame, final_column_name: str = "_gate_passed") -> Dict[str, Any]:
        """
        Retorna estatísticas sobre as validações.

        Args:
            df: DataFrame de entrada
            final_column_name: Nome da coluna de validação

        Returns:
            Dict: Dicionário com estatísticas de validação
        """
        result_df = self.process(df, final_column_name)

        total_rows = result_df.count()
        valid_rows = result_df.filter(F.col(final_column_name) == True).count()
        invalid_rows = total_rows - valid_rows

        stats = {
            "total_rows": total_rows,
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "valid_percentage": (valid_rows / total_rows * 100) if total_rows > 0 else 0,
            "invalid_percentage": (invalid_rows / total_rows * 100) if total_rows > 0 else 0,
            "validators": []
        }

        # Estatísticas por validador
        if self.keep_validation_columns:
            for validator in self.validators:
                col_name = validator.get_validation_column_name()
                validator_valid = result_df.filter(F.col(col_name) == True).count()
                validator_invalid = total_rows - validator_valid

                stats["validators"].append({
                    "name": validator.name,
                    "valid_rows": validator_valid,
                    "invalid_rows": validator_invalid,
                    "valid_percentage": (validator_valid / total_rows * 100) if total_rows > 0 else 0
                })

        return stats

    def add_validator(self, validator: Validator) -> 'Gate':
        """
        Adiciona um novo validador à lista.

        Args:
            validator: Validador a ser adicionado

        Returns:
            Gate: Retorna self para permitir method chaining
        """
        self.validators.append(validator)
        return self

    def __repr__(self) -> str:
        validators_repr = ", ".join([repr(v) for v in self.validators])
        return f"Gate(validators=[{validators_repr}], keep_validation_columns={self.keep_validation_columns})"
