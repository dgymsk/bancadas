"""
Gate Validators Module

Este módulo contém implementações de validadores comuns para uso com PySpark.
"""

from typing import List, Optional, Any, Callable
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
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
        condition = F.lit(True)
        for col in self.columns:
            condition = condition & F.col(col).isNotNull()
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
        condition = F.lit(True)
        for col in self.columns:
            condition = condition & (F.length(F.trim(F.col(col))) > 0)
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
        col = F.col(self.column)
        condition = F.lit(True)

        if self.min_value is not None:
            if self.inclusive:
                condition = condition & (col >= self.min_value)
            else:
                condition = condition & (col > self.min_value)

        if self.max_value is not None:
            if self.inclusive:
                condition = condition & (col <= self.max_value)
            else:
                condition = condition & (col < self.max_value)

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
        return F.col(self.column).rlike(self.pattern)

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
        return F.col(self.column).isin(self.allowed_values)

    def __repr__(self) -> str:
        return f"InListValidator(column='{self.column}', allowed_values={self.allowed_values}, name='{self.name}')"


class LengthValidator(Validator):
    """
    Valida o comprimento de strings.
    """

    def __init__(
        self,
        column: str,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        name: Optional[str] = None
    ):
        """
        Args:
            column: Nome da coluna a ser validada
            min_length: Comprimento mínimo (opcional)
            max_length: Comprimento máximo (opcional)
            name: Nome do validador (opcional)
        """
        super().__init__(name)
        self.column = column
        self.min_length = min_length
        self.max_length = max_length

    def validate(self, df: DataFrame) -> Column:
        """
        Retorna True se o comprimento da string está dentro dos limites.
        """
        length = F.length(F.col(self.column))
        condition = F.lit(True)

        if self.min_length is not None:
            condition = condition & (length >= self.min_length)

        if self.max_length is not None:
            condition = condition & (length <= self.max_length)

        return condition

    def __repr__(self) -> str:
        return (f"LengthValidator(column='{self.column}', "
                f"min_length={self.min_length}, max_length={self.max_length}, name='{self.name}')")


class UniqueValidator(Validator):
    """
    Valida que não há duplicatas para as colunas especificadas.
    Marca como inválidas todas as linhas duplicadas (mantém apenas a primeira ocorrência).
    """

    def __init__(self, columns: List[str], name: Optional[str] = None):
        """
        Args:
            columns: Lista de colunas que devem formar uma chave única
            name: Nome do validador (opcional)
        """
        super().__init__(name)
        self.columns = columns

    def validate(self, df: DataFrame) -> Column:
        """
        Retorna True apenas para a primeira ocorrência de cada combinação única.
        """
        # Usando row_number para identificar a primeira ocorrência
        from pyspark.sql.window import Window

        window_spec = Window.partitionBy(*self.columns).orderBy(F.monotonically_increasing_id())
        return F.row_number().over(window_spec) == 1

    def __repr__(self) -> str:
        return f"UniqueValidator(columns={self.columns}, name='{self.name}')"


class CustomValidator(Validator):
    """
    Validador customizado que aceita uma função que retorna uma Column booleana.
    """

    def __init__(self, validation_func: Callable[[DataFrame], Column], name: Optional[str] = None):
        """
        Args:
            validation_func: Função que recebe um DataFrame e retorna uma Column booleana
            name: Nome do validador (obrigatório para CustomValidator)
        """
        if name is None:
            raise ValueError("CustomValidator requer um nome explícito")
        super().__init__(name)
        self.validation_func = validation_func

    def validate(self, df: DataFrame) -> Column:
        """
        Executa a função de validação customizada.
        """
        return self.validation_func(df)

    def __repr__(self) -> str:
        return f"CustomValidator(name='{self.name}')"


class DateRangeValidator(Validator):
    """
    Valida que datas estão dentro de um intervalo.
    """

    def __init__(
        self,
        column: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        date_format: str = "yyyy-MM-dd",
        name: Optional[str] = None
    ):
        """
        Args:
            column: Nome da coluna a ser validada
            min_date: Data mínima como string (opcional)
            max_date: Data máxima como string (opcional)
            date_format: Formato da data (padrão: yyyy-MM-dd)
            name: Nome do validador (opcional)
        """
        super().__init__(name)
        self.column = column
        self.min_date = min_date
        self.max_date = max_date
        self.date_format = date_format

    def validate(self, df: DataFrame) -> Column:
        """
        Retorna True se a data está dentro do intervalo especificado.
        """
        col = F.col(self.column)
        condition = F.lit(True)

        if self.min_date is not None:
            min_date_lit = F.to_date(F.lit(self.min_date), self.date_format)
            condition = condition & (col >= min_date_lit)

        if self.max_date is not None:
            max_date_lit = F.to_date(F.lit(self.max_date), self.date_format)
            condition = condition & (col <= max_date_lit)

        return condition

    def __repr__(self) -> str:
        return (f"DateRangeValidator(column='{self.column}', "
                f"min_date='{self.min_date}', max_date='{self.max_date}', "
                f"date_format='{self.date_format}', name='{self.name}')")


class EmailValidator(Validator):
    """
    Valida que uma coluna contém endereços de email válidos.
    """

    # Regex simples para validação de email
    EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

    def __init__(self, column: str, name: Optional[str] = None):
        """
        Args:
            column: Nome da coluna a ser validada
            name: Nome do validador (opcional)
        """
        super().__init__(name)
        self.column = column

    def validate(self, df: DataFrame) -> Column:
        """
        Retorna True se a string é um email válido.
        """
        return F.col(self.column).rlike(self.EMAIL_PATTERN)

    def __repr__(self) -> str:
        return f"EmailValidator(column='{self.column}', name='{self.name}')"


class CPFValidator(Validator):
    """
    Valida que uma coluna contém CPFs no formato válido (apenas dígitos, 11 caracteres).
    Nota: Esta é uma validação de formato, não verifica o dígito verificador.
    """

    CPF_PATTERN = r'^\d{11}$'

    def __init__(self, column: str, name: Optional[str] = None):
        """
        Args:
            column: Nome da coluna a ser validada
            name: Nome do validador (opcional)
        """
        super().__init__(name)
        self.column = column

    def validate(self, df: DataFrame) -> Column:
        """
        Retorna True se a string é um CPF válido (formato).
        """
        return F.col(self.column).rlike(self.CPF_PATTERN)

    def __repr__(self) -> str:
        return f"CPFValidator(column='{self.column}', name='{self.name}')"


class CNPJValidator(Validator):
    """
    Valida que uma coluna contém CNPJs no formato válido (apenas dígitos, 14 caracteres).
    Nota: Esta é uma validação de formato, não verifica o dígito verificador.
    """

    CNPJ_PATTERN = r'^\d{14}$'

    def __init__(self, column: str, name: Optional[str] = None):
        """
        Args:
            column: Nome da coluna a ser validada
            name: Nome do validador (opcional)
        """
        super().__init__(name)
        self.column = column

    def validate(self, df: DataFrame) -> Column:
        """
        Retorna True se a string é um CNPJ válido (formato).
        """
        return F.col(self.column).rlike(self.CNPJ_PATTERN)

    def __repr__(self) -> str:
        return f"CNPJValidator(column='{self.column}', name='{self.name}')"
