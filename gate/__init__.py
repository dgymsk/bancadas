"""
Gate - Sistema de Validação para PySpark

Um sistema de validação flexível e extensível para DataFrames do PySpark,
inspirado no padrão Chain of Responsibility.

Uso básico:
    from gate import Gate, NotNullValidator, RangeValidator

    validators = [
        NotNullValidator(columns=['id', 'name']),
        RangeValidator(column='age', min_value=0, max_value=150)
    ]

    gate = Gate(validators)
    valid_df = gate.filter_valid(df)
"""

from .core import Gate, Validator
from .validators import (
    NotNullValidator,
    NotEmptyValidator,
    RangeValidator,
    RegexValidator,
    InListValidator,
)
from .exceptions import GateException, ValidationError, ConfigurationError

__version__ = "2.0.0"

__all__ = [
    # Core
    "Gate",
    "Validator",
    # Validators
    "NotNullValidator",
    "NotEmptyValidator",
    "RangeValidator",
    "RegexValidator",
    "InListValidator",
    # Exceptions
    "GateException",
    "ValidationError",
    "ConfigurationError",
]
