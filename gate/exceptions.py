"""
Gate Exceptions

Exceções customizadas para o sistema de validação Gate.
"""


class GateException(Exception):
    """Exceção base para todas as exceções do Gate."""
    pass


class ValidationError(GateException):
    """Exceção levantada quando uma validação falha."""

    def __init__(self, validator_name: str, message: str):
        self.validator_name = validator_name
        self.message = message
        super().__init__(f"[{validator_name}] {message}")


class ConfigurationError(GateException):
    """Exceção levantada quando há erro de configuração."""
    pass
