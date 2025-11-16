"""
Testes unitários para o Gate
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
sys.path.append('..')

from gate import (
    Gate,
    Validator,
    NotNullValidator,
    NotEmptyValidator,
    RangeValidator,
    RegexValidator,
    InListValidator,
    LengthValidator,
    CustomValidator,
    EmailValidator,
    CPFValidator,
    CNPJValidator,
    ConfigurationError,
)


@pytest.fixture(scope="session")
def spark():
    """Fixture para criar uma SparkSession para testes"""
    spark = SparkSession.builder \
        .appName("GateTests") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


class TestNotNullValidator:
    """Testes para NotNullValidator"""

    def test_not_null_validator_valid(self, spark):
        data = [(1, "A"), (2, "B")]
        df = spark.createDataFrame(data, ["id", "name"])

        validator = NotNullValidator(columns=["id", "name"])
        gate = Gate([validator])

        result = gate.filter_valid(df)
        assert result.count() == 2

    def test_not_null_validator_invalid(self, spark):
        data = [(1, "A"), (None, "B"), (3, None)]
        df = spark.createDataFrame(data, ["id", "name"])

        validator = NotNullValidator(columns=["id", "name"])
        gate = Gate([validator])

        result = gate.filter_valid(df)
        assert result.count() == 1


class TestNotEmptyValidator:
    """Testes para NotEmptyValidator"""

    def test_not_empty_validator_valid(self, spark):
        data = [("João",), ("Maria",)]
        df = spark.createDataFrame(data, ["name"])

        validator = NotEmptyValidator(columns=["name"])
        gate = Gate([validator])

        result = gate.filter_valid(df)
        assert result.count() == 2

    def test_not_empty_validator_invalid(self, spark):
        data = [("João",), ("",), ("  ",)]
        df = spark.createDataFrame(data, ["name"])

        validator = NotEmptyValidator(columns=["name"])
        gate = Gate([validator])

        result = gate.filter_valid(df)
        assert result.count() == 1


class TestRangeValidator:
    """Testes para RangeValidator"""

    def test_range_validator_min_max(self, spark):
        data = [(1, 10), (2, 20), (3, 30), (4, 5), (5, 35)]
        df = spark.createDataFrame(data, ["id", "value"])

        validator = RangeValidator(column="value", min_value=10, max_value=30)
        gate = Gate([validator])

        result = gate.filter_valid(df)
        assert result.count() == 3  # 10, 20, 30

    def test_range_validator_min_only(self, spark):
        data = [(1, 10), (2, 5), (3, 15)]
        df = spark.createDataFrame(data, ["id", "value"])

        validator = RangeValidator(column="value", min_value=10)
        gate = Gate([validator])

        result = gate.filter_valid(df)
        assert result.count() == 2  # 10, 15

    def test_range_validator_max_only(self, spark):
        data = [(1, 10), (2, 5), (3, 15)]
        df = spark.createDataFrame(data, ["id", "value"])

        validator = RangeValidator(column="value", max_value=10)
        gate = Gate([validator])

        result = gate.filter_valid(df)
        assert result.count() == 2  # 10, 5


class TestRegexValidator:
    """Testes para RegexValidator"""

    def test_regex_validator_valid(self, spark):
        data = [("ABC123",), ("DEF456",), ("GHI",)]
        df = spark.createDataFrame(data, ["code"])

        validator = RegexValidator(column="code", pattern=r"^[A-Z]{3}\d{3}$")
        gate = Gate([validator])

        result = gate.filter_valid(df)
        assert result.count() == 2  # ABC123, DEF456


class TestInListValidator:
    """Testes para InListValidator"""

    def test_in_list_validator(self, spark):
        data = [("SP",), ("RJ",), ("MG",), ("XY",)]
        df = spark.createDataFrame(data, ["state"])

        validator = InListValidator(column="state", allowed_values=["SP", "RJ", "MG"])
        gate = Gate([validator])

        result = gate.filter_valid(df)
        assert result.count() == 3


class TestLengthValidator:
    """Testes para LengthValidator"""

    def test_length_validator_min_max(self, spark):
        data = [("AB",), ("ABC",), ("ABCD",), ("ABCDE",), ("ABCDEF",)]
        df = spark.createDataFrame(data, ["text"])

        validator = LengthValidator(column="text", min_length=3, max_length=5)
        gate = Gate([validator])

        result = gate.filter_valid(df)
        assert result.count() == 3  # ABC, ABCD, ABCDE


class TestEmailValidator:
    """Testes para EmailValidator"""

    def test_email_validator(self, spark):
        data = [
            ("user@example.com",),
            ("invalid.email",),
            ("another@test.com.br",),
            ("@invalid.com",),
        ]
        df = spark.createDataFrame(data, ["email"])

        validator = EmailValidator(column="email")
        gate = Gate([validator])

        result = gate.filter_valid(df)
        assert result.count() == 2


class TestCPFValidator:
    """Testes para CPFValidator"""

    def test_cpf_validator(self, spark):
        data = [
            ("12345678901",),
            ("123",),
            ("98765432100",),
            ("1234567890a",),
        ]
        df = spark.createDataFrame(data, ["cpf"])

        validator = CPFValidator(column="cpf")
        gate = Gate([validator])

        result = gate.filter_valid(df)
        assert result.count() == 2


class TestCNPJValidator:
    """Testes para CNPJValidator"""

    def test_cnpj_validator(self, spark):
        data = [
            ("12345678901234",),
            ("123",),
            ("98765432109876",),
            ("1234567890123a",),
        ]
        df = spark.createDataFrame(data, ["cnpj"])

        validator = CNPJValidator(column="cnpj")
        gate = Gate([validator])

        result = gate.filter_valid(df)
        assert result.count() == 2


class TestCustomValidator:
    """Testes para CustomValidator"""

    def test_custom_validator(self, spark):
        data = [(1, 10), (2, 20), (3, 5)]
        df = spark.createDataFrame(data, ["id", "value"])

        # Validador customizado: value deve ser > 10
        def custom_validation(df):
            return F.col("value") > 10

        validator = CustomValidator(custom_validation, name="CustomTest")
        gate = Gate([validator])

        result = gate.filter_valid(df)
        assert result.count() == 1  # 20

    def test_custom_validator_requires_name(self, spark):
        def custom_validation(df):
            return F.col("value") > 10

        with pytest.raises(ValueError):
            CustomValidator(custom_validation)


class TestGate:
    """Testes para a classe Gate"""

    def test_gate_empty_validators_error(self, spark):
        with pytest.raises(ConfigurationError):
            Gate([])

    def test_gate_multiple_validators(self, spark):
        data = [
            (1, "João", 25),
            (2, None, 30),
            (3, "Maria", -5),
            (4, "Ana", 150),
            (5, "Carlos", 40),
        ]
        df = spark.createDataFrame(data, ["id", "name", "age"])

        validators = [
            NotNullValidator(columns=["name"]),
            RangeValidator(column="age", min_value=0, max_value=120),
        ]

        gate = Gate(validators)
        result = gate.filter_valid(df)

        assert result.count() == 2  # João e Carlos

    def test_gate_split(self, spark):
        data = [(1, "A"), (2, None), (3, "C")]
        df = spark.createDataFrame(data, ["id", "name"])

        validator = NotNullValidator(columns=["name"])
        gate = Gate([validator])

        valid_df, invalid_df = gate.split(df)

        assert valid_df.count() == 2
        assert invalid_df.count() == 1

    def test_gate_filter_invalid(self, spark):
        data = [(1, "A"), (2, None), (3, "C")]
        df = spark.createDataFrame(data, ["id", "name"])

        validator = NotNullValidator(columns=["name"])
        gate = Gate([validator])

        invalid_df = gate.filter_invalid(df)

        assert invalid_df.count() == 1

    def test_gate_keep_validation_columns(self, spark):
        data = [(1, "A"), (2, "B")]
        df = spark.createDataFrame(data, ["id", "name"])

        validator = NotNullValidator(columns=["name"])
        gate = Gate([validator], keep_validation_columns=True)

        result = gate.process(df)

        # Verifica que a coluna de validação foi mantida
        assert "_gate_valid_NotNullValidator" in result.columns

    def test_gate_statistics(self, spark):
        data = [(1, "A"), (2, None), (3, "C")]
        df = spark.createDataFrame(data, ["id", "name"])

        validator = NotNullValidator(columns=["name"])
        gate = Gate([validator], keep_validation_columns=True)

        stats = gate.get_validation_stats(df)

        assert stats["total_rows"] == 3
        assert stats["valid_rows"] == 2
        assert stats["invalid_rows"] == 1
        assert len(stats["validators"]) == 1

    def test_gate_add_validator(self, spark):
        data = [(1, "A", 10), (2, "B", 20)]
        df = spark.createDataFrame(data, ["id", "name", "value"])

        gate = Gate([NotNullValidator(columns=["name"])])
        gate.add_validator(RangeValidator(column="value", min_value=15))

        result = gate.filter_valid(df)
        assert result.count() == 1  # Apenas (2, "B", 20)


class TestIntegration:
    """Testes de integração"""

    def test_complex_validation_scenario(self, spark):
        """Testa um cenário complexo com múltiplas validações"""
        data = [
            (1, "João Silva", "12345678901", "joao@email.com", 25, "SP"),
            (2, "Maria", "98765432100", "maria@email.com", 30, "RJ"),
            (3, "", "11122233344", "pedro@email.com", 28, "MG"),
            (4, "Ana Costa", "123", "ana@email.com", 22, "ES"),
            (5, "Carlos", "55566677788", "invalid", 35, "XY"),
        ]

        df = spark.createDataFrame(
            data,
            ["id", "nome", "cpf", "email", "idade", "estado"]
        )

        validators = [
            NotEmptyValidator(columns=["nome"]),
            CPFValidator(column="cpf"),
            EmailValidator(column="email"),
            RangeValidator(column="idade", min_value=18, max_value=100),
            InListValidator(column="estado", allowed_values=["SP", "RJ", "MG", "ES"]),
        ]

        gate = Gate(validators)
        valid_df = gate.filter_valid(df)

        # Apenas as linhas 1 e 2 devem passar em todas as validações
        assert valid_df.count() == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
