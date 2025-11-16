"""
Exemplos de uso do Gate

Este arquivo demonstra diversos casos de uso do sistema de validação Gate.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Importa o Gate e validadores
import sys
sys.path.append('..')

from gate import (
    Gate,
    NotNullValidator,
    NotEmptyValidator,
    RangeValidator,
    RegexValidator,
    InListValidator,
    LengthValidator,
    UniqueValidator,
    CustomValidator,
    DateRangeValidator,
    EmailValidator,
    CPFValidator,
)


def exemplo_basico():
    """Exemplo básico de uso do Gate"""
    print("\n" + "="*80)
    print("EXEMPLO 1: Uso Básico")
    print("="*80)

    # Cria uma SparkSession
    spark = SparkSession.builder.appName("GateExampleBasic").getOrCreate()

    # Cria um DataFrame de exemplo
    data = [
        ("João", 25, "joao@email.com"),
        ("Maria", 30, "maria@email.com"),
        (None, 22, "pedro@email.com"),  # Nome nulo - será rejeitado
        ("Ana", -5, "ana@email.com"),   # Idade negativa - será rejeitada
        ("Carlos", 150, "carlos"),      # Email inválido - será rejeitado
    ]

    df = spark.createDataFrame(data, ["nome", "idade", "email"])

    print("\nDataFrame original:")
    df.show()

    # Define os validadores
    validators = [
        NotNullValidator(columns=["nome"], name="ValidaNomeNaoNulo"),
        RangeValidator(column="idade", min_value=0, max_value=120, name="ValidaIdade"),
        EmailValidator(column="email", name="ValidaEmail"),
    ]

    # Cria o Gate
    gate = Gate(validators)

    # Filtra apenas linhas válidas
    df_valido = gate.filter_valid(df)

    print("\nDataFrame após validação (apenas linhas válidas):")
    df_valido.show()

    # Mostra linhas inválidas com detalhes
    gate_debug = Gate(validators, keep_validation_columns=True)
    df_com_validacoes = gate_debug.process(df)

    print("\nDataFrame com colunas de validação (para debug):")
    df_com_validacoes.show(truncate=False)


def exemplo_split():
    """Exemplo usando split para separar válidos e inválidos"""
    print("\n" + "="*80)
    print("EXEMPLO 2: Split - Separando Válidos e Inválidos")
    print("="*80)

    spark = SparkSession.builder.appName("GateExampleSplit").getOrCreate()

    data = [
        (1, "Produto A", 100.0, "SP"),
        (2, "Produto B", 250.0, "RJ"),
        (3, "", 150.0, "MG"),        # Nome vazio
        (4, "Produto D", -50.0, "SP"), # Preço negativo
        (5, "Produto E", 300.0, "XY"), # Estado inválido
    ]

    df = spark.createDataFrame(data, ["id", "produto", "preco", "estado"])

    print("\nDataFrame original:")
    df.show()

    validators = [
        NotEmptyValidator(columns=["produto"], name="ValidaProduto"),
        RangeValidator(column="preco", min_value=0, name="ValidaPreco"),
        InListValidator(column="estado", allowed_values=["SP", "RJ", "MG", "ES"], name="ValidaEstado"),
    ]

    gate = Gate(validators)

    # Separa em dois DataFrames
    df_valido, df_invalido = gate.split(df)

    print("\nLinhas VÁLIDAS:")
    df_valido.show()

    print("\nLinhas INVÁLIDAS (com detalhes das validações):")
    df_invalido.show(truncate=False)


def exemplo_estatisticas():
    """Exemplo mostrando estatísticas de validação"""
    print("\n" + "="*80)
    print("EXEMPLO 3: Estatísticas de Validação")
    print("="*80)

    spark = SparkSession.builder.appName("GateExampleStats").getOrCreate()

    data = [
        (1, "Cliente A", "12345678901", "cliente.a@email.com"),
        (2, "Cliente B", "98765432100", "cliente.b@email.com"),
        (3, "Cliente C", "111", "invalido"),           # CPF e email inválidos
        (4, None, "22233344455", "cliente.d@email.com"), # Nome nulo
        (5, "Cliente E", "33344455566", "cliente.e@email.com"),
        (6, "Cliente F", "44455566677", "outro_invalido"), # Email inválido
    ]

    df = spark.createDataFrame(data, ["id", "nome", "cpf", "email"])

    print("\nDataFrame original:")
    df.show(truncate=False)

    validators = [
        NotNullValidator(columns=["nome"], name="ValidaNome"),
        CPFValidator(column="cpf", name="ValidaCPF"),
        EmailValidator(column="email", name="ValidaEmail"),
    ]

    gate = Gate(validators, keep_validation_columns=True)

    # Obtém estatísticas
    stats = gate.get_validation_stats(df)

    print("\nEstatísticas Gerais:")
    print(f"Total de linhas: {stats['total_rows']}")
    print(f"Linhas válidas: {stats['valid_rows']} ({stats['valid_percentage']:.1f}%)")
    print(f"Linhas inválidas: {stats['invalid_rows']} ({stats['invalid_percentage']:.1f}%)")

    print("\nEstatísticas por Validador:")
    for validator_stats in stats['validators']:
        print(f"\n  {validator_stats['name']}:")
        print(f"    - Válidas: {validator_stats['valid_rows']} ({validator_stats['valid_percentage']:.1f}%)")
        print(f"    - Inválidas: {validator_stats['invalid_rows']}")


def exemplo_custom_validator():
    """Exemplo usando validador customizado"""
    print("\n" + "="*80)
    print("EXEMPLO 4: Validador Customizado")
    print("="*80)

    spark = SparkSession.builder.appName("GateExampleCustom").getOrCreate()

    data = [
        (1, "Produto A", 100.0, 10),
        (2, "Produto B", 200.0, 20),
        (3, "Produto C", 50.0, 5),   # Total < 500, será rejeitado
        (4, "Produto D", 150.0, 4),  # Total = 600, será aceito
        (5, "Produto E", 300.0, 1),  # Total < 500, será rejeitado
    ]

    df = spark.createDataFrame(data, ["id", "produto", "preco", "quantidade"])

    print("\nDataFrame original:")
    df.show()

    # Validador customizado: total (preço * quantidade) deve ser >= 500
    def valida_total_minimo(df):
        return (F.col("preco") * F.col("quantidade")) >= 500

    validators = [
        CustomValidator(valida_total_minimo, name="ValidaTotalMinimo"),
    ]

    gate = Gate(validators)

    df_valido = gate.filter_valid(df)

    print("\nLinhas onde (preço * quantidade) >= 500:")
    df_valido.show()


def exemplo_unique_validator():
    """Exemplo usando validador de unicidade"""
    print("\n" + "="*80)
    print("EXEMPLO 5: Validador de Unicidade")
    print("="*80)

    spark = SparkSession.builder.appName("GateExampleUnique").getOrCreate()

    data = [
        (1, "cliente1@email.com", "2024-01-01"),
        (2, "cliente2@email.com", "2024-01-02"),
        (3, "cliente1@email.com", "2024-01-03"),  # Email duplicado
        (4, "cliente3@email.com", "2024-01-04"),
        (5, "cliente2@email.com", "2024-01-05"),  # Email duplicado
    ]

    df = spark.createDataFrame(data, ["id", "email", "data_cadastro"])

    print("\nDataFrame original (com duplicatas):")
    df.show()

    validators = [
        UniqueValidator(columns=["email"], name="ValidaEmailUnico"),
    ]

    gate = Gate(validators)

    df_valido = gate.filter_valid(df)

    print("\nDataFrame sem duplicatas (apenas primeira ocorrência mantida):")
    df_valido.show()


def exemplo_completo():
    """Exemplo completo com múltiplas validações"""
    print("\n" + "="*80)
    print("EXEMPLO 6: Validação Completa - Cadastro de Clientes")
    print("="*80)

    spark = SparkSession.builder.appName("GateExampleComplete").getOrCreate()

    # Dados de clientes com diversos problemas
    data = [
        (1, "João Silva", "12345678901", "joao@email.com", 25, "2024-01-15", "SP"),
        (2, "Maria Santos", "98765432100", "maria@email.com", 30, "2024-01-16", "RJ"),
        (3, "", "11122233344", "cliente3@email.com", 28, "2024-01-17", "MG"),  # Nome vazio
        (4, "Pedro Oliveira", "123", "pedro@email.com", 35, "2024-01-18", "SP"), # CPF inválido
        (5, "Ana Costa", "55566677788", "email_invalido", 22, "2024-01-19", "ES"), # Email inválido
        (6, "Carlos Souza", "99988877766", "carlos@email.com", -5, "2024-01-20", "RJ"), # Idade negativa
        (7, "Julia Lima", "44433322211", "julia@email.com", 150, "2024-01-21", "SP"), # Idade muito alta
        (8, "Roberto Alves", "77788899900", "roberto@email.com", 40, "2025-12-31", "XY"), # Data futura e estado inválido
        (9, "Fernanda Dias", "11111111111", "fernanda@email.com", 28, "2024-01-22", "MG"),
        (10, "Fernanda Dias", "11111111111", "fernanda@email.com", 28, "2024-01-23", "MG"), # Duplicata
    ]

    df = spark.createDataFrame(
        data,
        ["id", "nome", "cpf", "email", "idade", "data_cadastro", "estado"]
    )

    print("\nDataFrame original:")
    df.show(truncate=False)

    # Define todas as validações
    validators = [
        NotNullValidator(columns=["nome", "cpf", "email"], name="CamposObrigatorios"),
        NotEmptyValidator(columns=["nome"], name="NomeNaoVazio"),
        CPFValidator(column="cpf", name="CPFValido"),
        EmailValidator(column="email", name="EmailValido"),
        RangeValidator(column="idade", min_value=0, max_value=120, name="IdadeValida"),
        DateRangeValidator(
            column="data_cadastro",
            min_date="2024-01-01",
            max_date="2024-12-31",
            name="DataCadastroValida"
        ),
        InListValidator(
            column="estado",
            allowed_values=["SP", "RJ", "MG", "ES", "PR", "SC", "RS"],
            name="EstadoValido"
        ),
        UniqueValidator(columns=["cpf"], name="CPFUnico"),
    ]

    # Gate com validações detalhadas
    gate = Gate(validators, keep_validation_columns=True)

    # Processa e mostra resultados
    df_processado = gate.process(df)

    print("\nDataFrame processado (com colunas de validação):")
    df_processado.show(truncate=False)

    # Estatísticas
    stats = gate.get_validation_stats(df)

    print("\n" + "="*80)
    print("ESTATÍSTICAS DE VALIDAÇÃO")
    print("="*80)
    print(f"\nTotal de registros: {stats['total_rows']}")
    print(f"Registros válidos: {stats['valid_rows']} ({stats['valid_percentage']:.1f}%)")
    print(f"Registros inválidos: {stats['invalid_rows']} ({stats['invalid_percentage']:.1f}%)")

    print("\nDetalhamento por validador:")
    for validator_stats in stats['validators']:
        print(f"\n  {validator_stats['name']}:")
        print(f"    Passou: {validator_stats['valid_rows']} ({validator_stats['valid_percentage']:.1f}%)")
        print(f"    Falhou: {validator_stats['invalid_rows']}")

    # Separa válidos e inválidos
    gate_final = Gate(validators, keep_validation_columns=False)
    df_valido, df_invalido = gate_final.split(df)

    print("\n" + "="*80)
    print("REGISTROS VÁLIDOS (prontos para uso)")
    print("="*80)
    df_valido.show(truncate=False)

    print("\n" + "="*80)
    print("REGISTROS INVÁLIDOS (para análise/correção)")
    print("="*80)
    df_invalido.show(truncate=False)


if __name__ == "__main__":
    print("\n")
    print("╔" + "="*78 + "╗")
    print("║" + " "*25 + "EXEMPLOS DE USO DO GATE" + " "*30 + "║")
    print("╚" + "="*78 + "╝")

    # Executa todos os exemplos
    exemplo_basico()
    exemplo_split()
    exemplo_estatisticas()
    exemplo_custom_validator()
    exemplo_unique_validator()
    exemplo_completo()

    print("\n" + "="*80)
    print("Exemplos concluídos!")
    print("="*80 + "\n")
