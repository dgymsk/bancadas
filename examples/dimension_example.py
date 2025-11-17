"""
Exemplo de Validação de Dimensões

Este exemplo demonstra como usar validadores de dimensão para verificar
se valores existem (ou não existem) em tabelas de referência.

Casos de uso:
1. Validar que CPF existe em dimensão de clientes cadastrados
2. Validar que código NÃO existe (validação reversa) - evitar duplicatas
3. Combinar validações de dimensão com outras validações
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
import sys
sys.path.append('..')

from gate import (
    Gate,
    NotNullValidator,
    RangeValidator,
    DataFrameDimensionValidator,
    ListDimensionValidator
)


def exemplo_cpf_em_dimensao():
    """Exemplo: Validar que CPF existe em dimensão de clientes cadastrados"""
    print("\n" + "="*80)
    print("EXEMPLO 1: Validar CPF existe em Dimensão de Clientes")
    print("="*80)

    spark = SparkSession.builder \
        .appName("DimensionExample1") \
        .master("local[2]") \
        .getOrCreate()

    # Dimensão: Clientes cadastrados
    dim_clientes_data = [
        ("12345678901", "João Silva"),
        ("98765432100", "Maria Santos"),
        ("11122233344", "Ana Costa"),
        ("55566677788", "Carlos Oliveira"),
    ]
    dim_clientes = spark.createDataFrame(dim_clientes_data, ["cpf", "nome"])

    print("\n📊 Dimensão: Clientes Cadastrados")
    dim_clientes.show()

    # Dados de transações (algumas com CPFs não cadastrados)
    transacoes_data = [
        (1, "12345678901", 100.0),  # CPF válido
        (2, "98765432100", 250.0),  # CPF válido
        (3, "99999999999", 150.0),  # CPF NÃO EXISTE na dimensão - FALHA
        (4, "11122233344", 300.0),  # CPF válido
        (5, "00000000000", 50.0),   # CPF NÃO EXISTE na dimensão - FALHA
    ]
    df_transacoes = spark.createDataFrame(transacoes_data, ["id", "cpf", "valor"])

    print("\n📊 Dados: Transações")
    df_transacoes.show()

    # Validador: CPF deve existir na dimensão de clientes
    validators = [
        DataFrameDimensionValidator(
            column="cpf",
            dimension_df=dim_clientes,
            dimension_column="cpf",
            exists=True,  # Deve existir
            name="CPFCadastrado"
        ),
    ]

    gate = Gate(validators)

    # Processa
    df_processado = gate.process(df_transacoes)

    print("\n📋 Resultado com Histórico:")
    df_processado.show(truncate=False)

    # Separa válidos e inválidos
    df_valido, df_invalido = gate.split(df_transacoes)

    print(f"\n✅ Transações VÁLIDAS (CPF cadastrado): {df_valido.count()}")
    df_valido.show()

    print(f"\n❌ Transações INVÁLIDAS (CPF não cadastrado): {df_invalido.count()}")
    df_invalido.show(truncate=False)


def exemplo_validacao_reversa():
    """Exemplo: Validar que código NÃO existe (anti-duplicação)"""
    print("\n" + "="*80)
    print("EXEMPLO 2: Validação Reversa - Garantir que Código NÃO existe")
    print("="*80)

    spark = SparkSession.builder \
        .appName("DimensionExample2") \
        .master("local[2]") \
        .getOrCreate()

    # Dimensão: Produtos já cadastrados
    dim_produtos_data = [
        ("PROD001", "Notebook"),
        ("PROD002", "Mouse"),
        ("PROD003", "Teclado"),
    ]
    dim_produtos = spark.createDataFrame(dim_produtos_data, ["codigo", "nome"])

    print("\n📊 Dimensão: Produtos Já Cadastrados")
    dim_produtos.show()

    # Novos produtos a serem cadastrados
    novos_produtos_data = [
        ("PROD004", "Monitor", 1200.0),      # Código novo - OK
        ("PROD005", "Webcam", 300.0),        # Código novo - OK
        ("PROD002", "Mouse Gamer", 150.0),   # Código JÁ EXISTE - FALHA
        ("PROD006", "Headset", 400.0),       # Código novo - OK
        ("PROD001", "Notebook Pro", 5000.0), # Código JÁ EXISTE - FALHA
    ]
    df_novos = spark.createDataFrame(novos_produtos_data, ["codigo", "nome", "preco"])

    print("\n📊 Dados: Novos Produtos a Cadastrar")
    df_novos.show()

    # Validador: Código NÃO deve existir na dimensão (validação reversa)
    validators = [
        DataFrameDimensionValidator(
            column="codigo",
            dimension_df=dim_produtos,
            dimension_column="codigo",
            exists=False,  # NÃO deve existir (validação reversa)
            name="CodigoNovo"
        ),
    ]

    gate = Gate(validators)

    # Processa
    df_processado = gate.process(df_novos)

    print("\n📋 Resultado com Histórico:")
    print("   Códigos que JÁ EXISTEM falharão na validação 'CodigoNovo'")
    df_processado.show(truncate=False)

    # Separa
    df_valido, df_invalido = gate.split(df_novos)

    print(f"\n✅ Produtos com código NOVO (podem ser cadastrados): {df_valido.count()}")
    df_valido.show()

    print(f"\n❌ Produtos com código DUPLICADO (não podem ser cadastrados): {df_invalido.count()}")
    df_invalido.show(truncate=False)


def exemplo_combinado():
    """Exemplo: Combina validações de dimensão com outras validações"""
    print("\n" + "="*80)
    print("EXEMPLO 3: Validações Combinadas - Dimensão + Outras Regras")
    print("="*80)

    spark = SparkSession.builder \
        .appName("DimensionExample3") \
        .master("local[2]") \
        .getOrCreate()

    # Dimensão: Estados válidos
    dim_estados_data = [
        ("SP",), ("RJ",), ("MG",), ("ES",), ("PR",), ("SC",), ("RS",)
    ]
    dim_estados = spark.createDataFrame(dim_estados_data, ["uf"])

    # Dimensão: Clientes ativos
    dim_clientes_ativos_data = [
        ("12345678901",),
        ("98765432100",),
        ("11122233344",),
    ]
    dim_clientes_ativos = spark.createDataFrame(dim_clientes_ativos_data, ["cpf"])

    print("\n📊 Dimensão: Estados Válidos")
    dim_estados.show()

    print("\n📊 Dimensão: Clientes Ativos")
    dim_clientes_ativos.show()

    # Dados de pedidos
    pedidos_data = [
        (1, "12345678901", "SP", 100.0),   # Tudo OK
        (2, None, "RJ", 200.0),            # CPF nulo - FALHA (NotNull)
        (3, "99999999999", "MG", 150.0),   # CPF não ativo - FALHA (Dimensão)
        (4, "98765432100", "XY", 300.0),   # Estado inválido - FALHA (Dimensão)
        (5, "11122233344", "SP", -50.0),   # Valor negativo - FALHA (Range)
        (6, "98765432100", "RJ", 250.0),   # Tudo OK
        (7, "99999999999", "ZZ", -10.0),   # FALHA em 3 validações!
    ]
    df_pedidos = spark.createDataFrame(pedidos_data, ["id", "cpf", "estado", "valor"])

    print("\n📊 Dados: Pedidos")
    df_pedidos.show()

    # Múltiplas validações
    validators = [
        NotNullValidator(columns=["cpf"], name="CPFObrigatorio"),
        DataFrameDimensionValidator(
            column="cpf",
            dimension_df=dim_clientes_ativos,
            exists=True,
            name="ClienteAtivo"
        ),
        DataFrameDimensionValidator(
            column="estado",
            dimension_df=dim_estados,
            dimension_column="uf",
            exists=True,
            name="EstadoValido"
        ),
        RangeValidator(column="valor", min_value=0, name="ValorPositivo"),
    ]

    gate = Gate(validators)

    # Processa
    df_processado = gate.process(df_pedidos)

    print("\n📋 Resultado com Histórico Detalhado:")
    df_processado.show(truncate=False)

    # Análise de falhas
    print("\n🔍 Análise de Falhas por Tipo:")
    df_invalido = gate.filter_invalid(df_pedidos)

    df_invalido.select(
        explode(col("_gate_failures")).alias("validacao_falhada")
    ).groupBy("validacao_falhada").count().show()

    # Mostra pedidos com múltiplas falhas
    from pyspark.sql.functions import size
    print("\n⚠️  Pedidos com MÚLTIPLAS falhas:")
    df_invalido.filter(size(col("_gate_failures")) > 1).show(truncate=False)


def exemplo_list_dimension():
    """Exemplo: Validação com ListDimensionValidator"""
    print("\n" + "="*80)
    print("EXEMPLO 4: ListDimensionValidator - Lista Pré-definida")
    print("="*80)

    spark = SparkSession.builder \
        .appName("DimensionExample4") \
        .master("local[2]") \
        .getOrCreate()

    # Dados
    usuarios_data = [
        (1, "admin", "ativo"),
        (2, "user", "ativo"),
        (3, "guest", "inativo"),  # Role não permitido - FALHA
        (4, "moderator", "ativo"),
        (5, "superuser", "ativo"), # Role não permitido - FALHA
    ]
    df_usuarios = spark.createDataFrame(usuarios_data, ["id", "role", "status"])

    print("\n📊 Dados: Usuários")
    df_usuarios.show()

    # Validador com lista de roles permitidos
    validators = [
        ListDimensionValidator(
            column="role",
            valid_values=["admin", "user", "moderator"],
            exists=True,
            name="RolePermitido"
        ),
    ]

    gate = Gate(validators)

    df_processado = gate.process(df_usuarios)

    print("\n📋 Resultado:")
    df_processado.show(truncate=False)


def main():
    print("\n")
    print("╔" + "="*78 + "╗")
    print("║" + " "*20 + "EXEMPLOS DE VALIDAÇÃO DE DIMENSÕES" + " "*24 + "║")
    print("╚" + "="*78 + "╝")

    exemplo_cpf_em_dimensao()
    exemplo_validacao_reversa()
    exemplo_combinado()
    exemplo_list_dimension()

    print("\n" + "="*80)
    print("RESUMO - Validadores de Dimensão")
    print("="*80)
    print("""
DataFrameDimensionValidator:
- Valida se valores EXISTEM em outra tabela/DataFrame
- Parâmetro exists=True: valor DEVE existir (padrão)
- Parâmetro exists=False: valor NÃO DEVE existir (validação reversa)
- Usa cache interno para performance
- Ideal para dimensões pequenas/médias

ListDimensionValidator:
- Valida contra lista pré-definida
- Mais simples que DataFrameDimensionValidator
- Útil quando você já tem a lista de valores

Arquitetura Modular:
- DimensionValidator é a classe base
- Fácil criar novos validadores (API, tabela externa, etc.)
- Todos usam o histórico de falhas do Gate
    """)

    print("\n" + "="*80)
    print("Exemplos concluídos!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
