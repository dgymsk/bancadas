"""
Exemplo Simples do Gate - Demonstra a Ideia Central

Este exemplo mostra a funcionalidade principal do Gate:
manter um histórico (vetor) das validações que falharam para cada linha.
"""

from pyspark.sql import SparkSession
import sys
sys.path.append('..')

from gate import Gate, NotNullValidator, RangeValidator, NotEmptyValidator


def main():
    # Cria SparkSession
    spark = SparkSession.builder \
        .appName("GateSimpleExample") \
        .master("local[2]") \
        .getOrCreate()

    print("\n" + "="*80)
    print("EXEMPLO SIMPLES - GATE COM HISTÓRICO DE VALIDAÇÕES")
    print("="*80)

    # Cria DataFrame de exemplo
    data = [
        (1, "João Silva", 25),      # Válido
        (2, "Maria Santos", 30),    # Válido
        (3, None, 22),              # Nome nulo - FALHA
        (4, "Ana Costa", -5),       # Idade negativa - FALHA
        (5, "", 150),               # Nome vazio E idade alta - FALHA em 2 validações
        (6, "Carlos", 200),         # Idade muito alta - FALHA
    ]

    df = spark.createDataFrame(data, ["id", "nome", "idade"])

    print("\n📊 DataFrame Original:")
    df.show()

    # Define os validadores
    validators = [
        NotNullValidator(columns=["nome"], name="NomeNaoNulo"),
        NotEmptyValidator(columns=["nome"], name="NomeNaoVazio"),
        RangeValidator(column="idade", min_value=0, max_value=120, name="IdadeValida"),
    ]

    # Cria o Gate
    gate = Gate(validators)

    print("\n" + "="*80)
    print("PROCESSANDO COM GATE - Adicionando coluna de histórico")
    print("="*80)

    # Processa o DataFrame - adiciona coluna _gate_failures
    df_processado = gate.process(df)

    print("\n📋 DataFrame Processado (com coluna _gate_failures):")
    print("   Observe a coluna _gate_failures: array com os nomes das validações que falharam")
    df_processado.show(truncate=False)

    print("\n" + "="*80)
    print("SEPARANDO VÁLIDOS E INVÁLIDOS")
    print("="*80)

    # Filtra apenas válidos (array vazio)
    df_valido = gate.filter_valid(df)

    print("\n✅ Linhas VÁLIDAS (passaram em todas as validações):")
    df_valido.show()

    # Filtra apenas inválidos (array não vazio, contém histórico)
    df_invalido = gate.filter_invalid(df)

    print("\n❌ Linhas INVÁLIDAS (com histórico de falhas):")
    print("   A coluna _gate_failures mostra exatamente quais validações falharam:")
    df_invalido.show(truncate=False)

    print("\n" + "="*80)
    print("ANÁLISE DO HISTÓRICO")
    print("="*80)

    print("\n🔍 Análise detalhada das falhas:")

    # Mostra quantas linhas falharam em cada validação
    from pyspark.sql.functions import explode, col

    print("\n   Linhas que falharam e suas respectivas validações:")
    df_invalido.select("id", "nome", "idade", "_gate_failures").show(truncate=False)

    # Conta falhas por tipo de validação
    print("\n   Distribuição de falhas por validador:")
    df_failedures_exploded = df_invalido.select(
        explode(col("_gate_failures")).alias("validacao_falhada")
    )
    df_failedures_exploded.groupBy("validacao_falhada").count().show()

    print("\n" + "="*80)
    print("USANDO SPLIT - Obtendo ambos os DataFrames de uma vez")
    print("="*80)

    df_valido, df_invalido = gate.split(df)

    print(f"\n📊 Total de linhas: {df.count()}")
    print(f"✅ Linhas válidas: {df_valido.count()}")
    print(f"❌ Linhas inválidas: {df_invalido.count()}")

    print("\n" + "="*80)
    print("RESUMO DO CONCEITO")
    print("="*80)
    print("""
O Gate mantém um HISTÓRICO completo das validações:

1. Cada linha recebe uma coluna array chamada _gate_failures
2. Se a linha passar em todas as validações: array vazio []
3. Se a linha falhar: array contém os nomes dos validadores que falharam

VANTAGENS:
- Rastreabilidade completa: você sabe EXATAMENTE qual validação falhou
- Debug facilitado: identifica padrões de falhas
- Auditoria: mantém registro de problemas de qualidade de dados
- Flexibilidade: pode decidir o que fazer com cada tipo de falha
    """)

    spark.stop()


if __name__ == "__main__":
    main()
