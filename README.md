# Gate - Sistema de Validação para PySpark com Histórico

Gate é um sistema de validação para PySpark que mantém um **histórico completo** das validações que falharam. Inspirado no padrão Chain of Responsibility, mas onde **todas** as validações devem passar para que uma linha seja válida.

## 🎯 Ideia Central

A principal funcionalidade do Gate é manter um **vetor (array)** com os nomes dos validadores que falharam para cada linha. Isso fornece:

- **Rastreabilidade completa**: você sabe exatamente qual validação falhou
- **Debug facilitado**: identifica padrões de falhas
- **Auditoria**: mantém registro de problemas de qualidade
- **Flexibilidade**: pode decidir o que fazer com cada tipo de falha

## 📦 Instalação

```bash
pip install -r requirements.txt
```

## 🚀 Uso Básico

```python
from pyspark.sql import SparkSession
from gate import Gate, NotNullValidator, RangeValidator

# Criar SparkSession
spark = SparkSession.builder.appName("GateExample").getOrCreate()

# Dados de exemplo
data = [
    (1, "João", 25),      # Válido
    (2, None, 30),        # Nome nulo - FALHA
    (3, "Ana", -5),       # Idade negativa - FALHA
    (4, "", 150),         # Nome vazio E idade alta - FALHA em 2 validações
]

df = spark.createDataFrame(data, ["id", "nome", "idade"])

# Define validadores
validators = [
    NotNullValidator(columns=["nome"], name="NomeNaoNulo"),
    RangeValidator(column="idade", min_value=0, max_value=120, name="IdadeValida"),
]

# Cria o Gate
gate = Gate(validators)

# Processa - adiciona coluna _gate_failures
df_processado = gate.process(df)
df_processado.show(truncate=False)
```

**Saída:**
```
+---+-----+-----+---------------------------+
|id |nome |idade|_gate_failures             |
+---+-----+-----+---------------------------+
|1  |João |25   |[]                         |  ← Array vazio = VÁLIDO
|2  |null |30   |[NomeNaoNulo]              |  ← Falhou em 1 validação
|3  |Ana  |-5   |[IdadeValida]              |  ← Falhou em 1 validação
|4  |     |150  |[NomeNaoNulo, IdadeValida] |  ← Falhou em 2 validações
+---+-----+-----+---------------------------+
```

## 🔧 Métodos Principais

### `process(df, history_column="_gate_failures")`
Processa o DataFrame e adiciona coluna array com histórico de falhas.

```python
df_processado = gate.process(df)
# Retorna DataFrame com coluna _gate_failures
# Array vazio [] = linha válida
# Array com nomes = linha inválida (contém quais validações falharam)
```

### `filter_valid(df)`
Retorna apenas linhas que passaram em TODAS as validações.

```python
df_valido = gate.filter_valid(df)
# Retorna apenas linhas onde _gate_failures está vazio
# Remove a coluna de histórico
```

### `filter_invalid(df)`
Retorna apenas linhas que falharam em alguma validação, **mantendo o histórico**.

```python
df_invalido = gate.filter_invalid(df)
# Retorna apenas linhas onde _gate_failures NÃO está vazio
# MANTÉM a coluna de histórico para análise
```

### `split(df)`
Retorna dois DataFrames: válidos e inválidos.

```python
df_valido, df_invalido = gate.split(df)
# df_valido: sem coluna de histórico
# df_invalido: COM coluna de histórico
```

## 📚 Validadores Disponíveis

### NotNullValidator
Valida que colunas não são nulas.

```python
NotNullValidator(columns=["id", "nome"], name="CamposObrigatorios")
```

### NotEmptyValidator
Valida que strings não são vazias (após trim).

```python
NotEmptyValidator(columns=["nome"], name="NomePreenchido")
```

### RangeValidator
Valida intervalos numéricos.

```python
RangeValidator(
    column="idade",
    min_value=0,
    max_value=120,
    inclusive=True,  # Usa >= e <=
    name="IdadeValida"
)
```

### RegexValidator
Valida padrões regex.

```python
RegexValidator(
    column="codigo",
    pattern=r"^[A-Z]{3}\d{3}$",
    name="CodigoValido"
)
```

### InListValidator
Valida valores em uma lista permitida.

```python
InListValidator(
    column="estado",
    allowed_values=["SP", "RJ", "MG"],
    name="EstadoValido"
)
```

## 💡 Exemplo Completo com Análise de Histórico

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from gate import Gate, NotNullValidator, RangeValidator, NotEmptyValidator

spark = SparkSession.builder.appName("Gate").getOrCreate()

# Dados com vários problemas
data = [
    (1, "João Silva", 25),
    (2, "Maria Santos", 30),
    (3, None, 22),
    (4, "Ana Costa", -5),
    (5, "", 150),
    (6, "Carlos", 200),
]

df = spark.createDataFrame(data, ["id", "nome", "idade"])

# Validadores
validators = [
    NotNullValidator(columns=["nome"], name="NomeNaoNulo"),
    NotEmptyValidator(columns=["nome"], name="NomeNaoVazio"),
    RangeValidator(column="idade", min_value=0, max_value=120, name="IdadeValida"),
]

gate = Gate(validators)

# Separa válidos e inválidos
df_valido, df_invalido = gate.split(df)

print(f"Total: {df.count()}")
print(f"Válidos: {df_valido.count()}")
print(f"Inválidos: {df_invalido.count()}")

# Analisa as falhas
print("\nLinhas inválidas com histórico:")
df_invalido.show(truncate=False)

# Conta falhas por tipo de validação
print("\nDistribuição de falhas por validador:")
df_invalido.select(
    explode(col("_gate_failures")).alias("validador")
).groupBy("validador").count().show()
```

## 🎨 Criando Validadores Customizados

```python
from gate.core import Validator
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, lit

class MaiorIdadeValidator(Validator):
    """Valida que a pessoa é maior de idade"""

    def __init__(self, column: str, idade_minima: int = 18, name=None):
        super().__init__(name)
        self.column = column
        self.idade_minima = idade_minima

    def validate(self, df: DataFrame) -> Column:
        return col(self.column) >= lit(self.idade_minima)

# Usar
validators = [
    MaiorIdadeValidator(column="idade", idade_minima=21, name="MaiorDe21")
]
gate = Gate(validators)
```

## 🔍 Casos de Uso

### 1. Limpeza de Dados ETL
```python
# Separa dados válidos para processamento e inválidos para análise
df_valido, df_invalido = gate.split(df_raw)

# Processa apenas dados válidos
df_valido.write.parquet("dados_limpos.parquet")

# Salva inválidos com histórico para correção
df_invalido.write.parquet("dados_rejeitados.parquet")
```

### 2. Monitoramento de Qualidade
```python
# Processa dados mantendo histórico
df_processado = gate.process(df)

# Analisa quais validações mais falham
from pyspark.sql.functions import explode, col

df_processado.filter(col("_gate_failures").isNotNull()) \
    .select(explode(col("_gate_failures")).alias("falha")) \
    .groupBy("falha").count() \
    .orderBy("count", ascending=False) \
    .show()
```

### 3. Debug e Auditoria
```python
# Identifica registros com múltiplas falhas
from pyspark.sql.functions import size

df_invalido = gate.filter_invalid(df)
df_problematicos = df_invalido.filter(size(col("_gate_failures")) > 1)

print("Registros com múltiplas validações falhadas:")
df_problematicos.show(truncate=False)
```

## 📖 Estrutura do Projeto

```
bancadas/
├── gate/
│   ├── __init__.py          # Exports principais
│   ├── core.py              # Classes Gate e Validator
│   ├── validators.py        # Validadores pré-construídos
│   └── exceptions.py        # Exceções customizadas
├── examples/
│   ├── simple_example.py    # Exemplo simples focado na ideia central
│   └── usage_example.py     # Exemplos variados
├── tests/
│   └── test_gate.py         # Testes unitários
├── README.md
└── requirements.txt
```

## 🧪 Executando os Exemplos

```bash
cd examples
python simple_example.py  # Exemplo simples e direto
python usage_example.py   # Exemplos variados
```

## ✅ Boas Práticas

1. **Nomeie seus validadores** - Facilita identificação no histórico
   ```python
   NotNullValidator(columns=["id"], name="IDObrigatorio")
   ```

2. **Use o histórico** - Analise padrões de falhas
   ```python
   df_invalido = gate.filter_invalid(df)
   # Analise _gate_failures para entender problemas
   ```

3. **Separe válidos e inválidos** - Processe diferentemente
   ```python
   df_valido, df_invalido = gate.split(df)
   ```

4. **Validações mais baratas primeiro** - Otimiza performance
   ```python
   validators = [
       NotNullValidator(...),   # Rápido
       RangeValidator(...),     # Rápido
       RegexValidator(...),     # Mais custoso
   ]
   ```

## 📝 Licença

MIT License

## 🤝 Contribuindo

Contribuições são bem-vindas! Para adicionar novos validadores:

1. Crie uma classe que herda de `Validator`
2. Implemente o método `validate()` que retorna uma `Column` booleana
3. Use `col()`, `lit()` e outras funções diretamente (sem alias como `F.`)
4. Adicione testes

---

**Para mais exemplos**, consulte `examples/simple_example.py` que demonstra a ideia central do histórico de validações.
