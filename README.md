# Gate - Sistema de Validação para PySpark

Gate é um sistema flexível e extensível de validação de dados para PySpark, inspirado no padrão **Chain of Responsibility**. Diferentemente do padrão tradicional, o Gate requer que **todas** as validações sejam aprovadas para que uma linha de dados seja considerada válida.

## Características

- Validação declarativa e modular
- Múltiplos validadores pré-construídos para casos de uso comuns
- Suporte para validadores customizados
- Separação fácil entre dados válidos e inválidos
- Estatísticas detalhadas de validação
- Debug facilitado com colunas de validação intermediárias
- Otimizado para processamento distribuído com PySpark

## Instalação

```bash
pip install -r requirements.txt
```

## Uso Básico

```python
from pyspark.sql import SparkSession
from gate import Gate, NotNullValidator, RangeValidator, EmailValidator

# Criar SparkSession
spark = SparkSession.builder.appName("GateExample").getOrCreate()

# Criar DataFrame de exemplo
data = [
    ("João", 25, "joao@email.com"),
    ("Maria", 30, "maria@email.com"),
    (None, 22, "pedro@email.com"),      # Nome nulo - será rejeitado
    ("Ana", -5, "ana@email.com"),       # Idade negativa - será rejeitada
    ("Carlos", 150, "invalid_email"),   # Email inválido - será rejeitado
]

df = spark.createDataFrame(data, ["nome", "idade", "email"])

# Definir validadores
validators = [
    NotNullValidator(columns=["nome"]),
    RangeValidator(column="idade", min_value=0, max_value=120),
    EmailValidator(column="email"),
]

# Criar Gate
gate = Gate(validators)

# Filtrar apenas linhas válidas
df_valido = gate.filter_valid(df)

df_valido.show()
```

## Validadores Disponíveis

### NotNullValidator
Valida que colunas não são nulas.

```python
NotNullValidator(columns=["id", "nome"])
```

### NotEmptyValidator
Valida que strings não são vazias (após trim).

```python
NotEmptyValidator(columns=["nome", "descricao"])
```

### RangeValidator
Valida que valores numéricos estão dentro de um intervalo.

```python
RangeValidator(
    column="idade",
    min_value=0,
    max_value=120,
    inclusive=True  # Usa >= e <=
)
```

### RegexValidator
Valida que strings correspondem a um padrão regex.

```python
RegexValidator(
    column="codigo",
    pattern=r"^[A-Z]{3}\d{3}$"
)
```

### InListValidator
Valida que valores estão em uma lista permitida.

```python
InListValidator(
    column="estado",
    allowed_values=["SP", "RJ", "MG", "ES"]
)
```

### LengthValidator
Valida o comprimento de strings.

```python
LengthValidator(
    column="senha",
    min_length=8,
    max_length=128
)
```

### UniqueValidator
Valida que não há duplicatas (mantém apenas primeira ocorrência).

```python
UniqueValidator(columns=["cpf"])
```

### DateRangeValidator
Valida que datas estão dentro de um intervalo.

```python
DateRangeValidator(
    column="data_nascimento",
    min_date="1900-01-01",
    max_date="2024-12-31",
    date_format="yyyy-MM-dd"
)
```

### EmailValidator
Valida endereços de email.

```python
EmailValidator(column="email")
```

### CPFValidator
Valida formato de CPF (11 dígitos).

```python
CPFValidator(column="cpf")
```

### CNPJValidator
Valida formato de CNPJ (14 dígitos).

```python
CNPJValidator(column="cnpj")
```

### CustomValidator
Validador customizado com lógica própria.

```python
from pyspark.sql import functions as F

# Validador customizado: total (preço * quantidade) >= 500
def valida_total_minimo(df):
    return (F.col("preco") * F.col("quantidade")) >= 500

CustomValidator(valida_total_minimo, name="ValidaTotalMinimo")
```

## Métodos do Gate

### filter_valid()
Retorna apenas as linhas que passaram em todas as validações.

```python
df_valido = gate.filter_valid(df)
```

### filter_invalid()
Retorna apenas as linhas que falharam em alguma validação.

```python
df_invalido = gate.filter_invalid(df)
```

### split()
Retorna dois DataFrames: válidos e inválidos.

```python
df_valido, df_invalido = gate.split(df)
```

### process()
Processa o DataFrame e adiciona colunas de validação.

```python
df_processado = gate.process(df, final_column_name="_passou")
```

### get_validation_stats()
Retorna estatísticas detalhadas sobre as validações.

```python
gate = Gate(validators, keep_validation_columns=True)
stats = gate.get_validation_stats(df)

print(f"Total: {stats['total_rows']}")
print(f"Válidos: {stats['valid_rows']} ({stats['valid_percentage']:.1f}%)")
print(f"Inválidos: {stats['invalid_rows']} ({stats['invalid_percentage']:.1f}%)")

# Estatísticas por validador
for validator_stats in stats['validators']:
    print(f"{validator_stats['name']}: {validator_stats['valid_percentage']:.1f}% válidos")
```

## Exemplo Completo

```python
from pyspark.sql import SparkSession
from gate import (
    Gate,
    NotNullValidator,
    NotEmptyValidator,
    CPFValidator,
    EmailValidator,
    RangeValidator,
    InListValidator,
    UniqueValidator,
)

spark = SparkSession.builder.appName("GateCompleto").getOrCreate()

# Dados de clientes
data = [
    (1, "João Silva", "12345678901", "joao@email.com", 25, "SP"),
    (2, "Maria Santos", "98765432100", "maria@email.com", 30, "RJ"),
    (3, "", "11122233344", "cliente3@email.com", 28, "MG"),  # Nome vazio
    (4, "Pedro Oliveira", "123", "pedro@email.com", 35, "SP"), # CPF inválido
    (5, "Ana Costa", "55566677788", "email_invalido", 22, "ES"), # Email inválido
]

df = spark.createDataFrame(
    data,
    ["id", "nome", "cpf", "email", "idade", "estado"]
)

# Definir todas as validações
validators = [
    NotNullValidator(columns=["nome", "cpf", "email"]),
    NotEmptyValidator(columns=["nome"]),
    CPFValidator(column="cpf"),
    EmailValidator(column="email"),
    RangeValidator(column="idade", min_value=0, max_value=120),
    InListValidator(column="estado", allowed_values=["SP", "RJ", "MG", "ES"]),
    UniqueValidator(columns=["cpf"]),
]

# Criar Gate com debug habilitado
gate = Gate(validators, keep_validation_columns=True)

# Processar e obter estatísticas
stats = gate.get_validation_stats(df)

print(f"Total de registros: {stats['total_rows']}")
print(f"Registros válidos: {stats['valid_rows']} ({stats['valid_percentage']:.1f}%)")
print(f"Registros inválidos: {stats['invalid_rows']} ({stats['invalid_percentage']:.1f}%)")

print("\nDetalhamento por validador:")
for validator_stats in stats['validators']:
    print(f"  {validator_stats['name']}: {validator_stats['valid_percentage']:.1f}% válidos")

# Separar válidos e inválidos
gate_final = Gate(validators)
df_valido, df_invalido = gate_final.split(df)

print("\nRegistros válidos:")
df_valido.show()

print("\nRegistros inválidos:")
df_invalido.show()
```

## Criando Validadores Customizados

Você pode criar seus próprios validadores estendendo a classe `Validator`:

```python
from gate.core import Validator
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F

class MaiorIdadeValidator(Validator):
    """Valida que a pessoa é maior de idade"""

    def __init__(self, column: str, idade_minima: int = 18, name=None):
        super().__init__(name)
        self.column = column
        self.idade_minima = idade_minima

    def validate(self, df: DataFrame) -> Column:
        return F.col(self.column) >= self.idade_minima

# Usar o validador customizado
validators = [
    MaiorIdadeValidator(column="idade", idade_minima=21)
]
gate = Gate(validators)
```

## Estrutura do Projeto

```
bancadas/
├── gate/
│   ├── __init__.py          # Exports principais
│   ├── core.py              # Classes base (Gate, Validator)
│   ├── validators.py        # Validadores pré-construídos
│   └── exceptions.py        # Exceções customizadas
├── examples/
│   └── usage_example.py     # Exemplos de uso
├── tests/
│   ├── __init__.py
│   └── test_gate.py         # Testes unitários
├── requirements.txt         # Dependências
└── README.md               # Documentação
```

## Executando os Exemplos

```bash
cd examples
python usage_example.py
```

## Executando os Testes

```bash
pytest tests/test_gate.py -v
```

## Casos de Uso

### 1. Limpeza de Dados
Filtre dados inválidos antes de processar em pipelines ETL.

### 2. Validação de Entrada
Valide dados de entrada antes de gravar em tabelas finais.

### 3. Qualidade de Dados
Monitore a qualidade dos dados com estatísticas detalhadas.

### 4. Debugging
Use `keep_validation_columns=True` para identificar quais validações estão falhando.

### 5. Segregação de Dados
Separe dados válidos e inválidos para processamento diferenciado.

## Boas Práticas

1. **Nomeie seus validadores**: Use o parâmetro `name` para facilitar o debug
   ```python
   NotNullValidator(columns=["id"], name="ValidaIDNaoNulo")
   ```

2. **Use keep_validation_columns para debug**: Ative durante desenvolvimento
   ```python
   gate = Gate(validators, keep_validation_columns=True)
   ```

3. **Ordem importa**: Coloque validações mais baratas primeiro
   ```python
   validators = [
       NotNullValidator(...),      # Rápido
       RangeValidator(...),        # Rápido
       UniqueValidator(...),       # Mais custoso
   ]
   ```

4. **Reutilize validadores**: Crie validadores reutilizáveis para regras de negócio comuns

5. **Monitore estatísticas**: Use `get_validation_stats()` para monitorar qualidade dos dados

## Contribuindo

Contribuições são bem-vindas! Para adicionar novos validadores:

1. Crie uma classe que herda de `Validator`
2. Implemente o método `validate()` que retorna uma `Column` booleana
3. Adicione testes em `tests/test_gate.py`
4. Atualize a documentação

## Licença

MIT License

## Autores

Projeto criado para facilitar validações em pipelines PySpark no Databricks.

---

Para mais exemplos e uso avançado, consulte o arquivo `examples/usage_example.py`.
