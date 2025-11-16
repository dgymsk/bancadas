# Guia Rápido de Início - Gate

Este guia vai te ajudar a começar a usar o Gate em poucos minutos.

## Instalação

```bash
# Clone o repositório
git clone <repo-url>
cd bancadas

# Instale as dependências
pip install -r requirements.txt

# Ou instale o pacote
pip install -e .
```

## Exemplo Básico em 5 Minutos

### 1. Importe as bibliotecas

```python
from pyspark.sql import SparkSession
from gate import Gate, NotNullValidator, RangeValidator
```

### 2. Crie uma SparkSession

```python
spark = SparkSession.builder.appName("MeuApp").getOrCreate()
```

### 3. Crie seu DataFrame

```python
data = [
    ("João", 25),
    ("Maria", 30),
    (None, 22),     # Nome nulo - será rejeitado
    ("Ana", -5),    # Idade negativa - será rejeitada
]

df = spark.createDataFrame(data, ["nome", "idade"])
```

### 4. Defina suas validações

```python
validators = [
    NotNullValidator(columns=["nome"]),
    RangeValidator(column="idade", min_value=0, max_value=120),
]
```

### 5. Crie o Gate e filtre os dados

```python
gate = Gate(validators)
df_valido = gate.filter_valid(df)

# Pronto! df_valido contém apenas dados válidos
df_valido.show()
```

## Validadores Mais Usados

```python
from gate import (
    NotNullValidator,      # Não permite valores nulos
    NotEmptyValidator,     # Não permite strings vazias
    RangeValidator,        # Valida intervalo numérico
    EmailValidator,        # Valida formato de email
    CPFValidator,          # Valida formato de CPF
    InListValidator,       # Valida valores em lista
)

# Exemplo com múltiplas validações
validators = [
    NotNullValidator(columns=["id", "email"]),
    EmailValidator(column="email"),
    RangeValidator(column="idade", min_value=18, max_value=100),
    InListValidator(column="status", allowed_values=["ativo", "inativo"]),
]

gate = Gate(validators)
```

## Separando Válidos e Inválidos

```python
# Retorna dois DataFrames separados
df_valido, df_invalido = gate.split(df)

# Processa apenas os válidos
df_valido.write.parquet("dados_validos.parquet")

# Analisa os inválidos
df_invalido.show()
```

## Debug - Vendo quais validações falharam

```python
# Ativa o modo debug
gate = Gate(validators, keep_validation_columns=True)

# Processa o DataFrame
df_processado = gate.process(df)

# Agora você pode ver quais validações passaram/falharam
df_processado.show()
```

## Estatísticas de Validação

```python
gate = Gate(validators, keep_validation_columns=True)
stats = gate.get_validation_stats(df)

print(f"Total: {stats['total_rows']}")
print(f"Válidos: {stats['valid_rows']} ({stats['valid_percentage']:.1f}%)")

# Veja estatísticas por validador
for v in stats['validators']:
    print(f"{v['name']}: {v['valid_percentage']:.1f}% válidos")
```

## Criando Validador Customizado

```python
from pyspark.sql import functions as F
from gate import CustomValidator

# Validação customizada: nome deve ter pelo menos 2 palavras
def valida_nome_completo(df):
    return F.size(F.split(F.col("nome"), " ")) >= 2

validator = CustomValidator(valida_nome_completo, name="NomeCompleto")
```

## Próximos Passos

1. Leia o [README.md](README.md) completo para documentação detalhada
2. Explore os [exemplos](examples/usage_example.py) para casos de uso avançados
3. Execute os testes para entender melhor o comportamento: `pytest tests/`

## Dicas

1. Sempre dê nomes descritivos aos validadores:
   ```python
   NotNullValidator(columns=["id"], name="ValidaIDObrigatorio")
   ```

2. Use `keep_validation_columns=True` durante desenvolvimento para debug

3. Ordene validações da mais rápida para a mais lenta

4. Use `split()` quando precisar processar válidos e inválidos diferentemente

5. Monitore `get_validation_stats()` em produção para qualidade dos dados

## Dúvidas?

Consulte o [README.md](README.md) ou execute os exemplos:
```bash
python examples/usage_example.py
```
