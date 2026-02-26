# Pipeline Modular — Guia de Uso e Refatoração

## Visão Geral

Este documento descreve a arquitetura modular do pipeline do Data Lake ENEM,
como executar cada etapa de forma individual ou em conjunto, e como adicionar
novas fontes de dados.

---

## Arquitetura do Pipeline

O Data Lake segue a **arquitetura Medalhão** (Bronze → Prata → Ouro).
Cada camada tem um script independente por fonte de dados.

```
projeto_enem_datalake/
├── run.sh                        # Orquestrador principal (ponto de entrada)
├── docker-compose.yml            # Dois serviços: spark_enem + jupyter_enem
└── src/
    ├── spark_utils.py            # SparkSession centralizada (compartilhada)
    └── sources/
        └── enem/
            ├── 01_bronze.py     # Etapa 1: RAW → Bronze
            ├── 02_prata.py      # Etapa 2: Bronze → Prata
            └── 03_ouro.py       # Etapa 3: Prata → Ouro
```

### Responsabilidade de cada etapa

| Script | Camada | Entrada | Saída | O que faz |
|---|---|---|---|---|
| `01_bronze.py` | Bronze | `/app/dados/microdados_enem_<ano>/` | `/app/data_lake/bronze/enem/ano=<ANO>/` | Converte CSV latin1 para Parquet comprimido (Snappy), particionado por ano |
| `02_prata.py` | Prata | `/app/data_lake/bronze/enem/` | `/app/data_lake/prata/enem/ano=<ANO>/` | Seleciona colunas relevantes, converte tipos e preenche notas nulas com 0.0 |
| `03_ouro.py` | Ouro | `/app/data_lake/prata/enem/` | `/app/data_lake/ouro/enem/<tabela>/` | Gera três tabelas analíticas agregadas (ver detalhes abaixo) |

### Tabelas geradas na Camada Ouro

| Tabela | Descrição |
|---|---|
| `media_notas_por_uf_ano` | Média das 5 notas (CN, CH, LC, MT, Redação) por UF e por Ano |
| `distribuicao_perfil` | Contagem de candidatos por Faixa Etária, Sexo e Raça/Cor |
| `ranking_redacao_por_uf` | Média de redação e total de participantes por UF, ordenado do maior para o menor |

---

## Pré-requisitos

Os containers precisam estar rodando:

```bash
# Subir todos os serviços em background
docker-compose up -d

# Verificar se estão ativos
docker ps
```

Saída esperada:
```
NAMES          STATUS
jupyter_enem   Up ...   0.0.0.0:8888->8888/tcp
spark_enem     Up ...
```

> **Nota:** O container `spark_enem` agora fica **sempre ativo** (não encerra após a pipeline).
> A pipeline é acionada manualmente via `docker exec`.

---

## Como Executar

### Executar a pipeline completa (Bronze → Prata → Ouro)

```bash
docker exec spark_enem bash /app/run.sh --fonte enem --etapa all
```

### Executar apenas uma etapa específica

```bash
# Apenas Bronze — use quando chegar novos dados RAW
docker exec spark_enem bash /app/run.sh --fonte enem --etapa bronze

# Apenas Prata — use quando precisar corrigir/ajustar transformações
docker exec spark_enem bash /app/run.sh --fonte enem --etapa prata

# Apenas Ouro — use quando quiser novas agregações ou corrigir métricas
docker exec spark_enem bash /app/run.sh --fonte enem --etapa ouro
```

### Quando usar cada etapa individualmente?

| Situação | Etapa a rodar |
|---|---|
| Chegaram novos dados anuais (ex: ENEM 2025) | `bronze` |
| Precisou corrigir uma transformação na Prata | `prata` |
| Precisou adicionar uma nova agregação na Ouro | `ouro` |
| Primeiro setup / setup completo do zero | `all` |

---

## Adicionando uma Nova Fonte de Dados

Para integrar uma nova fonte (ex: Censo Escolar), siga os passos:

### 1. Criar a estrutura de scripts

```bash
mkdir -p src/sources/censo_escolar
```

Crie os três scripts seguindo o mesmo padrão do ENEM:

```
src/sources/censo_escolar/
├── 01_bronze.py     # Sua lógica de ingestão
├── 02_prata.py      # Sua lógica de limpeza
└── 03_ouro.py       # Suas agregações
```

> **Dica:** Copie os scripts do ENEM como ponto de partida e adapte os caminhos e transformações.

### 2. Usar o spark_utils para criar a SparkSession

Em cada script novo, importe o utilitário compartilhado:

```python
import sys
sys.path.insert(0, "/app/src")
from spark_utils import get_spark_session

spark = get_spark_session("NomeDaAplicacao")
```

### 3. Executar a pipeline da nova fonte

```bash
# Pipeline completa da nova fonte
docker exec spark_enem bash /app/run.sh --fonte censo_escolar --etapa all

# Ou etapa individual
docker exec spark_enem bash /app/run.sh --fonte censo_escolar --etapa bronze
```

O `run.sh` detecta automaticamente a pasta em `src/sources/<fonte>/` e executa
os scripts `01_bronze.py`, `02_prata.py` e `03_ouro.py` correspondentes.

---

## Refatoração Realizada (Histórico)

### Problema anterior

O `docker-compose.yml` original executava o `run_pipeline.sh` ao subir o container,
rodando sempre as 3 etapas sequencialmente. Após terminar, o container **morria**.
Não era possível rodar uma etapa isolada sem reiniciar tudo.

### O que foi alterado

| Arquivo | Mudança |
|---|---|
| `docker-compose.yml` | Serviço `spark` usa `tail -f /dev/null` para ficar sempre ativo |
| `run.sh` (novo) | Orquestrador modular com `--fonte` e `--etapa`. Substituiu `run_pipeline.sh` |
| `src/spark_utils.py` (novo) | Fábrica de SparkSession com configurações padrão otimizadas |
| `src/sources/enem/01_bronze.py` (novo) | Ingestão Bronze refatorada — evita reprocessar partições existentes |
| `src/sources/enem/02_prata.py` (novo) | Tratamento Prata refatorado com conversão de tipos explícita |
| `src/sources/enem/03_ouro.py` (novo) | Camada Ouro com 3 tabelas analíticas (antes inexistente) |

> Os scripts originais em `src/` (raiz) foram mantidos para preservar o histórico do Git.
