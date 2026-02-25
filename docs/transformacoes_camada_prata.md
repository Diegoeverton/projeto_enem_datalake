# Documentação Técnica: Transformações na Camada Prata 🥈

Esta etapa do pipeline (orquestrada pelo script `src/processamento_prata_spark.py`) é o coração da higienização dos dados do ENEM. Aqui, transformamos os dados "brutos" da Camada Bronze em um conjunto de dados refinado, focado e preparado para análise de negócio e treinamento de modelos de Machine Learning.

Abaixo detalhamos todas as transformações de engenharia e regras de negócio aplicadas no processo.

## 1. Otimização de Armazenamento (Seleção Direcionada de Colunas)
O dataset original do INEP/ENEM possui dezenas de colunas, contendo perguntas demográficas irrelevantes para o nosso foco e metadados de logística. Na Camada Prata, nós aplicamos um **filtro vertical (Select)** para manter a tabela apenas com atributos analíticos de alto valor.

As colunas mantidas e transportadas para a camada de ouro/análises são:
- **Identificação:** `NU_INSCRICAO`
- **Demografia:** `TP_FAIXA_ETARIA`, `TP_SEXO`, `TP_COR_RACA`, `TP_ESTADO_CIVIL`
- **Cenário Escolar:** `TP_ESCOLA` (Pública, Privada ou Exterior), `IN_TREINEIRO`
- **Geografia:** `SG_UF_PROVA` (Estado de realização)
- **Desempenho (Notas):** `NU_NOTA_CN` (Ciências da Natureza), `NU_NOTA_CH` (Ciências Humanas), `NU_NOTA_LC` (Linguagens e Códigos), `NU_NOTA_MT` (Matemática) e `NU_NOTA_REDACAO`.

> *Impacto Técnico:* Ao descartar colunas irrelevantes no Spark, o tamanho dos arquivos gerados (`.parquet`) é reduzido significativamente, barateando o armazenamento em nuvem (S3/MinIO) e permitindo que as consultas via Pandas sejam executadas num piscar de olhos, consumindo o mínimo de Memória RAM.

## 2. Tratamento de Ausentes (Limpeza de Nulos / Tratamento de Faltantes)
No domínio de negócio do ENEM, se um candidato falta em um dia de prova ou não comparece a nenhuma, as notas correspondentes (`NU_NOTA_*`) vêm registradas como **Nulas** (`NULL`/`NaN`) diretamente do INEP. 

Deixar valores `NULL` no meio das colunas de cálculo numérico quebra modelagens matemáticas futuras ou causa métricas de média infladas em relatórios.

**Regra Aplicada:** 
O pipeline verifica individualmente cada coluna do agrupamento de `notas = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]`. Se o registro (coluna daquele aluno em específico) não puder ser lido através de uma nota, **redefinimos essa nota para Zero (`0.0`)**.

## 3. Manutenção da Escalabilidade e Compressão
Esta transformação refaz a partição dos arquivos por ano (`ano=2021`, `ano=2022`) na pasta designada à tabela na Camada Prata, e mantém o sistema de compressão por blocos do **Snappy**. Assim, a arquitetura do Data Lake continua performática para os Jupyter Notebooks conectados.
