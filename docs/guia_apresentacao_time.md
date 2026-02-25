# Guia de Apresentação e Uso Prático do Data Lake (Time)

Este documento foi criado para guiar a apresentação do nosso Datalake conteinerizado para o time. Ele aborda como demonstrar a reprodutibilidade da pipeline a partir do zero e como os Analistas e Cientistas de Dados podem consumir estes dados via Jupyter Notebook.

## 1. Demonstração de Reprodutibilidade (Rodando do Zero)

A maior vantagem da nossa arquitetura é a **Idempotência**. O time pode apagar todos os dados extraídos sem medo, pois a nossa "fábrica" reconstrói tudo perfeitamente.

**Para mostrar ao vivo na reunião:**
1. Delete manualmente as pastas extraídas dentro da pasta `dados/` (deixe apenas os .zips originais).
2. Delete a pasta `data_lake/bronze/` e `data_lake/prata/` (se existirem).
3. Abra o terminal raiz do projeto e execute o comando mágico:
   ```bash
   docker-compose up --build
   ```
4. **O que acontecerá na tela:** O Docker iniciará os dois serviços simultaneamente (Spark e Jupyter).
   - O terminal exibirá o passo a passo elegante do `run_pipeline.sh`: Descompactação em andamento, seguida da conversão agressiva dos pesados CSVs para o formato `.parquet` otimizado (Camada Bronze), e por sim o tratamento dos dados na Camada Prata.
   - O time verá os dados recarregando instantaneamente no seu disco nas pastas espelhadas (Volumes).

## 2. Acesso ao Laboratório Analítico (Jupyter)

Junto com a pipeline rodando silenciosamente, o ambiente do Jupyter Lab estará imediatamente disponível para uso sem a necessidade de instalar Python no Windows.

### Como encontrar o Token e Logar:
Quando rodar o comando `docker-compose up`, procure nos logs do terminal por uma linha como esta:
`http://127.0.0.1:8888/tree?token=seu_token_aqui_12345`

1. Segure a tecla `Ctrl` e clique no link, ou copie e cole no seu navegador (Google Chrome).
2. Você entrará na interface do Jupyter, com acesso total à raiz de pastas do projeto!

## 3. Lendo os Dados no Notebook (Exemplo Pandas)

Para o time iniciar o uso, criem um novo `Notebook Python 3` lá na interface e usem o **Pandas**. Como os dados de *Big Data* já passaram pela nossa Ingestão (Spark), ler milhões de registros `.parquet` ficou banal e absurdamente rápido.

**Copie e cole na primeira célula do Jupyter:**

```python
import pandas as pd

# 1. Caminho da nossa camada Bronze (Substitua o ano se desejar)
# O caminho começa com /app/ pois o Jupyter está rodando dentro do container Linux
caminho_2021 = '/app/data_lake/bronze/enem/ano=2021'

print("⏳ Carregando dados do Data Lake em milissegundos...")
# 2. Leitura fulminante com PyArrow/FastParquet por baixo dos panos!
df_enem_2021 = pd.read_parquet(caminho_2021)

print("✅ Dados Carregados para Memória!")

# 3. Exibição básica
df_enem_2021.head()
```

Para demonstrar o volume da base ao time na reunião, faça uma segunda célula com:
```python
linhas, colunas = df_enem_2021.shape
print(f"Total de Candidatos na tabela analisada: {linhas:,}")
print(f"Total de Colunas Originais: {colunas}")
```

Com este fluxo montado, unimos o melhor do mundo de **Data Engineering** (PySpark fazendo o trabalho de força e conversão) e **Data Science** (Pandas no Jupyter pronto para modelagem imediata dos dados higienizados).
