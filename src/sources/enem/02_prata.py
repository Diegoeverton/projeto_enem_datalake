"""
02_prata.py  |  Fonte: ENEM  |  Camada: Prata
----------------------------------------------
Responsabilidade:
    1. Lê os dados Parquet da camada Bronze
    2. Seleciona as colunas relevantes para análise
    3. Padroniza tipos e preenche valores nulos nas notas com 0.0
    4. Salva na camada Prata particionado por ano

Entrada : /app/data_lake/bronze/enem/ano=<ANO>/*.parquet
Saída   : /app/data_lake/prata/enem/ano=<ANO>/*.parquet

Como rodar individualmente:
    docker exec spark_enem bash /app/run.sh --fonte enem --etapa prata
"""

import os
import sys

sys.path.insert(0, "/app/src")
from spark_utils import get_spark_session

from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType


# ── Caminhos ──────────────────────────────────────────────────────────────────
PASTA_BRONZE = "/app/data_lake/bronze/enem"
PASTA_PRATA  = "/app/data_lake/prata/enem"

# ── Colunas selecionadas para análise ─────────────────────────────────────────
# Perfil do candidato + notas por área + redação
COLUNAS_ANALISE = [
    "NU_INSCRICAO",
    "TP_FAIXA_ETARIA",
    "TP_SEXO",
    "TP_ESTADO_CIVIL",
    "TP_COR_RACA",
    "TP_ESCOLA",
    "IN_TREINEIRO",
    "SG_UF_PROVA",
    "NU_NOTA_CN",
    "NU_NOTA_CH",
    "NU_NOTA_LC",
    "NU_NOTA_MT",
    "NU_NOTA_REDACAO",
]

COLUNAS_NOTAS = [
    "NU_NOTA_CN",
    "NU_NOTA_CH",
    "NU_NOTA_LC",
    "NU_NOTA_MT",
    "NU_NOTA_REDACAO",
]


def processamento_camada_prata():
    spark = get_spark_session("ENEM_Prata")

    print("\n🥈 [PRATA] Iniciando tratamento ENEM → Camada Prata...")
    print(f"   Fonte Bronze : {PASTA_BRONZE}")
    print(f"   Destino Prata: {PASTA_PRATA}\n")

    # Lista as partições disponíveis na Bronze
    if not os.path.exists(PASTA_BRONZE):
        print("❌ Camada Bronze não encontrada. Execute 01_bronze.py primeiro.")
        spark.stop()
        return

    anos_disponiveis = sorted(
        [d for d in os.listdir(PASTA_BRONZE) if d.startswith("ano=")]
    )

    if not anos_disponiveis:
        print("⚠️  Nenhuma partição encontrada na Bronze. Execute 01_bronze.py primeiro.")
        spark.stop()
        return

    for diretorio_ano in anos_disponiveis:
        ano = diretorio_ano.split("=")[1]
        caminho_leitura = os.path.join(PASTA_BRONZE, diretorio_ano)
        caminho_escrita = os.path.join(PASTA_PRATA, f"ano={ano}")

        print(f"🔍 Tratando ano {ano}...")

        df = spark.read.parquet(caminho_leitura)

        # 1. Seleciona apenas colunas presentes no schema daquele ano
        colunas_presentes = [c for c in COLUNAS_ANALISE if c in df.columns]
        df_prata = df.select(*colunas_presentes)

        # 2. Converte notas para Double e substitui nulos por 0.0
        for nota in COLUNAS_NOTAS:
            if nota in df_prata.columns:
                df_prata = df_prata.withColumn(
                    nota,
                    when(col(nota).isNull(), 0.0)
                    .otherwise(col(nota).cast(DoubleType()))
                )

        # 3. Grava na Prata
        df_prata.write.mode("overwrite").parquet(caminho_escrita)
        print(f"✅ Ano {ano} → Prata concluído! ({df_prata.count():,} registros)\n")

    print("🏁 [PRATA] Finalizado!")
    spark.stop()


if __name__ == "__main__":
    processamento_camada_prata()
