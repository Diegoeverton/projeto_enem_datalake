"""
03_ouro.py  |  Fonte: ENEM  |  Camada: Ouro
--------------------------------------------
Responsabilidade:
    Lê os dados limpos da Prata e gera tabelas analíticas agregadas (Camada Ouro).
    Processa um ano por vez para evitar estouro de memória.

Tabelas geradas (acumuladas de todos os anos):
    - media_notas_por_uf_ano   : Média das 5 notas por UF e por Ano
    - distribuicao_perfil      : Candidatos por Faixa Etária, Sexo e Raça
    - ranking_redacao_por_uf   : Nota média de redação por UF (excluindo ausentes)

Entrada : /app/data_lake/prata/enem/ano=<ANO>/*.parquet
Saída   : /app/data_lake/ouro/enem/<nome_tabela>/*.parquet

Como rodar individualmente:
    docker exec spark_enem bash /app/run.sh --fonte enem --etapa ouro
"""

import os
import sys

sys.path.insert(0, "/app/src")
from spark_utils import get_spark_session

from pyspark.sql.functions import col, avg, count, lit, round as spark_round


# ── Caminhos ──────────────────────────────────────────────────────────────────
PASTA_PRATA = "/app/data_lake/prata/enem"
PASTA_OURO  = "/app/data_lake/ouro/enem"

NOTAS       = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]
COLS_PERFIL = ["TP_FAIXA_ETARIA", "TP_SEXO", "TP_COR_RACA"]


def criar_camada_ouro():
    spark = get_spark_session("ENEM_Ouro", memory="2g")

    print("\n🥇 [OURO] Iniciando agregações ENEM → Camada Ouro...")
    print(f"   Fonte Prata : {PASTA_PRATA}")
    print(f"   Destino Ouro: {PASTA_OURO}\n")

    if not os.path.exists(PASTA_PRATA):
        print("❌ Camada Prata não encontrada. Execute 02_prata.py primeiro.")
        spark.stop()
        return

    anos_disponiveis = sorted(
        [d for d in os.listdir(PASTA_PRATA) if d.startswith("ano=")]
    )

    if not anos_disponiveis:
        print("⚠️  Nenhuma partição encontrada na Prata. Execute 02_prata.py primeiro.")
        spark.stop()
        return

    # Acumuladores: lista de DataFrames por tabela (um por ano)
    dfs_media   = []
    dfs_perfil  = []
    dfs_ranking = []

    # ── Processa um ano por vez para não explodir a memória ───────────────────
    for diretorio_ano in anos_disponiveis:
        ano = diretorio_ano.split("=")[1]
        caminho_leitura = os.path.join(PASTA_PRATA, diretorio_ano)
        print(f"🔍 Processando ano {ano}...")

        df = spark.read.parquet(caminho_leitura)

        # Adiciona coluna 'ano' explícita (não depende da partição do path)
        df = df.withColumn("ano", lit(ano))

        # ── TABELA 1: Média das notas por UF ──────────────────────────────────
        notas_presentes = [c for c in NOTAS if c in df.columns]
        if notas_presentes and "SG_UF_PROVA" in df.columns:
            agg_media = [
                spark_round(avg(col(c)), 2).alias(c.replace("NU_NOTA_", "MEDIA_"))
                for c in notas_presentes
            ]
            df_media_ano = df.groupBy("ano", "SG_UF_PROVA").agg(*agg_media)
            dfs_media.append(df_media_ano)

        # ── TABELA 2: Distribuição de perfil ──────────────────────────────────
        cols_perfil_presentes = [c for c in COLS_PERFIL if c in df.columns]
        if cols_perfil_presentes:
            df_perfil_ano = (
                df.groupBy("ano", *cols_perfil_presentes)
                  .agg(count("*").alias("TOTAL_CANDIDATOS"))
            )
            dfs_perfil.append(df_perfil_ano)

        # ── TABELA 3: Ranking de redação por UF ───────────────────────────────
        if "NU_NOTA_REDACAO" in df.columns and "SG_UF_PROVA" in df.columns:
            df_ranking_ano = (
                df.filter(col("NU_NOTA_REDACAO") > 0)
                  .groupBy("ano", "SG_UF_PROVA")
                  .agg(
                      spark_round(avg("NU_NOTA_REDACAO"), 2).alias("MEDIA_REDACAO"),
                      count("*").alias("TOTAL_PARTICIPANTES"),
                  )
            )
            dfs_ranking.append(df_ranking_ano)

        print(f"✅ Ano {ano} agregado.\n")

    # ── Une todos os anos e grava as tabelas finais ───────────────────────────
    print("💾 Gravando tabelas finais na Camada Ouro...\n")

    if dfs_media:
        df_final_media = dfs_media[0]
        for df_ in dfs_media[1:]:
            df_final_media = df_final_media.union(df_)
        df_final_media.orderBy("ano", "SG_UF_PROVA") \
                      .write.mode("overwrite") \
                      .parquet(os.path.join(PASTA_OURO, "media_notas_por_uf_ano"))
        print(f"✅ media_notas_por_uf_ano → {df_final_media.count():,} linhas")

    if dfs_perfil:
        df_final_perfil = dfs_perfil[0]
        for df_ in dfs_perfil[1:]:
            df_final_perfil = df_final_perfil.union(df_)
        df_final_perfil.orderBy("ano") \
                       .write.mode("overwrite") \
                       .parquet(os.path.join(PASTA_OURO, "distribuicao_perfil"))
        print(f"✅ distribuicao_perfil → {df_final_perfil.count():,} linhas")

    if dfs_ranking:
        df_final_ranking = dfs_ranking[0]
        for df_ in dfs_ranking[1:]:
            df_final_ranking = df_final_ranking.union(df_)
        df_final_ranking.orderBy("ano", col("MEDIA_REDACAO").desc()) \
                        .write.mode("overwrite") \
                        .parquet(os.path.join(PASTA_OURO, "ranking_redacao_por_uf"))
        print(f"✅ ranking_redacao_por_uf → {df_final_ranking.count():,} linhas")

    print("\n🏁 [OURO] Todas as tabelas analíticas geradas com sucesso!")
    spark.stop()


if __name__ == "__main__":
    criar_camada_ouro()
