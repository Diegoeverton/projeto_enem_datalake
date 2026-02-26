"""
processamento_ouro_spark.py
---------------------------
Camada Ouro do Data Lake ENEM.

Lê os dados limpos da Camada Prata e gera tabelas analíticas agregadas
prontas para consumo em dashboards, relatórios e modelos de Machine Learning.

Tabelas geradas em /app/data_lake/ouro/enem/:
    - media_notas_por_uf_ano       : Média das 5 notas por UF e por Ano
    - distribuicao_perfil          : Candidatos por Faixa Etária, Sexo e Raça
    - ranking_redacao_por_uf       : Nota média de redação ordenada por UF

Uso direto (sem orquestrador):
    spark-submit src/processamento_ouro_spark.py

Ou pelo orquestrador modular:
    docker exec spark_enem bash /app/run.sh --fonte enem --etapa ouro
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round as spark_round
import os


# ── Caminhos ──────────────────────────────────────────────────────────────────
PASTA_PRATA = "/app/data_lake/prata/enem"
PASTA_OURO  = "/app/data_lake/ouro/enem"


def criar_camada_ouro():
    spark = (
        SparkSession.builder
        .appName("ProcessamentoOuroENEM")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n====================================================")
    print("   INICIANDO PROCESSAMENTO — CAMADA OURO (ENEM)   ")
    print("====================================================\n")

    # ── Validação da Camada Prata ──────────────────────────────────────────────
    if not os.path.exists(PASTA_PRATA):
        print("❌ Camada Prata não encontrada. Execute processamento_prata_spark.py primeiro.")
        spark.stop()
        return

    anos_disponiveis = [d for d in os.listdir(PASTA_PRATA) if d.startswith("ano=")]
    if not anos_disponiveis:
        print("⚠️  Nenhuma partição encontrada na Prata. Execute processamento_prata_spark.py primeiro.")
        spark.stop()
        return

    # ── Leitura de todos os anos de uma vez ───────────────────────────────────
    print(f"📖 Lendo dados da Camada Prata ({len(anos_disponiveis)} ano(s))...")
    df = spark.read.parquet(PASTA_PRATA)
    df.cache()
    total = df.count()
    print(f"   Total de registros: {total:,}\n")

    # ─────────────────────────────────────────────────────────────────────────
    # TABELA 1 — Média das notas por UF e por Ano
    # ─────────────────────────────────────────────────────────────────────────
    print("--> TABELA 1: Média das notas por UF e Ano")
    NOTAS = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]
    notas_presentes = [c for c in NOTAS if c in df.columns]

    agg_media = [
        spark_round(avg(col(c)), 2).alias(c.replace("NU_NOTA_", "MEDIA_"))
        for c in notas_presentes
    ]

    df_media = (
        df.groupBy("ano", "SG_UF_PROVA")
          .agg(*agg_media)
          .orderBy("ano", "SG_UF_PROVA")
    )
    saida_media = os.path.join(PASTA_OURO, "media_notas_por_uf_ano")
    df_media.write.mode("overwrite").parquet(saida_media)
    print(f"   ✅ Salvo em: {saida_media}  ({df_media.count():,} linhas)\n")

    # ─────────────────────────────────────────────────────────────────────────
    # TABELA 2 — Distribuição de candidatos por perfil
    # ─────────────────────────────────────────────────────────────────────────
    print("--> TABELA 2: Distribuição de candidatos por Faixa Etária, Sexo e Raça")
    COLS_PERFIL = ["TP_FAIXA_ETARIA", "TP_SEXO", "TP_COR_RACA"]
    cols_perfil_presentes = ["ano"] + [c for c in COLS_PERFIL if c in df.columns]

    df_perfil = (
        df.groupBy(*cols_perfil_presentes)
          .agg(count("*").alias("TOTAL_CANDIDATOS"))
          .orderBy("ano", "TP_FAIXA_ETARIA")
    )
    saida_perfil = os.path.join(PASTA_OURO, "distribuicao_perfil")
    df_perfil.write.mode("overwrite").parquet(saida_perfil)
    print(f"   ✅ Salvo em: {saida_perfil}  ({df_perfil.count():,} linhas)\n")

    # ─────────────────────────────────────────────────────────────────────────
    # TABELA 3 — Ranking de redação por UF e Ano
    # ─────────────────────────────────────────────────────────────────────────
    if "NU_NOTA_REDACAO" in df.columns:
        print("--> TABELA 3: Ranking de Redação por UF e Ano")
        df_ranking = (
            df.filter(col("NU_NOTA_REDACAO") > 0)  # Exclui candidatos ausentes
              .groupBy("ano", "SG_UF_PROVA")
              .agg(
                  spark_round(avg("NU_NOTA_REDACAO"), 2).alias("MEDIA_REDACAO"),
                  count("*").alias("TOTAL_PARTICIPANTES"),
              )
              .orderBy("ano", col("MEDIA_REDACAO").desc())
        )
        saida_ranking = os.path.join(PASTA_OURO, "ranking_redacao_por_uf")
        df_ranking.write.mode("overwrite").parquet(saida_ranking)
        print(f"   ✅ Salvo em: {saida_ranking}  ({df_ranking.count():,} linhas)\n")

    df.unpersist()
    print("====================================================")
    print("        CAMADA OURO FINALIZADA COM SUCESSO! ✅      ")
    print("====================================================\n")
    spark.stop()


if __name__ == "__main__":
    criar_camada_ouro()
