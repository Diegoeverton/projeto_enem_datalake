"""
01_bronze.py  |  Fonte: ENEM  |  Camada: Bronze
------------------------------------------------
Responsabilidade:
    1. Extrai os arquivos CSV de dentro das pastas já descompactadas (RAW)
    2. Converte cada CSV para formato Parquet otimizado (Snappy)
    3. Salva na camada Bronze particionado por ano

Entrada : /app/dados/microdados_enem_<ano>/DADOS/MICRODADOS_ENEM_<ANO>.csv
Saída   : /app/data_lake/bronze/enem/ano=<ANO>/*.parquet

Como rodar individualmente:
    docker exec spark_enem bash /app/run.sh --fonte enem --etapa bronze
"""

import os
import sys

# Permite importar spark_utils a partir de /app/src
sys.path.insert(0, "/app/src")
from spark_utils import get_spark_session


# ── Caminhos ──────────────────────────────────────────────────────────────────
PASTA_DADOS_RAW = "/app/dados"
PASTA_BRONZE    = "/app/data_lake/bronze/enem"


def criar_camada_bronze():
    spark = get_spark_session("ENEM_Bronze")

    print("\n🟫 [BRONZE] Iniciando ingestão do ENEM → Camada Bronze...")
    print(f"   Fonte RAW : {PASTA_DADOS_RAW}")
    print(f"   Destino   : {PASTA_BRONZE}\n")

    arquivos_processados = 0

    for diretorio in sorted(os.listdir(PASTA_DADOS_RAW)):
        caminho_dir = os.path.join(PASTA_DADOS_RAW, diretorio)

        # Ignora arquivos soltos (ex: .zip, .gitkeep)
        if not os.path.isdir(caminho_dir):
            continue

        # Extrai o ano do nome da pasta: "microdados_enem_2022" → "2022"
        ano = diretorio.split("_")[-1]

        # Tenta localizar o CSV (case-insensitive para compatibilidade Linux)
        candidatos = [
            os.path.join(caminho_dir, "DADOS", f"MICRODADOS_ENEM_{ano}.csv"),
            os.path.join(caminho_dir, "DADOS", f"MICRODADOS_ENEM_{ano}.CSV"),
        ]
        caminho_csv = next((p for p in candidatos if os.path.exists(p)), None)

        if not caminho_csv:
            print(f"⚠️  Ano {ano}: CSV não encontrado em {caminho_dir}/DADOS — pulando.")
            continue

        pasta_saida = os.path.join(PASTA_BRONZE, f"ano={ano}")

        # Verifica se o Parquet desta partição já existe (evita reprocessamento)
        if os.path.exists(pasta_saida) and os.listdir(pasta_saida):
            print(f"⏭️  Ano {ano}: partição Bronze já existe — pulando (use mode=overwrite para forçar).")
            continue

        print(f"⏳ Processando ano {ano}...")
        print(f"   Lendo  : {caminho_csv}")
        print(f"   Gravando: {pasta_saida}")

        df = spark.read.csv(
            caminho_csv,
            header=True,
            sep=";",
            encoding="latin1",
        )

        df.write.mode("overwrite").parquet(pasta_saida)
        arquivos_processados += 1
        print(f"✅ Ano {ano} → Bronze concluído! ({df.count():,} registros)\n")

    if arquivos_processados == 0:
        print("ℹ️  Nenhum arquivo novo para processar na Bronze.")
    else:
        print(f"🏁 [BRONZE] Finalizado! {arquivos_processados} ano(s) processado(s).")

    spark.stop()


if __name__ == "__main__":
    criar_camada_bronze()
