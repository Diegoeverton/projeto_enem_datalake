#!/bin/bash
set -e

echo "====================================================="
echo "   INICIANDO PIPELINE DO DATA LAKE DO ENEM (SPARK)   "
echo "====================================================="

echo ""
echo "--> ETAPA 1: Extração dos Arquivos ZIP (RAW)"
# Roda o script de descompactação. 
# Usamos apenas python3 pois é uma operação baseada no sistema de arquivos local (zipfile)
python3 src/extrair_dados_spark.py

echo ""
echo "--> ETAPA 2: Ingestão Camada Bronze (Convertendo CSV para Parquet Otimizado)"
# Roda o Spark formalmente para processar os gigabytes extraídos
spark-submit src/ingestao_bronze_spark.py

echo ""
echo "--> ETAPA 3: Tratamento e Limpeza (Camada Prata)"
spark-submit src/processamento_prata_spark.py

echo ""
echo "====================================================="
echo "        PIPELINE FINALIZADO COM SUCESSO! ✅        "
echo "====================================================="
