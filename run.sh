#!/bin/bash
# ============================================================
# run.sh — Orquestrador Modular do Data Lake ENEM
# ============================================================
# Uso:
#   bash /app/run.sh --fonte enem --etapa all
#   bash /app/run.sh --fonte enem --etapa bronze
#   bash /app/run.sh --fonte enem --etapa prata
#   bash /app/run.sh --fonte enem --etapa ouro
#
# Para rodar de fora do container:
#   docker exec spark_enem bash /app/run.sh --fonte enem --etapa prata
# ============================================================

set -e

# ── Defaults ──────────────────────────────────────────────
FONTE=""
ETAPA="all"

# ── Parse de argumentos ───────────────────────────────────
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --fonte) FONTE="$2"; shift ;;
        --etapa) ETAPA="$2"; shift ;;
        *)
            echo "❌ Argumento desconhecido: $1"
            echo "   Uso: bash run.sh --fonte <fonte> --etapa <bronze|prata|ouro|all>"
            exit 1
            ;;
    esac
    shift
done

# ── Validações ────────────────────────────────────────────
if [[ -z "$FONTE" ]]; then
    echo "❌ Você precisa informar a fonte com --fonte"
    echo "   Fontes disponíveis: $(ls /app/src/sources/)"
    exit 1
fi

SCRIPT_DIR="/app/src/sources/${FONTE}"

if [[ ! -d "$SCRIPT_DIR" ]]; then
    echo "❌ Fonte '${FONTE}' não encontrada em ${SCRIPT_DIR}"
    echo "   Fontes disponíveis: $(ls /app/src/sources/)"
    exit 1
fi

# ── Funções por etapa ─────────────────────────────────────
run_bronze() {
    echo ""
    echo "══════════════════════════════════════════"
    echo "  🟫 ETAPA: BRONZE  |  Fonte: ${FONTE}"
    echo "══════════════════════════════════════════"
    spark-submit --py-files /app/src/spark_utils.py \
        "${SCRIPT_DIR}/01_bronze.py"
}

run_prata() {
    echo ""
    echo "══════════════════════════════════════════"
    echo "  🥈 ETAPA: PRATA  |  Fonte: ${FONTE}"
    echo "══════════════════════════════════════════"
    spark-submit --py-files /app/src/spark_utils.py \
        "${SCRIPT_DIR}/02_prata.py"
}

run_ouro() {
    echo ""
    echo "══════════════════════════════════════════"
    echo "  🥇 ETAPA: OURO  |  Fonte: ${FONTE}"
    echo "══════════════════════════════════════════"
    spark-submit --py-files /app/src/spark_utils.py \
        "${SCRIPT_DIR}/03_ouro.py"
}

# ── Execução ──────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════╗"
echo "║   DATA LAKE — Pipeline Modular           ║"
echo "║   Fonte : ${FONTE}"
echo "║   Etapa : ${ETAPA}"
echo "╚══════════════════════════════════════════╝"

case "$ETAPA" in
    bronze) run_bronze ;;
    prata)  run_prata  ;;
    ouro)   run_ouro   ;;
    all)
        run_bronze
        run_prata
        run_ouro
        ;;
    *)
        echo "❌ Etapa inválida: '${ETAPA}'"
        echo "   Opções válidas: bronze | prata | ouro | all"
        exit 1
        ;;
esac

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║   ✅ Pipeline finalizado com sucesso!    ║"
echo "╚══════════════════════════════════════════╝"
