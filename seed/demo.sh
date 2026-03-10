#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# AthenaForge™ — End-to-End Demo with Seed Data
# Proves all 5 modules (M1–M5) work with realistic migration data.
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SEED="$SCRIPT_DIR"
OUTPUT="$SCRIPT_DIR/output"

# Use the seed data dir as the repo data dir so classify/wave commands find the inventory
export ATHENAFORGE_DATA_DIR="$SEED/data"

# Clean previous output
rm -rf "$OUTPUT"
mkdir -p "$OUTPUT"

# Activate venv if not already
if [ -z "${VIRTUAL_ENV:-}" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

GREEN='\033[0;32m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

section() {
    echo ""
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════${RESET}"
    echo -e "${BOLD}${CYAN}  $1${RESET}"
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════${RESET}"
    echo ""
}

step() {
    echo -e "${GREEN}▸ $1${RESET}"
}

# ────────────────────────────────────────────────────────────────
section "M1: FoundationForge — Project Scaffolding & Tier Classification"
# ────────────────────────────────────────────────────────────────

step "1.1  Generate Terraform scaffold from LOB manifest"
athenaforge foundation scaffold \
    --manifest "$SEED/manifests/lob_manifest.yaml" \
    --output-dir "$OUTPUT/terraform"

step "1.2  Classify tables into migration tiers"
athenaforge foundation classify \
    --inventory-id demo-inventory

step "1.3  Configure BigQuery slot pricing (500 slots, 3-year commitment)"
athenaforge foundation pricing \
    --slots 500 \
    --commitment-years 3 \
    --output-dir "$OUTPUT/terraform"

# ────────────────────────────────────────────────────────────────
section "M2: SQLForge — SQL Translation & UDF Classification"
# ────────────────────────────────────────────────────────────────

step "2.1  Translate Presto SQL → BigQuery GoogleSQL (4 queries)"
athenaforge sql translate \
    --source-dir "$SEED/sql/presto" \
    --output-dir "$OUTPUT/translated"

step "2.2  Analyse MAP/CASCADE dependency graph"
athenaforge sql map-cascade \
    --deps-file "$SEED/json/deps.json"

step "2.3  Normalise case sensitivity (wrap columns in UPPER)"
athenaforge sql normalise-case \
    --sql-file "$SEED/sql/presto/report_daily_revenue.sql" \
    --columns "region,product_category" \
    --output-file "$OUTPUT/normalised_revenue.sql"

step "2.4  Classify UDFs (SQL / JavaScript / Cloud Run)"
athenaforge sql classify-udfs \
    --udfs-file "$SEED/json/udfs.json"

# ────────────────────────────────────────────────────────────────
section "M3: TransferForge — Egress Cost Modelling"
# ────────────────────────────────────────────────────────────────

step "3.1  Model egress costs for 50 TB migration"
athenaforge transfer egress-cost \
    --total-size-gb 51200 \
    --credit-pct 25

step "3.2  Streaming cutover control (simulate)"
athenaforge transfer cutover \
    --job-id "stream-001" \
    --source-topic "user-click-events" \
    --target-topic "projects/acme-analytics-prod/topics/user-click-events" \
    --current-lag 150

# ────────────────────────────────────────────────────────────────
section "M4: WaveForge — Wave Planning, Gates & KPI Reconciliation"
# ────────────────────────────────────────────────────────────────

step "4.1  Plan migration waves (3 LOBs, max 3 parallel)"
athenaforge wave plan \
    --inventory-id demo-inventory \
    --lobs "finance,marketing,engineering" \
    --max-parallel 3

step "4.2  Rollback evaluation (healthy scenario)"
athenaforge wave rollback-check \
    --wave-id wave-001 \
    --dvt-pass-rate 0.995 \
    --latency-increase-pct 5.0

step "4.3  Rollback evaluation (critical scenario)"
athenaforge wave rollback-check \
    --wave-id wave-002 \
    --dvt-pass-rate 0.80 \
    --latency-increase-pct 50.0 \
    --data-loss-detected \
    --streaming-lag 5000

step "4.4  Enforce wave quality gate"
athenaforge wave gate \
    --wave-id wave-001 \
    --criteria-file "$SEED/json/gate_criteria.json"

step "4.5  Migrate dashboards"
athenaforge wave dashboards \
    --configs-file "$SEED/json/dashboard_configs.json"

step "4.6  Reconcile KPIs between legacy and new platform"
athenaforge wave kpi \
    --kpis-file "$SEED/json/kpis.json"

# ────────────────────────────────────────────────────────────────
section "M5: DependencyForge — DAG Rewriting, Kafka, Lambda & IAM"
# ────────────────────────────────────────────────────────────────

step "5.1  Rewrite Airflow DAGs (AthenaOperator → BigQuery)"
athenaforge dependency rewrite-dags \
    --dags-dir "$SEED/dags"

step "5.2  Migrate Kafka topic configurations"
athenaforge dependency kafka \
    --topics-file "$SEED/json/kafka_topics.json"

step "5.3  Scan & rewrite Lambda functions with Athena references"
athenaforge dependency lambdas \
    --sources-file "$SEED/json/lambda_sources.json"

step "5.4  Map Lake Formation permissions → BigQuery IAM roles"
athenaforge dependency iam \
    --policies-file "$SEED/json/iam_policies.json"

# ────────────────────────────────────────────────────────────────
section "DEMO COMPLETE"
# ────────────────────────────────────────────────────────────────

echo "Generated artifacts:"
echo ""
find "$OUTPUT" -type f | sort | while read f; do
    size=$(wc -c < "$f" | tr -d ' ')
    echo "  $(basename "$f") (${size} bytes)"
done

echo ""
echo -e "${GREEN}${BOLD}All 5 modules executed successfully with seed data!${RESET}"
echo -e "Output directory: $OUTPUT"
echo ""
