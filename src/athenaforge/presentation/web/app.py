"""AthenaForge™ Web Dashboard — FastAPI backend."""
from __future__ import annotations

import asyncio
import json
import os
from dataclasses import asdict
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from athenaforge.infrastructure.config import AppConfig, DependencyContainer

SEED_DIR = Path(__file__).resolve().parents[4] / "seed"
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="AthenaForge™ Dashboard")


def _get_container() -> DependencyContainer:
    config = AppConfig(
        gcp_project_id="acme-analytics-prod",
        data_dir=str(SEED_DIR / "data"),
    )
    return DependencyContainer(config)


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/demo/run")
async def run_demo():
    """Execute the full demo pipeline and return all results."""
    container = _get_container()
    results: dict = {}

    # ── M1: Foundation ────────────────────────────────────────
    foundation = {}

    # Scaffold
    manifest_path = str(SEED_DIR / "manifests" / "lob_manifest.yaml")
    output_dir = str(SEED_DIR / "output" / "terraform")
    scaffold = await container.generate_scaffold_use_case.execute(manifest_path, output_dir)
    foundation["scaffold"] = {
        "lob_count": len(set(f.split("/")[-2] for f in scaffold.terraform_files)),
        "terraform_files": len(scaffold.terraform_files),
        "lobs": list(set(f.split("/")[-2] for f in scaffold.terraform_files)),
    }

    # Classify
    classification = await container.classify_tiers_use_case.execute("demo-inventory")
    foundation["classification"] = asdict(classification)

    # Pricing
    pricing = await container.configure_pricing_use_case.execute(
        500, 3, str(SEED_DIR / "output" / "terraform")
    )
    foundation["pricing"] = {
        "edition": pricing.edition,
        "slots": pricing.slots,
        "monthly_cost_usd": pricing.monthly_cost_usd,
    }

    results["foundation"] = foundation

    # ── M2: SQL ───────────────────────────────────────────────
    sql_results = {}

    # Translate
    source_dir = str(SEED_DIR / "sql" / "presto")
    output_dir = str(SEED_DIR / "output" / "translated")
    translation = await container.translate_batch_use_case.execute(source_dir, output_dir)
    sql_results["translation"] = asdict(translation)

    # Read original and translated for diff display
    translations = []
    presto_dir = SEED_DIR / "sql" / "presto"
    translated_dir = SEED_DIR / "output" / "translated"
    for fname in sorted(os.listdir(presto_dir)):
        if fname.endswith(".sql"):
            original = (presto_dir / fname).read_text()
            translated = (translated_dir / fname).read_text() if (translated_dir / fname).exists() else original
            translations.append({
                "file": fname,
                "original": original,
                "translated": translated,
            })
    sql_results["translations_detail"] = translations

    # MAP/CASCADE
    with open(SEED_DIR / "json" / "deps.json") as f:
        deps = json.load(f)
    map_cascade = await container.analyse_map_cascade_use_case.execute(deps)
    sql_results["map_cascade"] = {
        "total_maps": map_cascade.total_maps,
        "cascade_depth": map_cascade.cascade_depth,
        "co_migration_batches": [list(b) for b in map_cascade.co_migration_batches],
    }

    # UDF Classification
    with open(SEED_DIR / "json" / "udfs.json") as f:
        udfs = json.load(f)
    udf_report = await container.classify_udfs_use_case.execute(udfs)
    sql_results["udf_classification"] = asdict(udf_report)

    results["sql"] = sql_results

    # ── M3: Transfer ──────────────────────────────────────────
    transfer = {}

    # Egress Cost
    total_size_bytes = int(51200 * 1_073_741_824)
    egress = await container.model_egress_cost_use_case.execute(total_size_bytes, 25.0)
    transfer["egress_cost"] = asdict(egress)

    # Streaming Cutover
    cutover = await container.control_streaming_cutover_use_case.execute(
        "stream-001", "user-click-events",
        "projects/acme-analytics-prod/topics/user-click-events", 150,
    )
    transfer["cutover"] = {
        "job_id": cutover.job_id,
        "source_topic": cutover.source_topic,
        "target_topic": cutover.target_topic,
        "status": str(cutover.status),
        "lag_at_cutover": cutover.lag_at_cutover,
    }

    results["transfer"] = transfer

    # ── M4: Wave ──────────────────────────────────────────────
    wave_results = {}

    # Plan Waves
    wave_plan = await container.plan_waves_use_case.execute(
        "demo-inventory", ["finance", "marketing", "engineering"], 3,
    )
    wave_results["plan"] = asdict(wave_plan)

    # Rollback - healthy
    rollback_ok = await container.evaluate_rollback_use_case.execute(
        "wave-001", 0.995, 5.0, False, 0, False,
    )
    wave_results["rollback_healthy"] = asdict(rollback_ok)

    # Rollback - critical
    rollback_bad = await container.evaluate_rollback_use_case.execute(
        "wave-002", 0.80, 50.0, True, 5000, False,
    )
    wave_results["rollback_critical"] = asdict(rollback_bad)

    # Gate
    with open(SEED_DIR / "json" / "gate_criteria.json") as f:
        criteria = json.load(f)
    gate = await container.enforce_wave_gate_use_case.execute("wave-001", criteria)
    wave_results["gate"] = asdict(gate)

    # Dashboards
    with open(SEED_DIR / "json" / "dashboard_configs.json") as f:
        configs = json.load(f)
    dashboards = await container.migrate_dashboards_use_case.execute(configs)
    wave_results["dashboards"] = asdict(dashboards)

    # KPIs
    with open(SEED_DIR / "json" / "kpis.json") as f:
        kpis = json.load(f)
    kpi_result = await container.reconcile_kpis_use_case.execute(kpis)
    wave_results["kpis"] = asdict(kpi_result)

    results["wave"] = wave_results

    # ── M5: Dependency ────────────────────────────────────────
    dep_results = {}

    # DAG Rewrite
    dags_dir = SEED_DIR / "dags"
    dag_contents = {}
    for fname in sorted(os.listdir(dags_dir)):
        if fname.endswith(".py"):
            dag_contents[str(dags_dir / fname)] = (dags_dir / fname).read_text()
    dag_report = await container.rewrite_dags_use_case.execute(dag_contents)
    dep_results["dag_rewrite"] = asdict(dag_report)

    # Kafka
    with open(SEED_DIR / "json" / "kafka_topics.json") as f:
        topics = json.load(f)
    kafka = await container.migrate_kafka_topics_use_case.execute(topics)
    dep_results["kafka"] = asdict(kafka)

    # Lambda
    with open(SEED_DIR / "json" / "lambda_sources.json") as f:
        lambdas = json.load(f)
    lambda_report = await container.rewrite_lambdas_use_case.execute(lambdas)
    dep_results["lambdas"] = asdict(lambda_report)

    # IAM
    with open(SEED_DIR / "json" / "iam_policies.json") as f:
        policies = json.load(f)
    iam = await container.map_iam_permissions_use_case.execute(policies)
    dep_results["iam"] = asdict(iam)

    results["dependency"] = dep_results

    return JSONResponse(results)


# Serve static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def main():
    import uvicorn
    print("\n  AthenaForge™ Dashboard")
    print("  http://localhost:8000\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")


if __name__ == "__main__":
    main()
