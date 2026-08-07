# commonroad-rag

Production-style Databricks pipeline that ingests raw CommonRoad XML
scenario files into Unity Catalog (bronze → silver), as the data
foundation for a Driving-RAG system (arXiv:2504.04419).

## Why this structure

The original notebook-based ingestion silently duplicated/truncated rows
on re-run (61,464 parse failures out of 61,464 rows). This repo fixes
that at the architecture level:

- **Auto Loader with `trigger(availableNow=True)`** instead of a static
  `spark.read` — checkpoint-tracked, so re-running never reprocesses or
  duplicates a file.
- **Explicit quality gate task** (`quality_checks.py`) that fails the job
  loudly (bronze row count vs distinct file count, parse failure rate)
  instead of letting bad data pass silently into silver.
- **Every parse failure is logged with its exact exception**, not
  swallowed — `silver.parse_failures` is always queryable.

## Local development

```bash
docker build -t commonroad-pipeline .
docker run --rm commonroad-pipeline          # runs lint + pytest
```

Or without Docker:
```bash
pip install -r requirements.txt -r requirements-dev.txt
pip install -e .
ruff check src/ tests/
pytest tests/ -v
```

## Deployment (Databricks Asset Bundles)

Requires the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html) (v0.220+).

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev      # deploys job in PAUSED schedule state
databricks bundle deploy --target prod     # requires DATABRICKS_TOKEN_PROD in CI secrets
```

`databricks.yml` + `resources/commonroad_job.yml` define the Job: three
chained tasks (`ingest_bronze` → `parse_silver` → `quality_check`) on a
job cluster, scheduled daily. Edit the `workspace.host` placeholders in
`databricks.yml` before first deploy.

## CI/CD

`.github/workflows/ci.yml`:
- every PR: build Docker image, lint, run tests, deploy bundle to `dev`
- merge to `main`: same, then deploy to `prod` (with environment approval gate if configured in repo settings)

Required GitHub secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN_DEV`, `DATABRICKS_TOKEN_PROD`.

## Roadmap (Driving-RAG, arXiv:2504.04419)

This repo covers the **bronze/silver ingestion** stage only. Not yet built:

- [ ] `silver.atom_scenarios` — slice scenarios into interaction segments, build per-frame vehicle-to-vehicle / vehicle-to-lane graphs
- [ ] Graph-DTW distance labeling between scenario pairs
- [ ] RGCN + Transformer embedding model (training pipeline, MLflow tracking)
- [ ] `gold.scenario_embeddings` — 64-dim vectors per atom scenario
- [ ] HNSW-TSD indexing (density-based typical-scenario sampling + Faiss/HNSW)
- [ ] Retrieval reorganization (graph-relation filtering + level selection)
- [ ] LLM planning prompt construction + serving endpoint
