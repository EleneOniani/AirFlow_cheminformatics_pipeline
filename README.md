# Cheminformatics Pipeline

Airflow pipeline that turns scaffold + R-group CSV files (stored in S3) into
generated molecules, computed properties, and K-means clusters.

## Input
Two CSVs per dataset in `s3://<bucket>/<input_prefix>`:
- `<id>_scaffolds.csv`  (column: `smiles`)
- `<id>_r_groups.csv`   (column: `smiles`)

## Pipeline stages
1. Molecule generation (scaffold × R-groups via RDKit `molzip`)
2. Property calculation (logP, HBA, HBD, MW, TPSA, rings, rot. bonds)
3. K-means clustering on Morgan fingerprints
4. (optional) ChemProp prediction, Faerun graph

## Output
`s3://<bucket>/<output_prefix>/<id>/{properties,clusters}.csv`

## Step 1 — run
DAG `chem_pipeline_v1`. Trigger with config:
```json
{ "dataset_id": "batch01" }
```
## Step 2 — weekly, incremental
DAG `chem_pipeline_v2` runs `@weekly` and processes every dataset that is new
since the last run (based on the run's `data_interval_start` and whether output
already exists). Datasets are processed in parallel via dynamic task mapping.

### `overwrite` param (default `False`)
- `False`: only unprocessed datasets are handled; existing outputs are kept.
- `True`: all discovered datasets are (re)processed and outputs overwritten.

Manual run with reprocess:
```json
{ "overwrite": true }
```

## Step 3 — data quality + MS Teams alerts
Each dataset is validated before and after generation:
- **Input**: `smiles` column present, non-empty file, no nulls, valid RDKit SMILES
- **Output**: at least one molecule generated

Results are posted to a Teams channel via an incoming webhook
(`TEAMS_WEBHOOK_URL`): green card on success (with molecule count), red card on
failure (with the reason). A failed dataset raises and is marked failed in Airflow
without blocking the other mapped datasets.

## Local dev
```bash
cp .env.example .env       # fill values
docker compose up airflow-init
docker compose up
# Airflow UI: http://localhost:8080  (admin/admin)
```
Create an `aws_default` connection in the UI (or via env) with S3 credentials.
