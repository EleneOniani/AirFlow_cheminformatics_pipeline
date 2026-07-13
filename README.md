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

## Local dev
```bash
cp .env.example .env       # fill values
docker compose up airflow-init
docker compose up
# Airflow UI: http://localhost:8080  (admin/admin)
```
Create an `aws_default` connection in the UI (or via env) with S3 credentials.
