import os
import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from chem_pipeline import s3_utils as s3
from chem_pipeline.molecule_generation import generate_molecules
from chem_pipeline.properties import calc_properties_frame
from chem_pipeline.clustering import cluster_molecules

BUCKET = os.environ["S3_BUCKET"]
IN_PREFIX = os.environ.get("S3_INPUT_PREFIX", "input/")
OUT_PREFIX = os.environ.get("S3_OUTPUT_PREFIX", "output/")
CONN = os.environ.get("AWS_CONN_ID", "aws_default")


@dag(
    dag_id="chem_pipeline_v2",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_tasks=4,
    params={"overwrite": Param(False, type="boolean",
                               description="Reprocess datasets even if output exists")},
    tags=["cheminformatics"],
)
def chem_pipeline():

    @task
    def discover(**ctx):
        overwrite = ctx["params"]["overwrite"]
        since = ctx["data_interval_start"]
        ids = s3.list_dataset_ids(BUCKET, IN_PREFIX, CONN,
                                  modified_after=None if overwrite else since)
        if overwrite:
            return ids
        todo = []
        for i in ids:
            done = s3.key_exists(BUCKET, s3.output_key(i, OUT_PREFIX, "clusters"), CONN)
            if not done:
                todo.append(i)
        return todo

    @task
    def process(dataset_id, **ctx):
        import pandas as pd
        overwrite = ctx["params"]["overwrite"]
        clusters_key = s3.output_key(dataset_id, OUT_PREFIX, "clusters")
        if not overwrite and s3.key_exists(BUCKET, clusters_key, CONN):
            return f"{dataset_id}: skipped (exists)"

        sk, rk = s3.input_keys(dataset_id, IN_PREFIX)
        scaf = s3.read_csv_from_s3(BUCKET, sk, CONN)["smiles"].dropna().tolist()
        rg = s3.read_csv_from_s3(BUCKET, rk, CONN)["smiles"].dropna().tolist()

        mols = generate_molecules(scaf, rg)

        props = calc_properties_frame(mols)
        s3.write_csv_to_s3(props, BUCKET,
                           s3.output_key(dataset_id, OUT_PREFIX, "properties"), CONN)

        mapping = cluster_molecules(mols)
        clus = pd.DataFrame([{"smiles": k, "cluster": v} for k, v in mapping.items()])
        s3.write_csv_to_s3(clus, BUCKET, clusters_key, CONN)
        return f"{dataset_id}: {len(mols)} molecules"

    process.expand(dataset_id=discover())


chem_pipeline()
