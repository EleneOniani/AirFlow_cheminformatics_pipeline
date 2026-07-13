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
    dag_id="chem_pipeline_v1",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    params={"dataset_id": Param("", type="string",
                                description="Dataset id to process")},
    tags=["cheminformatics"],
)
def chem_pipeline():

    @task
    def load(**ctx):
        did = ctx["params"]["dataset_id"]
        if not did:
            raise ValueError("dataset_id parameter is required")
        sk, rk = s3.input_keys(did, IN_PREFIX)
        scaf = s3.read_csv_from_s3(BUCKET, sk, CONN)["smiles"].dropna().tolist()
        rg = s3.read_csv_from_s3(BUCKET, rk, CONN)["smiles"].dropna().tolist()
        return {"dataset_id": did, "scaffolds": scaf, "r_groups": rg}

    @task
    def generate(payload):
        mols = generate_molecules(payload["scaffolds"], payload["r_groups"])
        return {"dataset_id": payload["dataset_id"], "molecules": mols}

    @task
    def properties(payload):
        df = calc_properties_frame(payload["molecules"])
        s3.write_csv_to_s3(df, BUCKET,
                           s3.output_key(payload["dataset_id"], OUT_PREFIX, "properties"),
                           CONN)
        return payload

    @task
    def clusters(payload):
        import pandas as pd
        mapping = cluster_molecules(payload["molecules"])
        df = pd.DataFrame([{"smiles": k, "cluster": v} for k, v in mapping.items()])
        s3.write_csv_to_s3(df, BUCKET,
                           s3.output_key(payload["dataset_id"], OUT_PREFIX, "clusters"),
                           CONN)
        return payload["dataset_id"]

    p = generate(load())
    properties(p) >> clusters(p)


chem_pipeline()
