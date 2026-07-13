import io
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

SCAF_SUFFIX = "_scaffolds.csv"
RG_SUFFIX = "_r_groups.csv"

def key_exists(bucket, key, conn_id="aws_default"):
    return _hook(conn_id).check_for_key(key, bucket_name=bucket)


def list_dataset_ids(bucket, prefix, conn_id="aws_default", modified_after=None):
    """Return ids that have BOTH a scaffolds and r_groups file.
    If modified_after is set, keep only ids whose files changed after it."""
    hook = _hook(conn_id)
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix) or []
    scaf, rg = {}, {}
    for k in keys:
        name = k.split("/")[-1]
        if name.endswith(SCAF_SUFFIX):
            scaf[name[:-len(SCAF_SUFFIX)]] = k
        elif name.endswith(RG_SUFFIX):
            rg[name[:-len(RG_SUFFIX)]] = k
    ids = sorted(set(scaf) & set(rg))
    if modified_after is None:
        return ids
    kept = []
    for i in ids:
        lm = max(hook.get_key(scaf[i], bucket).last_modified,
                 hook.get_key(rg[i], bucket).last_modified)
        if lm.replace(tzinfo=None) > modified_after.replace(tzinfo=None):
            kept.append(i)
    return kept

def _hook(conn_id="aws_default"):
    return S3Hook(aws_conn_id=conn_id)


def read_csv_from_s3(bucket, key, conn_id="aws_default"):
    body = _hook(conn_id).get_key(key, bucket_name=bucket).get()["Body"].read()
    return pd.read_csv(io.BytesIO(body))


def write_csv_to_s3(df, bucket, key, conn_id="aws_default"):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    _hook(conn_id).load_string(buf.getvalue(), key=key,
                               bucket_name=bucket, replace=True)


def key_exists(bucket, key, conn_id="aws_default"):
    return _hook(conn_id).check_for_key(key, bucket_name=bucket)


def input_keys(dataset_id, prefix):
    return (f"{prefix}{dataset_id}{SCAF_SUFFIX}",
            f"{prefix}{dataset_id}{RG_SUFFIX}")


def output_key(dataset_id, prefix, stage):
    # stage in {molecules, properties, clusters}
    return f"{prefix}{dataset_id}/{stage}.csv"


def list_dataset_ids(bucket, prefix, conn_id="aws_default", modified_after=None):
    """Return ids that have BOTH a scaffolds and r_groups file.
    If modified_after is set, keep only ids whose files changed after it."""
    hook = _hook(conn_id)
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix) or []
    scaf, rg = {}, {}
    for k in keys:
        name = k.split("/")[-1]
        if name.endswith(SCAF_SUFFIX):
            scaf[name[:-len(SCAF_SUFFIX)]] = k
        elif name.endswith(RG_SUFFIX):
            rg[name[:-len(RG_SUFFIX)]] = k
    ids = sorted(set(scaf) & set(rg))
    if modified_after is None:
        return ids
    kept = []
    for i in ids:
        lm = max(hook.get_key(scaf[i], bucket).last_modified,
                 hook.get_key(rg[i], bucket).last_modified)
        if lm.replace(tzinfo=None) > modified_after.replace(tzinfo=None):
            kept.append(i)
    return kept
