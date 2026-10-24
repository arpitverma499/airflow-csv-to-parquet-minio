"""DHAP-42: CSV -> validate (schema contract) -> transform -> partitioned Parquet in MinIO.

Tasks: read_csv >> validate_contract >> transform >> write_parquet_to_minio
validate_contract raises AirflowFailException on any contract violation, so nothing is
ever written to MinIO for a bad file.
"""
from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

DAG_DIR = Path(__file__).resolve().parent
CONTRACT_PATH = Path(os.environ.get("CONTRACT_PATH", DAG_DIR.parent / "schema_contract.yaml"))
DEFAULT_CSV = str(DAG_DIR / "extraction" / "sample.csv")
STAGING_DIR = Path(os.environ.get("STAGING_DIR", "/tmp/dhap42_staging"))
MINIO_CONN_ID = "minio_default"          # supplied via AIRFLOW_CONN_MINIO_DEFAULT (.env)
BUCKET = os.environ.get("MINIO_BUCKET", "datalake")

_BOOL_TRUE = {"true", "1", "yes", "t", "y"}
_BOOL_FALSE = {"false", "0", "no", "f", "n"}


# --------------------------------------------------------------------------- helpers
def load_contract() -> dict:
    import yaml

    with open(CONTRACT_PATH, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def read_raw(csv_path: str):
    """Read everything as strings so type problems are caught by the contract, not by pandas."""
    import pandas as pd

    return pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding="utf-8-sig")


def _rows(mask, limit: int = 5) -> str:
    idx = list(mask[mask].index[:limit])
    return ", ".join(str(i + 2) for i in idx) + (", ..." if mask.sum() > limit else "")  # +2: header, 1-based


def _valid_type(values, ctype: str):
    import pandas as pd

    if ctype == "string":
        return pd.Series(True, index=values.index)
    if ctype == "integer":
        return values.str.fullmatch(r"[+-]?\d+")
    if ctype == "float":
        return pd.to_numeric(values, errors="coerce").notna()
    if ctype == "boolean":
        return values.str.lower().isin(_BOOL_TRUE | _BOOL_FALSE)
    if ctype == "date":
        return pd.to_datetime(values, format="%Y-%m-%d", errors="coerce").notna()
    if ctype == "timestamp":
        return pd.to_datetime(values, format="ISO8601", errors="coerce").notna()
    raise AirflowFailException(f"Contract error: unknown type '{ctype}'")


def check_contract(df, contract: dict) -> list[str]:
    import pandas as pd

    errors: list[str] = []
    expected = [c["name"] for c in contract["columns"]]
    actual = list(df.columns)

    missing = [c for c in expected if c not in actual]
    extra = [c for c in actual if c not in expected]
    if missing:
        errors.append(f"missing columns: {missing}")
    if extra and contract.get("strict_columns", True):
        errors.append(f"unexpected columns: {extra}")
    if len(df) < contract.get("min_rows", 1):
        errors.append(f"expected at least {contract.get('min_rows', 1)} rows, got {len(df)}")

    for col in contract["columns"]:
        name, ctype = col["name"], col["type"]
        if name not in df.columns:
            continue
        raw = df[name].str.strip()
        is_null = raw == ""
        if is_null.any() and not col.get("nullable", False):
            errors.append(f"{name}: {int(is_null.sum())} null/blank values (rows {_rows(is_null)})")
        values = raw[~is_null]
        if values.empty:
            continue

        ok = _valid_type(values, ctype)
        if not ok.all():
            bad = ~ok
            errors.append(
                f"{name}: {int(bad.sum())} values are not {ctype} "
                f"(rows {_rows(bad.reindex(df.index, fill_value=False))}, e.g. {values[bad].iloc[0]!r})"
            )
            continue  # value rules make no sense on a mistyped column

        if col.get("unique") and values.duplicated().any():
            errors.append(f"{name}: {int(values.duplicated().sum())} duplicate values")
        if "allowed" in col:
            bad = ~values.isin([str(a) for a in col["allowed"]])
            if bad.any():
                errors.append(f"{name}: {int(bad.sum())} values not in {col['allowed']} (e.g. {values[bad].iloc[0]!r})")
        if "regex" in col:
            bad = ~values.str.fullmatch(col["regex"])
            if bad.any():
                errors.append(f"{name}: {int(bad.sum())} values do not match /{col['regex']}/ (e.g. {values[bad].iloc[0]!r})")
        if ("min" in col or "max" in col) and ctype in ("integer", "float"):
            nums = pd.to_numeric(values)
            if "min" in col and (nums < col["min"]).any():
                errors.append(f"{name}: {int((nums < col['min']).sum())} values below min {col['min']}")
            if "max" in col and (nums > col["max"]).any():
                errors.append(f"{name}: {int((nums > col['max']).sum())} values above max {col['max']}")
    return errors


def cast_frame(df, contract: dict):
    """Trim, cast to the contract types, drop exact duplicates, derive the partition column."""
    import pandas as pd

    out = pd.DataFrame(index=df.index)
    for col in contract["columns"]:
        name, ctype = col["name"], col["type"]
        s = df[name].str.strip().replace("", pd.NA).astype("string")
        if ctype == "integer":
            out[name] = s.astype("Int64")
        elif ctype == "float":
            out[name] = s.astype("Float64")
        elif ctype == "boolean":
            out[name] = s.str.lower().map({**{k: True for k in _BOOL_TRUE}, **{k: False for k in _BOOL_FALSE}}).astype("boolean")
        elif ctype == "date":
            out[name] = pd.to_datetime(s, format="%Y-%m-%d")
        elif ctype == "timestamp":
            out[name] = pd.to_datetime(s, format="ISO8601", utc=True)
        else:
            out[name] = s

    out = out.drop_duplicates().reset_index(drop=True)

    part = contract["partition"]
    fmt = {"year": "%Y", "month": "%Y-%m", "day": "%Y-%m-%d"}[part.get("granularity", "month")]
    if out[part["source_column"]].isna().any():
        raise AirflowFailException(f"Partition source column '{part['source_column']}' contains nulls")
    out[part["column"]] = out[part["source_column"]].dt.strftime(fmt)
    return out


# --------------------------------------------------------------------------- DAG
@dag(
    dag_id="csv_to_parquet_minio",
    description="DHAP-42: validated CSV -> partitioned Parquet in MinIO",
    schedule=None,                 # trigger manually; catchup is off regardless
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 0},   # a contract violation is not transient; don't retry
    params={"csv_path": Param(DEFAULT_CSV, type="string", description="CSV to ingest")},
    tags=["DHAP-42", "minio", "parquet"],
)
def csv_to_parquet_minio():
    @task
    def read_csv(**context) -> str:
        path = Path(context["params"]["csv_path"])
        if not path.is_file():
            raise AirflowFailException(f"CSV not found: {path}")
        return str(path)

    @task
    def validate_contract(csv_path: str) -> str:
        contract = load_contract()
        df = read_raw(csv_path)
        errors = check_contract(df, contract)
        if errors:
            raise AirflowFailException("Schema contract violated:\n - " + "\n - ".join(errors))
        print(f"Contract OK: {len(df)} rows, {len(df.columns)} columns")
        return csv_path

    @task
    def transform(csv_path: str, **context) -> str:
        import pyarrow as pa
        import pyarrow.parquet as pq

        contract = load_contract()
        out = cast_frame(read_raw(csv_path), contract)
        table = pa.Table.from_pandas(out, preserve_index=False)
        for col in contract["columns"]:             # Arrow date32 instead of timestamp for date columns
            if col["type"] == "date":
                i = table.schema.get_field_index(col["name"])
                table = table.set_column(i, col["name"], table.column(i).cast(pa.date32()))

        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        staging = STAGING_DIR / (re.sub(r"[^A-Za-z0-9_.-]", "_", context["run_id"]) + ".parquet")
        pq.write_table(table, staging)
        print(f"Transformed {table.num_rows} rows -> {staging}")
        return str(staging)

    @task
    def write_parquet_to_minio(staging_path: str) -> dict:
        import pyarrow as pa
        import pyarrow.dataset as ds
        import pyarrow.parquet as pq
        from airflow.hooks.base import BaseHook
        from pyarrow import fs

        contract = load_contract()
        conn = BaseHook.get_connection(MINIO_CONN_ID)
        scheme, _, host = conn.extra_dejson["endpoint_url"].partition("://")
        s3 = fs.S3FileSystem(access_key=conn.login, secret_key=conn.password, endpoint_override=host, scheme=scheme, allow_bucket_creation=True)
        s3.create_dir(BUCKET)

        table = pq.read_table(staging_path)
        pcol = contract["partition"]["column"]
        base_dir = f"{BUCKET}/{contract['dataset']}"
        ds.write_dataset(
            table,
            base_dir=base_dir,
            filesystem=s3,
            format="parquet",
            partitioning=ds.partitioning(pa.schema([table.schema.field(pcol)]), flavor="hive"),
            basename_template="part-{i}.parquet",
            existing_data_behavior="delete_matching",   # re-runs overwrite the partitions they touch
        )
        Path(staging_path).unlink(missing_ok=True)
        result = {"rows": table.num_rows, "location": f"s3://{base_dir}"}
        print(result)
        return result

    write_parquet_to_minio(transform(validate_contract(read_csv())))


csv_to_parquet_minio()
