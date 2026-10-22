"""Run the DHAP-42 pipeline logic WITHOUT Docker or Airflow.
validate (schema contract) -> transform -> partitioned Parquet in ./local_output

Setup:  pip install pandas pyarrow pyyaml
Usage:  python run_local.py                                   # good file, expect success
        python run_local.py dags/extraction/bad_renamed_column.csv   # expect contract failure
"""
import importlib.util
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def _stub_airflow():
    """The DAG file imports Airflow at the top; stub it so its helper functions can be reused."""
    mods = {n: types.ModuleType(n) for n in
            ["airflow", "airflow.decorators", "airflow.exceptions", "airflow.models", "airflow.models.param"]}
    mods["airflow.decorators"].dag = lambda *a, **k: (lambda f: (lambda *a2, **k2: None))
    mods["airflow.decorators"].task = lambda *a, **k: (a[0] if a and callable(a[0]) else (lambda f: f))
    mods["airflow.exceptions"].AirflowFailException = type("AirflowFailException", (Exception,), {})
    mods["airflow.models.param"].Param = lambda *a, **k: None
    sys.modules.update(mods)


_stub_airflow()
spec = importlib.util.spec_from_file_location("dag", ROOT / "dags" / "csv_to_parquet_minio.py")
dag = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dag)

csv_path = sys.argv[1] if len(sys.argv) > 1 else str(ROOT / "dags" / "extraction" / "sample.csv")
contract = dag.load_contract()
raw = dag.read_raw(csv_path)

errors = dag.check_contract(raw, contract)
if errors:
    print("CONTRACT FAILED - nothing written:")
    for e in errors:
        print("  -", e)
    sys.exit(1)
print(f"Contract OK: {len(raw)} rows")

import pyarrow as pa
import pyarrow.dataset as ds

out = dag.cast_frame(raw, contract)
table = pa.Table.from_pandas(out, preserve_index=False)
for col in contract["columns"]:
    if col["type"] == "date":
        i = table.schema.get_field_index(col["name"])
        table = table.set_column(i, col["name"], table.column(i).cast(pa.date32()))

pcol = contract["partition"]["column"]
base = ROOT / "local_output" / contract["dataset"]
ds.write_dataset(
    table, base_dir=str(base), format="parquet",
    partitioning=ds.partitioning(pa.schema([table.schema.field(pcol)]), flavor="hive"),
    basename_template="part-{i}.parquet", existing_data_behavior="delete_matching",
)
print(f"Wrote {table.num_rows} rows to {base}")
for p in sorted(base.rglob("*.parquet")):
    print("  ", p.relative_to(ROOT))