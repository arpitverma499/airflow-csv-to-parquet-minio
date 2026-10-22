# DHAP-42 — Local CSV → validated, partitioned Parquet in MinIO

Airflow DAG `csv_to_parquet_minio`: `read_csv → validate_contract → transform → write_parquet_to_minio`.
Airflow and MinIO run locally with Docker Compose. The contract (`schema_contract.yaml`) is enforced at
runtime; a violating CSV fails the DAG **before anything is written**.

## Run
```bash
cp .env.example .env          # edit passwords; on Linux set AIRFLOW_UID=$(id -u)
mkdir -p logs
docker compose up -d          # first start installs pandas/pyarrow/pyyaml, give it a minute
```
- Airflow UI: http://localhost:8080 (user/password from `.env`)
- MinIO console: http://localhost:9001 (root user/password from `.env`) — bucket `datalake` exists, empty.

Unpause and trigger `csv_to_parquet_minio` in the UI (or `docker compose exec airflow-scheduler airflow dags trigger csv_to_parquet_minio`).

## Verify
Successful run → in the console, `datalake/orders/order_month=2026-01/part-0.parquet` (and `-02`, `-03`).

Contract failure (negative tests) — trigger with a config override, expect `validate_contract` to fail and no new objects:
```bash
docker compose exec airflow-scheduler airflow dags trigger csv_to_parquet_minio \
  --conf '{"csv_path": "/opt/airflow/dags/extraction/bad_renamed_column.csv"}'
docker compose exec airflow-scheduler airflow dags trigger csv_to_parquet_minio \
  --conf '{"csv_path": "/opt/airflow/dags/extraction/bad_mistyped_value.csv"}'
```
The task log lists every violation (missing/unexpected columns, bad types, nulls, duplicates, allowed values, ranges) with row numbers.

## Contract format (`schema_contract.yaml`)
Per column: `name`, `type` (string/integer/float/boolean/date/timestamp), `nullable`, and optional
`unique`, `min`, `max`, `allowed`, `regex`. `strict_columns: true` rejects extra columns. `partition`
derives the Hive partition column from a date column (`year|month|day`).

## Design notes
- Everything is read as strings first, so type mismatches are reported by the contract, not by pandas.
- Writes use `existing_data_behavior="delete_matching"`: re-running overwrites the partitions it touches (idempotent).
- Credentials come from `.env` → Airflow connection `minio_default` (`AIRFLOW_CONN_MINIO_DEFAULT`); none in code.
- Transform → write hand-off uses a staging Parquet in `/tmp` (fine for LocalExecutor; use object storage for distributed executors).
- No heavy imports or I/O at DAG top level; `catchup=False`, `retries=0`.

## Reset
`docker compose down -v` removes Postgres and MinIO data.
