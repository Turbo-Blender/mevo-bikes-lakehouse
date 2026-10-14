from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

SPARK_CONTAINER = "mevo-bikes-spark"
MINIO_CONTAINER = "minio"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
BUCKET = "lakehouse"

default_args = {
    "owner": "TurboBlender",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def spark_job(script):
    return f"docker exec {SPARK_CONTAINER} {SPARK_SUBMIT} /app/src/etl/{script}"


with DAG(
    dag_id="mevo_lakehouse",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    # Runs share the same Delta tables, so they must not overlap.
    max_active_runs=1,
    tags=["mevo", "lakehouse", "clickhouse"],
) as dag:

    ensure_bucket = BashOperator(
        task_id="ensure_bucket",
        bash_command=(
            f"docker exec {MINIO_CONTAINER} sh -c "
            "'mc alias set local http://localhost:9000 \"$MINIO_ROOT_USER\" \"$MINIO_ROOT_PASSWORD\" "
            f"&& mc mb --ignore-existing local/{BUCKET}'"
        ),
    )

    # API -> bronze (Delta in MinIO)
    extract = BashOperator(
        task_id="extract_bronze",
        bash_command=spark_job("extract.py"),
    )

    # bronze -> silver
    transform = BashOperator(
        task_id="transform_silver",
        bash_command=spark_job("transform.py"),
    )

    # silver -> gold (star schema + aggregates, Delta in MinIO)
    gold = BashOperator(
        task_id="build_gold",
        bash_command=spark_job("gold.py"),
    )

    # gold -> ClickHouse (data warehouse)
    load = BashOperator(
        task_id="load_clickhouse",
        bash_command=spark_job("load.py"),
    )

    ensure_bucket >> extract >> transform >> gold >> load
