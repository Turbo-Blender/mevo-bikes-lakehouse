from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

SPARK_CONTAINER = "mevo-bikes-spark"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"

default_args = {
    "owner": "TurboBlender",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def spark_job(script):
    # The OpenLineage macros pass this Airflow task as the parent of the Spark run,
    # so the lineage graph links each Spark job to its task.
    return (
        "docker exec "
        "-e OPENLINEAGE_PARENT_JOB_NAMESPACE='{{ macros.OpenLineageProviderPlugin.lineage_job_namespace() }}' "
        "-e OPENLINEAGE_PARENT_JOB_NAME='{{ macros.OpenLineageProviderPlugin.lineage_job_name(task_instance) }}' "
        "-e OPENLINEAGE_PARENT_RUN_ID='{{ macros.OpenLineageProviderPlugin.lineage_run_id(task_instance) }}' "
        f"{SPARK_CONTAINER} {SPARK_SUBMIT} /app/src/etl/{script}"
    )


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

    extract >> transform >> gold >> load
