"""OpenLineage helpers shared by the Spark jobs.

Lineage is sent to Marquez (OPENLINEAGE_URL). When the variable is not set, every
helper is a no-op, so the jobs also run without a lineage backend.
"""
import os
import uuid
from datetime import datetime, timezone

import requests


OPENLINEAGE_URL = os.getenv("OPENLINEAGE_URL")
OPENLINEAGE_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", "mevo")

# Set by the Airflow DAG (docker exec -e ...) so Spark runs appear under their Airflow task.
PARENT_JOB_NAMESPACE = os.getenv("OPENLINEAGE_PARENT_JOB_NAMESPACE")
PARENT_JOB_NAME = os.getenv("OPENLINEAGE_PARENT_JOB_NAME")
PARENT_RUN_ID = os.getenv("OPENLINEAGE_PARENT_RUN_ID")

PRODUCER = "https://github.com/Turbo-Blender/mevo-bikes-lakehouse"
RUN_EVENT_SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
PARENT_FACET_SCHEMA = "https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json#/$defs/ParentRunFacet"

LAKEHOUSE_NAMESPACE = "s3://lakehouse"


def has_parent():
    return all([PARENT_JOB_NAMESPACE, PARENT_JOB_NAME, PARENT_RUN_ID])


def with_lineage(builder):
    """Attach the OpenLineage Spark listener to a SparkSession builder."""
    if not OPENLINEAGE_URL:
        return builder

    builder = (
        builder
        .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", OPENLINEAGE_URL)
        .config("spark.openlineage.namespace", OPENLINEAGE_NAMESPACE)
    )

    if has_parent():
        builder = (
            builder
            .config("spark.openlineage.parentJobNamespace", PARENT_JOB_NAMESPACE)
            .config("spark.openlineage.parentJobName", PARENT_JOB_NAME)
            .config("spark.openlineage.parentRunId", PARENT_RUN_ID)
        )

    return builder


def emit_run_event(event_type, job_name, inputs, outputs):
    """Send one OpenLineage run event for a step Spark cannot report itself."""
    if not OPENLINEAGE_URL:
        return

    run = {"runId": str(uuid.uuid4())}
    if has_parent():
        run["facets"] = {
            "parent": {
                "_producer": PRODUCER,
                "_schemaURL": PARENT_FACET_SCHEMA,
                "run": {"runId": PARENT_RUN_ID},
                "job": {"namespace": PARENT_JOB_NAMESPACE, "name": PARENT_JOB_NAME},
            }
        }

    event = {
        "eventType": event_type,
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "run": run,
        "job": {"namespace": OPENLINEAGE_NAMESPACE, "name": job_name},
        "inputs": inputs,
        "outputs": outputs,
        "producer": PRODUCER,
        "schemaURL": RUN_EVENT_SCHEMA,
    }

    try:
        response = requests.post(f"{OPENLINEAGE_URL}/api/v1/lineage", json=event, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        # Lineage must never break the pipeline.
        print(f"Lineage event for {job_name} was not sent: {e}")


def api_dataset(base_url, feed_name):
    namespace, _, path = base_url.partition("//")[2].partition("/")
    return {"namespace": f"https://{namespace}", "name": f"{path}/{feed_name}.json"}


def bronze_dataset(table):
    return {"namespace": LAKEHOUSE_NAMESPACE, "name": f"bronze/mevo/{table}"}


def gold_dataset(table):
    return {"namespace": LAKEHOUSE_NAMESPACE, "name": f"gold/mevo/{table}"}


def clickhouse_dataset(host, port, database, table):
    return {"namespace": f"clickhouse://{host}:{port}", "name": f"{database}.{table}"}
