import os
import time
import json
import boto3
from datetime import datetime, date
from typing import List, Dict, Any

rsd = boto3.client("redshift-data")

# Serverless Redshift
WORKGROUP = "nyc-taxi-ns"
DATABASE = "dev"
SECRET_ARN = "arn:aws:secretsmanager:us-east-2:887549718907:secret:redshift-cred-BYTFPM"

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "2"))
MAX_POLLS = int(os.getenv("MAX_POLLS", "600"))

# Safety: Redshift Data API batch has practical limits. Keep chunks reasonable.
BATCH_CHUNK_SIZE = int(os.getenv("BATCH_CHUNK_SIZE", "15"))


def json_safe(obj: Any) -> Any:
    """
    Convert non-JSON-serializable objects (datetime/date) into safe values.
    Applies recursively to dicts/lists.
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_safe(v) for v in obj]
    return obj


def _poll(statement_id: str) -> Dict[str, Any]:
    """Poll DescribeStatement until finished/failed/aborted."""
    for _ in range(MAX_POLLS):
        desc = rsd.describe_statement(Id=statement_id)
        status = desc.get("Status")
        if status in ("FINISHED", "FAILED", "ABORTED"):
            return desc
        time.sleep(POLL_SECONDS)

    # Timed out waiting
    return {
        "Id": statement_id,
        "Status": "TIMEOUT",
        "Error": f"Timed out waiting after {MAX_POLLS * POLL_SECONDS} seconds"
    }


def _run_batch(sqls: List[str], label: str) -> Dict[str, Any]:
    """Run a list of statements as a single batch."""
    resp = rsd.batch_execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        SecretArn=SECRET_ARN,
        Sqls=sqls,
        StatementName=label
    )
    statement_id = resp["Id"]
    final = _poll(statement_id)

    # If failed, include error details
    if final.get("Status") in ("FAILED", "ABORTED", "TIMEOUT"):
        return {
            "ok": False,
            "batch_label": label,
            "statement_id": statement_id,
            "final_status": final.get("Status"),
            "error": final.get("Error"),
            "describe": final
        }

    return {
        "ok": True,
        "batch_label": label,
        "statement_id": statement_id,
        "final_status": final.get("Status"),
        "describe": final
    }


def _run_select(sql: str, label: str, max_rows: int = 50) -> Dict[str, Any]:
    """Run a SELECT and return results."""
    resp = rsd.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        SecretArn=SECRET_ARN,
        Sql=sql,
        StatementName=label
    )
    statement_id = resp["Id"]
    final = _poll(statement_id)

    if final.get("Status") in ("FAILED", "ABORTED", "TIMEOUT"):
        return {
            "ok": False,
            "label": label,
            "statement_id": statement_id,
            "final_status": final.get("Status"),
            "error": final.get("Error"),
            "sql": sql
        }

    # Fetch up to max_rows (Data API paginates; keep demo simple)
    result = rsd.get_statement_result(Id=statement_id)

    return {
        "ok": True,
        "label": label,
        "statement_id": statement_id,
        "rows": result.get("Records", [])[:max_rows],
        "columns": [c.get("name") for c in result.get("ColumnMetadata", [])]
    }


def lambda_handler(event, context):
    """
    Expected event format:
    {
      "pipeline_label": "nyc-taxi-load",
      "sql_batches": [
        {"label": "ddl_and_load", "sql": ["stmt1", "stmt2", ...]},
        {"label": "scd2_zone", "sql": ["stmtA", "stmtB", ...]}
      ],
      "select_checks": [
        {"label": "count_fact", "sql": "select count(*) as cnt from dw.fact_trips;"}
      ]
    }
    """
    if not WORKGROUP or not DATABASE or not SECRET_ARN:
        raise ValueError("Missing required config: WORKGROUP, DATABASE, SECRET_ARN")

    pipeline_label = event.get("pipeline_label", "nyc-taxi-redshift")
    sql_batches = event.get("sql_batches", [])
    select_checks = event.get("select_checks", [])

    results = {
        "pipeline_label": pipeline_label,
        "batches": [],
        "checks": []
    }

    # Run batches (DDL/DML)
    for batch in sql_batches:
        label = batch["label"]
        sqls = [s.strip().rstrip(";") + ";" for s in batch.get("sql", []) if s and str(s).strip()]

        # Chunk to avoid oversized requests
        for i in range(0, len(sqls), BATCH_CHUNK_SIZE):
            chunk = sqls[i:i + BATCH_CHUNK_SIZE]
            chunk_label = f"{label}_{i // BATCH_CHUNK_SIZE + 1}"

            out = _run_batch(chunk, f"{pipeline_label}:{chunk_label}")
            results["batches"].append(out)

            if not out["ok"]:
                # Fail fast so Step Functions fails cleanly
                raise Exception(json.dumps({
                    "message": "Redshift batch failed",
                    "failed_batch": out
                }))

    # Optional validation queries (SELECT)
    for chk in select_checks:
        out = _run_select(chk["sql"], f"{pipeline_label}:check:{chk['label']}")
        results["checks"].append(out)

        if not out["ok"]:
            raise Exception(json.dumps({
                "message": "Redshift validation check failed",
                "failed_check": out
            }))

    # ✅ MAKE RESPONSE JSON-SAFE (fixes Runtime.MarshalError)
    results = json_safe(results)

    return results
