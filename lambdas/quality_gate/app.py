import json
import boto3
import os

s3 = boto3.client("s3")

PROJECT_PREFIX = os.environ.get("PROJECT_PREFIX", "project_step_fuunction")
DEFAULT_THRESHOLD = float(os.environ.get("DEFAULT_THRESHOLD", "0.7"))

def handler(event, context):
    bucket = event["BUCKET"]
    run_id = event["run_id"]
    threshold = float(event.get("threshold", DEFAULT_THRESHOLD))

    key = f"{PROJECT_PREFIX}/audit/run_summaries/{run_id}/jobA_summary.json"

    obj = s3.get_object(Bucket=bucket, Key=key)
    summary = json.loads(obj["Body"].read().decode("utf-8"))

    score = float(summary["metrics"]["overall_quality_score"])

    return {
        "run_id": run_id,
        "bucket": bucket,
        "summary_key": key,
        "threshold": threshold,
        "quality_score": score,
        "pass": score >= threshold
    }


