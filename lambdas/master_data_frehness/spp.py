import json
import boto3
from datetime import datetime, timezone

s3 = boto3.client("s3")
sns = boto3.client("sns")


def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def lambda_handler(event, context):
    bucket = event["BUCKET"]
    run_id = event["run_id"]
    thresholds = event.get("thresholds", {})
    sns_topic_arn = event.get("sns_topic_arn")

    summary_key = f"project_step_fuunction/audit/run_summaries/{run_id}/jobB_summary.json"

    # --- Load Job B summary ---
    try:
        obj = s3.get_object(Bucket=bucket, Key=summary_key)
        summary = json.loads(obj["Body"].read())
    except Exception as e:
        raise Exception(f"Failed to read Job B summary: {e}")

    now = datetime.now(timezone.utc)

    results = []
    status = "PASS"

    for dataset, meta in summary.get("datasets", {}).items():
        if dataset not in thresholds:
            continue

        max_ts = parse_ts(meta["max_record_ts"])
        age_days = (now - max_ts).days

        warn_days = thresholds[dataset]["warn_days"]
        fail_days = thresholds[dataset]["fail_days"]

        dataset_status = "PASS"
        if age_days >= fail_days:
            dataset_status = "FAIL"
            status = "FAIL"
        elif age_days >= warn_days:
            dataset_status = "WARN"
            if status != "FAIL":
                status = "WARN"

        results.append({
            "dataset": dataset,
            "age_days": age_days,
            "warn_days": warn_days,
            "fail_days": fail_days,
            "status": dataset_status
        })

    # --- Send alert if needed ---
    if status in ("WARN", "FAIL") and sns_topic_arn:
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject=f"Master Data Freshness {status}",
            Message=json.dumps({
                "run_id": run_id,
                "overall_status": status,
                "details": results
            }, indent=2)
        )

    return {
        "pass": status == "PASS",
        "status": status,
        "checked_at": now.isoformat(),
        "results": results
    }
