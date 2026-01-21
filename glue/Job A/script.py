import sys
import uuid
import logging
import json
import boto3
from urllib.parse import urlparse
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# -----------------------------
# Job args
# -----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------
# Logging (CloudWatch)
# -----------------------------
logger = logging.getLogger("jobA_raw_to_validated")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
if not logger.handlers:
    logger.addHandler(handler)

# -----------------------------
# Helpers
# -----------------------------
def put_json_to_s3(s3_uri: str, payload: dict):
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

# -----------------------------
# Config
# -----------------------------
BUCKET = args["BUCKET"]
PROJECT_PREFIX = "project_step_fuunction"

RAW_TRIPS_PATH = f"s3://{BUCKET}/{PROJECT_PREFIX}/raw/yellow_trips/"
RAW_ZONES_PATH = f"s3://{BUCKET}/{PROJECT_PREFIX}/raw/taxi_zone_lookup/"

VALID_TRIPS_PATH = f"s3://{BUCKET}/{PROJECT_PREFIX}/validated/yellow_trips/"
VALID_ZONES_PATH = f"s3://{BUCKET}/{PROJECT_PREFIX}/validated/taxi_zone_lookup/"

REJECT_TRIPS_PATH = f"s3://{BUCKET}/{PROJECT_PREFIX}/rejects/validated/yellow_trips/"
REJECT_ZONES_PATH = f"s3://{BUCKET}/{PROJECT_PREFIX}/rejects/validated/taxi_zones/"

run_id = None
if "--run_id" in sys.argv:
    try:
        run_id = getResolvedOptions(sys.argv, ["run_id"]).get("run_id")
    except Exception:
        run_id = None
run_id = run_id or str(uuid.uuid4())

ts = datetime.now(timezone.utc).isoformat()

AUDIT_SUMMARY_PATH = (
    f"s3://{BUCKET}/{PROJECT_PREFIX}/audit/run_summaries/{run_id}/jobA_summary.json"
)

logger.info(f"START JobA run_id={run_id} ts={ts}")

try:
    # =====================================================
    # READ RAW DATA
    # =====================================================
    logger.info("Reading raw trips...")
    trips_raw = spark.read.parquet(RAW_TRIPS_PATH)

    logger.info("Reading raw zones...")
    zones_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(RAW_ZONES_PATH)
    )

    # =====================================================
    # PREPARE TRIPS
    # =====================================================
    trips = (
        trips_raw
        .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
    )

    # Cast amount columns safely
    amount_cols = [
        "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", "total_amount",
        "congestion_surcharge", "airport_fee", "cbd_congestion_fee"
    ]

    for c in amount_cols:
        if c in trips.columns:
            trips = trips.withColumn(c, F.col(c).cast("double"))

    # =====================================================
    # TRIP VALIDATION RULES (BASE + AMOUNT CHECKS)
    # =====================================================
    amount_rules = (
        F.col("total_amount").isNotNull() &
        (F.col("total_amount") > 0) &
        (F.col("fare_amount") >= 0) &
        (F.col("tip_amount").isNull() | (F.col("tip_amount") >= 0)) &
        (F.col("tolls_amount").isNull() | (F.col("tolls_amount") >= 0)) &
        (F.col("extra").isNull() | (F.col("extra") >= 0)) &
        (F.col("mta_tax").isNull() | (F.col("mta_tax") >= 0)) &
        (F.col("improvement_surcharge").isNull() | (F.col("improvement_surcharge") >= 0)) &
        (F.col("congestion_surcharge").isNull() | (F.col("congestion_surcharge") >= 0)) &
        (F.col("airport_fee").isNull() | (F.col("airport_fee") >= 0)) &
        (F.col("cbd_congestion_fee").isNull() | (F.col("cbd_congestion_fee") >= 0))
    )

    trip_rules = (
        F.col("VendorID").isNotNull() &
        F.col("RatecodeID").isNotNull() &
        F.col("PULocationID").isNotNull() &
        F.col("DOLocationID").isNotNull() &
        F.col("pickup_ts").isNotNull() &
        F.col("dropoff_ts").isNotNull() &
        (F.col("pickup_ts") <= F.col("dropoff_ts")) &
        (F.col("trip_distance") >= 0) &
        amount_rules
    )

    validated_trips = trips.filter(trip_rules)
    rejected_trips = trips.filter(~trip_rules)

    # Counts
    total_trips = trips.count()
    valid_trips = validated_trips.count()
    rej_trips = rejected_trips.count()
    trip_quality = (valid_trips / total_trips) if total_trips else 0.0

    # Rejection breakdown (amount-related)
    rej_total_amount_le_0 = rejected_trips.filter(
        F.col("total_amount").isNull() | (F.col("total_amount") <= 0)
    ).count()

    rej_negative_amount_any = rejected_trips.filter(
        (F.col("fare_amount") < 0) |
        (F.col("tip_amount") < 0) |
        (F.col("tolls_amount") < 0) |
        (F.col("extra") < 0) |
        (F.col("mta_tax") < 0) |
        (F.col("improvement_surcharge") < 0) |
        (F.col("congestion_surcharge") < 0) |
        (F.col("airport_fee") < 0) |
        (F.col("cbd_congestion_fee") < 0)
    ).count()

    logger.info(f"Trips total={total_trips} valid={valid_trips} rejected={rej_trips}")

    validated_trips.write.mode("overwrite").parquet(VALID_TRIPS_PATH)
    rejected_trips.write.mode("overwrite").parquet(REJECT_TRIPS_PATH)

    # =====================================================
    # ZONE VALIDATION
    # =====================================================
    zones = (
        zones_raw
        .withColumnRenamed("LocationID", "location_id")
        .withColumnRenamed("Borough", "borough")
        .withColumnRenamed("Zone", "zone_name")
    )

    zone_rules = (
        F.col("location_id").isNotNull() &
        F.col("borough").isNotNull() &
        (F.length(F.trim(F.col("borough"))) > 0) &
        F.col("zone_name").isNotNull() &
        (F.length(F.trim(F.col("zone_name"))) > 0)
    )

    validated_zones = zones.filter(zone_rules)
    rejected_zones = zones.filter(~zone_rules)

    total_zones = zones.count()
    valid_zones = validated_zones.count()
    rej_zones = rejected_zones.count()
    zone_quality = (valid_zones / total_zones) if total_zones else 0.0

    validated_zones.write.mode("overwrite").parquet(VALID_ZONES_PATH)
    rejected_zones.write.mode("overwrite").parquet(REJECT_ZONES_PATH)

    # =====================================================
    # SUMMARY JSON
    # =====================================================
    summary = {
        "run_id": run_id,
        "timestamp_utc": ts,
        "metrics": {
            "trips": {
                "total": int(total_trips),
                "valid": int(valid_trips),
                "rejected": int(rej_trips),
                "quality_score": float(trip_quality),
                "rejection_breakdown": {
                    "total_amount_null_or_le_0": int(rej_total_amount_le_0),
                    "any_negative_amount_field": int(rej_negative_amount_any)
                }
            },
            "zones": {
                "total": int(total_zones),
                "valid": int(valid_zones),
                "rejected": int(rej_zones),
                "quality_score": float(zone_quality)
            },
            "overall_quality_score": float(min(trip_quality, zone_quality))
        }
    }

    put_json_to_s3(AUDIT_SUMMARY_PATH, summary)
    logger.info("SUCCESS JobA completed")

except Exception as e:
    logger.exception(f"FAILED JobA run_id={run_id}")
    raise

finally:
    job.commit()
