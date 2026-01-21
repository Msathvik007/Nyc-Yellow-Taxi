import json
from datetime import datetime, timezone

import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ----------------------------
# JOB ARGS (recommended)
# ----------------------------
# In Glue Job parameters, pass:
# --BUCKET=nyc-yellowtaxi-s3-datalake
# --run_id=<some-run-id>
# Optional:
# --sns_topic_arn=arn:aws:sns:...
# --audit_table_name=<dynamodb-table-name>

import sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BUCKET", "run_id"]  # required
)
BUCKET = args["BUCKET"]
RUN_ID = args["run_id"]
PROJECT_PREFIX = "project_step_fuunction"

# optional args
SNS_TOPIC_ARN = None
AUDIT_TABLE_NAME = None

for opt in ["sns_topic_arn", "audit_table_name"]:
    if f"--{opt}" in " ".join(sys.argv):
        extra = getResolvedOptions(sys.argv, [opt])
        if opt == "sns_topic_arn":
            SNS_TOPIC_ARN = extra.get(opt)
        if opt == "audit_table_name":
            AUDIT_TABLE_NAME = extra.get(opt)

s3 = boto3.client("s3")
sns = boto3.client("sns")
dynamodb = boto3.resource("dynamodb") if AUDIT_TABLE_NAME else None

# ----------------------------
# CONFIG PATHS
# ----------------------------
VALID_TRIPS_PATH = f"s3://{BUCKET}/{PROJECT_PREFIX}/validated/yellow_trips/"

# Master inputs (from Job B)
MASTER_ZONES_GOLDEN_PATH = f"s3://{BUCKET}/{PROJECT_PREFIX}/master/taxi_zone_master/"
MASTER_ZONES_XREF_PATH   = f"s3://{BUCKET}/{PROJECT_PREFIX}/master/taxi_zone_master_xref/"
MASTER_VENDOR_PATH       = f"s3://{BUCKET}/{PROJECT_PREFIX}/master/vendor_master/"
MASTER_RATECODE_PATH     = f"s3://{BUCKET}/{PROJECT_PREFIX}/master/ratecode_master/"

# Curated outputs
CURATED_TRIPS_PATH  = f"s3://{BUCKET}/{PROJECT_PREFIX}/curated/yellow_trips_enriched/"
NEEDS_REVIEW_PATH   = f"s3://{BUCKET}/{PROJECT_PREFIX}/curated/yellow_trips_needs_review/"

# Audit output (S3)
AUDIT_PREFIX = f"{PROJECT_PREFIX}/audit/job_c/{RUN_ID}/"
AUDIT_KEY = f"{AUDIT_PREFIX}summary.json"

CURATED_BY = "system"

# ----------------------------
# READ
# ----------------------------
trips_v = spark.read.parquet(VALID_TRIPS_PATH)

zones_g = spark.read.parquet(MASTER_ZONES_GOLDEN_PATH)   # master_zone_id, zone_name, borough, service_zone, ...
zones_x = spark.read.parquet(MASTER_ZONES_XREF_PATH)     # master_zone_id, source_location_id, ...

vendors_m = spark.read.parquet(MASTER_VENDOR_PATH)       # vendor_id, vendor_name, ...
rate_m    = spark.read.parquet(MASTER_RATECODE_PATH)     # ratecode_id, ratecode_desc, ...

# ----------------------------
# REQUIRED COLUMNS CHECK (validated schema)
# ----------------------------
required_cols = ["VendorID", "RatecodeID", "PULocationID", "DOLocationID",
                 "tpep_pickup_datetime", "tpep_dropoff_datetime"]
missing = [c for c in required_cols if c not in trips_v.columns]
if missing:
    raise ValueError(f"Validated trips missing required columns: {missing}")

# ----------------------------
# STANDARDIZE join columns
# ----------------------------
trips = (
    trips_v
    .withColumn("pickup_location_id", F.col("PULocationID").cast("int"))
    .withColumn("dropoff_location_id", F.col("DOLocationID").cast("int"))
    .withColumn("vendor_id", F.col("VendorID").cast("int"))
    .withColumn("ratecode_id", F.col("RatecodeID").cast("int"))
    .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
    .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
)

# ----------------------------
# 1) Resolve pickup/dropoff -> master_zone_id using XREF
# ----------------------------
xref_pu = zones_x.select(
    F.col("source_location_id").cast("int").alias("pickup_location_id"),
    F.col("master_zone_id").alias("pickup_master_zone_id")
)

xref_do = zones_x.select(
    F.col("source_location_id").cast("int").alias("dropoff_location_id"),
    F.col("master_zone_id").alias("dropoff_master_zone_id")
)

cur = (
    trips
    .join(xref_pu, on="pickup_location_id", how="left")
    .join(xref_do, on="dropoff_location_id", how="left")
)

# ----------------------------
# 2) Enrich from Golden zones
# ----------------------------
gold_pu = zones_g.select(
    F.col("master_zone_id").alias("pickup_master_zone_id"),
    F.col("zone_name").alias("pickup_zone_name"),
    F.col("borough").alias("pickup_borough"),
    F.col("service_zone").alias("pickup_service_zone")
)

gold_do = zones_g.select(
    F.col("master_zone_id").alias("dropoff_master_zone_id"),
    F.col("zone_name").alias("dropoff_zone_name"),
    F.col("borough").alias("dropoff_borough"),
    F.col("service_zone").alias("dropoff_service_zone")
)

cur = (
    cur
    .join(gold_pu, on="pickup_master_zone_id", how="left")
    .join(gold_do, on="dropoff_master_zone_id", how="left")
)

# ----------------------------
# 3) Enrich Vendor & Ratecode
# ----------------------------
vendors = vendors_m.select(
    F.col("vendor_id").cast("int").alias("vendor_id"),
    F.col("vendor_name").alias("vendor_name")
)

ratecodes = rate_m.select(
    F.col("ratecode_id").cast("int").alias("ratecode_id"),
    F.col("ratecode_desc").alias("ratecode_desc")
)

cur = (
    cur
    .join(vendors, on="vendor_id", how="left")
    .join(ratecodes, on="ratecode_id", how="left")
)

# ----------------------------
# Curated audit columns
# ----------------------------
curated = (
    cur
    .withColumn("curated_flag", F.lit("Y"))
    .withColumn("curated_by", F.lit(CURATED_BY))
    .withColumn("curated_ts", F.current_timestamp())
    .withColumn("run_id", F.lit(RUN_ID))
)

# ----------------------------
# Needs-review (UNMAPPED lookups)
# - review_reason built via SQL filter(array(...)) to avoid NOT_ITERABLE error
# ----------------------------
needs_review = curated.filter(
    (F.col("pickup_master_zone_id").isNull()) |
    (F.col("dropoff_master_zone_id").isNull()) |
    ((F.col("vendor_id").isNotNull()) & (F.col("vendor_name").isNull())) |
    ((F.col("ratecode_id").isNotNull()) & (F.col("ratecode_desc").isNull()))
).withColumn(
    "review_reason",
    F.expr("""
      filter(array(
        case when pickup_master_zone_id is null then 'UNMAPPED_PICKUP_ZONE' end,
        case when dropoff_master_zone_id is null then 'UNMAPPED_DROPOFF_ZONE' end,
        case when vendor_id is not null and vendor_name is null then 'UNMAPPED_VENDOR' end,
        case when ratecode_id is not null and ratecode_desc is null then 'UNMAPPED_RATECODE' end
      ), x -> x is not null)
    """)
)

# ----------------------------
# Clean curated output
# Rule: require both zone mappings
# (Optionally you can also require vendor/ratecode mapping; keep it flexible)
# ----------------------------
curated_clean = curated.filter(
    (F.col("pickup_master_zone_id").isNotNull()) &
    (F.col("dropoff_master_zone_id").isNotNull())
)

# ----------------------------
# WRITE OUTPUTS
# Use partitioning to reduce scan cost
# Common: partition by pickup date
# ----------------------------
curated_clean = curated_clean.withColumn("pickup_date", F.to_date("pickup_ts"))
needs_review = needs_review.withColumn("pickup_date", F.to_date("pickup_ts"))

# For idempotency in demo pipelines, overwrite is fine.
# In production, prefer dynamic partitions + run_id paths.
(
    curated_clean
    .write
    .mode("overwrite")
    .partitionBy("pickup_date")
    .parquet(CURATED_TRIPS_PATH)
)

(
    needs_review
    .write
    .mode("overwrite")
    .partitionBy("pickup_date")
    .parquet(NEEDS_REVIEW_PATH)
)

# ----------------------------
# METRICS + AUDIT SUMMARY
# ----------------------------
total_rows = trips.count()
curated_rows = curated_clean.count()
needs_review_rows = needs_review.count()

join_miss_pickup = curated.filter(F.col("pickup_master_zone_id").isNull()).count()
join_miss_dropoff = curated.filter(F.col("dropoff_master_zone_id").isNull()).count()

vendor_unmapped = curated.filter((F.col("vendor_id").isNotNull()) & (F.col("vendor_name").isNull())).count()
ratecode_unmapped = curated.filter((F.col("ratecode_id").isNotNull()) & (F.col("ratecode_desc").isNull())).count()

now = datetime.now(timezone.utc).isoformat()

audit_summary = {
    "job": "job_c_validated_to_curated",
    "run_id": RUN_ID,
    "bucket": BUCKET,
    "generated_at": now,
    "inputs": {
        "validated_trips_path": VALID_TRIPS_PATH,
        "master_paths": {
            "zones_golden": MASTER_ZONES_GOLDEN_PATH,
            "zones_xref": MASTER_ZONES_XREF_PATH,
            "vendor": MASTER_VENDOR_PATH,
            "ratecode": MASTER_RATECODE_PATH
        }
    },
    "outputs": {
        "curated_trips_path": CURATED_TRIPS_PATH,
        "needs_review_path": NEEDS_REVIEW_PATH
    },
    "row_counts": {
        "validated_rows": total_rows,
        "curated_rows": curated_rows,
        "needs_review_rows": needs_review_rows
    },
    "join_quality": {
        "pickup_zone_unmapped_rows": join_miss_pickup,
        "dropoff_zone_unmapped_rows": join_miss_dropoff,
        "vendor_unmapped_rows": vendor_unmapped,
        "ratecode_unmapped_rows": ratecode_unmapped
    },
    "decision": {
        "needs_review": needs_review_rows > 0
    }
}

# Write audit summary JSON to S3
s3.put_object(
    Bucket=BUCKET,
    Key=AUDIT_KEY,
    Body=json.dumps(audit_summary, indent=2).encode("utf-8"),
    ContentType="application/json"
)


# ----------------------------
# OPTIONAL: Audit log to DynamoDB (dedicated audit table)
# Table design recommendation:
# - PK: run_id (string)
# - SK: job_name or timestamp (string)  [optional]
# - store summary JSON as attribute or selected fields
# ----------------------------
if AUDIT_TABLE_NAME:
    table = dynamodb.Table(AUDIT_TABLE_NAME)

    # Keep DynamoDB item small: store key metrics + audit s3 pointer
    table.put_item(
        Item={
            "run_id": RUN_ID,
            "job": "job_c_validated_to_curated",
            "generated_at": now,
            "validated_rows": total_rows,
            "curated_rows": curated_rows,
            "needs_review_rows": needs_review_rows,
            "needs_review_path": NEEDS_REVIEW_PATH,
            "curated_path": CURATED_TRIPS_PATH,
            "audit_summary_s3": f"s3://{BUCKET}/{AUDIT_KEY}"
        }
    )

print("Job C completed successfully.")
print(json.dumps(audit_summary, indent=2))
