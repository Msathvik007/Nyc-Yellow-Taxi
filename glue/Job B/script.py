import sys
import uuid
import json
import boto3
import logging
from urllib.parse import urlparse
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------
# Glue bootstrap
# -----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger("jobB_validated_to_master")
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

def normalize_text(c):
    # lower + trim + collapse multiple spaces
    return F.regexp_replace(F.lower(F.trim(c)), r"\s+", " ")

def add_master_audit_cols(df, created_by: str, approved_by: str, version: int):
    return (
        df.withColumn("active_flag", F.lit("Y"))
          .withColumn("created_by", F.lit(created_by))
          .withColumn("created_ts", F.current_timestamp())
          .withColumn("updated_by", F.lit(None).cast("string"))
          .withColumn("updated_ts", F.lit(None).cast("timestamp"))
          .withColumn("approved_by", F.lit(approved_by))
          .withColumn("approved_ts", F.current_timestamp())
          .withColumn("version", F.lit(version).cast("int"))
    )

# -----------------------------
# Config
# -----------------------------
BUCKET = args["BUCKET"].strip()  # protect against accidental leading/trailing spaces
PROJECT_PREFIX = "project_step_fuunction"

# Optional run_id param
run_id = None
if "--run_id" in sys.argv:
    try:
        run_id = getResolvedOptions(sys.argv, ["run_id"]).get("run_id")
    except Exception:
        run_id = None
run_id = run_id or str(uuid.uuid4())

ts = datetime.now(timezone.utc).isoformat()

# Inputs (Validated)
VALID_ZONES_PATH = f"s3://{BUCKET}/{PROJECT_PREFIX}/validated/taxi_zone_lookup/"

# Outputs (Master)
MASTER_ZONES_GOLDEN_PATH = f"s3://{BUCKET}/{PROJECT_PREFIX}/master/taxi_zone_master/"
MASTER_ZONES_XREF_PATH   = f"s3://{BUCKET}/{PROJECT_PREFIX}/master/taxi_zone_master_xref/"
MASTER_VENDOR_PATH       = f"s3://{BUCKET}/{PROJECT_PREFIX}/master/vendor_master/"
MASTER_RATECODE_PATH     = f"s3://{BUCKET}/{PROJECT_PREFIX}/master/ratecode_master/"

# Audit summary for governance
AUDIT_SUMMARY_PATH = f"s3://{BUCKET}/{PROJECT_PREFIX}/audit/run_summaries/{run_id}/jobB_summary.json"

# Master audit columns
CREATED_BY  = "data_owner_and_data_steward"
APPROVED_BY = "data_steward"
VERSION     = 1

# Fuzzy settings
FUZZY_THRESHOLD = 93
FUZZY_ALGO = "token_sort_ratio"  # token_sort_ratio | token_set_ratio

logger.info(f"START JobB run_id={run_id} ts={ts}")
logger.info(f"VALID_ZONES_PATH={VALID_ZONES_PATH}")
logger.info(f"MASTER_ZONES_GOLDEN_PATH={MASTER_ZONES_GOLDEN_PATH}")
logger.info(f"MASTER_ZONES_XREF_PATH={MASTER_ZONES_XREF_PATH}")
logger.info(f"MASTER_VENDOR_PATH={MASTER_VENDOR_PATH}")
logger.info(f"MASTER_RATECODE_PATH={MASTER_RATECODE_PATH}")
logger.info(f"AUDIT_SUMMARY_PATH={AUDIT_SUMMARY_PATH}")
logger.info(f"FUZZY settings algo={FUZZY_ALGO} threshold={FUZZY_THRESHOLD}")

try:
    # ----------------------------
    # 1) TAXI ZONE MASTER (FuzzyWuzzy golden + XREF)
    # ----------------------------
    logger.info("Reading validated zones parquet...")
    zones_v = spark.read.parquet(VALID_ZONES_PATH)

    # Expecting validated columns:
    # location_id, borough, zone_name, service_zone
    # (If your validated uses "Zone" or different names, fix here.)
    zones_base = (
        zones_v
        .withColumn("location_id", F.col("location_id").cast("int"))
        .withColumn("borough", F.col("borough").cast("string"))
        .withColumn("zone_name", F.col("zone_name").cast("string"))
        .withColumn("service_zone", F.col("service_zone").cast("string"))
    )

    # Basic DQ filters
    zones_base = zones_base.filter(
        F.col("location_id").isNotNull() &
        F.col("borough").isNotNull() &
        F.col("zone_name").isNotNull()
    )

    zones = (
        zones_base
        .withColumn("borough_norm", normalize_text(F.col("borough")))
        .withColumn("zone_norm", normalize_text(F.col("zone_name")))
        .withColumn("service_zone_norm", normalize_text(F.col("service_zone")))
    )

    validated_zone_rows = zones.count()
    logger.info(f"validated_zone_rows={validated_zone_rows}")

    # ---- Fuzzy clustering on driver (OK for taxi zones ~265 rows) ----
    logger.info("Running fuzzy clustering on driver...")
    from fuzzywuzzy import fuzz  # ensure dependency available in Glue runtime

    def fuzz_score(a: str, b: str) -> int:
        if FUZZY_ALGO == "token_set_ratio":
            return fuzz.token_set_ratio(a, b)
        return fuzz.token_sort_ratio(a, b)

    rows = (
        zones.select("location_id", "borough_norm", "zone_norm")
            .dropDuplicates(["location_id", "borough_norm", "zone_norm"])
            .collect()
    )

    # Union-Find clustering
    parent = {}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for r in rows:
        parent[r["location_id"]] = r["location_id"]

    # block by borough
    by_borough = {}
    for r in rows:
        by_borough.setdefault(r["borough_norm"], []).append((r["location_id"], r["zone_norm"]))

    fuzzy_comparisons = 0
    fuzzy_links = 0

    for borough, items in by_borough.items():
        n = len(items)
        for i in range(n):
            id_i, z_i = items[i]
            for j in range(i + 1, n):
                id_j, z_j = items[j]
                fuzzy_comparisons += 1
                if fuzz_score(z_i, z_j) >= FUZZY_THRESHOLD:
                    union(id_i, id_j)
                    fuzzy_links += 1

    logger.info(f"fuzzy_comparisons={fuzzy_comparisons} fuzzy_links={fuzzy_links}")

    cluster_map = [(loc_id, find(loc_id)) for loc_id in parent.keys()]
    cluster_df = spark.createDataFrame(cluster_map, ["location_id", "cluster_id"])

    zones_clustered = zones.join(cluster_df, on="location_id", how="left")

    # ---- Survivorship ----
    # Prefer:
    # 1) non-null service_zone
    # 2) longest zone_name
    # 3) smallest location_id
    w = Window.partitionBy("cluster_id").orderBy(
        F.col("service_zone").isNull().cast("int").asc(),
        F.length(F.col("zone_name")).desc(),
        F.col("location_id").asc()
    )

    survivors = (
        zones_clustered
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Golden record ID (stable): hash of business identity (borough_norm + zone_norm)
    zones_golden = (
        survivors
        .withColumn("biz_key", F.concat_ws("|", F.col("borough_norm"), F.col("zone_norm")))
        .withColumn("master_zone_id", F.sha2(F.col("biz_key"), 256))
    )

    zones_golden_master = (
        add_master_audit_cols(zones_golden, CREATED_BY, APPROVED_BY, VERSION)
        .select(
            "master_zone_id",
            F.col("location_id").alias("survivor_location_id"),
            "borough",
            "zone_name",
            "service_zone",
            "active_flag", "created_by", "created_ts",
            "updated_by", "updated_ts",
            "approved_by", "approved_ts",
            "version"
        )
    )

    golden_zone_count = zones_golden_master.count()
    logger.info(f"golden_zone_count={golden_zone_count}")

    # ---- XREF: map all source location_ids to the golden master_zone_id ----
    zones_xref = (
        zones_clustered
        .join(
            zones_golden.select("cluster_id", "master_zone_id"),
            on="cluster_id",
            how="left"
        )
    )

    zones_xref_master = (
        add_master_audit_cols(zones_xref, CREATED_BY, APPROVED_BY, VERSION)
        .select(
            "master_zone_id",
            F.col("location_id").alias("source_location_id"),
            F.lit("NYC_TLC").alias("source_system"),
            "active_flag", "created_by", "created_ts",
            "updated_by", "updated_ts",
            "approved_by", "approved_ts",
            "version"
        )
        .dropDuplicates(["master_zone_id", "source_location_id", "source_system"])
    )

    xref_rows = zones_xref_master.count()
    logger.info(f"zones_xref_rows={xref_rows}")

    # Write outputs
    logger.info("Writing Golden Zones Master...")
    zones_golden_master.write.mode("overwrite").parquet(MASTER_ZONES_GOLDEN_PATH)

    logger.info("Writing Zones XREF Master...")
    zones_xref_master.write.mode("overwrite").parquet(MASTER_ZONES_XREF_PATH)

    # ----------------------------
    # 2) VENDOR MASTER (dictionary-based)
    # ----------------------------
    logger.info("Building Vendor Master...")
    vendor_master = spark.createDataFrame(
        [
            (1, "Creative Mobile Technologies, LLC"),
            (2, "Curb Mobility, LLC"),
            (6, "Myle Technologies Inc"),
            (7, "Helix")
        ],
        ["vendor_id", "vendor_name"]
    )

    vendor_master = (
        add_master_audit_cols(vendor_master, CREATED_BY, APPROVED_BY, VERSION)
        .select(
            "vendor_id", "vendor_name",
            "active_flag", "created_by", "created_ts",
            "updated_by", "updated_ts",
            "approved_by", "approved_ts",
            "version"
        )
    )

    vendor_master_count = vendor_master.count()
    logger.info(f"vendor_master_count={vendor_master_count}")

    logger.info("Writing Vendor Master...")
    vendor_master.write.mode("overwrite").parquet(MASTER_VENDOR_PATH)

    # ----------------------------
    # 3) RATECODE MASTER (TLC enumeration)
    # ----------------------------
    logger.info("Building Ratecode Master...")
    ratecode_master = spark.createDataFrame(
        [
            (1, "Standard rate"),
            (2, "JFK"),
            (3, "Newark"),
            (4, "Nassau or Westchester"),
            (5, "Negotiated fare"),
            (6, "Group ride")
        ],
        ["ratecode_id", "ratecode_desc"]
    )

    ratecode_master = (
        add_master_audit_cols(ratecode_master, CREATED_BY, APPROVED_BY, VERSION)
        .select(
            "ratecode_id", "ratecode_desc",
            "active_flag", "created_by", "created_ts",
            "updated_by", "updated_ts",
            "approved_by", "approved_ts",
            "version"
        )
    )

    ratecode_master_count = ratecode_master.count()
    logger.info(f"ratecode_master_count={ratecode_master_count}")

    logger.info("Writing Ratecode Master...")
    ratecode_master.write.mode("overwrite").parquet(MASTER_RATECODE_PATH)

    # ----------------------------
    # Summary JSON (audit) for governance
    # ----------------------------
    summary = {
        "run_id": run_id,
        "bucket": BUCKET,
        "project_prefix": PROJECT_PREFIX,
        "timestamp_utc": ts,
        "inputs": {
            "validated_zones_path": VALID_ZONES_PATH
        },
        "outputs": {
            "master_zones_golden_path": MASTER_ZONES_GOLDEN_PATH,
            "master_zones_xref_path": MASTER_ZONES_XREF_PATH,
            "master_vendor_path": MASTER_VENDOR_PATH,
            "master_ratecode_path": MASTER_RATECODE_PATH
        },
        "metrics": {
            "validated_zone_rows": int(validated_zone_rows),
            "golden_zone_count": int(golden_zone_count),
            "zones_xref_rows": int(xref_rows),
            "vendor_master_count": int(vendor_master_count),
            "ratecode_master_count": int(ratecode_master_count),
            "fuzzy_algo": FUZZY_ALGO,
            "fuzzy_threshold": int(FUZZY_THRESHOLD),
            "fuzzy_comparisons": int(fuzzy_comparisons),
            "fuzzy_links": int(fuzzy_links)
        }
    }

    logger.info(f"Writing JobB summary JSON -> {AUDIT_SUMMARY_PATH}")
    put_json_to_s3(AUDIT_SUMMARY_PATH, summary)
    logger.info("JobB summary JSON written successfully.")

    logger.info("✅ Job B completed: MASTER tables written")
    logger.info(f"Golden Zones: {MASTER_ZONES_GOLDEN_PATH}")
    logger.info(f"Zones XREF: {MASTER_ZONES_XREF_PATH}")
    logger.info(f"Vendor Master: {MASTER_VENDOR_PATH}")
    logger.info(f"Ratecode Master: {MASTER_RATECODE_PATH}")

except Exception as e:
    logger.exception(f"FAILED JobB run_id={run_id} error={str(e)}")
    raise

finally:
    job.commit()
