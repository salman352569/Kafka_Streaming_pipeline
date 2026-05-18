from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, when, upper, lit
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
import logging
import os
import sys

# Set Python executable for Spark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log =logging.getLogger(__name__)
# ─────────────────────────────────────────────────────────────
#  CONFIG  (use environment variables so paths work on any machine)
# ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC",             "test_Topic")
SILVER_OUTPUT_PATH      = os.getenv("SILVER_OUTPUT_PATH",      "/tmp/kafka_project/silver_output")
CHECKPOINT_PATH         = os.getenv("CHECKPOINT_PATH",         "/tmp/kafka_project/checkpoint/silver")
TRIGGER_INTERVAL        = os.getenv("TRIGGER_INTERVAL",        "10 seconds")
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "200"))
 

# Define Schema
BRONZE_SCHEMA = (
     StructType()
    .add("id",             StringType())
    .add("timestamp",      StringType())
    .add("first_name",     StringType())
    .add("last_name",      StringType())
    .add("dob",            StringType())
    .add("age",            IntegerType())
    .add("gender",         StringType())
    .add("phone",          StringType())
    .add("address",        StringType())
    .add("doctor_id",      IntegerType())
    .add("doctor_name",    StringType())
    .add("ward",           IntegerType())
    .add("room",           IntegerType())
    .add("wbc",            DoubleType())
    .add("hgb",            DoubleType())
    .add("allergy",        StringType())
    .add("severity",       StringType())
    .add("diagnosis_code", StringType())
    .add("diagnosis_desc", StringType())
)
def clean_silver_data(df):
    """SILVER LAYER: Clean, validate, and transform data"""
    
    df = df.dropDuplicates(["id"])
    
    df = df.filter(
        (col("age").isNotNull()) & (col("age") > 0) & (col("age") < 120) &
        (col("wbc").isNotNull()) & (col("wbc") > 0) & (col("wbc") < 30) &
        (col("hgb").isNotNull()) & (col("hgb") > 5) & (col("hgb") < 25) &
        (col("id").isNotNull()) & (col("id") != "") &
        (col("first_name").isNotNull()) & (col("first_name") != "")
    )
    
    df = df.withColumn("gender", 
                       when(col("gender") == "M", "MALE")
                       .when(col("gender") == "F", "FEMALE")
                       .otherwise("UNKNOWN"))
    for field in ("first_name","last_name","doctor_name","severity"):
        df=df.withColumn(field,upper(col(field)))
    
     # STEP 5 — Standardise allergy (None the string → NO ALLERGY, rest → UPPERCASE)
    df = df.withColumn(
        "allergy",
        when(col("allergy") == "None", "NO ALLERGY").otherwise(upper(col("allergy"))),
    )
 
    # STEP 6 — Add age bucket (useful for grouping in gold layer)
    df = df.withColumn(
        "age_group",
        when(col("age") < 30, "YOUNG_ADULT")
        .when(col("age") < 50, "MIDDLE_AGED")
        .when(col("age") < 70, "SENIOR")
        .otherwise("ELDERLY"),
    )
 
    # STEP 7 — Classify WBC (White Blood Cell count)
    # Normal range: 4.0–10.0 × 10⁹/L
    df = df.withColumn(
        "wbc_status",
        when(col("wbc") < 4.0,  "LOW")
        .when(col("wbc") > 10.0, "HIGH")
        .otherwise("NORMAL"),
    )
 
    # STEP 8 — Classify HGB (Haemoglobin)
    # Normal range: 12–15 g/dL (general)
    df = df.withColumn(
        "hgb_status",
        when(col("hgb") < 12.0, "LOW")
        .when(col("hgb") > 15.0, "HIGH")
        .otherwise("NORMAL"),
    )
 
    # STEP 9 — Convert severity text → numeric score (useful for averaging in gold)
    df = df.withColumn(
        "severity_score",
        when(col("severity") == "MILD",     1)
        .when(col("severity") == "MODERATE", 2)
        .when(col("severity") == "SEVERE",   3)
        .otherwise(0),
    )
 
    # STEP 10 — Metadata columns
    df = df.withColumn("processed_date",      current_timestamp())
    df = df.withColumn("data_quality_status", lit("CLEAN"))
 
    # STEP 11 — Fill any remaining NULLs with safe defaults
    df = df.fillna({
        "allergy":    "UNKNOWN",
        "severity":   "UNKNOWN",
        "age_group":  "UNKNOWN",
        "wbc_status": "UNKNOWN",
        "hgb_status": "UNKNOWN",
    })
 
    return df
def write_silver_batch(df,batch_id:int):

    if df.rdd.isEmpty():
        log.info("Batch %d - empty,skipping.",batch_id)
        return
    
    record_count=df.count()
    log.info("Batch %d - writing %d records...",batch_id,record_count)

    (
        df.write
          .mode("append")
          .partitionBy("age_group")
          .json(SILVER_OUTPUT_PATH)
          
    )
    log.info("Batch %d - done. Records written : %d",batch_id,record_count)

# ─────────────────────────────────────────────────────────────
#  SPARK SESSION
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return(
        SparkSession.builder
        .appName("Kafka_cosumer_silver")
        .master("local[*]")
        .getOrCreate()
    ) 
#─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
def main():
    spark=create_spark_session()
    spark.sparkContext.setLogLevel("Warn")

    log.info("=" * 60)
    log.info("SILVER LAYER PROCESSING STARTED")
    log.info("Kafka topic    : %s", KAFKA_TOPIC)
    log.info("Silver output  : %s", SILVER_OUTPUT_PATH)
    log.info("Checkpoint     : %s", CHECKPOINT_PATH)
    log.info("=" * 60)
    
    bronze_df= (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVERS)
             .option("Subscribe", KAFKA_TOPIC)
             .option("startingOffsets","earliest")
             .option("maxOffsetsPerTrigger",MAX_OFFSETS_PER_TRIGGER)
             .option("failOnDataLoss","false")
             .load()
)
    parsed_df=(
        bronze_df
            .selectExpr("CAST(value As STRING) AS json_data")
            .withColumn("data", from_json(col("json_data"),BRONZE_SCHEMA))
            .select("data.*")
            .withColumn("event_time",current_timestamp())
        )
    # ── Apply silver cleaning rules ────────────────────────────
    silver_df = clean_silver_data(parsed_df)
 
    # ── Write each micro-batch using our batch writer ──────────
    query = (
        silver_df.writeStream
        .foreachBatch(write_silver_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
 
    log.info("Silver layer streaming is LIVE. Listening every %s...", TRIGGER_INTERVAL)
    query.awaitTermination()
 
 
if __name__ == "__main__":
    main()
    

