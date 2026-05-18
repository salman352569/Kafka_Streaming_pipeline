from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col, count, avg, round as spark_round, when, to_date
import os
import sys
import logging

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log=logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
SILVER_PATH = os.getenv("SILVER_OUTPUT_PATH", "/tmp/kafka_project/silver_output")
GOLD_PATH   = os.getenv("GOLD_OUTPUT_PATH",   "/tmp/kafka_project/gold_data")

# ─────────────────────────────────────────────────────────────
#  HELPER: save one gold table
# ─────────────────────────────────────────────────────────────
def save_gold_table(df: DataFrame, name: str) -> None:
    path =f"{GOLD_PATH}/{name}"
    df.coalesce(1).write.mode("overwrite").json(path)
    log.info(" Saved gold tables -> %s (%d rows )",name,df.count())


def build_gold_tables(silver_df:DataFrame)->None:
    log.info("Building gold tables ....")
    


    # 1. Severity Summary
    # Q: How many patients per severity level? What's their avg age/labs?
    gold_severity = (
    silver_df.groupBy("severity") \
        .agg(
            count("*").alias("patient_count"),
            spark_round(avg("age"), 1).alias("avg_age"),
            spark_round(avg("wbc"), 1).alias("avg_wbc"),
            spark_round(avg("hgb"), 1).alias("avg_hgb")
        ) \
        .orderBy(col("patient_count").desc())
    )
    save_gold_table(gold_severity,"Severity Summary")
    # 2. Age Group Summary
     # Q: Which age bracket has the highest average severity?
    gold_age_group = silver_df.groupBy("age_group") \
        .agg(
            count("*").alias("patient_count"),
            spark_round(avg("severity_score"), 2).alias("avg_severity_score"),
            spark_round(avg("wbc"),2).alias("avg_wbc"),
            spark_round(avg("hgb"),2).alias("avg_hgb"),
        ) \
        .orderBy(col("age_group"))
    save_gold_table(gold_age_group,"age_group_summary")

    # 3. Gender Summary
    # Q: Male vs female patient distribution?
    # FIX: gender is now consistently uppercase (MALE / FEMALE)
    gold_gender = silver_df.groupBy("gender") \
            .agg(count("*").alias("patient_count")) \
            .orderBy(col("patient_count").desc())
    
    save_gold_table(gold_gender,"gender_summary")

    # 4. WBC Status Summary
    # Q: How many patients have abnormal white blood cell counts?
    gold_wbc =( 
    silver_df.groupBy("wbc_status") \
            .agg(
                count("*").alias("patient_count"),
                spark_round(avg("age"), 1).alias("avg_age")
            ) \
            .orderBy(col("patient_count").desc())
    )
    save_gold_table(gold_wbc,"wbc_summary")

    # 5. HGB Status Summary
    # Q: How many patients have low/high haemoglobin?
    gold_hgb = (
    silver_df.groupBy("hgb_status") \
        .agg(count("*").alias("patient_count")) \
        .orderBy(col("patient_count").desc())
    )
    save_gold_table(gold_hgb,"hgb_summary")

    # 6. Daily Trends
    # Q: How many patients arrived each day? How many were severe?
    if "timestamp" in silver_df.columns:
        gold_daily = (
        silver_df.withColumn("date", to_date(col("timestamp"))) \
            .groupBy("date") \
            .agg(
                count("*").alias("patient_per_day"),
                spark_round(avg("age"), 1).alias("avg_age"),
                count(when(col("severity") == "SEVERE", True)).alias("severe_cases")
            ) \
            .orderBy(col("date").desc())
        )
        save_gold_table(gold_daily,"daily_trends")
    else:
        log.warning("'timestamp' column missing - skipping daily_trends table")

    # 7. Top 10 Doctors ------------------------------------------------------------------
    # Q: Which doctors are handling the most patients?
    #
    gold_top_doctors = (
    silver_df.groupBy("doctor_id", "doctor_name") \
        .agg(count("*").alias("patients_treated")) \
        .orderBy(col("patients_treated").desc()) \
        .limit(10)
    )
    save_gold_table(gold_top_doctors,"top_doctors")

    # 8. Diagnosis Summary
    # Q: What are the 10 most common diagnoses in this hospital?
    gold_diagnosis = (
    silver_df.groupBy("diagnosis_code", "diagnosis_desc") \
        .agg(count("*").alias("patient_count")) \
        .orderBy(col("patient_count").desc()) \
        .limit(10)
    )
    save_gold_table(gold_diagnosis,"diagnosis summary")
     # ── 9. Allergy Summary ───────────────────────────────────
    # Q: What allergies are most common? Critical for prescribing doctors.
    gold_allergy = (
        silver_df.groupBy("allergy")
        .agg(count("*").alias("patient_count"))
        .orderBy(col("patient_count").desc())
    )
    save_gold_table(gold_allergy, "allergy_summary")

    log.info("All gold tables saved to : %s", GOLD_PATH)
def main():
    spark =(
         SparkSession.builder
         .appName("gold_layer_batch")
         .master("local[*]")
         .getOrCreate()
     )
    spark.sparkContext.setLogLevel("WARN")

    log.info("=" * 60)
    log.info("GOLD LAYER BATCH STARTED")
    log.info("Reading silver data from : %s",SILVER_PATH)
    log.info("Writing gold data to     :%s",GOLD_PATH)
    log.info("=" * 60)

     
    # ── Load silver data ──────────────────────────────────────

    try:
        silver_df=spark.read.json(SILVER_PATH)
    except Exception as exc:
        log.error("Silver layer is empty .Run consumer.py first. then retry ")
        spark.stop()
        raise SystemExit(1)
    
    total_records =silver_df.count()
    log.info("Silver record loaded : %d",total_records)

    build_gold_tables(silver_df)

    log.info("Gold layer job completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()