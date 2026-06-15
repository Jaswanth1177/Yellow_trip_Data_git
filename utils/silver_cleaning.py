# utils/silver_cleaning.py
from pyspark.sql.functions import col, to_timestamp, when, count, lit
from pyspark.sql.types import TimestampType

def run_silver(spark, bronze_path, silver_path, quarantine_path):
    """
    Reads from Bronze Delta.
    Applies quality rules — invalid rows go to quarantine (NOT dead-letter,
    dead-letter is for corrupt JSON, quarantine is for business rule violations).
    Clean rows written to Silver Delta.
    """

    print("📥 Reading from Bronze...")
    bronze_df = spark.read.format("delta").load(bronze_path)

    print(f"   Bronze row count: {bronze_df.count()}")

    # -----------------------------------------------------------
    # STEP 1: Type casting — convert string datetime to Timestamp
    # -----------------------------------------------------------
    typed_df = bronze_df.withColumn(
        "pickup_datetime",
        to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
    )

    # -----------------------------------------------------------
    # STEP 2: Define quality rules explicitly
    # Each rule has a name so you can explain WHY a row was rejected
    # -----------------------------------------------------------
    rules = {
        "vendor_id_not_null":       col("vendor_id").isNotNull(),
        "pickup_datetime_not_null": col("pickup_datetime").isNotNull(),
        "fare_amount_positive":     col("fare_amount") > 0,
        "passenger_count_valid":    col("passenger_count").between(1, 6),
        "trip_distance_positive":   col("trip_distance") > 0,
    }

    # -----------------------------------------------------------
    # STEP 3: Tag each row — does it pass ALL rules?
    # -----------------------------------------------------------
    validated_df = typed_df

    # Add a column for each rule result (True/False)
    for rule_name, rule_condition in rules.items():
        validated_df = validated_df.withColumn(rule_name, rule_condition)

    # A row passes only if ALL rules are True
    all_rules = list(rules.keys())
    pass_condition = " AND ".join(all_rules)

    good_df = validated_df.filter(pass_condition).drop(*all_rules)
    bad_df  = validated_df.filter(f"NOT ({pass_condition})")

    # -----------------------------------------------------------
    # STEP 4: Deduplication on Silver
    # -----------------------------------------------------------
    deduped_df = good_df.dropDuplicates(["vendor_id", "pickup_datetime"])

    print(f"   Rows passing quality rules: {deduped_df.count()}")
    print(f"   Rows quarantined:           {bad_df.count()}")

    # -----------------------------------------------------------
    # STEP 5: Write clean rows to Silver
    # -----------------------------------------------------------
    (deduped_df.write
     .format("delta")
     .mode("overwrite")
     .save(silver_path))

    # -----------------------------------------------------------
    # STEP 6: Write bad rows to quarantine with reason tagging
    # -----------------------------------------------------------
    if bad_df.count() > 0:
        (bad_df.write
         .format("delta")
         .mode("overwrite")
         .save(quarantine_path))

    print(f"✅ Silver complete → {silver_path}")
    print(f"⚠️  Quarantine    → {quarantine_path}")

    return deduped_df


def print_quality_report(spark, bronze_path, silver_path, quarantine_path):
    """
    Prints a quality summary — show this on Saturday to demonstrate
    you built programmatic validation checkpoints between layers.
    """
    bronze_count     = spark.read.format("delta").load(bronze_path).count()
    silver_count     = spark.read.format("delta").load(silver_path).count()
    quarantine_count = spark.read.format("delta").load(quarantine_path).count()

    print("""
╔══════════════════════════════════════════╗
║        DATA QUALITY REPORT               ║
╠══════════════════════════════════════════╣""")
    print(f"║  Bronze  (raw ingested rows):  {bronze_count:<10}║")
    print(f"║  Silver  (clean rows):         {silver_count:<10}║")
    print(f"║  Quarantine (failed rules):    {quarantine_count:<10}║")
    print(f"║  Pass rate:  {round(silver_count/bronze_count*100,1)}%{' '*25}║")
    print("╚══════════════════════════════════════════╝")