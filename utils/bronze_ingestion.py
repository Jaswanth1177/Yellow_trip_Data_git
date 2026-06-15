# utils/bronze_ingestion.py
from utils.schemas import bronze_schema

def run_bronze(spark, source_path, bronze_path, dead_letter_path):
    """
    Reads raw JSON from source_path.
    Good rows  → Delta table at bronze_path
    Bad rows   → dead_letter_path (JSON, for inspection)
    Uses PERMISSIVE mode so corrupt rows never halt execution.
    """

    print(f"📥 Reading raw data from: {source_path}")

    raw_df = (spark.read
              .schema(bronze_schema)          # enforce — no inference
              .option("mode", "PERMISSIVE")   # don't crash on bad rows
              .option("columnNameOfCorruptRecord", "_corrupt_record")
              .json(source_path))

    # Write ALL rows first (to satisfy serverless compute requirement)
    # Then split good/bad by reading back
    temp_path = bronze_path + "_temp"
    (raw_df.write
     .format("delta")
     .mode("overwrite")
     .save(temp_path))

    # Read back the saved data to enable _corrupt_record queries
    saved_df = spark.read.format("delta").load(temp_path)

    # Split: good rows vs bad rows
    good_df = saved_df.filter(saved_df._corrupt_record.isNull()) \
                      .drop("_corrupt_record")

    bad_df = saved_df.filter(saved_df._corrupt_record.isNotNull())

    # Write good rows to Bronze Delta
    (good_df.write
     .format("delta")
     .mode("append")
     .save(bronze_path))

    # Write bad rows to dead-letter (keep as JSON for debugging)
    bad_count = bad_df.count()
    if bad_count > 0:
        (bad_df.write
         .mode("append")
         .json(dead_letter_path))
        print(f"⚠️  Dead-letter: {bad_count} corrupt rows → {dead_letter_path}")

    good_count = good_df.count()
    print(f"✅ Bronze complete: {good_count} good rows → {bronze_path}")
    return good_df