# utils/schemas.py
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

# Bronze schema — mirrors raw source exactly
# _corrupt_record is REQUIRED to catch malformed rows in PERMISSIVE mode
bronze_schema = StructType([
    StructField("vendor_id",        StringType(),    True),
    StructField("pickup_datetime",  StringType(),    True),  # keep as string in bronze
    StructField("passenger_count",  IntegerType(),   True),
    StructField("trip_distance",    DoubleType(),    True),
    StructField("fare_amount",      DoubleType(),    True),
    StructField("payment_type",     StringType(),    True),
    StructField("_corrupt_record",  StringType(),    True),  # catches bad JSON rows
])

# Silver schema — after cleaning, proper types enforced
silver_schema = StructType([
    StructField("vendor_id",        StringType(),    False),  # NOT nullable now
    StructField("pickup_datetime",  TimestampType(), False),  # proper timestamp
    StructField("passenger_count",  IntegerType(),   True),
    StructField("trip_distance",    DoubleType(),    True),
    StructField("fare_amount",      DoubleType(),    False),  # NOT nullable
    StructField("payment_type",     StringType(),    True),
])