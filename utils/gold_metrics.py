# utils/gold_metrics.py
from pyspark.sql.functions import (
    col, round, count, avg, sum, max, min,
    when, dayofweek, hour, desc
)

def compute_gold(spark, silver_path, gold_path):
    """
    Reads from Silver Delta.
    Computes two distinct business metrics → Gold Delta tables.
    These are the analytics-ready aggregates for reporting.
    """

    print("📥 Reading from Silver...")
    silver_df = spark.read.format("delta").load(silver_path)

    # -----------------------------------------------------------
    # GOLD METRIC 1: Revenue & Trip Summary per Vendor
    # Business question: Which vendor generates more revenue
    #                    and what is their average trip profile?
    # -----------------------------------------------------------
    metric1 = (silver_df
               .groupBy("vendor_id")
               .agg(
                   count("*")                          .alias("total_trips"),
                   round(sum("fare_amount"), 2)        .alias("total_revenue"),
                   round(avg("fare_amount"), 2)        .alias("avg_fare"),
                   round(avg("trip_distance"), 2)      .alias("avg_distance_miles"),
                   round(avg("passenger_count"), 2)    .alias("avg_passengers"),
                   round(max("fare_amount"), 2)        .alias("max_fare"),
               )
               .orderBy(desc("total_revenue")))

    # -----------------------------------------------------------
    # GOLD METRIC 2: Payment Type Distribution + Revenue Share
    # Business question: What % of revenue comes from each
    #                    payment method? (business flag insight)
    # -----------------------------------------------------------
    total_revenue = silver_df.agg(sum("fare_amount")).collect()[0][0]

    metric2 = (silver_df
               .groupBy("payment_type")
               .agg(
                   count("*")                    .alias("trip_count"),
                   round(sum("fare_amount"), 2)  .alias("revenue"),
               )
               .withColumn(
                   "revenue_share_%",
                   round((col("revenue") / total_revenue) * 100, 2)
               )
               .withColumn(
                   "payment_label",
                   when(col("payment_type") == "1", "Credit Card")
                   .when(col("payment_type") == "2", "Cash")
                   .when(col("payment_type") == "3", "No Charge")
                   .when(col("payment_type") == "4", "Dispute")
                   .otherwise("Unknown")
               )
               .orderBy(desc("revenue")))

    # -----------------------------------------------------------
    # Write both metrics to Gold Delta
    # -----------------------------------------------------------
    (metric1.write
     .format("delta")
     .mode("overwrite")
     .save(gold_path + "/vendor_revenue_summary"))

    (metric2.write
     .format("delta")
     .mode("overwrite")
     .save(gold_path + "/payment_distribution"))

    print(f"✅ Gold Metric 1 → {gold_path}/vendor_revenue_summary")
    print(f"✅ Gold Metric 2 → {gold_path}/payment_distribution")

    return metric1, metric2