# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Library import
from datetime import date

from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    dayofweek,
    dayofyear,
    month,
    quarter,
    sha2,
    weekofyear,
    year,
    when,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Start spark session if it is not already running
spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Parameter constants cell
# These values are specific to this notebook/table - update them for each notebook/table being loaded
lakehouse_name = "lk_bronze"
source_schema = "bronze"
destination_schema = "gold"
destination_table = "dim_date"

# Date params 
start_date = "2001-01-01"
end_date = date.today().strftime("%Y-%m-%d")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build fully-qualified destination table name within the specified lakehouse
tgt_path_fqn = f"{lakehouse_name}.{destination_schema}.{destination_table}"

print(f"Destination path FQN: {tgt_path_fqn}")
print(f"Date range: {start_date} to {end_date}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the destination schema if it does not already exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {lakehouse_name}.{destination_schema}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate one row per calendar day between start_date and end_date
date_seq_query = f"""
    SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS full_date
"""

df = spark.sql(date_seq_query)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Derive date dimension attributes from full_date
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    dayofweek,
    dayofyear,
    month,
    quarter,
    sha2,
    weekofyear,
    year,
    when,
)

df = (
    df.withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year(col("full_date")))
    .withColumn("quarter", quarter(col("full_date")))
    .withColumn("month", month(col("full_date")))
    .withColumn("month_name", date_format(col("full_date"), "MMMM"))
    .withColumn("day_of_month", dayofmonth(col("full_date")))
    .withColumn("day_of_year", dayofyear(col("full_date")))
    .withColumn("day_of_week", dayofweek(col("full_date")))
    .withColumn("day_name", date_format(col("full_date"), "EEEE"))
    .withColumn("week_of_year", weekofyear(col("full_date")))
    .withColumn("is_weekend", when(dayofweek(col("full_date")).isin(1, 7), True).otherwise(False))
    .withColumn("_datehash", sha2(date_format(col("full_date"), "yyyy-MM-dd"), 256))
)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the date dimension, with validation/error handling
error_msg = None

try:
    df.write.format("delta").mode("overwrite").saveAsTable(tgt_path_fqn)

    # Ensure _datehash is unique after overwriting the destination table
    loaded_df = spark.table(tgt_path_fqn)
    total_count = loaded_df.count()
    distinct_hash_count = loaded_df.select("_datehash").distinct().count()
    if distinct_hash_count != total_count:
        raise ValueError(
            f"Duplicate _datehash values detected: {total_count} rows but only {distinct_hash_count} distinct hashes."
        )
except Exception as e:
    error_msg = str(e)

if error_msg:
    result_msg = f"FAILED: {error_msg}"
    print(result_msg)
else:
    result_msg = f"SUCCESS: Loaded {tgt_path_fqn} ({start_date} to {end_date})"
    print(result_msg)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
