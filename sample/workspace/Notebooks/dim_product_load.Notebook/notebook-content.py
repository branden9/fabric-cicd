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
from pyspark.sql.functions import col, sha2, concat_ws

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

# Parameter cell - values are supplied by the pipeline run
lakehouse_name = "lk_silver"
source_schema = "Cleansed"
destination_schema = "Conformed"
destination_table = "dim_product"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build fully-qualified source and destination table names within the specified lakehouse
src_path_fqn = f"{lakehouse_name}.{source_schema}"
tgt_path_fqn = f"{lakehouse_name}.{destination_schema}.{destination_table}"

print(f"Source path FQN: {src_path_fqn}")
print(f"Destination path FQN: {tgt_path_fqn}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Optional cell, add source table name params and field list
source_table = "StockItems"

source_fields = (
    """
    StockItemID
    ,StockItemName
    ,Brand
    ,Size
    ,LeadTimeDays
    ,QuantityPerOuter
    ,IsChillerStock
    ,Barcode
    ,TaxRate
    ,UnitPrice
    ,RecommendedRetailPrice
    ,TypicalWeightPerUnit
    ,MarketingComments
    ,get_json_object(CustomFields, '$.CountryOfManufacture') as CountryOfManufacture
    """
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build the source query.
# Where rw = 1 will get current/non duplicated records from source table
# Update/add criteria in the WHERE clause below as needed for this ingestion.
source_query = f"""
    SELECT
        {source_fields}
    FROM {src_path_fqn}.{source_table}
"""

#print(source_query)
# Load the source query into a DataFrame
df = spark.sql(source_query)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add metadata fields to the df
# _producthash: hash of the natural key (StockItemID), used to detect duplicate products
# _rowhash: hash of all columns, used to detect changed records
df = df.withColumn("_producthash", sha2(concat_ws("||", col("StockItemID")), 256)) \
       .withColumn("_rowhash", sha2(concat_ws("||", *[col(c) for c in df.columns]), 256))

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the product dimension, with validation/error handling
error_msg = None

try:
    df.write.format("delta").mode("overwrite").saveAsTable(tgt_path_fqn)

    # Ensure _producthash is unique after overwriting the destination table
    loaded_df = spark.table(tgt_path_fqn)
    total_count = loaded_df.count()
    distinct_hash_count = loaded_df.select("_producthash").distinct().count()
    if distinct_hash_count != total_count:
        raise ValueError(
            f"Duplicate _producthash values detected: {total_count} rows but only {distinct_hash_count} distinct hashes."
        )
except Exception as e:
    error_msg = str(e)

if error_msg:
    result_msg = f"FAILED: {error_msg}"
    print(result_msg)
else:
    result_msg = f"SUCCESS: Loaded {tgt_path_fqn}"
    print(result_msg)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
