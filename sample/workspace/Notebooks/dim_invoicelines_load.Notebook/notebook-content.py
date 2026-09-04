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
destination_table = "dim_invoicelines"

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
source_table = "InvoiceLines"

source_fields = (
    """
    InvoiceLineID
    ,InvoiceID
    ,StockItemID
    ,Description
    ,PackageTypeID
    ,Quantity
    ,UnitPrice
    ,TaxRate
    ,TaxAmount
    ,LineProfit
    ,ExtendedPrice
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
# _invoicelinehash: hash of the natural key (InvoiceLineID), used to detect duplicate invoice lines
# _rowhash: hash of all columns, used to detect changed records
# _invoicehash: hash of invoiceId, used to reference invoice table
# _producthash: hash of StockItemId, used to reference stock table
df = df.withColumn("_invoicelinehash", sha2(concat_ws("||", col("InvoiceLineID")), 256)) \
       .withColumn("_rowhash", sha2(concat_ws("||", *[col(c) for c in df.columns]), 256)) \
       .withColumn("_invoicehash", sha2(concat_ws("||", col("InvoiceID")), 256)) \
       .withColumn("_producthash", sha2(concat_ws("||", col("StockItemID")), 256))

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the invoice lines dimension, with validation/error handling
error_msg = None

try:
    df.write.format("delta").mode("overwrite").saveAsTable(tgt_path_fqn)

    # Ensure _invoicelinehash is unique after overwriting the destination table
    loaded_df = spark.table(tgt_path_fqn)
    total_count = loaded_df.count()
    distinct_hash_count = loaded_df.select("_invoicelinehash").distinct().count()
    if distinct_hash_count != total_count:
        raise ValueError(
            f"Duplicate _invoicelinehash values detected: {total_count} rows but only {distinct_hash_count} distinct hashes."
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
