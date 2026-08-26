# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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
SqlServerName = ""
DatabaseName = ""
load_id = ""
WatermarkColumn = ""
MergeKeys = ""
WaterMarkValue = ""
LoadType = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parameter constants cell
# NOTE: These values are specific to this notebook/table - update them for each notebook/table being loaded
lakehouse_name = "lk_silver"
source_schema = "Cleansed"
destination_schema = "Conformed"
destination_table = "mytable"

from datetime import datetime
valid_ts = datetime.now()

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

# Build a dynamic watermark filter clause - only applied for incremental loads
watermark_filter = ""
if LoadType.lower() == "incremental":
    watermark_filter = f"AND {WatermarkColumn} > '{WaterMarkValue}'"

print(watermark_filter)

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
        person_id
        , first_name
        , last_name
        , email
        , phone_number
    FROM {src_path_fqn}.my_source_table
    WHERE rw = 1
    {watermark_filter}
"""

print(source_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the source query into a DataFrame
df = spark.sql(source_query)

# Add new columns

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a full row hash over all columns in the df, used to detect changed records
from pyspark.sql.functions import sha2, concat_ws, col, lit

df = df.withColumn("_rowhash", sha2(concat_ws("||", *[col(c) for c in df.columns]), 256))

# Add SCD2 tracking columns
df = df.withColumn("_valid_from_ts", lit(valid_ts)) \
       .withColumn("_valid_to_ts", lit(None).cast("timestamp")) \
       .withColumn("is_current", lit(True)) \
       .withColumn("_load_ts", lit(valid_ts))

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the destination table based on LoadType, with validation/error handling
error_msg = None

try:
    merge_keys = [key.strip() for key in MergeKeys.split(",") if key.strip()]

    if LoadType.lower() == "full":
        # Full overwrite of the destination table
        df.write.format("delta").mode("overwrite").saveAsTable(tgt_path_fqn)

    elif LoadType.lower() == "incremental":
        # Merge into the destination table based on the MergeKeys parameter
        # Rules:
        #   1. Merge keys don't match target          -> insert as a new record
        #   2. Merge keys match and _rowhash matches   -> no-op (same record)
        #   3. Merge keys match and _rowhash differs   -> insert new version, and close out (end-date) the existing row
        if not merge_keys:
            raise ValueError("MergeKeys parameter must contain at least one column for an incremental load.")

        # Register source df as a temp view so SQL can reference it
        df.createOrReplaceTempView("_source_view")

        # Column lists for the source view and the target table (used to build the dynamic merge SQL)
        source_columns = spark.table("_source_view").columns
        target_columns = spark.table(tgt_path_fqn).columns

        col_list = ", ".join(source_columns)
        source_col_list = ", ".join([f"source.{c}" for c in source_columns])
        merge_key_list = ", ".join(merge_keys)
        key_join = " AND ".join([f"s.{key} = e.{key}" for key in merge_keys])
        on_clause = (
            " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
            + " AND source._insert_only = false"
            + " AND target.is_current = true"
        )

        merge_sql = f"""
        MERGE INTO {tgt_path_fqn} AS target
        USING (

            -- "close" entries
            -- Rows that already exist as is_current=TRUE but whose hash has changed.
            -- _insert_only=FALSE lets these rows satisfy the ON condition -> WHEN MATCHED fires -> old row closed.
            SELECT
                {col_list},
                FALSE AS _insert_only
            FROM _source_view AS s
            INNER JOIN (
                SELECT {merge_key_list}, _rowhash
                FROM {tgt_path_fqn}
                WHERE is_current = true
            ) AS e ON {key_join}
            WHERE s._rowhash <> e._rowhash

            UNION ALL

            -- "insert" entries
            -- New rows (no key match) + new versions of changed rows.
            -- _insert_only=TRUE makes the ON condition FALSE -> these rows fall through to WHEN NOT MATCHED -> inserted.
            SELECT
                {col_list},
                TRUE AS _insert_only
            FROM _source_view AS s
            LEFT JOIN (
                SELECT {merge_key_list}, _rowhash
                FROM {tgt_path_fqn}
                WHERE is_current = true
            ) AS e ON {key_join}
            WHERE e.{merge_keys[0]} IS NULL   -- brand-new key
               OR s._rowhash <> e._rowhash    -- key exists but data changed

        ) AS source
        ON {on_clause}

        -- Hash changed on a current row -> close it
        WHEN MATCHED AND target._rowhash <> source._rowhash THEN
            UPDATE SET
                target.is_current = FALSE,
                target._valid_to_ts = source._valid_from_ts

        -- No match (new key, or _insert_only=TRUE) -> insert new row
        WHEN NOT MATCHED THEN
            INSERT ({col_list})
            VALUES ({source_col_list})
        """

        spark.sql(merge_sql)

    else:
        raise ValueError(f"Unsupported LoadType: {LoadType}")

except Exception as e:
    error_msg = str(e)

if error_msg:
    result_msg = f"FAILED: {error_msg}"
    print(result_msg)
else:
    result_msg = f"SUCCESS: Loaded {tgt_path_fqn} (LoadType={LoadType})"
    print(result_msg)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

