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
# NOTE: file_path is relative to the lakehouse Files root, e.g. "Files/mydata.csv"
# lakehouse_name must be attached to this notebook as the default lakehouse, or the Files location won't resolve.
lakehouse_name = "lk_silver"
file_path = "Files/mydata.csv"
destination_schema = "Conformed"
destination_table = "mytable"
has_header = True
infer_schema = False
delimiter = ","

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build fully-qualified destination table name within the specified lakehouse
tgt_path_fqn = f"{lakehouse_name}.{destination_schema}.{destination_table}"

print(f"Source file path: {file_path}")
print(f"Destination path FQN: {tgt_path_fqn}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the CSV file from the lakehouse Files location into a DataFrame
df = (
    spark.read.format("csv")
    .option("header", has_header)
    .option("inferSchema", infer_schema)
    .option("delimiter", delimiter)
    .load(file_path)
)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sanitize column names for Delta compatibility.
# Delta rejects the characters: space , ; { } ( ) \n \t = in column names.
# Replace each invalid character with an underscore.
import re

invalid_chars_pattern = r"[ ,;{}()\n\t=]"
df = df.toDF(*[re.sub(invalid_chars_pattern, "_", c) for c in df.columns])

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert a string column to a date column.
# Update date_column and date_format to match the source data.
from pyspark.sql.functions import col, to_date

date_columns = ["Order_Date", "Ship_Date"]
date_format = "dd/MM/yyyy"

for date_column in date_columns:
    df = df.withColumn(date_column, to_date(col(date_column), date_format))

display(df)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the CSV data into a new Delta table, with validation/error handling
error_msg = None

try:
    df.write.format("delta").mode("overwrite").saveAsTable(tgt_path_fqn)
except Exception as e:
    error_msg = str(e)

if error_msg:
    result_msg = f"FAILED: {error_msg}"
    print(result_msg)
else:
    result_msg = f"SUCCESS: Loaded {file_path} into {tgt_path_fqn}"
    print(result_msg)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
