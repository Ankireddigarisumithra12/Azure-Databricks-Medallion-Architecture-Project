# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog earthquake;
# MAGIC use schema seismic;

# COMMAND ----------

from azure.storage.blob import BlobServiceClient
import pandas as pd
import os
import io
connection_string = "DefaultEndpointsProtocol=https;AccountName=dataengstorage1998;AccountKey=Gm9auIyFLGCHtOcSYPRrZJqtzcrtVc1eXaOG+TOSww9yfNjb4S5Idkt5Dz7andEqCpkiL7pusX7K+AStBbZzuw==;EndpointSuffix=core.windows.net"
container_name = "source"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)
blob_list = container_client.list_blobs()
for blob in blob_list:
    print(blob.name)

for blob in container_client.list_blobs():
    table_name = (
        blob.name
        .replace(".csv", "")
        .replace(".parquet ", "")
        .replace("-", "_")
        .replace(" ","_")
        .replace(" - ","_")
        .lower()
    )
    data = container_client.get_blob_client(blob.name).download_blob().readall()
    df = pd.read_csv(io.BytesIO(data),encoding ="latin1")
    df = spark.createDataFrame(df)

    df = df.toDF(*[
    c.replace(" ", "_").replace("-", "_")
    for c in df.columns
])

    df.write \
        .format("delta") \
        .mode("overwrite") \
      .saveAsTable(f"earthquake.seismic.{table_name}")

    

# COMMAND ----------

from datetime import datetime

pipeline_start_time = datetime.now()

print("Pipeline Started:", pipeline_start_time)

# COMMAND ----------

### Source Validation

file_path = "/Volumes/earthquake/seismic/csv_files/earthquake dataset.csv"

try:
    df = spark.read \
        .option("header", "true") \
        .csv(file_path)

    print("Source file is available.")

except Exception as e:
    raise Exception(f"Source file not found: {e}")

# COMMAND ----------

# File Format Validation


file_name = file_path.split("/")[-1]

if file_name.endswith(".csv"):
    print("File format validation passed.")
else:
    raise Exception("Invalid file format.")

# COMMAND ----------

# File Size Validation
files = dbutils.fs.ls("/Volumes/earthquake/seismic/csv_files/earthquake dataset.csv")

for file in files:
    if file.name == "database.csv":
        print("File Size:", file.size)

        if file.size == 0:
            raise Exception("File is empty.")

print("File size validation passed.")

# COMMAND ----------

# File Schema Validation
if len(df.columns) == 0:
    raise Exception("Schema validation failed.")

print("Schema validation passed.")
print(df.columns)

# COMMAND ----------

# File Naming Convention

file_name = file_path.split("/")[-1]

if file_name.startswith("earthquake dataset.csv"):
    print("File naming convention passed.")
else:
    raise Exception("Invalid file name.")

# COMMAND ----------

# df = spark.read \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .option("include_metadata", "true") \
#     .csv("/Volumes/dataengineering/default/csv_files/database.csv")

# df = df.withColumn("source_file", df["_metadata.file_path"])

# display(df)

# COMMAND ----------

# Capture ingestion timestamp

from pyspark.sql.functions import current_timestamp

df = df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)

df.select("ingestion_timestamp").show()

# COMMAND ----------

# from pyspark.sql.functions import col

# df = df.withColumn(
#     "source_file",
#     col("_metadata.file_path")
# )

# df = df.drop("_metadata")

# COMMAND ----------


#  Capture Batch Identifier
from datetime import datetime
from pyspark.sql.functions import lit

batch_id = datetime.now().strftime("%Y%m%d%H%M%S")

df = df.withColumn(
    "batch_id",
    lit(batch_id)
)

display(df)


# COMMAND ----------


# Capture Processing Date

from pyspark.sql.functions import current_date

df = df.withColumn(
    "processing_date",
    current_date()
)
df.select("processing_date").show()


# COMMAND ----------


#  Total Records Read

total_records = df.count()

print("Total Records:", total_records)


# COMMAND ----------


# Empty File Validation

if total_records == 0:
    raise Exception("File is Empty")

print("Empty File Validation Passed")


# COMMAND ----------


# Corrupt File Validation


try:
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)

    print("Corrupt File Validation Passed")

except Exception as e:
    raise Exception(f"Corrupt File: {e}")

# COMMAND ----------

# Duplicate File Check

current_file = "earthquake dataset.csv"

processed_files = [
    "sales.csv",
    "customer.csv"
]

if current_file in processed_files:
    raise Exception("Duplicate File Found")

print("Duplicate File Validation Passed")

# COMMAND ----------

audit_data = [
    (
        batch_id,
        current_file,
        total_records,
        "SUCCESS"
    )
]

audit_df = spark.createDataFrame(
    audit_data,
    [
        "batch_id",
        "file_name",
        "record_count",
        "status"
    ]
)

display(audit_df)

# COMMAND ----------

pipeline_end_time = datetime.now()

log_data = [
    (
        pipeline_start_time,
        pipeline_end_time,
        current_file,
        total_records,
        "SUCCESS",
        ""
    )
]

log_df = spark.createDataFrame(
    log_data,
    [
        "start_time",
        "end_time",
        "file_name",
        "record_count",
        "status",
        "error_message"
    ]
)

display(log_df)

# COMMAND ----------

normalized_columns = [
    col_name.replace(" ", "_")
    for col_name in df.columns
]

df = df.toDF(*normalized_columns)



# COMMAND ----------

spark.sql("SHOW CATALOGS").show(truncate=False)

# COMMAND ----------

spark.sql("SHOW SCHEMAS").show(truncate=False)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

from pyspark.sql.functions import col

df = df.withColumn(
    "Time",
    col("Time").cast("string")
)

# COMMAND ----------

from pyspark.sql.functions import col

df = df \
    .withColumn("Time", col("Time").cast("string")) \
    .withColumn(
        "Depth_Seismic_Stations",
        col("Depth_Seismic_Stations").cast("integer")
    ) \
    .withColumn(
        "Magnitude_Seismic_Stations",
        col("Magnitude_Seismic_Stations").cast("integer")
    )

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print(df.dtypes)

# COMMAND ----------

df.select("Depth_Seismic_Stations").printSchema()

# COMMAND ----------

dbutils.fs.ls("/Volumes/earthquake/seismic/csv_files/earthquake dataset.csv")

# COMMAND ----------

