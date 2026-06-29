# Databricks notebook source
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/earthquake/seismic/csv_files/earthquake dataset.csv")

display(df)

# COMMAND ----------

df = df.toDF(*[c.lower() for c in df.columns])

# COMMAND ----------

from datetime import datetime
batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
source_system = "earthquake dataset.csv"

display(batch_id)

# COMMAND ----------


# Add Audit Columns
from pyspark.sql import functions as F
bronze_df = df \
    .withColumn("batch_id", F.lit(batch_id)) \
    .withColumn("load_timestamp", F.current_timestamp()) \
    .withColumn("processing_date", F.current_date()) \
    .withColumn("source_system", F.lit(source_system)) \
    .withColumn("created_timestamp",F.current_timestamp())



# COMMAND ----------

### Validation Requirements

source_record_count = df.count()

print(f"Source Record Count : {source_record_count}")

# COMMAND ----------

# Null count per column
null_counts = bronze_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in bronze_df.columns
])

display(null_counts)

# COMMAND ----------


#  Duplicate Record Count

duplicate_count = bronze_df.groupBy("id") \
                           .count() \
                           .filter(F.col("count") > 1) \
                           .count()

print(f"Duplicate Count : {duplicate_count}")

# COMMAND ----------

from pyspark.sql import functions as F

for col_name in df.columns:
    
    dup_count = (
        df.groupBy(col_name)
          .count()
          .filter(F.col("count") > 1)
          .count()
    )
    
    print(f"{col_name}: {dup_count}")

# COMMAND ----------

#  Schema Validation

expected_columns = [
    "id",
    "place",
    "mag",
    "time"
]

actual_columns = df.columns

missing_columns = list(set(expected_columns) - set(actual_columns))

if len(missing_columns) == 0:
    print("Schema Validation Passed")
else:
    print("Missing Columns :", missing_columns)

# COMMAND ----------

 
#  Mandatory Column Validation
mandatory_columns = ["ID", "Magnitude"]

for col_name in mandatory_columns:
    
    null_count = bronze_df.filter(
        F.col(col_name).isNull()
    ).count()

    print(f"{col_name} Null Count = {null_count}")  

# COMMAND ----------


#  Validation Status

mandatory_validation = "PASS"

for col_name in mandatory_columns:
    
    cnt = bronze_df.filter(
        F.col(col_name).isNull()
    ).count()

    if cnt > 0:
        mandatory_validation = "FAIL"

print(mandatory_validation)


# COMMAND ----------

bronze_df = bronze_df.toDF(
    *[c.replace(" ", "_") for c in bronze_df.columns]
)

# COMMAND ----------

print(bronze_df.columns)

# COMMAND ----------

spark.table(
    "earthquake.seismic.earthquake_dataset"
).printSchema()

# COMMAND ----------

print(bronze_df.columns)

# COMMAND ----------

spark.sql("SHOW TABLES IN earthquake.seismic").show()

# COMMAND ----------


# Target Record Count

target_df = spark.table("earthquake.seismic.earthquake_dataset")

target_record_count = target_df.count()

print(f"Target Record Count: {target_record_count}")


# COMMAND ----------

spark.sql("SHOW CATALOGS").show(truncate=False)

# COMMAND ----------



# COMMAND ----------

spark.sql("SHOW TABLES IN earthquake.seismic").show(truncate=False)

# COMMAND ----------

from datetime import datetime

# Pipeline Start Time
start_time = datetime.now()

end_time = datetime.now()

# Processing Duration
processing_duration = end_time - start_time

print("Pipeline Start Time:", start_time)
print("Pipeline End Time:", end_time)
print("Processing Duration:", processing_duration)

# COMMAND ----------


# Validation Report

validation_report = {
    "Source_Count": source_record_count,\
    "Target_Count": target_record_count,\
    "Duplicate_Count": duplicate_count,\
    "Mandatory_Validation": mandatory_validation
}

print(validation_report)


# COMMAND ----------

audit_report = {
    "Batch_ID": batch_id,
    "Source_System": source_system,
    "Load_Time": str(datetime.now()),
    "Source_Count": source_record_count,
    "Target_Count": target_record_count,
    "Status": "SUCCESS"
}

print(audit_report)

# COMMAND ----------


# Execution Log DataFrame

log_data = [
    (
        batch_id,
        source_record_count,
        target_record_count,
        duplicate_count,
        mandatory_validation,
        processing_duration,
        "SUCCESS"
    )
]

log_df = spark.createDataFrame(
    log_data,
    [
        "batch_id",
        "source_count",
        "target_count",
        "duplicate_count",
        "validation_status",
        "processing_duration",
        "job_status"
    ]
)

display(log_df)

# COMMAND ----------

bronze_df = spark.table("earthquake.seismic.bronze_earthquake")


# COMMAND ----------

spark.sql("SHOW TABLES IN earthquake.seismic").show(truncate=False)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

display(spark.table("earthquake.seismic.bronze_earthquake"))

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS earthquake.seismic.bronze_earthquake")

# COMMAND ----------

df = df.toDF(*[
    c.lower().replace(" ", "_")
    for c in df.columns
])

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("earthquake.seismic.bronze_earthquake")

# COMMAND ----------

spark.sql("SHOW TABLES IN earthquake.seismic").show(truncate=False)