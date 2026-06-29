# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# COMMAND ----------

bronze_df = spark.table(
    "earthquake.seismic.bronze_earthquake"
)

silver_df = bronze_df

display(silver_df)

# COMMAND ----------

start_time = time.time()

batch_id = int(time.time())
processing_time = current_timestamp()

# COMMAND ----------


#Records Received Count

records_received = silver_df.count()

print("Records Received :", records_received)


# COMMAND ----------


df_dedup = silver_df.dropDuplicates(["id"])

duplicates_removed = records_received - df_dedup.count()

print("Duplicates Removed :", duplicates_removed)


# COMMAND ----------

df_dedup = silver_df.dropDuplicates(["id","time"])
silver_df.select("id").show()
silver_df.select("time").show()

# COMMAND ----------

silver_df = silver_df.dropDuplicates()
silver_df.show()

# COMMAND ----------


# Handle Null Values
silver_df.select(col("Depth_Seismic_Stations").isNull().alias("name is null")).show()

# COMMAND ----------

from pyspark.sql.functions import col

silver_df.select([
    col(c).isNull().alias(f"{c}_is_null")
    for c in silver_df.columns
]).show()

# COMMAND ----------

# null values count count 
from pyspark.sql.functions import col, when, count

silver_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in silver_df.columns
]).show()

# COMMAND ----------

#filter() with isNotNull()
# Get rows containing NULL values
silver_df.filter(col("Root_Mean_Square").isNotNull()).show()

# COMMAND ----------

# is not null boolean values
from pyspark.sql.functions import col

silver_df.select([
    col(c).isNotNull().alias(f"{c}_is_null")
    for c in silver_df.columns
]).show()

# COMMAND ----------

# is not null count values
from pyspark.sql.functions import col, when, count

silver_df.select([
    count(when(col(c).isNotNull(), c)).alias(c)
    for c in bronze_df.columns
]).show()

# COMMAND ----------

# fillna() - Replace NULL with constant value
silver_df.fillna(1729,subset=["Depth_Seismic_Stations"]).show()

# COMMAND ----------

#fillna() Multiple Columns
silver_df.fillna({
    "Depth_Error":12.34,
    "Depth_Seismic_Stations":1729,
    "Magnitude_Error":7.69,
}).show()

# COMMAND ----------

# dropna(how="any")
# Drop row if ANY column is NULL
silver_df.dropna(how="any").show()

# COMMAND ----------

# dropna(how="all")
# Drop row if ALL columns are NULL
silver_df.dropna(how="all").show()

# COMMAND ----------

# dropna(subset=[])
# Drop rows if specified column contains NULL
silver_df.dropna(subset=["Depth_Seismic_Stations"]).show()

# COMMAND ----------

# dropna(thresh=3)
# Keep rows having at least 3 non-null values
silver_df.dropna(thresh=3).show()

# COMMAND ----------

# coalesce()
# Returns first non-null value
silver_df.withColumn(
    "depth_error_final",
    coalesce(col("Depth_Error"),lit("18.17"))
).show()


# COMMAND ----------

for c in silver_df.columns:
    valid_df = silver_df.withColumnRenamed(
        c,
        c.lower().replace(" ","_")
    )

# COMMAND ----------

from pyspark.sql.functions import initcap, trim, col

string_cols = [
    field.name
    for field in silver_df.schema.fields
    if field.dataType.simpleString() == "string"
]

for c in string_cols:
    silver_df = silver_df.withColumn(
        c,
        initcap(trim(col(c)))
    )

print("Standardize Text Values is successfully validated")

# COMMAND ----------

print(silver_df.columns)

# COMMAND ----------

silver_df = valid_df \
.withColumn("latitude", col("latitude").cast(DoubleType())) \
.withColumn("longitude", col("longitude").cast(DoubleType())) \
.withColumn("depth", col("depth").cast(DoubleType())) \
.withColumn("Magnitude", col("magnitude").cast(DoubleType())) \
.withColumn("time", to_timestamp(col("time")))


# COMMAND ----------


#Data Type Validation

from pyspark.sql.functions import col

datatype_errors = silver_df.filter(
    col("latitude").cast("double").isNull() & col("latitude").isNotNull() |
    col("longitude").cast("double").isNull() & col("longitude").isNotNull() |
    col("mag").cast("double").isNull() & col("mag").isNotNull()
)
print("Data Type Validation is passed")


# COMMAND ----------


#Business Rule Validation

# Rule no 1
invalid_mag = valid_df.filter(
    col("magnitude") > 0
)
print("Rule 1 passed")


# COMMAND ----------

# rule 2 Latitude Range
invalid_lat = silver_df.filter(
    (col("latitude") < -90) |
    (col("latitude") > 90)
)
print("Rule 2 passed")

# COMMAND ----------

invalid_long = silver_df.filter(
    (col("longitude") < -180) |
    (col("longitude") > 180)
)
print("Rule 3 passed")

# COMMAND ----------


total_records = silver_df.count()
for c in silver_df.columns:
    null_count = silver_df.filter(col(c).isNull()).count()
    percentage = (null_count / total_records) * 100
    print(f"{c} {percentage:.2f} %")

# COMMAND ----------

from pyspark.sql.functions import col

null_records = silver_df.filter(
    col("Magnitude").isNull()
)

# COMMAND ----------

invalid_magnitude = silver_df.filter(
    col("Magnitude") <= 0
)

# COMMAND ----------

invalid_lat = silver_df.filter(
    (col("Latitude") < -90) |
    (col("Latitude") > 90)
)

# COMMAND ----------

invalid_long = silver_df.filter(
    (col("Longitude") < -180) |
    (col("Longitude") > 180)
)

# COMMAND ----------

datatype_errors = silver_df.filter(
    col("Magnitude").cast("double").isNull() &
    col("Magnitude").isNotNull()
)

# COMMAND ----------

invalid_records = (
    null_records
    .union(datatype_errors)
    .union(invalid_magnitude)
    .union(invalid_lat)
    .union(invalid_long)
)

display(invalid_records)

# COMMAND ----------

invalid_records = invalid_records.withColumn(
    "reject_reason",
    lit("Data Quality Failure")
)

# COMMAND ----------


# Valid Records

records_received = silver_df.count()


# COMMAND ----------

from pyspark.sql.functions import col

processed_df = silver_df.filter(
    col("id").isNotNull() &
    col("magnitude").isNotNull() &
    (col("magnitude") > 0)
)

# COMMAND ----------

records_processed = processed_df.count()

# COMMAND ----------

records_rejected = records_received - records_processed

# COMMAND ----------


#Remove Duplicates


dedup_df = bronze_df.dropDuplicates()

duplicates_removed = records_received - dedup_df.count()

# COMMAND ----------


#Validation Report

validation_report = [
    ("Records Received", records_received),
    ("Records Processed", records_processed),
    ("Records Rejected", records_rejected),
    ("Duplicates Removed", duplicates_removed)
]

report_df = spark.createDataFrame(
    validation_report,
    ["Metric","Value"]
)

display(report_df)



# COMMAND ----------


#Audit Report
audit_data = [
(
batch_id,
records_received,
records_processed,
records_rejected
)
]

audit_df = spark.createDataFrame(
    audit_data,
    [
        "batch_id",
        "records_received",
        "records_processed",
        "records_rejected"
    ]
)

audit_df.write \
.format("delta") \
.mode("append") \
.saveAsTable(
    "silver.audit_report"
)


# COMMAND ----------


#Execution Logs

end_time = time.time()

duration = end_time - start_time

print("Processing Duration :", duration, "seconds")


# COMMAND ----------


#Silver Delta Table
print(silver_df.columns)

# COMMAND ----------

silver_df= silver_df.withColumnRenamed("Magnitude", "mag")

# COMMAND ----------

if "mag" in silver_df.columns:
    print("mag exists")
else:
    print("mag missing")

# COMMAND ----------

display(
    spark.table(
        "earthquake.seismic.earthquake_dataset"
    )
)

# COMMAND ----------

silver_df = silver_df.toDF(*[c.lower() for c in silver_df.columns])

# COMMAND ----------

print(bronze_df.columns)
print(silver_df.columns)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS earthquake.seismic.silver_earthquake")

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("earthquake.seismic.silver_earthquake")

# COMMAND ----------

display(spark.table("earthquake.seismic.silver_earthquake"))