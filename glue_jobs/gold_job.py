import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# -----------------------------
# Glue boilerplate
# -----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------
# Performance tuning (safe + cost efficient)
# -----------------------------
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "50")
spark.conf.set("spark.sql.files.maxRecordsPerFile", "500000")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# -----------------------------
# Parameters
# -----------------------------
SOURCE_DB = "gold_db"       # ✅ change if needed
SOURCE_TABLE = "silver"

GOLD_S3_PATH = "s3://netflix-raw-123456-dev/netflix/gold/"
GOLD_SUCCESS_MARKER = GOLD_S3_PATH + "_SUCCESS/"

# -----------------------------
# Read Silver (only required columns)
# -----------------------------
df = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DB,
    table_name=SOURCE_TABLE
).toDF()

df = (
    df.select("year_part", "type_part", "rating", "audience_group", "country")
      .withColumn("country", F.trim(F.col("country")))
      .withColumn("rating", F.trim(F.col("rating")))
      .withColumn("audience_group", F.trim(F.col("audience_group")))
)

# Reduce shuffle + memory
df = df.filter(
    F.col("year_part").isNotNull() &
    F.col("type_part").isNotNull()
)

# -----------------------------
# GOLD 1: Titles by Year + Type
# -----------------------------
gold_titles_by_year_type = (
    df.groupBy("year_part", "type_part")
      .agg(F.count("*").alias("total_titles"))
)

(
    gold_titles_by_year_type
    .repartition("year_part")
    .write.mode("overwrite")
    .format("parquet")
    .partitionBy("year_part")
    .option("compression", "snappy")
    .save(GOLD_S3_PATH + "titles_by_year_type/")
)

# -----------------------------
# GOLD 2: Rating Summary
# -----------------------------
gold_rating_summary = (
    df.groupBy("rating")
      .agg(F.count("*").alias("total_titles"))
)

(
    gold_rating_summary
    .repartition(4)
    .write.mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_S3_PATH + "rating_summary/")
)

# -----------------------------
# GOLD 3: Audience Group Summary
# -----------------------------
gold_audience_group_summary = (
    df.groupBy("audience_group")
      .agg(F.count("*").alias("total_titles"))
)

(
    gold_audience_group_summary
    .repartition(2)
    .write.mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_S3_PATH + "audience_group_summary/")
)

# -----------------------------
# GOLD 4: Country Summary
# -----------------------------
gold_country_summary = (
    df.groupBy("country")
      .agg(F.count("*").alias("total_titles"))
)

(
    gold_country_summary
    .repartition(8)
    .write.mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_S3_PATH + "country_summary/")
)

# -----------------------------
# ✅ Success Marker
# -----------------------------
(
    spark.createDataFrame([("GOLD_SUCCESS",)], ["status"])
    .repartition(1)
    .write.mode("overwrite")
    .text(GOLD_SUCCESS_MARKER)
)

job.commit()