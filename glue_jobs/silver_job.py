import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, trim, regexp_replace, upper

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
# Performance tuning (safe defaults)
# -----------------------------
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")  # tune based on size
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.broadcastTimeout", "600")

# -----------------------------
# Parameters
# -----------------------------
SOURCE_DB = "netflix_db"
SOURCE_TABLE = "netflix_raw"

SILVER_S3_PATH = "s3://netflix-raw-123456-dev/netflix/silver/"
QUARANTINE_S3_PATH = "s3://netflix-raw-123456-dev/netflix/quarantine/"

# ✅ Success markers (IMPORTANT)
SILVER_SUCCESS_MARKER = "s3://netflix-raw-123456-dev/netflix/silver/_SUCCESS_MARKER/"
QUARANTINE_SUCCESS_MARKER = "s3://netflix-raw-123456-dev/netflix/quarantine/_SUCCESS_MARKER/"

# -----------------------------
# Read from Glue Data Catalog
# -----------------------------
dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DB,
    table_name=SOURCE_TABLE
)
df = dyf.toDF()

# -----------------------------
# Helper cleaning functions
# -----------------------------
def clean_string(c):
    """
    Trim + remove unwanted extra spaces + normalize quotes.
    """
    return (
        F.when(col(c).isNull(), F.lit(None))
        .otherwise(
            trim(
                regexp_replace(
                    regexp_replace(col(c), r"\s+", " "),
                    r"[“”]",
                    '"'
                )
            )
        )
    )

def null_if_empty(c):
    """
    Convert empty strings to null.
    """
    return F.when(
        (col(c).isNull()) | (trim(col(c)) == ""), F.lit(None)
    ).otherwise(col(c))

# -----------------------------
# Clean + Defaulting
# -----------------------------
df_clean = (
    df
    .withColumn("show_id", clean_string("show_id"))
    .withColumn("type", upper(clean_string("type")))
    .withColumn("title", clean_string("title"))
    .withColumn("director", clean_string("director"))
    .withColumn("cast", clean_string("cast"))
    .withColumn("country", clean_string("country"))
    .withColumn("date_added_raw", clean_string("date_added"))
    .withColumn("rating", upper(clean_string("rating")))
    .withColumn("duration_raw", clean_string("duration"))
    .withColumn("listed_in", clean_string("listed_in"))
    .withColumn("description", clean_string("description"))
)

# Convert empty string to null
for c in [
    "show_id", "type", "title", "director", "cast", "country",
    "date_added_raw", "rating", "duration_raw", "listed_in", "description"
]:
    df_clean = df_clean.withColumn(c, null_if_empty(c))

# -----------------------------
# Parse date_added properly
# Input format example: "September 9, 2021"
# -----------------------------
df_clean = df_clean.withColumn(
    "date_added",
    F.to_date(col("date_added_raw"), "MMMM d, yyyy")
)

# -----------------------------
# release_year casting
# -----------------------------
df_clean = df_clean.withColumn(
    "release_year",
    F.col("release_year").cast(IntegerType())
)

# -----------------------------
# Extract duration fields (minutes or seasons)
# -----------------------------
df_clean = (
    df_clean
    .withColumn(
        "duration_minutes",
        F.when(
            col("duration_raw").rlike(r"(?i)min"),
            F.regexp_extract(col("duration_raw"), r"(\d+)", 1).cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )
    .withColumn(
        "seasons",
        F.when(
            col("duration_raw").rlike(r"(?i)season"),
            F.regexp_extract(col("duration_raw"), r"(\d+)", 1).cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )
)

# -----------------------------
# Default values (Silver defaults)
# -----------------------------
df_clean = (
    df_clean
    .withColumn("director", F.coalesce(col("director"), F.lit("UNKNOWN")))
    .withColumn("cast", F.coalesce(col("cast"), F.lit("UNKNOWN")))
    .withColumn("country", F.coalesce(col("country"), F.lit("UNKNOWN")))
    .withColumn("rating", F.coalesce(col("rating"), F.lit("UNKNOWN")))
    .withColumn("listed_in", F.coalesce(col("listed_in"), F.lit("UNKNOWN")))
    .withColumn("description", F.coalesce(col("description"), F.lit("UNKNOWN")))
)

# -----------------------------
# Add partition columns
# -----------------------------
df_clean = (
    df_clean
    .withColumn("year_part", F.coalesce(col("release_year"), F.year(col("date_added"))))
    .withColumn("type_part", F.when(col("type").isNull(), F.lit("UNKNOWN")).otherwise(col("type")))
)

# -----------------------------
# VALIDATION RULES (Silver)
# -----------------------------
current_year = int(
    spark.sql("SELECT year(current_date()) AS y").collect()[0]["y"]
)

valid_type = col("type").isin(["MOVIE", "TV SHOW"])

valid_release_year = (
    col("release_year").isNotNull()
    & (col("release_year") >= F.lit(1900))
    & (col("release_year") <= F.lit(current_year + 1))
)

valid_movie_duration = (col("type") != "MOVIE") | (col("duration_minutes").isNotNull())
valid_tv_seasons = (col("type") != "TV SHOW") | (col("seasons").isNotNull())

validation_condition = (
    col("show_id").isNotNull()
    & col("title").isNotNull()
    & valid_type
    & valid_release_year
    & valid_movie_duration
    & valid_tv_seasons
)

df_valid = df_clean.filter(validation_condition)

df_invalid = (
    df_clean.filter(~validation_condition)
    .withColumn(
        "validation_error",
        F.concat_ws(
            ";",
            F.when(col("show_id").isNull(), F.lit("MISSING_SHOW_ID")),
            F.when(col("title").isNull(), F.lit("MISSING_TITLE")),
            F.when(~valid_type, F.lit("INVALID_TYPE")),
            F.when(~valid_release_year, F.lit("INVALID_RELEASE_YEAR")),
            F.when((col("type") == "MOVIE") & col("duration_minutes").isNull(), F.lit("MISSING_DURATION_MINUTES")),
            F.when((col("type") == "TV SHOW") & col("seasons").isNull(), F.lit("MISSING_SEASONS")),
        )
    )
)

# -----------------------------
# Broadcast Join Example (Ratings Dimension)
# -----------------------------
rating_dim_data = [
    ("G", "Kids"),
    ("PG", "Kids"),
    ("PG-13", "Teen"),
    ("R", "Adult"),
    ("NC-17", "Adult"),
    ("TV-Y", "Kids"),
    ("TV-Y7", "Kids"),
    ("TV-G", "Kids"),
    ("TV-PG", "Teen"),
    ("TV-14", "Teen"),
    ("TV-MA", "Adult"),
    ("UNKNOWN", "Unknown")
]

rating_dim = spark.createDataFrame(rating_dim_data, ["rating", "audience_group"])

df_valid = (
    df_valid
    .join(F.broadcast(rating_dim), on="rating", how="left")
    .withColumn("audience_group", F.coalesce(col("audience_group"), F.lit("Unknown")))
)

# -----------------------------
# Repartition for optimized write
# -----------------------------
df_valid_repart = df_valid.repartition("year_part", "type_part")

# -----------------------------
# Write Valid Silver Data (Parquet)
# -----------------------------
(
    df_valid_repart
    .drop("date_added_raw", "duration_raw")
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year_part", "type_part")
    .option("compression", "snappy")
    .save(SILVER_S3_PATH)
)

# -----------------------------
# Write Invalid (Quarantine) Records (Parquet)
# -----------------------------
df_invalid_repart = df_invalid.repartition(10)

(
    df_invalid_repart
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(QUARANTINE_S3_PATH)
)

# -----------------------------
# ✅ SUCCESS MARKERS
# -----------------------------
marker_df = spark.createDataFrame([("SUCCESS",)], ["status"])

# ✅ Silver success marker
(
    marker_df
    .repartition(1)
    .write.mode("overwrite")
    .format("text")
    .save(SILVER_SUCCESS_MARKER)
)

# ✅ Quarantine success marker
(
    marker_df
    .repartition(1)
    .write.mode("overwrite")
    .format("text")
    .save(QUARANTINE_SUCCESS_MARKER)
)

job.commit()
