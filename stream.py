from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os

# Create SparkSession
spark = SparkSession.builder \
    .appName("StructuredStreamingExample") \
    .getOrCreate()

# Show less log data
spark.sparkContext.setLogLevel("ERROR")

# disable generating success file
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Read CSV files and create Streaming DataFrame
inputPath = "dir/input"
df = spark.readStream \
    .format("csv") \
    .option("header", "False") \
    .schema(schema) \
    .load(inputPath)

# Convert "name" to lowercase
df = df.withColumn("name", lower(df["name"]))

# Add city
df = df.withColumn("city", lit("taipei"))

# Define output path
outputPath = "dir/output"

# Define foreachBatch function to write JSON output to not generating metadata file
def writeBatch(batchDF, batchId):
    if not batchDF.isEmpty():
        # To produce only one json file
        batchDF.coalesce(1).write.json(outputPath, mode="append")

# Write streaming data using foreachBatch
query = df.writeStream \
    .foreachBatch(writeBatch) \
    .outputMode("append") \
    .option("checkpointLocation", "dir/checkpoint") \
    .start()

query.awaitTermination()

