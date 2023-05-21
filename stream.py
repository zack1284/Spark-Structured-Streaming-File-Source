from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# create SparkSession
spark = SparkSession.builder \
    .appName("StructuredStreamingExample") \
    .getOrCreate()

# show less log data
spark.sparkContext.setLogLevel("ERROR")

# define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])


# read csv files and create Streaming DataFrame

inputPath = "dir/input"  #make sure stream.py & dir at same location 
df = spark.readStream \
    .format("csv") \
    .option("header", "False") \
    .schema(schema) \
    .load(inputPath)

# convert "name" to lowercase
df = df.withColumn("name", lower(df["name"]))

# add city 
df = df.withColumn("city", lit("taipei"))

    
    
# export as json file 
outputPath = "dir/output"  
query = df.coalesce(1) \
    .writeStream \
    .format("json") \
    .outputMode("append") \
    .option("checkpointLocation", "dir/checkpoint") \
    .option("path", outputPath) \
    .start()
    

query.awaitTermination()



