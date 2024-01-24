from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("DF query 2 execution - RDD") \
    .getOrCreate()

crime_schema = StructType([
    StructField("TIME OCC", IntegerType()),
    StructField("Premis Desc", StringType()),
])

# Read the CSV file and create a PySpark DataFrame
df = spark.read.csv("hdfs://okeanos-master:54310/data/data.csv", header=True, schema=crime_schema)

# Convert DataFrame to RDD
rdd = df.rdd

# Define the time segments function
def get_time_segment(row):
    time_occ = row["TIME OCC"]
    if 500 <= time_occ < 1200:
        return "Morning"
    elif 1200 <= time_occ < 1700:
        return "Afternoon"
    elif 1700 <= time_occ < 2100:
        return "Evening"
    elif 2100 <= time_occ or time_occ < 500:
        return "Night"

# Filter rows, map to key-value pairs, and reduce by key
result_rdd = rdd \
    .filter(lambda row: row["Premis Desc"] == "STREET") \
    .map(lambda row: (get_time_segment(row), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)  # Sort by count

# Collect and print the results
for time_segment, count in result_rdd.collect():
    print(f"{time_segment}: {count}")

# Stop the SparkSession
spark.stop()

