from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, row_number, when, count, desc
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType, FloatType, DoubleType

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("DF query 2 execution - Dataframe") \
    .getOrCreate()

crime_schema = StructType([
    StructField("TIME OCC", IntegerType()),
    StructField("Premis Desc", StringType()),
])

# Read the CSV file and create a PySpark DataFrame
df = spark.read.csv("hdfs://okeanos-master:54310/data/data.csv", header=True, inferSchema=True)

# Corrected code for defining time segments and creating new column
morning = (col("TIME OCC") >= 500) & (col("TIME OCC") < 1200)
afternoon = (col("TIME OCC") >= 1200) & (col("TIME OCC") < 1700)
evening = (col("TIME OCC") >= 1700) & (col("TIME OCC") < 2100)
night = (col("TIME OCC") >= 2100) | (col("TIME OCC") < 500)

df = df.withColumn("Time Seg", when(morning, "Morning")
                               .when(afternoon, "Afternoon")
                               .when(evening, "Evening")
                               .when(night, "Night"))

# Filter rows where "Premis Desc" is equal to "STREET"
df_street = df.filter(col("Premis Desc") == "STREET")

# Group by TimeSegment and count the number of rows for each group
time_segment_counts = df_street.groupBy("Time Seg").agg(count("*").alias("Count"))

# Order the result in descending order
time_segment_counts = time_segment_counts.orderBy(desc("Count"))

# Show the resulting DataFrame
time_segment_counts.show()
