from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("DF query 2 execution - RDD") \
    .getOrCreate()

# Define the schema for the crime dataset
crime_schema = StructType([
    StructField("DR_NO", StringType()),
    StructField("Date Rptd", StringType()),
    StructField("DATE OCC", StringType()),
    StructField("TIME OCC", IntegerType()),
    StructField("AREA", StringType()),
    StructField("AREA NAME", StringType()),
    StructField("Rpt Dist No", StringType()),
    StructField("Part 1-2", StringType()),
    StructField("Crm Cd", StringType()),
    StructField("Crm Cd Desc", StringType()),
    StructField("Mocodes", StringType()),
    StructField("Vict Age", StringType()),
    StructField("Vict Sex", StringType()),
    StructField("Vict Descent", StringType()),
    StructField("Premis Cd", StringType()),
    StructField("Premis Desc", StringType()),
    StructField("Weapon Used Cd", StringType()),
    StructField("Weapon Desc", StringType()),
    StructField("Status", StringType()),
    StructField("Status Desc", StringType()),
    StructField("Crm Cd 1", StringType()),
    StructField("Crm Cd 2", StringType()),
    StructField("Crm Cd 3", StringType()),
    StructField("Crm Cd 4", StringType()),
    StructField("LOCATION", StringType()),
    StructField("Cross Street", StringType()),
    StructField("LAT", StringType()),
    StructField("LON", StringType())
])

df1 = spark.read.csv("hdfs://okeanos-master:54310/data/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
df2 = spark.read.csv("hdfs://okeanos-master:54310/data/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
df = df1.union(df2)


# Convert DataFrame to RDD
rdd = df.rdd.filter(lambda x: x["Premis Desc"] == "STREET")

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

