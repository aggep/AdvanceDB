from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, year
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Initialize Spark session
spark = SparkSession.builder.appName("Crime Analysis - Query 1 DataFrame").getOrCreate()

# Load data from CSV file
df = spark.read.csv("hdfs://okeanos-master:54310/data/data.csv", header=True, inferSchema=True)

# Extract year and month from the 'dateOCC' column
df = df.withColumn("Year", year(col("DATE OCC"))).withColumn("Month", month(col("DATE OCC")))

# Group by Year and Month, and count the number of crimes
crime_counts = df.groupBy("Year", "Month").agg(count("*").alias("Crime_Count"))

# Define a window spec partitioned by Year and ordered by Crime_Count in descending order
windowSpec = Window.partitionBy("Year").orderBy(col("Crime_Count").desc())

# Rank the data within each year
ranked_data = crime_counts.withColumn("Month_Rank", rank().over(windowSpec))

# Filter to get the top 3 months for each year
top_months = ranked_data.filter(col("Month_Rank") <= 3)

# Show the result
top_months.show()

# Stop the Spark session
spark.stop()



