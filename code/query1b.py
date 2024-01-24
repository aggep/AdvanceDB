from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, year
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Crime Analysis - Query 1 SQL API").getOrCreate()

# Load data from CSV file
df = spark.read.csv("hdfs://okeanos-master:54310/data/data.csv", header=True, inferSchema=True)

# Extract year and month from the 'DATE OCC' column
df = df.withColumn("Year", year(col("DATE OCC"))).withColumn("Month", month(col("DATE OCC")))

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("crime_data")

# Now you can run your SQL queries on the registered view

# SQL query for aggregating crime counts
#sql_query = """
#    SELECT Year, Month, COUNT(*) AS Crime_Count
#    FROM crime_data
#    GROUP BY Year, Month
#    ORDER BY Year, Crime_Count DESC
#"""

# Execute the SQL query
#result_df = spark.sql(sql_query)

# Show the result

# SQL query to create a view with the top 3 months for each year
top_months_query = """
    WITH RankedMonths AS (
        SELECT
            Year,
            Month,
            Crime_Count,
            RANK() OVER (PARTITION BY Year ORDER BY Crime_Count DESC) AS Month_Rank
        FROM (
            SELECT
                Year,
                Month,
                COUNT(*) AS Crime_Count
            FROM crime_data
            GROUP BY Year, Month
        ) AS MonthlyCrimeData
    )
    SELECT Year, Month, Crime_Count
    FROM RankedMonths
    WHERE Month_Rank <= 3
"""

# Execute the top months query
top_months_df = spark.sql(top_months_query)

# Show the top months result
top_months_df.show()

# Stop the Spark session
spark.stop()

