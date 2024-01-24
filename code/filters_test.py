from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder.appName("Filter Top and Bottom ZIP Codes").getOrCreate()

# Define the schema for the income DataFrame
income_schema = StructType([
    StructField("Zip Code", StringType()),
    StructField("Community", StringType()),
    StructField("Estimated Median Income", IntegerType())
    ])

# Sample data for the income DataFrame
income_data = [
        ("ZIP1", "Community1", 1000),
        ("ZIP2", "Community2", 2000),
        ("ZIP3", "Community3", 4000),
        ("ZIP3", "Community3", 300),
        ("ZIP3", "Community3", 10000),
        ("ZIP3", "Community3", 6000),
        ("ZIP3", "Community3", 5000),
        ("ZIP3", "Community3", 9000),
        ("ZIP3", "Community3", 8000),
        ("ZIP4", "Community4", 4100),
        ("ZIP5", "Community5", 7000),
        ("ZIP6", "Community6", 2500)
        ]

# Create the income DataFrame
income_df = spark.createDataFrame(income_data, schema=income_schema)

# Sort the DataFrame by "Estimated Median Income" in descending order to get the highest incomes
highest_incomes_df = income_df.orderBy(col("Estimated Median Income").desc()).limit(3)

# Sort the DataFrame by "Estimated Median Income" in ascending order to get the lowest incomes
lowest_incomes_df = income_df.orderBy(col("Estimated Median Income")).limit(3)

# Combine the DataFrames to get the final result with the top and bottom ZIP codes
filtered_income_df = highest_incomes_df.union(lowest_incomes_df)

# Show the resulting DataFrame
filtered_income_df.show()

# Stop the SparkSession
spark.stop()

