from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DateType, DoubleType
from pyspark.sql.functions import to_date

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrame Creation")\
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
    .getOrCreate()

# Define the schema for the required columns
schema = StructType([
    StructField("Date Rptd", StringType()),
    StructField("DATE OCC", StringType()),
    StructField("Vict Age", IntegerType()),
    StructField("LAT", DoubleType()),
    StructField("LON", DoubleType()),
])

# Read CSV files into Spark DataFrames
df1 = spark.read.csv("hdfs://okeanos-master:54310/data/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
df2 = spark.read.csv("hdfs://okeanos-master:54310/data/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
df3 = spark.read.csv("hdfs://okeanos-master:54310/data/revgecoding.csv", header=True, inferSchema=True)

# Convert LAT and LON columns in df3 to IntegerType
#df3 = df3.withColumn("LAT", col("LAT").cast(DoubleType()))
#df3 = df3.withColumn("LON", col("LON").cast(DoubleType()))
df1.show(5)

# Select specific columns from df1
selected_columns_df1 = df1.select("Date Rptd", "DATE OCC", "Vict Age", "LAT", "LON")

# Select specific columns from df2
selected_columns_df2 = df2.select("Date Rptd", "DATE OCC", "Vict Age", "LAT", "LON")
# Combine DataFrames df1 and df2
combined_df = df1.union(df2)

# Convert columns to appropriate types
combined_df = combined_df.withColumn("Vict Age", col("Vict Age").cast(IntegerType()))

# Join the third CSV file using a common identifier (LAT, LON)
joined_df = combined_df.join(df3,['LAT', 'LON'], 'left_outer')

# Convert date columns to timestamp type
# Define the format of your date strings
dateFormat = "MM/dd/yyyy hh:mm:ss a"  # Notice 'hh' for hour and 'a' for AM/PM

# Convert 'DATE OCC' column to DateType with the specified format
joined_df = joined_df.withColumn('Date Rptd', to_date(col('Date Rptd'), dateFormat))
joined_df = joined_df.withColumn('DATE OCC', to_date(col('DATE OCC'), dateFormat))

# Print the total number of rows and column types
print("Total number of rows:", joined_df.count())
print("Column types:")
selected_df = joined_df.select("Date Rptd", "DATE OCC", "Vict Age", "LAT", "LON")
selected_df.printSchema()
selected_df.show(5)
# Write the joined DataFrame to a new CSV file (use HDFS path)
joined_df.write.csv("hdfs://okeanos-master:54310/data/data.csv", header=True, mode='overwrite')

# Stop the Spark session
spark.stop()

