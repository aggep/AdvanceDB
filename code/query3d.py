from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, desc, year, rank, regexp_replace, to_date
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, LongType, FloatType
from pyspark.sql.functions import col, when
from pyspark.sql.functions import col, to_timestamp, year, month, row_number, when, count, desc
import time 


spark = SparkSession.builder.appName("Query 3").getOrCreate()
start_time = time.time()
# Define the schema for the income dataset
income_schema = StructType([
    StructField("Zip Code", StringType()),
    StructField("Community", StringType()),
    StructField("Estimated Median Income", StringType())
    ])

# Read the income data CSV file and create a DataFrame
income_df = spark.read.csv("hdfs://okeanos-master:54310/data/LA_income_2015.csv", header=True, schema=income_schema)

# Clean the "Estimated Median Income" column
income_df = income_df.withColumn(
    "Estimated Median Income",
    regexp_replace(col("Estimated Median Income"), "[^0-9]", "")  # Remove non-numeric characters
).withColumn(
    "Estimated Median Income",
    col("Estimated Median Income").cast(IntegerType())  # Cast to Integer
)

#income_df.show(5)
# Define the schema for the revgecoding dataset
revgecoding_schema = StructType([
    StructField("LAT", DoubleType()),
    StructField("LON", DoubleType()),
    StructField("ZIPcode", StringType())
    ])

# Read the revgecoding data CSV file and create a DataFrame
revgecoding_df = spark.read.csv("hdfs://okeanos-master:54310/data/revgecoding.csv", header=True, schema=revgecoding_schema)


# Define the schema for the crime dataset
crime_schema = StructType([
    StructField("DR_NO", StringType()),
    StructField("Date Rptd", StringType()),
    StructField("DATE OCC", StringType()),
    StructField("TIME OCC", StringType()),
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

# Read the crime data CSV file and create a DataFrame
df = spark.read.csv("hdfs://okeanos-master:54310/data/Crime_Data_from_2010_to_2019.csv", header=True, schema=crime_schema)

# Convert the Integer type columns
df = df.withColumn("DR_NO", col("DR_NO").cast(LongType()))  #Int 64
df = df.withColumn("TIME OCC", col("TIME OCC").cast(IntegerType()))
df = df.withColumn("AREA", col("AREA").cast(IntegerType()))
df = df.withColumn("Rpt Dist No", col("Rpt Dist No").cast(IntegerType()))
df = df.withColumn("Part 1-2", col("Part 1-2").cast(IntegerType()))
df = df.withColumn("Crm Cd", col("Crm Cd").cast(IntegerType()))
df = df.withColumn("Vict Age", col("Vict Age").cast(IntegerType()))

##
df = df.withColumn("Date Rptd", to_timestamp(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a"))
df = df.withColumn("DATE OCC", to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))


#Conver the Float type columns
df = df.withColumn("Premis Cd", col("Premis Cd").cast(FloatType()))
df = df.withColumn("Weapon Used Cd", col("Weapon Used Cd").cast(FloatType()))
df = df.withColumn("Crm Cd 1", col("Crm Cd 1").cast(FloatType()))
df = df.withColumn("Crm Cd 2", col("Crm Cd 2").cast(FloatType()))
df = df.withColumn("Crm Cd 3", col("Crm Cd 3").cast(FloatType()))
df = df.withColumn("Crm Cd 4", col("Crm Cd 4").cast(FloatType()))

#Convert the Double type columns
df = df.withColumn("LAT", col("LAT").cast(DoubleType()))
df = df.withColumn("LON", col("LON").cast(DoubleType()))


# Add "Year" and "Month" columns
df = df.withColumn("Year", year(col("Date Rptd")))
df = df.withColumn("Month", month(col("Date Rptd")))

white = col("Vict Descent") == "W"
black = col("Vict Descent") == "B"
hispanic = (col("Vict Descent") == "H") | (col("Vict Descent") == "L") | (col("Vict Descent") == "M")
unknown = (
    (col("Vict Descent") == "A") | (col("Vict Descent") == "C") |
    (col("Vict Descent") == "D") | (col("Vict Descent") == "F") |
    (col("Vict Descent") == "G") | (col("Vict Descent") == "I") |
    (col("Vict Descent") == "J") | (col("Vict Descent") == "K") |
    (col("Vict Descent") == "O") | (col("Vict Descent") == "P") |
    (col("Vict Descent") == "S") | (col("Vict Descent") == "U") |
    (col("Vict Descent") == "V") | (col("Vict Descent") == "W") |
    (col("Vict Descent") == "X") | (col("Vict Descent") == "Z")
)

# Filter out rows with no victim information and filter victimless crimes
df = df.withColumn("Victim Descent", when(white, "White")
                               .when(black, "Black")
                               .when(hispanic, "Hispanic/Latin/Mexican")
                               .when(unknown, "Unknown"))

# Filter out rows with no victim information and filter victimless crimes
df = df.withColumn("Vict Descent", when(white, "White")
                               .when(black, "Black")
                               .when(hispanic, "Hispanic/Latin/Mexican")
                               .when(unknown, "Unknown"))

# Filter out rows with no victim information and filter victimless crimes
filtered_crime_df_first = df.filter(
        (col("Victim Descent").isin(
            "White", "Black", "Hispanic/Latin/Mexican", "Unknown")) | col("Vict Sex").isin("F", "M", "X")
        )


# Filter for the year 2015
filtered_crime_df =filtered_crime_df_first.filter(year("DATE OCC") == 2015)

# Define a window specification to rank ZIP codes based on ZIPcode
window_spec = Window.partitionBy("LAT", "LON").orderBy("ZIPcode")

# Add a rank column to the DataFrame
df = df.withColumn("Premis Cd", col("Premis Cd").cast(FloatType()))
df = df.withColumn("Weapon Used Cd", col("Weapon Used Cd").cast(FloatType()))
df = df.withColumn("Crm Cd 1", col("Crm Cd 1").cast(FloatType()))
df = df.withColumn("Crm Cd 2", col("Crm Cd 2").cast(FloatType()))
df = df.withColumn("Crm Cd 3", col("Crm Cd 3").cast(FloatType()))
df = df.withColumn("Crm Cd 4", col("Crm Cd 4").cast(FloatType()))

#Convert the Double type columns
df = df.withColumn("LAT", col("LAT").cast(DoubleType()))
df = df.withColumn("LON", col("LON").cast(DoubleType()))

# Add a rank column to the DataFrame
df_with_rank = revgecoding_df.withColumn("rank", rank().over(window_spec))

# Filter for rows where rank is 1 to keep only the first ZIP code
filtered_df = df_with_rank.filter(col("rank") == 1)

# Drop the rank column if no longer needed
filtered_revgecoding_df = filtered_df.drop("rank")

top_zip_codes_df = income_df.orderBy(col("Estimated Median Income").desc()).limit(3)
bottom_zip_codes_df = income_df.orderBy(col("Estimated Median Income")).limit(3)

# Combine the DataFrames to get the final result with the top and bottom ZIP codes
filtered_income_df = top_zip_codes_df.union(bottom_zip_codes_df)
#filtered_income_df.show()
# filtered_income_df.show()
# Join filtered_crime_df with filtered_revgecoding_df to get the ZIP code for each crime
crime_with_zip_df = filtered_crime_df.join(
        filtered_revgecoding_df,
        ["LAT", "LON"],
        "left"
        )

# Join the result with filtered_income_df to associate each crime with an income level
crime_with_income_df = crime_with_zip_df.join(
        filtered_income_df,
        crime_with_zip_df["ZIPcode"] == filtered_income_df["Zip Code"],
        "inner"  # Use inner join to keep only relevant ZIP Codes
        )
zip_codes = [row['Zip Code'] for row in filtered_income_df.select("Zip Code").collect()]



# Group the data by "Zip Code" and "Vict Descent" and count the number of crimes for each group
result_df = crime_with_income_df.groupBy(
        "Zip Code",
        "Victim Descent"
        ).agg(
                count("*").alias("Crime Count")
                ).orderBy(col("Crime Count").desc())  # Order by crime count descending


# Dictionary to hold the DataFrames for each ZIP code
zip_code_dfs = {}

for zip_code in zip_codes:
    zip_code_df = result_df.filter(col("Zip Code") == zip_code)
    if zip_code_df.rdd.isEmpty():
        # Create a new DataFrame with default values
        default_row = spark.createDataFrame([("none", 0)], ["Victim Descent", "Crime Count"])
        zip_code_df = default_row
    else:
        # If not empty, order by crime count and select required columns
        zip_code_df = zip_code_df.orderBy(col("Crime Count").desc()).select("Victim Descent", "Crime Count")

# Filter for each ZIP code and order by crime count
 #   zip_code_df = result_df.filter(col("Zip Code") == zip_code).orderBy(col("Crime Count").desc()).select("Victim Descent", "Crime Count")
    zip_code_dfs[zip_code] = zip_code_df

    # Show the DataFrame for this ZIP code
    print(f"Crime data for ZIP Code: {zip_code}")
    zip_code_df.show()
end_time = time.time()
print(f"Execution time with X executors: {end_time - start_time} seconds")
# Stop the SparkSession
spark.stop()
