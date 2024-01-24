from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, desc, year, rank, regexp_replace, to_date
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, LongType, FloatType
from pyspark.sql.functions import col, when
from pyspark.sql.functions import col, to_timestamp, year, month, row_number, when, count, desc
from pyspark.sql.functions import sin, cos, avg, count, sqrt, atan2, radians, asin
from pyspark.sql.functions import udf
import math
from pyspark.sql import DataFrame
from functools import reduce
import time

spark = SparkSession.builder.appName("Query 4 with MERGE").getOrCreate()


start_time = time.time()
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

df_test = df.filter((col("Year") >2019))
df_test.show(5)

#FILTER CRIME_DF FOR NULL ISLAND
df = df.filter((col("LAT") != 0) & (col("LON") != 0))

# Filter the dataset to keep only crimes involving firearms (Weapon Used Cd in the format "1xx")
df = df.filter(col("Weapon Used Cd").like("1%%"))
# Now, df contains only the crimes involving firearms (codes in the "Weapon Used Cd" column starting with "1").

# Display a sample of the filtered DataFrame to inspect the data
df.sample(fraction=0.001, seed=42).show()


# Define the original schema for LAPD_Stations dataset
lapd_stations_schema = StructType([
    StructField("X", StringType()),
    StructField("Y", StringType()),
    StructField("FID", StringType()),
    StructField("DIVISION", StringType()),
    StructField("LOCATION", StringType()),
    StructField("PREC", StringType())
    ])

# Read the LAPD_Stations data CSV file and create a DataFrame
lapd_stations_df = spark.read.csv("hdfs://okeanos-master:54310/data/LAPD_Police_Stations.csv", header=True, schema=lapd_stations_schema)

# Cast columns to their appropriate data types
lapd_stations_df = lapd_stations_df.withColumn("X", col("X").cast(DoubleType()))
lapd_stations_df = lapd_stations_df.withColumn("Y", col("Y").cast(DoubleType()))
lapd_stations_df = lapd_stations_df.withColumn("FID", col("FID").cast(IntegerType()))
lapd_stations_df = lapd_stations_df.withColumn("PREC", col("PREC").cast(IntegerType()))



# Rename the "LAT" and "LON" columns in df for clarity
df = df.withColumnRenamed("LAT", "Crime_Latitude")
df = df.withColumnRenamed("LON", "Crime_Longitude")



# Rename the "X" and "Y" columns in lapd_stations_df for clarity
lapd_stations_df = lapd_stations_df.withColumnRenamed("X", "Station_Longitude")
lapd_stations_df = lapd_stations_df.withColumnRenamed("Y", "Station_Latitude")

#RENAME LOCATION FOR CRIME AND LAPD STATIONS
df = df.withColumnRenamed("LOCATION", "Crime_Location")
lapd_stations_df = lapd_stations_df.withColumnRenamed("LOCATION", "Station_Location")


# Join the two DataFrames on "AREA" and "PREC"
joined_df = df.hint("MERGE").join(
        lapd_stations_df,
        (df["AREA"] == lapd_stations_df["PREC"]),
        how="inner"
        )

joined_df.explain()
# Select the columns you want to keep in the joined DataFrame
selected_columns = [
        "DR_NO", "Date Rptd", "DATE OCC", "TIME OCC", "AREA", "AREA NAME",
        "Rpt Dist No", "Part 1-2", "Crm Cd", "Crm Cd Desc", "Mocodes", "Vict Age",
        "Vict Sex", "Vict Descent", "Premis Cd", "Premis Desc", "Weapon Used Cd",
        "Weapon Desc", "Status", "Status Desc", "Crm Cd 1", "Crm Cd 2", "Crm Cd 3",
        "Crm Cd 4", "Crime_Location", "Cross Street", "Crime_Latitude", "Crime_Longitude",
        "Year", "Month", "Station_Latitude", "Station_Longitude", "FID", "DIVISION",
        "Station_Location", "PREC"
        ]

# Select the desired columns from the joined DataFrame
result_df = joined_df.select(selected_columns)
def calculate_distance(lat1, lon1, lat2, lon2):
    def radians(deg):
        return deg * (math.pi / 180)

    # Convert latitude and longitude from degrees to radians
    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)
    # debug
    # Calculate the differences in latitudes and longitudes
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    # Haversine formula for distance calculation
    a = math.sin(delta_lat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    radius_of_earth_km = 6373  # Radius of the Earth in kilometers
    distance_km = radius_of_earth_km * c

    return distance_km

# Define the UDF for Spark using the function
distance_udf = udf(calculate_distance, DoubleType())

# Applying the UDF to the DataFrame
result_df = result_df.withColumn(
    "Crime_Station_Distance",
    distance_udf(
        col("Crime_Latitude"),
        col("Crime_Longitude"),
        col("Station_Latitude"),
        col("Station_Longitude")
    )
)
result_df.select("Crime_Latitude", "Crime_Longitude", "Station_Latitude", "Crime_Longitude", "Crime_Station_Distance").show(5)

# Group by PREC (police department) and Year and calculate the count of crimes and the average distance
year_list = result_df.select("Year").distinct().rdd.flatMap(lambda x: x).collect()
df_list = []
# Create and display a separate DataFrame for each police department
for year in year_list:
        year_df = result_df.filter(result_df["Year"] == year)
        final_df = year_df.groupBy("Year").agg(avg("Crime_Station_Distance").alias("Average_Distance_Km"),count("DR_NO").alias("Crime_Count"),)
        final_df = final_df.orderBy("Year")
        df_list.append(final_df)
#result_df.sample(fraction=0.001, seed=42).show()

# Concatenate all the DataFrames in the list into a single DataFrame
combined_df = reduce(DataFrame.unionAll, df_list)
combined_df = combined_df.orderBy("Year")
# Display the combined DataFrame
combined_df.show()

end_time = time.time()

execution_time = end_time - start_time

print(f"Execution time: {execution_time} seconds")


spark.stop()
