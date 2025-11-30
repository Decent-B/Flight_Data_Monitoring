from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType, IntegerType
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkFlight") \
    .getOrCreate()

# 1. Define the schema for your JSON data
# This must match the structure of your JSON messages
json_schema = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("time_position", LongType(), True),
    StructField("last_contact", LongType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("baro_altitude", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("true_track", DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
    StructField("sensors", IntegerType(), True),
    StructField("geo_altitude", DoubleType(), True),
    StructField("squawk", StringType(), True),
    StructField("spi", BooleanType(), True),
    StructField("position_source", IntegerType(), True),
    StructField("fetch_timestamp", LongType(), True),
    StructField("ingestion_timestamp", LongType(), True),
])

# 2. Subscribe to 1 topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092,localhost:29093,localhost:29094") \
    .option("subscribe", "flights_raw") \
    .load()

value_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# 3. Parse string to JSON
parsed_df = value_df.withColumn("json_data", from_json(col("value"), json_schema))

flights_df = parsed_df.select("json_data.*")

# 4. Finalize
query = flights_df \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()



