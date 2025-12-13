from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType, IntegerType
from pyspark.sql.functions import window, countDistinct, to_timestamp, pandas_udf, from_unixtime, avg, max as spark_max, min as spark_min, approx_count_distinct, count
import airportsdata
import pycountry

def get_country_name(country_code: str) -> str:
    """
    Converts a country code to its full country name.
    
    Args:
        country_code: The ISO 3166-1 alpha-2 country code

    Returns:
        Full country name or 'Unknown' if not found
    """
    try:
        airports = airportsdata.load('ICAO')
        country = pycountry.countries.get(alpha_2=country_code)
        return country.name if country else "Unknown"
    except Exception:
        return "Unknown"
    
def get_country_code_from_airport_icao(icao_code: str) -> str:
    airports = airportsdata.load('ICAO')
    try:
        # Fetch the airport data
        airport = airports[icao_code.upper()]
        return airport['country']  # Returns the ISO 2-letter country code (e.g., 'GB')
    except KeyError:
        return "Unknown Code"

def get_flight_schema():
    """Returns the schema for flight JSON data."""
    return StructType([
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


@pandas_udf(StringType()) # pyright: ignore[reportCallIssue]
def country_code_from_icao_udf(icao_series):
    return icao_series.apply(get_country_code_from_airport_icao)

def read_flight_stream(spark: SparkSession, 
                       bootstrap_servers: str = "localhost:29092,localhost:29093,localhost:29094",
                       topic: str = "flights_raw") -> DataFrame:
    """
    Reads flight data from Kafka stream and returns parsed DataFrame.
    
    Args:   
        spark: SparkSession instance
        bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        topic: Kafka topic name
        
    Returns:
        DataFrame with parsed flight data
    """
    json_schema = get_flight_schema()
    
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .load()
    
    parsed_stream = raw_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                    .withColumn("data", from_json(col("value"), json_schema)) \
                    .select("data.*")
    
    parsed_stream = parsed_stream.withColumn("timestamp", from_unixtime("time_position").cast("timestamp")) 

    parsed_stream = parsed_stream.withColumn("country_code", country_code_from_icao_udf(parsed_stream["icao24"]))
    
    return parsed_stream

def get_aircrafts_by_icao(flights_df: DataFrame, icao24) -> DataFrame:
    """
    Filters the flights DataFrame for aircrafts by a specific ICAO24 code.
    
    Args:
        flights_df: DataFrame containing flight data
        icao24: The ICAO24 code to filter by
    """
    if icao24 is None:
        return flights_df.select(
            "icao24",
            "callsign",
            col("latitude").alias("lat"),
            col("longitude").alias("lon"),
        )
    return flights_df.filter(col("icao24") == icao24).select(
        "icao24",
        "callsign",
        col("latitude").alias("lat"),
        col("longitude").alias("lon"),
    )
    

def get_activeaircrafts_by_country_hour(
        flights_df: DataFrame,
        country_code: str,
        hour_bucket: IntegerType,
):
    filtered_df = flights_df.filter(col("country_code") == country_code)
    
    result_df = (
        filtered_df.groupBy(
        window(col("timestamp"), "10 seconds").alias("hour_window")
    )
    .agg(
        count("icao24").alias("active_aircraft_count")
    )
    )

    return result_df

    
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("StructuredNetworkFlight") \
        .config("spark.cassandra.connection.host", "cassandra-1,cassandra-2,cassandra-3") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.connection.keepAliveMS", "60000") \
        .getOrCreate()
    checkpoint_path = "/tmp/checkpoints/"
    
    flights_df = read_flight_stream(spark)

    icao24 = None
    flights_df = get_aircrafts_by_icao(flights_df, icao24)

    flights_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", checkpoint_path + "aircrafts_by_icao/") \
        .options(table="aircrafts_by_icao24", keyspace="flight_analytics") \
        .outputMode("append") \
        .start() \
        .awaitTermination()
