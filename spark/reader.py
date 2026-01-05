from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType, IntegerType, ArrayType, FloatType
from pyspark.sql.functions import window, countDistinct, to_timestamp, pandas_udf, from_unixtime, avg, max as spark_max, min as spark_min, approx_count_distinct, count, col, from_json, explode, unix_timestamp, lag, floor
from pyspark.sql.window import Window
import airportsdata
import pycountry

############## Load flight data ################
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
                       bootstrap_servers: str = "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092",
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
    
    return parsed_stream
#####################################################


############## Load track data ################
def get_track_schema():
    """Returns the schema for flight trajectory JSON data with nested path array.
    
    Path format: array of arrays where each inner array is:
    [time, latitude, longitude, baro_altitude, true_track, on_ground]
    """
    
    # Since path comes as array of arrays, we need to read it as such
    # Each inner array has 6 elements in order: time, lat, lon, alt, track, on_ground
    path_array_schema = ArrayType(
        ArrayType(StringType())  # Read as string first, will cast later
    )
    
    # Define the main schema
    return StructType([
        StructField("icao24", StringType(), True),
        StructField("startTime", IntegerType(), True),
        StructField("endTime", IntegerType(), True),
        StructField("callsign", StringType(), True),
        StructField("path", path_array_schema, True)
    ])


def read_track_stream(spark: SparkSession, 
                       bootstrap_servers: str = "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092",
                       topic: str = "flight_track") -> DataFrame:
    """Reads flight track data from Kafka and transforms array elements to named fields."""
    json_schema = get_track_schema()

    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .load()
    
    # Parse JSON 
    parsed_stream = raw_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .withColumn("data", from_json(col("value"), json_schema)) \
        .select("data.*")
    
    # Transform path array of arrays to array of structs with named fields
    # Each array element: [time, latitude, longitude, baro_altitude, true_track, on_ground]
    parsed_stream = parsed_stream.selectExpr(
        "icao24",
        "startTime",
        "endTime",
        "callsign",
        """transform(path, x -> struct(
            cast(x[0] as int) as time,
            cast(x[1] as float) as latitude,
            cast(x[2] as float) as longitude,
            cast(x[3] as float) as baro_altitude,
            cast(x[4] as float) as true_track,
            cast(x[5] as boolean) as on_ground
        )) as path"""
    )
    
    return parsed_stream
#####################################################

############## Load flight info ################
def get_flightinfo_schema():
    """Returns the schema for flight info JSON data.
    
    Contains departure/arrival airport estimates and distance information.
    """
    return StructType([
        StructField("icao24", StringType(), True),
        StructField("firstSeen", IntegerType(), True),
        StructField("estDepartureAirport", StringType(), True),
        StructField("lastSeen", IntegerType(), True),
        StructField("estArrivalAirport", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("estDepartureAirportHorizDistance", IntegerType(), True),
        StructField("estDepartureAirportVertDistance", IntegerType(), True),
        StructField("estArrivalAirportHorizDistance", IntegerType(), True),
        StructField("estArrivalAirportVertDistance", IntegerType(), True),
        StructField("departureAirportCandidatesCount", IntegerType(), True),
        StructField("arrivalAirportCandidatesCount", IntegerType(), True)
    ])


def read_flightinfo_stream(spark: SparkSession,
                           bootstrap_servers: str = "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092",
                           topic: str = "flight_data") -> DataFrame:
    """Reads flight info data from Kafka stream and returns parsed DataFrame.
    
    Args:
        spark: SparkSession instance
        bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        topic: Kafka topic name
        
    Returns:
        DataFrame with parsed flight info data including country_code from departure airport
    """
    json_schema = get_flightinfo_schema()
    
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .load()
    
    parsed_stream = raw_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .withColumn("data", from_json(col("value"), json_schema)) \
        .select("data.*")
    
    # Add country_code column derived from estDepartureAirport
    parsed_stream = parsed_stream.withColumn(
        "country_code", 
        country_code_from_icao_udf(col("estDepartureAirport"))
    )
    
    return parsed_stream
#####################################################


#################### Table 1 ########################
def get_aircrafts_by_icao(flights_df: DataFrame) -> DataFrame:
    """
    Filters the flights DataFrame for aircrafts.
    """
    return flights_df.select(
        "icao24",
        "callsign",
        col("latitude").alias("lat"),
        col("longitude").alias("lon"),
    )
######################################################
    
#################### Table 2 ########################
def get_aircraftstates_by_icao24(
        track_df: DataFrame,
) -> DataFrame:
    """
    Transforms flight track data with optional filtering by icao24 and/or starttime.
    """
    # Apply filters if provided
    filtered_df = track_df
    
    # Explode and transform
    flattened_df = filtered_df.withColumn("path_point", explode(col("path")))
    
    result_df = flattened_df.select(
        col("icao24"),
        col("startTime").alias("starttime"),
        col("path_point.time").alias("time"),
        col("path_point.latitude").alias("lat"),
        col("path_point.longitude").alias("lon"),
        col("path_point.baro_altitude").alias("baro_altitude"),
        col("path_point.true_track").alias("true_track"),
        col("path_point.on_ground").alias("on_ground"),
        col("callsign"),
        col("endTime").alias("endtime")
    )
    
    return result_df
######################################################

#################### Table 5 #########################
def get_activeaircraft_by_country_hour(
    flightinfo_df: DataFrame
) -> DataFrame:
    """
    Aggregates active aircraft counts by country and hour.
    Uses firstSeen timestamp from flight info data.
    
    Args:
        flightinfo_df: DataFrame containing flight info data with firstSeen and country_code columns
        
    Returns:
        DataFrame with country_code, hour_bucket, and active_aircraft_cnt
    """
    
    # Convert firstSeen (Unix timestamp in seconds) to timestamp type
    df_with_timestamp = flightinfo_df.withColumn(
        "timestamp",
        from_unixtime(col("firstSeen")).cast("timestamp")
    )
    
    # Add watermark to handle late data (allow 10 minutes of late data)
    df_with_watermark = df_with_timestamp.withWatermark("timestamp", "10 minutes")
    
    # Group by country and hourly window, count distinct aircraft
    result_df = df_with_watermark.groupBy(
        col("country_code"),
        window(col("timestamp"), "1 hour").alias("hour_window")
    ).agg(
        approx_count_distinct("icao24").alias("active_aircraft_cnt")
    )
    
    # Extract hour_bucket as Unix timestamp from window start
    # Only select columns that exist in Cassandra table
    result_df = result_df.select(
        col("country_code"),
        unix_timestamp(col("hour_window.start")).alias("hour_bucket"),
        col("active_aircraft_cnt")
    )
    
    return result_df
######################################################


#################### Table 7 #########################
def get_arrivals_by_country_hour(
    flights_df: DataFrame
) -> DataFrame:
    """
    Detects arrivals (landings) by tracking on_ground transitions and aggregates by country and hour.
    
    Args:
        flights_df: DataFrame containing flight data with on_ground, timestamp, and country_code columns
        country_code: Optional country code to filter by (partition key)
        hour_bucket: Optional Unix timestamp for hour to filter by (clustering key)
        
    Returns:
        DataFrame with country_code, hour_bucket, and arrivals_cnt
    """
    # Apply filters if provided
    filtered_df = flights_df
    
    # Define window partitioned by aircraft, ordered by timestamp
    window_spec = Window.partitionBy("icao24").orderBy("timestamp")
    
    # Get previous on_ground state for each aircraft
    df_with_prev = filtered_df.withColumn(
        "prev_on_ground",
        lag("on_ground", 1).over(window_spec)
    )
    
    # Detect arrivals: transition from not on_ground (false) to on_ground (true)
    arrivals_df = df_with_prev.filter(
        (col("prev_on_ground") == False) & (col("on_ground") == True)
    )
    
    # Group by country and hourly window, count arrivals
    result_df = arrivals_df.groupBy(
        col("country_code"),
        window(col("timestamp"), "1 hour").alias("hour_window")
    ).agg(
        count("icao24").alias("arrivals_cnt")
    )
    
    # Extract hour_bucket as Unix timestamp from window start
    result_df = result_df.select(
        col("country_code"),
        unix_timestamp(col("hour_window.start")).alias("hour_bucket"),
        col("arrivals_cnt")
    )
    
    return result_df
######################################################

    
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("StructuredNetworkFlight") \
        .config("spark.cassandra.connection.host", "cassandra-1,cassandra-2,cassandra-3") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.connection.keepAliveMS", "60000") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .getOrCreate()
    
    flightinfo_df = read_flightinfo_stream(spark)
    flightinfo_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .option("numRows", 5) \
        .start()

    activeaircrafts = get_activeaircraft_by_country_hour(flightinfo_df)

    activeaircrafts.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .option("numRows", 5) \
        .start()

    # Write to Cassandra
    activeaircrafts.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="activeaircraft_by_country_hour", keyspace="flight_analytics") \
        .outputMode("append") \
        .start() \
        .awaitTermination()
