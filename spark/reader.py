from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType, IntegerType, ArrayType, FloatType
from pyspark.sql.functions import window, countDistinct, to_timestamp, pandas_udf, from_unixtime, avg, max as spark_max, min as spark_min, approx_count_distinct, count, col, from_json, explode, unix_timestamp, lag, floor, last, year, month, dayofmonth
from pyspark.sql.window import Window
import airportsdata
import pycountry
import reverse_geocode
import math
from datetime import datetime




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
    

def country_code_from_latlon(lat: float, lon: float) -> str | None:
    if lat is None or lon is None:
        return "Unknown"
    # Check for NaN or infinite values
    if math.isnan(lat) or math.isnan(lon) or math.isinf(lat) or math.isinf(lon):
        return "Unknown"
    try:
        result = reverse_geocode.get((lat, lon))
        cc = result.get("country_code")
        return cc.upper() if cc else None
    except (ValueError, Exception):
        return "Unknown"

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

@pandas_udf(StringType())
def country_code_from_latlon_udf(lat_series, lon_series):
    return lat_series.combine(lon_series, country_code_from_latlon)

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

#################### Table 3 ########################
def get_aircrafts_by_cell_minute(flights_df: DataFrame) -> DataFrame:
    """
    Transforms flight data into minute-bucketed aircraft positions for live map visualization.
    Groups aircraft by minute windows and returns latest position data for each aircraft.
   
    Args:
        flights_df: DataFrame containing flight data with timestamp column
       
    Returns:
        DataFrame with minute_bucket, icao24, and latest position/velocity data
    """
    # Add minute_bucket column: floor time_position to nearest minute
    df_with_bucket = flights_df.withColumn(
        "minute_bucket",
        (col("time_position") / 60).cast("long") * 60
    )
   
    # Add watermark for handling late data (10 seconds tolerance)
    df_watermarked = df_with_bucket.withWatermark("timestamp", "10 seconds")
   
    # Group by 1-minute tumbling window and icao24, aggregate latest values
    windowed_df = df_watermarked \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("icao24")
        ) \
        .agg(
            spark_max("time_position").alias("last_seen_ts"),
            last("latitude", ignorenulls=True).alias("lat"),
            last("longitude", ignorenulls=True).alias("lon"),
            last("geo_altitude", ignorenulls=True).alias("geo_altitude"),
            last("velocity", ignorenulls=True).alias("velocity"),
            last("true_track", ignorenulls=True).alias("true_track")
        )
   
    # Extract minute_bucket from window and select final columns
    result_df = windowed_df.select(
        (col("window.start").cast("long")).alias("minute_bucket"),
        col("icao24"),
        col("last_seen_ts"),
        col("lat"),
        col("lon"),
        col("geo_altitude"),
        col("velocity"),
        col("true_track")
    )
   
    return result_df
######################################################

#################### Table 5 #########################
def get_activeaircraft_by_country_hour(
    flights_df: DataFrame
) -> DataFrame:
    """
    Aggregates active aircraft counts by country and hour.
    
    Args:
        flights_df: DataFrame containing flight wdata with country_code and timestamp columns
        
    Returns:
        DataFrame with country_code, hour_bucket, and active_aircraft_cnt
    """
    # Add country_code column to flights_df
    filtered_df = flights_df.withColumn("country_code", country_code_from_latlon_udf(col("latitude"), col("longitude")))

    # Add watermark to handle late data (allow 10 minutes of late data)
    df_with_watermark = filtered_df.withWatermark("timestamp", "10 minutes")
    
    # Group by country and hourly window, count distinct aircraft
    result_df = df_with_watermark.groupBy(
        col("country_code"),
        window(col("timestamp"), "1 hour").alias("hour_window")
    ).agg(
        approx_count_distinct("icao24").alias("active_aircraft_cnt")
    )
    
    # Extract hour_bucket as Unix timestamp from window start
    result_df = result_df.select(
        col("country_code"),
        unix_timestamp(col("hour_window.start")).alias("hour_bucket"),
        col("active_aircraft_cnt")
    )
    
    return result_df
    
    
######################################################


#################### Table 6 #########################
def get_departures_by_country_hour(
    flightinfo_df: DataFrame
) -> DataFrame:
    """
    Detects departures (takeoffs) by tracking on_ground transitions and aggregates by country and hour.
    
    Args:
        flights_df: DataFrame containing flight data with on_ground, timestamp, and country_code columns
        
    Returns:
        DataFrame with country_code, hour_bucket, and arrivals_cnt
    """
    flitered_df = flightinfo_df.withColumn(
        "country_code", 
        country_code_from_icao_udf(col("estDepartureAirport"))
    )

    # Apply filters if provided
    df_with_timestamp = flitered_df.withColumn(
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
        approx_count_distinct("icao24").alias("departures_cnt")
    )
    
    # Extract hour_bucket as Unix timestamp from window start
    # Only select columns that exist in Cassandra table
    result_df = result_df.select(
        col("country_code"),
        unix_timestamp(col("hour_window.start")).alias("hour_bucket"),
        col("departures_cnt")
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
    
    checkpoint_path = "s3a://checkpoints/"
    
    # MinIO archival functions
    def archive_to_minio(batch_df: DataFrame, batch_id: int, bucket: str, timestamp_col: str = "timestamp"):
        """
        Archives batch data to MinIO as Parquet with date partitioning.
        
        Args:
            batch_df: Batch DataFrame to archive
            batch_id: Batch ID from Spark Structured Streaming
            bucket: S3A bucket path (e.g., 's3a://flight-raw')
            timestamp_col: Column name for timestamp partitioning
        """
        if batch_df.isEmpty():
            return
        
        # Add partition columns from timestamp
        partitioned_df = batch_df \
            .withColumn("year", year(col(timestamp_col))) \
            .withColumn("month", month(col(timestamp_col))) \
            .withColumn("day", dayofmonth(col(timestamp_col)))
        
        # Write as Parquet with date partitioning
        partitioned_df.write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .option("compression", "snappy") \
            .parquet(bucket)
    
    def archive_flight_data_to_minio(batch_df: DataFrame, batch_id: int):
        """Archives flight connection data to MinIO."""
        if batch_df.isEmpty():
            return
        
        # Add partition columns from firstSeen_timestamp
        partitioned_df = batch_df \
            .withColumn("year", year(col("firstSeen_timestamp"))) \
            .withColumn("month", month(col("firstSeen_timestamp"))) \
            .withColumn("day", dayofmonth(col("firstSeen_timestamp")))
        
        # Write as Parquet with date partitioning
        partitioned_df.write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .option("compression", "snappy") \
            .parquet("s3a://flight-data/")
    
    def archive_tracks_to_minio(batch_df: DataFrame, batch_id: int):
        """Archives flight track data to MinIO."""
        if batch_df.isEmpty():
            return
        
        # Add partition columns from startTime
        partitioned_df = batch_df \
            .withColumn("start_timestamp", from_unixtime(col("startTime")).cast("timestamp")) \
            .withColumn("year", year(col("start_timestamp"))) \
            .withColumn("month", month(col("start_timestamp"))) \
            .withColumn("day", dayofmonth(col("start_timestamp")))
        
        # Write as Parquet with date partitioning
        partitioned_df.write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .option("compression", "snappy") \
            .parquet("s3a://flight-tracks/")
    
    # Load raw data
    flights_df = read_flight_stream(spark)
    track_df = read_track_stream(spark)
    flightinfo_df = read_flightinfo_stream(spark)


    # Running each query and writing to Cassandra
    aircrafts_by_icao24 = get_aircrafts_by_icao(flights_df)
    query1 = aircrafts_by_icao24.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", checkpoint_path + "aircrafts_by_icao/") \
        .options(table="aircrafts_by_icao24", keyspace="flight_analytics") \
        .outputMode("append") \
        .start()
    
    # MinIO Archival: Raw flight states (10-minute micro-batches)
    query_archive_flights = flights_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: archive_to_minio(batch_df, batch_id, "s3a://flight-raw/", "timestamp")) \
        .option("checkpointLocation", checkpoint_path + "archive_flights/") \
        .trigger(processingTime="1 minute") \
        .start()
    
    aircraftstates_by_icao24 = get_aircraftstates_by_icao24(track_df)
    query2 = aircraftstates_by_icao24.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", checkpoint_path + "aircraftstates_by_icao/") \
        .options(table="aircraftstates_by_icao24", keyspace="flight_analytics") \
        .outputMode("append") \
        .start()
    
    # MinIO Archival: Flight tracks (10-minute micro-batches)
    query_archive_tracks = track_df.writeStream \
        .foreachBatch(archive_tracks_to_minio) \
        .option("checkpointLocation", checkpoint_path + "archive_tracks/") \
        .trigger(processingTime="1 minute") \
        .start()
    
    aircrafts_by_cell_minute = get_aircrafts_by_cell_minute(flights_df)
    query3 = aircrafts_by_cell_minute.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", checkpoint_path + "aircrafts_by_cell_minute/") \
        .options(table="aircrafts_by_cell_minute", keyspace="flight_analytics") \
        .outputMode("append") \
        .start()

    activeaircraft_by_country_hour = get_activeaircraft_by_country_hour(flights_df)
    query4 = activeaircraft_by_country_hour.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", checkpoint_path + "activeaircraft_by_country_hour/") \
        .options(table="activeaircraft_by_country_hour", keyspace="flight_analytics") \
        .outputMode("append") \
        .start()
    
    departures_by_country_hour = get_departures_by_country_hour(flightinfo_df)
    query5 = departures_by_country_hour.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", checkpoint_path + "departures_by_country_hour/") \
        .options(table="departures_by_country_hour", keyspace="flight_analytics") \
        .outputMode("append") \
        .start()
    
    # MinIO Archival: Flight connection data (10-minute micro-batches)
    query_archive_flight_data = flightinfo_df.writeStream \
        .foreachBatch(archive_flight_data_to_minio) \
        .option("checkpointLocation", checkpoint_path + "archive_flight_data/") \
        .trigger(processingTime="1 minute") \
        .start()
    
    # Keep all streams running
    spark.streams.awaitAnyTermination()
    
