"""
Weather Data Processor for Flight Data Monitoring System
Processes METAR, TAF, and International SIGMET data from Kafka and writes to Cassandra

This module implements streaming transformations for weather data following the 
Cassandra schema defined in cassandra/schema/weather_tables.cql
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, 
    DoubleType, ArrayType, MapType
)
from pyspark.sql.functions import (
    col, from_json, to_json, window, unix_timestamp, to_timestamp,
    row_number, from_unixtime, year, month, dayofmonth, floor as spark_floor,
    get_json_object, expr, struct, array, explode, when, lit
)
from pyspark.sql.window import Window


############## Schema Definitions ################

def get_metar_schema() -> StructType:
    """
    Returns the schema for METAR (Meteorological Aerodrome Report) JSON data.
    Source: Aviation Weather API via NiFi
    Topic: metar
    """
    clouds_schema = ArrayType(
        StructType([
            StructField("base", IntegerType(), True),
            StructField("cover", StringType(), True)
        ])
    )
    
    properties_schema = StructType([
        StructField("id", StringType(), True),
        StructField("site", StringType(), True),
        StructField("obsTime", StringType(), True),  # ISO 8601
        StructField("temp", IntegerType(), True),
        StructField("dewp", IntegerType(), True),
        StructField("wdir", IntegerType(), True),
        StructField("wspd", IntegerType(), True),
        StructField("wgst", IntegerType(), True),
        StructField("ceil", IntegerType(), True),
        StructField("cover", StringType(), True),
        StructField("fltcat", StringType(), True),
        StructField("visib", StringType(), True),
        StructField("wx", StringType(), True),
        StructField("altim", IntegerType(), True),
        StructField("slp", IntegerType(), True),
        StructField("rawOb", StringType(), True),
        StructField("clouds", clouds_schema, True)
    ])
    
    geometry_schema = StructType([
        StructField("type", StringType(), True),
        StructField("coordinates", ArrayType(DoubleType()), True)
    ])
    
    return StructType([
        StructField("type", StringType(), True),
        StructField("properties", properties_schema, True),
        StructField("geometry", geometry_schema, True)
    ])


def get_taf_schema() -> StructType:
    """
    Returns the schema for TAF (Terminal Aerodrome Forecast) JSON data.
    Source: Aviation Weather API via NiFi
    Topic: taf
    """
    clouds_schema = ArrayType(
        StructType([
            StructField("base", IntegerType(), True),
            StructField("cover", StringType(), True)
        ])
    )
    
    properties_schema = StructType([
        StructField("id", StringType(), True),
        StructField("site", StringType(), True),
        StructField("issueTime", StringType(), True),  # ISO 8601
        StructField("validTimeFrom", StringType(), True),  # ISO 8601
        StructField("validTimeTo", StringType(), True),  # ISO 8601
        StructField("timeGroup", IntegerType(), True),
        StructField("fcstType", StringType(), True),
        StructField("wdir", IntegerType(), True),
        StructField("wspd", IntegerType(), True),
        StructField("wgst", IntegerType(), True),
        StructField("visib", StringType(), True),
        StructField("ceil", IntegerType(), True),
        StructField("clouds", clouds_schema, True),
        StructField("fltcat", StringType(), True),
        StructField("rawTAF", StringType(), True),
        StructField("cover", StringType(), True)
    ])
    
    geometry_schema = StructType([
        StructField("type", StringType(), True),
        StructField("coordinates", ArrayType(DoubleType()), True)
    ])
    
    return StructType([
        StructField("type", StringType(), True),
        StructField("properties", properties_schema, True),
        StructField("geometry", geometry_schema, True)
    ])


def get_isigmet_schema() -> StructType:
    """
    Returns the schema for International SIGMET JSON data.
    Source: Aviation Weather API via NiFi
    Topic: isigmet
    """
    properties_schema = StructType([
        StructField("icaoId", StringType(), True),
        StructField("firId", StringType(), True),
        StructField("firName", StringType(), True),
        StructField("seriesId", StringType(), True),
        StructField("hazard", StringType(), True),
        StructField("qualifier", StringType(), True),
        StructField("validTimeFrom", StringType(), True),  # ISO 8601
        StructField("validTimeTo", StringType(), True),  # ISO 8601
        StructField("base", IntegerType(), True),
        StructField("top", IntegerType(), True),
        StructField("dir", StringType(), True),
        StructField("spd", StringType(), True),
        StructField("chng", StringType(), True),
        StructField("rawSigmet", StringType(), True)
    ])
    
    # Polygon coordinates: array of arrays of arrays [[[lon, lat], ...]]
    geometry_schema = StructType([
        StructField("type", StringType(), True),
        StructField("coordinates", ArrayType(ArrayType(ArrayType(DoubleType()))), True)
    ])
    
    return StructType([
        StructField("type", StringType(), True),
        StructField("properties", properties_schema, True),
        StructField("geometry", geometry_schema, True)
    ])


############## Stream Readers ################

def read_metar_stream(
    spark: SparkSession,
    bootstrap_servers: str = "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092",
    topic: str = "metar"
) -> DataFrame:
    """
    Reads METAR data from Kafka stream and returns parsed DataFrame.
    
    Args:
        spark: SparkSession instance
        bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        topic: Kafka topic name
        
    Returns:
        DataFrame with parsed METAR data
    """
    json_schema = get_metar_schema()
    
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .load()
    
    # Parse JSON and flatten structure
    parsed_stream = raw_stream \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .withColumn("data", from_json(col("value"), json_schema)) \
        .select(
            col("data.properties.id").alias("station_id"),
            col("data.properties.site").alias("site"),
            col("data.properties.obsTime").alias("obs_time_iso"),
            col("data.properties.temp").alias("temp"),
            col("data.properties.dewp").alias("dewp"),
            col("data.properties.wdir").alias("wdir"),
            col("data.properties.wspd").alias("wspd"),
            col("data.properties.wgst").alias("wgst"),
            col("data.properties.ceil").alias("ceil"),
            col("data.properties.cover").alias("cover"),
            col("data.properties.fltcat").alias("fltcat"),
            col("data.properties.visib").alias("visib"),
            col("data.properties.wx").alias("wx"),
            col("data.properties.altim").alias("altim"),
            col("data.properties.slp").alias("slp"),
            col("data.properties.rawOb").alias("raw_ob"),
            col("data.properties.clouds").alias("clouds_array"),
            col("data.geometry.coordinates").alias("coordinates")
        )
    
    # Convert ISO 8601 timestamp to Unix timestamp
    parsed_stream = parsed_stream \
        .withColumn("obs_time", unix_timestamp(col("obs_time_iso"))) \
        .withColumn("timestamp", to_timestamp(col("obs_time_iso"))) \
        .withColumn("lat", col("coordinates").getItem(1)) \
        .withColumn("lon", col("coordinates").getItem(0)) \
        .drop("obs_time_iso", "coordinates")
    
    return parsed_stream


def read_taf_stream(
    spark: SparkSession,
    bootstrap_servers: str = "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092",
    topic: str = "taf"
) -> DataFrame:
    """
    Reads TAF data from Kafka stream and returns parsed DataFrame.
    
    Args:
        spark: SparkSession instance
        bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        topic: Kafka topic name
        
    Returns:
        DataFrame with parsed TAF data
    """
    json_schema = get_taf_schema()
    
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .load()
    
    # Parse JSON and flatten structure
    parsed_stream = raw_stream \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .withColumn("data", from_json(col("value"), json_schema)) \
        .select(
            col("data.properties.id").alias("station_id"),
            col("data.properties.site").alias("site"),
            col("data.properties.issueTime").alias("issue_time_iso"),
            col("data.properties.validTimeFrom").alias("valid_from_iso"),
            col("data.properties.validTimeTo").alias("valid_to_iso"),
            col("data.properties.timeGroup").alias("time_group"),
            col("data.properties.fcstType").alias("fcst_type"),
            col("data.properties.wdir").alias("wdir"),
            col("data.properties.wspd").alias("wspd"),
            col("data.properties.wgst").alias("wgst"),
            col("data.properties.visib").alias("visib"),
            col("data.properties.ceil").alias("ceil"),
            col("data.properties.clouds").alias("clouds_array"),
            col("data.properties.fltcat").alias("fltcat"),
            col("data.properties.rawTAF").alias("raw_taf"),
            col("data.properties.cover").alias("cover"),
            col("data.geometry.coordinates").alias("coordinates")
        )
    
    # Convert ISO 8601 timestamps to Unix timestamps
    parsed_stream = parsed_stream \
        .withColumn("issue_time", unix_timestamp(col("issue_time_iso"))) \
        .withColumn("valid_from", unix_timestamp(col("valid_from_iso"))) \
        .withColumn("valid_to", unix_timestamp(col("valid_to_iso"))) \
        .withColumn("timestamp", to_timestamp(col("issue_time_iso"))) \
        .withColumn("lat", col("coordinates").getItem(1)) \
        .withColumn("lon", col("coordinates").getItem(0)) \
        .drop("issue_time_iso", "valid_from_iso", "valid_to_iso", "coordinates")
    
    return parsed_stream


def read_isigmet_stream(
    spark: SparkSession,
    bootstrap_servers: str = "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092",
    topic: str = "isigmet"
) -> DataFrame:
    """
    Reads International SIGMET data from Kafka stream and returns parsed DataFrame.
    
    Args:
        spark: SparkSession instance
        bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        topic: Kafka topic name
        
    Returns:
        DataFrame with parsed ISIGMET data
    """
    json_schema = get_isigmet_schema()
    
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .load()
    
    # Parse JSON and flatten structure
    parsed_stream = raw_stream \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .withColumn("data", from_json(col("value"), json_schema)) \
        .select(
            col("data.properties.icaoId").alias("icao_id"),
            col("data.properties.firId").alias("fir_id"),
            col("data.properties.firName").alias("fir_name"),
            col("data.properties.seriesId").alias("series_id"),
            col("data.properties.hazard").alias("hazard"),
            col("data.properties.qualifier").alias("qualifier"),
            col("data.properties.validTimeFrom").alias("valid_from_iso"),
            col("data.properties.validTimeTo").alias("valid_to_iso"),
            col("data.properties.base").alias("base"),
            col("data.properties.top").alias("top"),
            col("data.properties.dir").alias("dir"),
            col("data.properties.spd").alias("spd"),
            col("data.properties.chng").alias("chng"),
            col("data.properties.rawSigmet").alias("raw_sigmet"),
            col("data.geometry.coordinates").alias("polygon_coords")
        )
    
    # Convert ISO 8601 timestamps to Unix timestamps
    parsed_stream = parsed_stream \
        .withColumn("valid_from", unix_timestamp(col("valid_from_iso"))) \
        .withColumn("valid_to", unix_timestamp(col("valid_to_iso"))) \
        .withColumn("timestamp", to_timestamp(col("valid_from_iso"))) \
        .drop("valid_from_iso", "valid_to_iso")
    
    return parsed_stream


############## Table Transformations ################

def get_metar_latest_by_station(metar_df: DataFrame) -> DataFrame:
    """
    Table 8: metar_latest_by_station
    
    Transforms METAR stream to store ONLY the latest observation per station.
    Supports Weather Observation Map visualization.
    
    Spark Responsibilities:
    - Deduplicate: Keep only latest obs_time per station
    - Filter: Remove observations with null lat/lon
    - Stringify: Convert clouds array to JSON text
    
    Args:
        metar_df: Parsed METAR DataFrame from read_metar_stream
        
    Returns:
        DataFrame with latest METAR per station
    """
    # Filter out observations with null coordinates
    filtered_df = metar_df.filter(
        col("lat").isNotNull() & col("lon").isNotNull()
    )
    
    # Add watermark for late data handling (10 minutes)
    watermarked_df = filtered_df.withWatermark("timestamp", "10 minutes")
    
    # Window specification: partition by station, order by obs_time descending
    window_spec = Window.partitionBy("station_id").orderBy(col("obs_time").desc())
    
    # Convert clouds array to JSON string for Cassandra storage
    with_clouds_json = watermarked_df.withColumn(
        "clouds",
        to_json(col("clouds_array"))
    )
    
    # Select columns matching Cassandra table schema
    result_df = with_clouds_json.select(
        col("station_id"),
        col("obs_time"),
        col("lat"),
        col("lon"),
        col("site"),
        col("temp"),
        col("dewp"),
        col("wdir"),
        col("wspd"),
        col("wgst"),
        col("visib"),
        col("ceil"),
        col("cover"),
        col("fltcat"),
        col("wx"),
        col("altim"),
        col("slp"),
        col("clouds"),
        col("raw_ob")
    )
    
    return result_df


def get_metar_history_by_station(metar_df: DataFrame) -> DataFrame:
    """
    Table 9: metar_history_by_station
    
    Stores time-series METAR history for comparing observations vs forecast.
    Supports Forecast Timeline View.
    
    Spark Responsibilities:
    - Watermark: Handle 10-minute late arrivals
    - Filter: Remove duplicates (same station_id + obs_time)
    - Minimal fields: Only store timeline-critical fields
    
    Args:
        metar_df: Parsed METAR DataFrame from read_metar_stream
        
    Returns:
        DataFrame with METAR history (time-series)
    """
    # Add watermark for late data handling
    watermarked_df = metar_df.withWatermark("timestamp", "10 minutes")
    
    # Select only timeline-critical fields (minimal for performance)
    result_df = watermarked_df.select(
        col("station_id"),
        col("obs_time"),
        col("fltcat"),
        col("temp"),
        col("wspd"),
        col("visib"),
        col("ceil"),
        col("wx"),
        col("raw_ob")
    )
    
    # Note: Cassandra handles deduplication based on PRIMARY KEY (station_id, obs_time)
    return result_df


def get_sigmet_by_validtime(isigmet_df: DataFrame) -> DataFrame:
    """
    Table 10: sigmet_by_validtime
    
    Stores active SIGMETs with time-based partitioning for efficient time-slider queries.
    Supports Weather Observation Map (SIGMET polygons).
    
    Spark Responsibilities:
    - Compute hour_bucket: valid_from / 3600 * 3600 (floor to hour)
    - Stringify polygon: GeoJSON coordinates → JSON text
    - Filter: Remove SIGMETs with missing polygon data
    
    Args:
        isigmet_df: Parsed ISIGMET DataFrame from read_isigmet_stream
        
    Returns:
        DataFrame with SIGMETs partitioned by hour bucket
    """
    # Filter out SIGMETs with null polygon data
    filtered_df = isigmet_df.filter(col("polygon_coords").isNotNull())
    
    # Add watermark for late data handling
    watermarked_df = filtered_df.withWatermark("timestamp", "10 minutes")
    
    # Compute hour_bucket: floor valid_from to nearest hour
    with_hour_bucket = watermarked_df.withColumn(
        "hour_bucket",
        (spark_floor(col("valid_from") / 3600) * 3600).cast("long")
    )
    
    # Convert polygon coordinates to JSON string
    with_polygon_json = with_hour_bucket.withColumn(
        "polygon",
        to_json(col("polygon_coords"))
    )
    
    # Select columns matching Cassandra table schema
    result_df = with_polygon_json.select(
        col("hour_bucket"),
        col("icao_id"),
        col("series_id"),
        col("valid_from"),
        col("valid_to"),
        col("fir_id"),
        col("fir_name"),
        col("hazard"),
        col("qualifier"),
        col("base"),
        col("top"),
        col("dir"),
        col("spd"),
        col("chng"),
        col("polygon"),
        col("raw_sigmet")
    )
    
    return result_df


def get_taf_by_station(taf_df: DataFrame) -> DataFrame:
    """
    Table 11: taf_by_station
    
    Stores TAF forecast periods grouped by bulletin issuance.
    Supports Forecast Timeline View.
    
    Spark Responsibilities:
    - Parse TAF bulletins: Already split into periods by NiFi (timeGroup field)
    - Stringify clouds: array → JSON text
    - Deduplicate: Remove duplicate periods
    
    Args:
        taf_df: Parsed TAF DataFrame from read_taf_stream
        
    Returns:
        DataFrame with TAF forecast periods
    """
    # Add watermark for late data handling
    watermarked_df = taf_df.withWatermark("timestamp", "10 minutes")
    
    # Convert clouds array to JSON string
    with_clouds_json = watermarked_df.withColumn(
        "clouds",
        to_json(col("clouds_array"))
    )
    
    # Select columns matching Cassandra table schema
    result_df = with_clouds_json.select(
        col("station_id"),
        col("issue_time"),
        col("time_group"),
        col("valid_from"),
        col("valid_to"),
        col("lat"),
        col("lon"),
        col("site"),
        col("fcst_type"),
        col("wdir"),
        col("wspd"),
        col("wgst"),
        col("visib"),
        col("ceil"),
        col("cover"),
        col("fltcat"),
        col("clouds"),
        col("raw_taf")
    )
    
    # Note: Cassandra handles deduplication based on 
    # PRIMARY KEY ((station_id, issue_time), time_group, valid_from)
    return result_df


############## Main Execution ################

if __name__ == "__main__":
    # Create SparkSession with Cassandra configuration
    spark = SparkSession \
        .builder \
        .appName("WeatherDataProcessor") \
        .config("spark.cassandra.connection.host", "cassandra-1,cassandra-2,cassandra-3") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.connection.keepAliveMS", "60000") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .getOrCreate()
    
    checkpoint_path = "s3a://checkpoints/"
    
    # MinIO archival function for weather data
    def archive_weather_to_minio(batch_df: DataFrame, batch_id: int, bucket: str, timestamp_col: str = "timestamp"):
        """
        Archives weather data batches to MinIO as Parquet with date partitioning.
        
        Args:
            batch_df: Batch DataFrame to archive
            batch_id: Batch ID from Spark Structured Streaming
            bucket: S3A bucket path (e.g., 's3a://weather-metar')
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
    
    print("="*60)
    print("Starting Weather Data Processor")
    print("="*60)
    
    # Read raw streams from Kafka
    print("\n[1/3] Reading weather data streams from Kafka...")
    metar_df = read_metar_stream(spark)
    taf_df = read_taf_stream(spark)
    isigmet_df = read_isigmet_stream(spark)
    print("✓ Weather streams initialized")
    
    # Apply transformations
    print("\n[2/3] Applying transformations...")
    
    # Table 8: Latest METAR per station (for map visualization)
    metar_latest = get_metar_latest_by_station(metar_df)
    
    # Table 9: METAR history (for timeline comparison)
    metar_history = get_metar_history_by_station(metar_df)
    
    # Table 10: SIGMETs by valid time (for map polygons)
    sigmet_by_time = get_sigmet_by_validtime(isigmet_df)
    
    # Table 11: TAF forecast periods (for timeline forecast)
    taf_periods = get_taf_by_station(taf_df)
    
    print("✓ Transformations defined")
    
    # Write to Cassandra and start streaming queries
    print("\n[3/3] Starting streaming writes to Cassandra...")
    
    # Query 1: Latest METAR per station
    query_metar_latest = metar_latest.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", checkpoint_path + "metar_latest_by_station/") \
        .options(table="metar_latest_by_station", keyspace="flight_analytics") \
        .outputMode("append") \
        .start()
    print("✓ Query 1: metar_latest_by_station started")
    
    # Query 2: METAR history
    query_metar_history = metar_history.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", checkpoint_path + "metar_history_by_station/") \
        .options(table="metar_history_by_station", keyspace="flight_analytics") \
        .outputMode("append") \
        .start()
    print("✓ Query 2: metar_history_by_station started")
    
    # Query 3: SIGMETs by valid time
    query_sigmet = sigmet_by_time.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", checkpoint_path + "sigmet_by_validtime/") \
        .options(table="sigmet_by_validtime", keyspace="flight_analytics") \
        .outputMode("append") \
        .start()
    print("✓ Query 3: sigmet_by_validtime started")
    
    # Query 4: TAF forecast periods
    query_taf = taf_periods.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", checkpoint_path + "taf_by_station/") \
        .options(table="taf_by_station", keyspace="flight_analytics") \
        .outputMode("append") \
        .start()
    print("✓ Query 4: taf_by_station started")
    
    # MinIO Archival Queries (5-minute micro-batches)
    print("\nStarting MinIO archival streams...")
    
    # Archive raw METAR data
    query_archive_metar = metar_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: archive_weather_to_minio(
            batch_df, batch_id, "s3a://weather-metar/", "timestamp"
        )) \
        .option("checkpointLocation", checkpoint_path + "archive_metar/") \
        .trigger(processingTime="5 minutes") \
        .start()
    print("✓ Archive: weather-metar bucket")
    
    # Archive raw TAF data
    query_archive_taf = taf_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: archive_weather_to_minio(
            batch_df, batch_id, "s3a://weather-taf/", "timestamp"
        )) \
        .option("checkpointLocation", checkpoint_path + "archive_taf/") \
        .trigger(processingTime="5 minutes") \
        .start()
    print("✓ Archive: weather-taf bucket")
    
    # Archive raw ISIGMET data
    query_archive_isigmet = isigmet_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: archive_weather_to_minio(
            batch_df, batch_id, "s3a://weather-isigmet/", "timestamp"
        )) \
        .option("checkpointLocation", checkpoint_path + "archive_isigmet/") \
        .trigger(processingTime="5 minutes") \
        .start()
    print("✓ Archive: weather-isigmet bucket")
    
    print("\n" + "="*60)
    print("Weather Data Processor Running")
    print("="*60)
    print("\nActive Streaming Queries:")
    print("  • metar_latest_by_station    → Cassandra")
    print("  • metar_history_by_station   → Cassandra")
    print("  • sigmet_by_validtime        → Cassandra")
    print("  • taf_by_station             → Cassandra")
    print("  • archive_metar              → MinIO")
    print("  • archive_taf                → MinIO")
    print("  • archive_isigmet            → MinIO")
    print("\nPress Ctrl+C to stop...")
    print("="*60 + "\n")
    
    # Keep all streams running
    spark.streams.awaitAnyTermination()
