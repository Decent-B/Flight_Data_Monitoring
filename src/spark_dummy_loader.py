"""
Spark Dummy Data Loader for Cassandra Tables
Generates synthetic data and loads all 7 Cassandra tables
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType
import time

print("=" * 80)
print("Spark Dummy Data Loader - Flight Analytics")
print("=" * 80)

# Initialize Spark Session with Cassandra configuration
spark = SparkSession \
    .builder \
    .appName("DummyDataLoader") \
    .config("spark.cassandra.connection.host", "cassandra-1,cassandra-2,cassandra-3") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.connection.keepAliveMS", "60000") \
    .getOrCreate()

print("\n✓ Spark Session initialized")
print(f"Spark Version: {spark.version}")

# Generate timestamps for different time buckets
current_ts = int(time.time())
current_minute = (current_ts // 60) * 60
current_hour = (current_ts // 3600) * 3600
current_date = int(time.strftime("%Y%m%d", time.gmtime(current_ts)))

# Define aircraft IDs
aircraft_ids = ["ABC123", "XYZ789", "DEF456", "GHI012", "JKL345"]

# Define geo cells
geo_cells = ["cell_hanoi", "cell_newyork", "cell_singapore", "cell_london"]

# Define countries
countries = ["VN", "US", "SG", "GB", "GLOBAL"]

print("\n" + "=" * 80)
print("TABLE 1: aircrafts_by_icao24")
print("=" * 80)

# Table 1: aircrafts_by_icao24 - Current aircraft state
aircrafts_data = [
    ("ABC123", "VN123", current_ts, 21.0285, 105.8542, 10000.0, 250.0, 180.0, 0.0, False, "cell_hanoi", "VN", "Vietnam"),
    ("XYZ789", "US456", current_ts - 10, 40.7128, -74.0060, 9500.0, 240.0, 90.0, -5.0, False, "cell_newyork", "US", "United States"),
    ("DEF456", "SG789", current_ts - 20, 1.3521, 103.8198, 11000.0, 260.0, 270.0, 10.0, False, "cell_singapore", "SG", "Singapore"),
    ("GHI012", "GB234", current_ts - 30, 51.5074, -0.1278, 8000.0, 220.0, 45.0, 0.0, False, "cell_london", "GB", "United Kingdom"),
    ("JKL345", "VN567", current_ts - 5, 16.0544, 108.2022, 3000.0, 150.0, 135.0, -15.0, True, "cell_hanoi", "VN", "Vietnam"),
]

aircrafts_schema = StructType([
    StructField("icao24", StringType(), False),
    StructField("callsign", StringType(), True),
    StructField("last_seen_ts", LongType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("geo_altitude", DoubleType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("true_track", DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("geo_cell", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("origin_country", StringType(), True),
])

aircrafts_df = spark.createDataFrame(aircrafts_data, schema=aircrafts_schema)
print(f"Generated {aircrafts_df.count()} dummy records")

# Write to Cassandra
aircrafts_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="aircrafts_by_icao24", keyspace="flight_analytics") \
    .save()

print("✓ Written to aircrafts_by_icao24")

# Read back and verify
read_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="aircrafts_by_icao24", keyspace="flight_analytics") \
    .load() \
    .limit(5)

print("\nSample data from aircrafts_by_icao24:")
read_df.show(truncate=False)

print("\n" + "=" * 80)
print("TABLE 2: aircraftstates_by_icao24_date")
print("=" * 80)

# Table 2: aircraftstates_by_icao24_date - Time series data
aircraftstates_data = []
for aircraft in aircraft_ids[:3]:  # Use first 3 aircraft
    for i in range(5):  # 5 data points per aircraft
        ts = current_ts - (i * 60)  # Every minute
        aircraftstates_data.append((
            aircraft,
            current_date,
            ts,
            10000.0 - (i * 100),  # Descending altitude
            250.0 - (i * 5),      # Decreasing velocity
            21.0 + (i * 0.01),    # Changing latitude
            105.8 + (i * 0.01),   # Changing longitude
            180.0,
            -5.0 if i > 0 else 0.0,
            False
        ))

aircraftstates_schema = StructType([
    StructField("icao24", StringType(), False),
    StructField("date_bucket", LongType(), False),
    StructField("ts", LongType(), False),
    StructField("geo_altitude", DoubleType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("true_track", DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
])

aircraftstates_df = spark.createDataFrame(aircraftstates_data, schema=aircraftstates_schema)
print(f"Generated {aircraftstates_df.count()} dummy records")

aircraftstates_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="aircraftstates_by_icao24_date", keyspace="flight_analytics") \
    .save()

print("✓ Written to aircraftstates_by_icao24_date")

read_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="aircraftstates_by_icao24_date", keyspace="flight_analytics") \
    .load() \
    .limit(5)

print("\nSample data from aircraftstates_by_icao24_date:")
read_df.show(truncate=False)

print("\n" + "=" * 80)
print("TABLE 3: aircrafts_by_cell_minute")
print("=" * 80)

# Table 3: aircrafts_by_cell_minute - Aircraft by location and time
aircrafts_cell_data = []
for cell in geo_cells:
    for aircraft in aircraft_ids[:3]:
        for i in range(2):  # 2 time buckets per cell
            minute_bucket = current_minute - (i * 60)
            aircrafts_cell_data.append((
                cell,
                minute_bucket,
                aircraft,
                current_ts - (i * 60),
                21.0 + (geo_cells.index(cell) * 10),
                105.8 + (geo_cells.index(cell) * 10),
                10000.0,
                250.0,
                180.0
            ))

aircrafts_cell_schema = StructType([
    StructField("geo_cell", StringType(), False),
    StructField("minute_bucket", LongType(), False),
    StructField("icao24", StringType(), False),
    StructField("last_seen_ts", LongType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("geo_altitude", DoubleType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("true_track", DoubleType(), True),
])

aircrafts_cell_df = spark.createDataFrame(aircrafts_cell_data, schema=aircrafts_cell_schema)
print(f"Generated {aircrafts_cell_df.count()} dummy records")

aircrafts_cell_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="aircrafts_by_cell_minute", keyspace="flight_analytics") \
    .save()

print("✓ Written to aircrafts_by_cell_minute")

read_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="aircrafts_by_cell_minute", keyspace="flight_analytics") \
    .load() \
    .limit(5)

print("\nSample data from aircrafts_by_cell_minute:")
read_df.show(truncate=False)

print("\n" + "=" * 80)
print("TABLE 4: trafficdensity_by_cell_minute")
print("=" * 80)

# Table 4: trafficdensity_by_cell_minute - Aggregated traffic density
trafficdensity_data = []
for i in range(5):  # 5 minute buckets
    minute_bucket = current_minute - (i * 60)
    for cell in geo_cells:
        aircraft_count = 3 + (i % 3)  # Varying counts
        trafficdensity_data.append((
            minute_bucket,
            cell,
            aircraft_count
        ))

trafficdensity_schema = StructType([
    StructField("minute_bucket", LongType(), False),
    StructField("geo_cell", StringType(), False),
    StructField("aircraft_count", LongType(), True),
])

trafficdensity_df = spark.createDataFrame(trafficdensity_data, schema=trafficdensity_schema)
print(f"Generated {trafficdensity_df.count()} dummy records")

trafficdensity_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="trafficdensity_by_cell_minute", keyspace="flight_analytics") \
    .save()

print("✓ Written to trafficdensity_by_cell_minute")

read_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="trafficdensity_by_cell_minute", keyspace="flight_analytics") \
    .load() \
    .limit(5)

print("\nSample data from trafficdensity_by_cell_minute:")
read_df.show(truncate=False)

print("\n" + "=" * 80)
print("TABLE 5: activeaircraft_by_country_hour")
print("=" * 80)

# Table 5: activeaircraft_by_country_hour - Active aircraft per country per hour
activeaircraft_data = []
for country in countries:
    for i in range(4):  # 4 hour buckets
        hour_bucket = current_hour - (i * 3600)
        active_count = 10 + (i * 5) if country != "GLOBAL" else 50 + (i * 20)
        activeaircraft_data.append((
            country,
            hour_bucket,
            active_count
        ))

activeaircraft_schema = StructType([
    StructField("country_code", StringType(), False),
    StructField("hour_bucket", LongType(), False),
    StructField("active_aircraft_cnt", LongType(), True),
])

activeaircraft_df = spark.createDataFrame(activeaircraft_data, schema=activeaircraft_schema)
print(f"Generated {activeaircraft_df.count()} dummy records")

activeaircraft_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="activeaircraft_by_country_hour", keyspace="flight_analytics") \
    .save()

print("✓ Written to activeaircraft_by_country_hour")

read_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="activeaircraft_by_country_hour", keyspace="flight_analytics") \
    .load() \
    .limit(5)

print("\nSample data from activeaircraft_by_country_hour:")
read_df.show(truncate=False)

print("\n" + "=" * 80)
print("TABLE 6: departures_by_country_hour")
print("=" * 80)

# Table 6: departures_by_country_hour - Departures per country per hour
departures_data = []
for country in countries:
    for i in range(4):  # 4 hour buckets
        hour_bucket = current_hour - (i * 3600)
        departures_count = 5 + (i * 2) if country != "GLOBAL" else 25 + (i * 10)
        departures_data.append((
            country,
            hour_bucket,
            departures_count
        ))

departures_schema = StructType([
    StructField("country_code", StringType(), False),
    StructField("hour_bucket", LongType(), False),
    StructField("departures_cnt", LongType(), True),
])

departures_df = spark.createDataFrame(departures_data, schema=departures_schema)
print(f"Generated {departures_df.count()} dummy records")

departures_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="departures_by_country_hour", keyspace="flight_analytics") \
    .save()

print("✓ Written to departures_by_country_hour")

read_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="departures_by_country_hour", keyspace="flight_analytics") \
    .load() \
    .limit(5)

print("\nSample data from departures_by_country_hour:")
read_df.show(truncate=False)

print("\n" + "=" * 80)
print("TABLE 7: arrivals_by_country_hour")
print("=" * 80)

# Table 7: arrivals_by_country_hour - Arrivals per country per hour
arrivals_data = []
for country in countries:
    for i in range(4):  # 4 hour buckets
        hour_bucket = current_hour - (i * 3600)
        arrivals_count = 4 + (i * 2) if country != "GLOBAL" else 20 + (i * 10)
        arrivals_data.append((
            country,
            hour_bucket,
            arrivals_count
        ))

arrivals_schema = StructType([
    StructField("country_code", StringType(), False),
    StructField("hour_bucket", LongType(), False),
    StructField("arrivals_cnt", LongType(), True),
])

arrivals_df = spark.createDataFrame(arrivals_data, schema=arrivals_schema)
print(f"Generated {arrivals_df.count()} dummy records")

arrivals_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="arrivals_by_country_hour", keyspace="flight_analytics") \
    .save()

print("✓ Written to arrivals_by_country_hour")

read_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="arrivals_by_country_hour", keyspace="flight_analytics") \
    .load() \
    .limit(5)

print("\nSample data from arrivals_by_country_hour:")
read_df.show(truncate=False)

# Summary
print("\n" + "=" * 80)
print("✓✓✓ DUMMY DATA LOADING COMPLETE ✓✓✓")
print("=" * 80)
print("\nSuccessfully loaded data into all 7 tables:")
print("  ✓ aircrafts_by_icao24")
print("  ✓ aircraftstates_by_icao24_date")
print("  ✓ aircrafts_by_cell_minute")
print("  ✓ trafficdensity_by_cell_minute")
print("  ✓ activeaircraft_by_country_hour")
print("  ✓ departures_by_country_hour")
print("  ✓ arrivals_by_country_hour")
print("\nAll tables verified with sample reads!")
print("=" * 80)

spark.stop()
