"""
Spark Dummy Data Loader for Cassandra Tables
Generates synthetic data and loads all 7 Cassandra tables
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType
import time
import random

NUM_AIRCRAFT = 200
NUM_AIRCRAFT_STATES = 60
NUM_HOURS_HISTORY = 24
NUM_GEO_CELLS = 20
NUM_MINUTE_BUCKETS = 30
NUM_HOUR_BUCKETS = 24

print("=" * 80)
print("Spark Dummy Data Loader - Flight Analytics")
print("=" * 80)

# Initialize Spark Session
# Connection details are passed via spark-submit command line arguments
# See cassandra/test_cassandra_setup.sh for the actual configuration
spark = SparkSession \
    .builder \
    .appName("DummyDataLoader") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n✓ Spark Session initialized")
print(f"Spark Version: {spark.version}")

def write_and_preview(df, table):
    print(f"✓ Writing to {table}")
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table=table, keyspace="flight_analytics") \
        .save()

    print(f"\nSample data from {table}:")
    preview_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace="flight_analytics") \
        .load() \
        .limit(5)

    preview_df.show(truncate=False)

# Generate timestamps for different time buckets
current_ts = int(time.time())
current_minute = (current_ts // 60) * 60
current_hour = (current_ts // 3600) * 3600
current_date = int(time.strftime("%Y%m%d", time.gmtime(current_ts)))

# Define aircraft IDs (AC000001, AC000002, ...)
aircraft_ids = [f"AC{i:06d}" for i in range(1, NUM_AIRCRAFT + 1)]

# Define geo cells (cell_000, cell_001, ...)
geo_cells = [f"cell_{i:03d}" for i in range(NUM_GEO_CELLS)]

# Countries (keep as is or extend)
countries = ["VN", "US", "SG", "GB", "GLOBAL"]

print("\n" + "=" * 80)
print("TABLE 1: aircrafts_by_icao24")
print("=" * 80)

# Table 1: aircrafts_by_icao24 - Current aircraft state
aircrafts_data = []
for icao24 in aircraft_ids:
    callsign = f"{random.choice(countries)}{random.randint(100, 999)}"
    # random offset within last 5 minutes
    ts = current_ts - random.randint(0, 300)

    lat = random.uniform(-80, 80)
    lon = random.uniform(-180, 180)
    geo_altitude = random.uniform(0, 12000)
    velocity = random.uniform(100, 280)
    true_track = random.uniform(0, 360)
    vertical_rate = random.uniform(-20, 20)
    on_ground = random.random() < 0.1

    cell = random.choice(geo_cells)
    country_code = random.choice(countries[:-1])  # skip GLOBAL
    origin_country = country_code  # or map to full names

    aircrafts_data.append((
        icao24,
        callsign,
        ts,
        lat,
        lon,
        geo_altitude,
        velocity,
        true_track,
        vertical_rate,
        on_ground,
        cell,
        country_code,
        origin_country,
    ))

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
print(f"Generated {len(aircrafts_data)} dummy records")

# Write to Cassandra
write_and_preview(aircrafts_df, "aircrafts_by_icao24")

print("\n" + "=" * 80)
print("TABLE 2: aircraftstates_by_icao24")
print("=" * 80)

# Table 2: aircraftstates_by_icao24 - Flight tracks with waypoints
aircraftstates_data = []
for aircraft in aircraft_ids[:50]:   # first 50 aircraft, or use all
    # Generate 2-3 flights per aircraft
    num_flights = random.randint(2, 3)
    for flight_num in range(num_flights):
        # Each flight starts at a different time (spread across last 24 hours)
        flight_start = current_ts - (flight_num * 8 * 3600) - random.randint(0, 3600)
        flight_duration = random.randint(7200, 14400)  # 2-4 hour flights
        flight_end = flight_start + flight_duration
        
        # Generate callsign for this flight
        flight_callsign = f"{random.choice(countries[:-1])}{random.randint(100, 999)}"
        
        # Generate waypoints for this flight (every 2 minutes)
        num_waypoints = flight_duration // 120
        for i in range(num_waypoints):
            waypoint_time = flight_start + (i * 120)
            
            # Simulate flight trajectory
            base_lat = random.uniform(-80, 80)
            base_lon = random.uniform(-180, 180)
            
            aircraftstates_data.append((
                aircraft,
                flight_start,
                waypoint_time,
                base_lat + (i * 0.01),              # latitude changes slightly
                base_lon + (i * 0.01),              # longitude changes slightly
                10000.0 + random.uniform(-500, 500), # cruising altitude with variation
                random.uniform(0, 360),              # true_track
                i == 0 or i == (num_waypoints - 1), # on_ground at start/end
                flight_callsign,
                flight_end,
            ))

aircraftstates_schema = StructType([
    StructField("icao24", StringType(), False),
    StructField("starttime", LongType(), False),
    StructField("time", LongType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("baro_altitude", DoubleType(), True),
    StructField("true_track", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("callsign", StringType(), True),
    StructField("endtime", LongType(), True),
])

aircraftstates_df = spark.createDataFrame(aircraftstates_data, schema=aircraftstates_schema)
print(f"Generated {aircraftstates_df.count()} dummy records")

write_and_preview(aircraftstates_df, "aircraftstates_by_icao24")

print("\n" + "=" * 80)
print("TABLE 3: aircrafts_by_cell_minute")
print("=" * 80)

# Table 3: aircrafts_by_cell_minute - Aircraft by location and time
aircrafts_cell_data = []
for cell in geo_cells:
    for i in range(NUM_MINUTE_BUCKETS):
        minute_bucket = current_minute - (i * 60)
        # sample a few aircraft in that cell
        for j, aircraft in enumerate(random.sample(aircraft_ids, k=5)):
            # Unique timestamp for each aircraft to avoid overwriting
            unique_ts = current_ts - (i * 60) + j
            aircrafts_cell_data.append((
                cell,
                minute_bucket,
                unique_ts,
                random.uniform(-80, 80),
                random.uniform(-180, 180),
                random.uniform(0, 12000),
                random.uniform(100, 280),
                random.uniform(0, 360),
                aircraft,
            ))

aircrafts_cell_schema = StructType([
    StructField("geo_cell", StringType(), False),
    StructField("minute_bucket", LongType(), False),
    StructField("last_seen_ts", LongType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("geo_altitude", DoubleType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("true_track", DoubleType(), True),
    StructField("icao24", StringType(), False),
])

aircrafts_cell_df = spark.createDataFrame(aircrafts_cell_data, schema=aircrafts_cell_schema)
print(f"Generated {aircrafts_cell_df.count()} dummy records")

write_and_preview(aircrafts_cell_df, "aircrafts_by_cell_minute")

print("\n" + "=" * 80)
print("TABLE 4: trafficdensity_by_cell_minute")
print("=" * 80)

# Table 4: trafficdensity_by_cell_minute - Aggregated traffic density
trafficdensity_data = []
for i in range(NUM_MINUTE_BUCKETS):  # more minute buckets
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

write_and_preview(trafficdensity_df, "trafficdensity_by_cell_minute")

print("\n" + "=" * 80)
print("TABLE 5: activeaircraft_by_country_hour")
print("=" * 80)

# Table 5: activeaircraft_by_country_hour - Active aircraft per country per hour
activeaircraft_data = []
for country in countries:
    for i in range(NUM_HOUR_BUCKETS):  # more hour buckets
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

write_and_preview(activeaircraft_df, "activeaircraft_by_country_hour")

print("\n" + "=" * 80)
print("TABLE 6: departures_by_country_hour")
print("=" * 80)

# Table 6: departures_by_country_hour - Departures per country per hour
departures_data = []
for country in countries:
    for i in range(NUM_HOUR_BUCKETS):  # more hour buckets
        hour_bucket = current_hour - (i * 3600)
        departures_count = 5 + (i * 3) if country != "GLOBAL" else 15 + (i * 8)
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

write_and_preview(departures_df, "departures_by_country_hour")

print("\n" + "=" * 80)
print("TABLE 7: arrivals_by_country_hour")
print("=" * 80)

# Table 7: arrivals_by_country_hour - Arrivals per country per hour
arrivals_data = []
for country in countries:
    for i in range(NUM_HOUR_BUCKETS):  # more hour buckets
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

write_and_preview(arrivals_df, "arrivals_by_country_hour")

# Summary
print("\n" + "=" * 80)
print("✓✓✓ DUMMY DATA LOADING COMPLETE ✓✓✓")
print("=" * 80)
print("\nSuccessfully loaded data into all 7 tables:")
print("  ✓ aircrafts_by_icao24")
print("  ✓ aircraftstates_by_icao24")
print("  ✓ aircrafts_by_cell_minute")
print("  ✓ trafficdensity_by_cell_minute")
print("  ✓ activeaircraft_by_country_hour")
print("  ✓ departures_by_country_hour")
print("  ✓ arrivals_by_country_hour")
print("\nAll tables verified with sample reads!")
print("=" * 80)

spark.stop()