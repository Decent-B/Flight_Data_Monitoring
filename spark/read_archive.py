from pyspark.sql import SparkSession
 
# Create Spark session with S3A configuration
spark = SparkSession \
    .builder \
    .appName("ReadMinIOArchive") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()
 
# Read all archived flight data
flights_archive = spark.read.parquet("s3a://flight-raw/")
flights_archive.show(10)
 
# Query specific date partition
flights_jan_7 = spark.read.parquet("s3a://flight-raw/year=2026/month=1/day=7")
flights_jan_7.count()
 
# Filter with SQL
flights_archive.createOrReplaceTempView("flights")
spark.sql("""
    SELECT icao24, callsign, latitude, longitude, timestamp
    FROM flights
    WHERE year = 2026 AND month = 1 AND day = 7
    LIMIT 100
""").show()
 
# Read flight tracks
tracks_archive = spark.read.parquet("s3a://flight-tracks/")
tracks_archive.show(5, truncate=False)
 
# Read flight connection data
flight_data = spark.read.parquet("s3a://flight-data/")
flight_data.show(10)