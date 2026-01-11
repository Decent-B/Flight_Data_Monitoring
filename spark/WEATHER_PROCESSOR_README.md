# Weather Data Processor - Implementation Documentation

## Overview

The `weather_processor.py` module processes weather data from Aviation Weather API and writes to Cassandra tables defined in `cassandra/schema/weather_tables.cql`.

## Architecture

```
[NiFi] → [Kafka Topics] → [Spark Streaming] → [Cassandra Tables]
                                           ↓
                                        [MinIO Archive]
```

## Data Flow

### Input Sources (Kafka Topics)

| Topic | Source | Description |
|-------|--------|-------------|
| `metar` | Aviation Weather API | Meteorological Aerodrome Reports (surface observations) |
| `taf` | Aviation Weather API | Terminal Aerodrome Forecasts (aviation weather forecasts) |
| `isigmet` | Aviation Weather API | International SIGMETs (significant meteorological warnings) |

### Output Targets

#### Cassandra Tables (4 tables)

| Table | Purpose | Update Pattern |
|-------|---------|----------------|
| `metar_latest_by_station` | Latest METAR per station for map visualization | UPSERT |
| `metar_history_by_station` | Time-series METAR history for timeline | APPEND (3-day TTL) |
| `sigmet_by_validtime` | Active SIGMETs with hour-based partitioning | APPEND (7-day TTL) |
| `taf_by_station` | TAF forecast periods grouped by bulletin | APPEND (3-day TTL) |

#### MinIO Buckets (Archive)

- `s3a://weather-metar/` - Raw METAR data (5-minute batches)
- `s3a://weather-taf/` - Raw TAF data (5-minute batches)
- `s3a://weather-isigmet/` - Raw ISIGMET data (5-minute batches)

## Implementation Details

### Schema Definitions

#### METAR Schema
- **GeoJSON Feature** with properties and geometry
- Key fields: station ID, observation time, temperature, wind, visibility, ceiling, flight category
- Cloud layers as array of structs
- Coordinates as [longitude, latitude]

#### TAF Schema
- **GeoJSON Feature** with forecast periods
- Key fields: station ID, issue time, validity period, forecast type, weather parameters
- Multiple periods per bulletin (timeGroup: 0=PREVAIL, 1+=FM/TEMPO/BECMG)

#### ISIGMET Schema
- **GeoJSON Feature** with polygon geometry
- Key fields: ICAO ID, FIR, hazard type, validity period, altitude range
- Polygon coordinates for affected area

### Transformations

#### Table 8: `metar_latest_by_station`
**Purpose**: Store ONLY the latest METAR per station for map markers

**Transformations**:
1. Filter null coordinates (lat/lon must exist)
2. Add 10-minute watermark for late data
3. Convert clouds array → JSON string
4. Select columns matching Cassandra schema

**Query Pattern**: Get latest METAR for stations in viewport

#### Table 9: `metar_history_by_station`
**Purpose**: Time-series METAR for comparing observations vs forecasts

**Transformations**:
1. Add 10-minute watermark
2. Select only timeline-critical fields (minimal for performance)
3. Cassandra handles deduplication via PRIMARY KEY (station_id, obs_time)

**Query Pattern**: Get METAR observations in time window

#### Table 10: `sigmet_by_validtime`
**Purpose**: Active SIGMETs with time-based partitioning for time-slider queries

**Transformations**:
1. Filter null polygon data
2. Add 10-minute watermark
3. **Compute hour_bucket**: `floor(valid_from / 3600) * 3600` (floor to hour)
4. Convert polygon coordinates → JSON string
5. Select columns matching Cassandra schema

**Query Pattern**: Get active SIGMETs for time slider (query 3-4 hour buckets)

#### Table 11: `taf_by_station`
**Purpose**: TAF forecast periods grouped by bulletin issuance

**Transformations**:
1. Add 10-minute watermark
2. Convert clouds array → JSON string
3. NiFi already splits bulletins into periods (timeGroup field)
4. Cassandra deduplicates via PRIMARY KEY ((station_id, issue_time), time_group, valid_from)

**Query Pattern**: Get all forecast periods for latest TAF bulletin

### Key Processing Features

#### Watermarking
All streams use **10-minute watermark** to handle late-arriving data:
```python
.withWatermark("timestamp", "10 minutes")
```

#### Timestamp Conversions
- **Input**: ISO 8601 format (e.g., "2025-12-13T02:30:00.000Z")
- **Output**: Unix timestamp (seconds since epoch)
```python
unix_timestamp(col("obs_time_iso"))
```

#### JSON Serialization
Cloud layers and polygon coordinates stored as JSON strings:
```python
to_json(col("clouds_array"))
```

#### Hour Bucketing (SIGMET only)
```python
(spark_floor(col("valid_from") / 3600) * 3600).cast("long")
```

### Checkpointing

Each streaming query has dedicated checkpoint location in MinIO:
- `s3a://checkpoints/metar_latest_by_station/`
- `s3a://checkpoints/metar_history_by_station/`
- `s3a://checkpoints/sigmet_by_validtime/`
- `s3a://checkpoints/taf_by_station/`
- `s3a://checkpoints/archive_metar/`
- `s3a://checkpoints/archive_taf/`
- `s3a://checkpoints/archive_isigmet/`

## Running the Processor

### Standalone Execution
```bash
spark-submit \
  --master local[4] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.cassandra.connection.host=cassandra-1,cassandra-2,cassandra-3 \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.sql.streaming.checkpointLocation=s3a://checkpoints/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
  spark/weather_processor.py
```

### Docker Deployment

#### Option 1: Create separate Docker Compose file
Create `docker/docker-weather-spark.yml`:
```yaml
version: '3.8'

services:
  weather-spark:
    build:
      context: ..
      dockerfile: docker/Dockerfile.spark
    image: flight-data-spark:latest
    container_name: weather-spark
    ports:
      - "4041:4040"  # Different UI port
    command: >
      /opt/spark/bin/spark-submit
        --master local[4]
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262
        --conf spark.sql.streaming.checkpointLocation=s3a://checkpoints/
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000
        --conf spark.hadoop.fs.s3a.access.key=minioadmin
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin123
        --conf spark.hadoop.fs.s3a.path.style.access=true
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
        --conf spark.cassandra.connection.host=cassandra-1,cassandra-2,cassandra-3
        --conf spark.cassandra.connection.port=9042
        --conf spark.cassandra.auth.username=cassandra
        --conf spark.cassandra.auth.password=cassandra
        /app/spark/weather_processor.py
    restart: unless-stopped
    networks:
      - flight-network

networks:
  flight-network:
    external: true
    name: docker_flight-network
```

Run:
```bash
docker compose -f docker/docker-weather-spark.yml up -d
```

#### Option 2: Modify Dockerfile.spark to support both processors
Add sed replacement for weather_processor.py in `docker/Dockerfile.spark`:
```dockerfile
RUN sed -i 's/localhost:29092,localhost:29093,localhost:29094/kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092/g' /app/spark/*.py
```

## Monitoring

### Spark UI
- Flight processor: http://localhost:4040
- Weather processor: http://localhost:4041 (if running separately)

### Check Running Queries
```python
spark.streams.active  # List all active queries
```

### Query Statistics
Each query provides:
- Processing rate
- Input rows per second
- Batch duration
- Checkpoint location

## Data Validation

### Verify Data in Cassandra

```bash
docker exec -it cassandra-1 cqlsh
```

```sql
USE flight_analytics;

-- Check latest METAR
SELECT * FROM metar_latest_by_station LIMIT 10;
SELECT COUNT(*) FROM metar_latest_by_station;

-- Check METAR history
SELECT * FROM metar_history_by_station WHERE station_id = 'KJFK' LIMIT 10;

-- Check SIGMETs
SELECT * FROM sigmet_by_validtime WHERE hour_bucket >= 1704931200 LIMIT 10;

-- Check TAF forecasts
SELECT * FROM taf_by_station WHERE station_id = 'KJFK' LIMIT 10;
```

### Verify MinIO Archive

```bash
# List archived METAR files
aws s3 ls s3://weather-metar/ --recursive --endpoint-url http://localhost:9000

# List archived TAF files
aws s3 ls s3://weather-taf/ --recursive --endpoint-url http://localhost:9000

# List archived ISIGMET files
aws s3 ls s3://weather-isigmet/ --recursive --endpoint-url http://localhost:9000
```

## Troubleshooting

### No Data in Tables
1. **Check Kafka topics exist and have data**:
   ```bash
   docker exec kafka-broker-1 kafka-topics --bootstrap-server localhost:9092 --list
   docker exec kafka-broker-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic metar --max-messages 1
   ```

2. **Check NiFi is publishing to topics**:
   - Access NiFi UI: https://localhost:8443/nifi
   - Verify processors are running
   - Check bulletin board for errors

3. **Check Spark logs**:
   ```bash
   docker logs weather-spark -f
   ```

### Checkpoint Issues
If checkpoint corruption occurs:
```bash
# Delete checkpoint (will restart from beginning)
docker exec minio mc rm -r --force minio/checkpoints/metar_latest_by_station/
```

### Schema Mismatch
If Cassandra schema changes, you must:
1. Stop Spark processor
2. Delete checkpoints
3. Restart processor

### Late Data
The 10-minute watermark allows late data within that window. Data arriving later will be dropped.

## Performance Tuning

### Micro-batch Intervals
- **Default**: Process as data arrives
- **For archival**: 5-minute batches (`trigger(processingTime="5 minutes")`)

### Partitioning
- METAR/TAF: Partitioned by station_id in Cassandra
- SIGMET: Partitioned by hour_bucket for efficient time queries

### Memory Settings
Adjust in docker-compose if needed:
```yaml
environment:
  - SPARK_DRIVER_MEMORY=2g
  - SPARK_EXECUTOR_MEMORY=2g
```

## Related Files

- **Schema**: `cassandra/schema/weather_tables.cql` - Cassandra table definitions
- **Data Source**: `system_docs/data_source_readme.md` - Kafka topic schemas
- **Flight Processor**: `spark/reader.py` - Flight data processing
- **Docker Config**: `docker/docker-spark.yml` - Deployment configuration
- **Dockerfile**: `docker/Dockerfile.spark` - Spark container build

## Future Enhancements

1. **Add AIRMET processing** (if available from API)
2. **Implement weather-flight correlation** (join weather with flight data)
3. **Add weather alerts table** (severe weather affecting airports)
4. **Create weather quality metrics** (data freshness, completeness)
5. **Add weather trend analysis** (detect improving/deteriorating conditions)
