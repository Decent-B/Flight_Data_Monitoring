# Cassandra Architecture Testing Guide

## What This Guide Is For

This guide helps you test the **Cassandra data storage layer** of the Flight Data Monitoring system from scratch. You'll learn how to:

- Start a 3-node Cassandra cluster
- Create the database schema (keyspace + 7 tables)
- Use Spark to write test data to Cassandra
- Verify everything works correctly

This is a **complete end-to-end test** of the Cassandra architecture, designed for someone with no prior Cassandra experience. By the end, you'll have a working Cassandra cluster with sample flight data ready for querying.

---

## Prerequisites

Before starting, make sure you have:

- ✅ **Docker Desktop** installed and running
- ✅ **Git** installed
- ✅ **At least 4GB of free RAM** (for the 3-node cluster)
- ✅ **PowerShell or Command Prompt** (Windows) or **Terminal** (Mac/Linux)

No Cassandra installation needed — everything runs in Docker containers!

---

## Clone & Checkout Branch

### Step 1: Clone the Repository

Open your terminal and clone the project:

```bash
git clone <repository-url>
cd Flight_Data_Monitoring
```

### Step 2: Checkout the Correct Branch

Switch to the branch that contains the Cassandra setup:

```bash
git checkout <cassandra-branch-name>
```

### Step 3: Verify Project Structure

Make sure you have these important files:

```
Flight_Data_Monitoring/
├── docker/
│   ├── docker-cassandra.yml    # Cassandra cluster definition
│   ├── docker-spark.yml         # Spark service definition
│   └── Dockerfile.spark         # Spark container image
├── schema/
│   ├── init_keyspace.cql        # Keyspace creation script
│   └── create_tables.cql        # Table schemas (7 tables)
└── src/
    └── spark_dummy_loader.py    # Test data generator
```

If you see old test files like `spark_cassandra_test.py`, you can **ignore or delete them** — we'll use a simpler command-based workflow instead.

---

## Start Cassandra Cluster

### What Is This Step?

This starts a **3-node Cassandra cluster** using Docker Compose. Each node stores a copy of your data for reliability. The cluster will automatically:

- Create three Cassandra containers (cassandra-1, cassandra-2, cassandra-3)
- Set up a shared network for communication
- Create persistent storage volumes for your data

### Step 4: Create the Docker Network

First, create a network that allows all containers to communicate:

```bash
docker network create docker_flight-network
```

**What this does:** Creates a virtual network named `docker_flight-network` that Cassandra, Spark, and Kafka containers will use to talk to each other.

### Step 5: Start the Cassandra Cluster

Navigate to the docker directory and start the cluster:

```bash
cd docker
docker-compose -f docker-cassandra.yml up -d
```

**What this does:**
- `-f docker-cassandra.yml` tells Docker which configuration file to use
- `up` starts the services
- `-d` runs them in the background (detached mode)

### Step 6: Wait for Cluster to Initialize

Cassandra takes time to start up. Wait about **60-90 seconds**, then check the cluster status:

```bash
docker exec cassandra-1 nodetool status
```

**What to look for:**

```
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address     Load    Tokens  Owns    Host ID    Rack
UN  172.18.0.8  ...     256     100%    ...        rack1
UN  172.18.0.6  ...     256     100%    ...        rack1
UN  172.18.0.7  ...     256     100%    ...        rack1
```

**UN** means **Up and Normal** — all three nodes should show this status. If you see **UJ** (Up and Joining), wait another 30 seconds and check again.

---

## Initialize Keyspace & Tables

### What Is This Step?

A **keyspace** is like a database in traditional SQL systems. Inside it, you'll create 7 tables designed for different types of flight data queries.

### Step 7: Create the Keyspace

Return to the project root and run:

```bash
cd ..
docker exec -i cassandra-1 cqlsh < schema/init_keyspace.cql
```

**What this does:**
- Connects to the first Cassandra node
- Runs CQL (Cassandra Query Language) commands from `init_keyspace.cql`
- Creates a keyspace called `flight_analytics` with 3 replicas (one on each node)

**Expected output:** You should see confirmation that the keyspace was created.

### Step 8: Create the Tables

Now create all 7 tables inside the keyspace:

```bash
docker exec -i cassandra-1 cqlsh < schema/create_tables.cql
```

**What this does:** Creates these tables in the `flight_analytics` keyspace:

1. **aircrafts_by_icao24** — Current state of each aircraft
2. **aircraftstates_by_icao24_date** — Historical time-series data per aircraft
3. **aircrafts_by_cell_minute** — Aircraft positions by geographic region and time
4. **trafficdensity_by_cell_minute** — Traffic density heatmap data
5. **activeaircraft_by_country_hour** — Hourly active aircraft counts by country
6. **departures_by_country_hour** — Hourly departure statistics
7. **arrivals_by_country_hour** — Hourly arrival statistics

### Step 9: Verify Tables Were Created

Check that all tables exist:

```bash
docker exec -i cassandra-1 cqlsh -e "DESCRIBE TABLES;"
```

**What to expect:** You should see all 7 table names listed.

---

## Run Spark Dummy Loader (Writes + Reads)

### What Is This Step?

Spark is a data processing engine. Here, we'll use it to:
- Generate realistic test data (aircraft positions, flight statistics, etc.)
- Write that data to all 7 Cassandra tables
- Read it back to verify everything works

### Step 10: Build the Spark Container Image

First, build the Docker image that contains Spark and your Python scripts:

```bash
docker-compose -f docker/docker-spark.yml build
```

**What this does:** Creates a container image with:
- Apache Spark 3.4.0
- Python libraries for Kafka and Cassandra
- Your dummy data loader script

This may take 2-3 minutes the first time.

### Step 11: Run the Dummy Data Loader

Now run the script that generates and loads test data:

```bash
docker run --rm --network docker_flight-network flight-data-spark:latest /opt/spark/bin/spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 --conf spark.cassandra.connection.host=cassandra-1,cassandra-2,cassandra-3 --conf spark.cassandra.connection.port=9042 --conf spark.cassandra.connection.keepAliveMS=60000 /app/spark_dummy_loader.py
```

**What this does:**
- Starts a temporary Spark container
- Loads necessary libraries for Cassandra connectivity
- Runs `spark_dummy_loader.py` which generates and writes test data
- Automatically removes the container when done (`--rm`)

**What to expect:** You'll see lots of Spark logging. Look for these success messages:

```
✓ Written to aircrafts_by_icao24
✓ Written to aircraftstates_by_icao24_date
✓ Written to aircrafts_by_cell_minute
✓ Written to trafficdensity_by_cell_minute
✓ Written to activeaircraft_by_country_hour
✓ Written to departures_by_country_hour
✓ Written to arrivals_by_country_hour

✓✓✓ DUMMY DATA LOADING COMPLETE ✓✓✓
```

The script generates approximately **125 records** across all tables with realistic flight data.

---

## Verify Data Using cqlsh

### What Is This Step?

Now that data is loaded, let's verify it by querying Cassandra directly using **cqlsh** (Cassandra Query Language Shell).

### Step 12: Check Record Counts

See how many records are in each table:

```bash
docker exec -i cassandra-1 cqlsh -e "SELECT COUNT(*) FROM flight_analytics.aircrafts_by_icao24;"
docker exec -i cassandra-1 cqlsh -e "SELECT COUNT(*) FROM flight_analytics.aircraftstates_by_icao24_date;"
docker exec -i cassandra-1 cqlsh -e "SELECT COUNT(*) FROM flight_analytics.aircrafts_by_cell_minute;"
```

**What to expect:** You should see counts like 5, 15, 24, etc. (depending on how much test data was generated).

### Step 13: View Sample Data

Look at actual records from different tables:

#### Current Aircraft States
```bash
docker exec -i cassandra-1 cqlsh -e "SELECT * FROM flight_analytics.aircrafts_by_icao24 LIMIT 3;"
```

**What you'll see:** Current position, altitude, speed, and status of 3 aircraft.

#### Time-Series Data
```bash
docker exec -i cassandra-1 cqlsh -e "SELECT * FROM flight_analytics.aircraftstates_by_icao24_date LIMIT 5;"
```

**What you'll see:** Historical snapshots of aircraft positions over time.

#### Traffic Density Heatmap
```bash
docker exec -i cassandra-1 cqlsh -e "SELECT * FROM flight_analytics.trafficdensity_by_cell_minute WHERE minute_bucket=1764759780;"
```

**What you'll see:** How many aircraft were in each geographic cell at a specific minute.

#### Country Statistics
```bash
docker exec -i cassandra-1 cqlsh -e "SELECT * FROM flight_analytics.activeaircraft_by_country_hour WHERE country_code='US';"
docker exec -i cassandra-1 cqlsh -e "SELECT * FROM flight_analytics.departures_by_country_hour WHERE country_code='VN';"
docker exec -i cassandra-1 cqlsh -e "SELECT * FROM flight_analytics.arrivals_by_country_hour WHERE country_code='GLOBAL';"
```

**What you'll see:** Hourly statistics for active aircraft, departures, and arrivals by country.

---

## Full End-to-End Test Script

Want to test everything in one go? Copy and paste this complete sequence:

```bash
# 1. Create network
docker network create docker_flight-network

# 2. Start Cassandra cluster
cd docker
docker-compose -f docker-cassandra.yml up -d
cd ..

# 3. Wait for cluster (adjust sleep time if needed)
echo "Waiting 90 seconds for Cassandra to initialize..."
sleep 90

# 4. Check cluster health
docker exec cassandra-1 nodetool status

# 5. Create keyspace and tables
docker exec -i cassandra-1 cqlsh < schema/init_keyspace.cql
docker exec -i cassandra-1 cqlsh < schema/create_tables.cql

# 6. Verify tables exist
docker exec -i cassandra-1 cqlsh -e "DESCRIBE TABLES;"

# 7. Build Spark image
docker-compose -f docker/docker-spark.yml build

# 8. Run dummy data loader
docker run --rm --network docker_flight-network flight-data-spark:latest /opt/spark/bin/spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 --conf spark.cassandra.connection.host=cassandra-1,cassandra-2,cassandra-3 --conf spark.cassandra.connection.port=9042 --conf spark.cassandra.connection.keepAliveMS=60000 /app/spark_dummy_loader.py

# 9. Verify data was written
docker exec -i cassandra-1 cqlsh -e "SELECT COUNT(*) FROM flight_analytics.aircrafts_by_icao24;"
docker exec -i cassandra-1 cqlsh -e "SELECT * FROM flight_analytics.aircrafts_by_icao24 LIMIT 3;"

echo "✓ End-to-end test complete!"
```

**On Windows PowerShell**, replace `sleep 90` with `Start-Sleep -Seconds 90`.

---

## Reset & Cleanup Instructions

### When to Use This Section

Use these commands if you want to:
- Start fresh with a clean database
- Remove old test data
- Free up disk space
- Troubleshoot cluster issues

### Stop the Cluster

To stop all Cassandra containers without deleting data:

```bash
cd docker
docker-compose -f docker-cassandra.yml down
```

**What this does:** Stops and removes containers, but keeps your data volumes intact.

### Remove Data Volumes (Complete Reset)

To completely delete all Cassandra data and start from scratch:

```bash
docker-compose -f docker-cassandra.yml down -v
```

**⚠️ Warning:** This deletes all data permanently! You'll need to re-run the schema creation and data loading steps.

### Check Data Volume Size

To see how much disk space your Cassandra data is using:

```bash
docker system df -v | grep cassandra
```

### Delete Specific Volumes

If you want to remove volumes manually:

```bash
docker volume rm docker_cassandra-1-data
docker volume rm docker_cassandra-2-data
docker volume rm docker_cassandra-3-data
```

### Clean Up Test Data (Without Removing Volumes)

To keep the cluster running but delete all records:

```bash
docker exec -i cassandra-1 cqlsh -e "TRUNCATE flight_analytics.aircrafts_by_icao24;"
docker exec -i cassandra-1 cqlsh -e "TRUNCATE flight_analytics.aircraftstates_by_icao24_date;"
docker exec -i cassandra-1 cqlsh -e "TRUNCATE flight_analytics.aircrafts_by_cell_minute;"
docker exec -i cassandra-1 cqlsh -e "TRUNCATE flight_analytics.trafficdensity_by_cell_minute;"
docker exec -i cassandra-1 cqlsh -e "TRUNCATE flight_analytics.activeaircraft_by_country_hour;"
docker exec -i cassandra-1 cqlsh -e "TRUNCATE flight_analytics.departures_by_country_hour;"
docker exec -i cassandra-1 cqlsh -e "TRUNCATE flight_analytics.arrivals_by_country_hour;"
```

**What this does:** Empties all tables while keeping the schema intact.

---

## Troubleshooting

### Problem: "UN" status not appearing for all nodes

**Solution:** Cassandra needs time to gossip and synchronize. Wait another 30-60 seconds and check again:

```bash
docker exec cassandra-1 nodetool status
```

### Problem: "Connection refused" errors

**Solution:** Make sure the Docker network exists:

```bash
docker network ls | grep flight-network
```

If missing, create it:

```bash
docker network create docker_flight-network
```

### Problem: Spark job fails with "ClassNotFoundException"

**Solution:** Ensure you're using the complete `spark-submit` command with the `--packages` flag that downloads the Cassandra connector.

### Problem: Tables show 0 records after loading

**Solution:** Check Spark logs for errors. The dummy loader prints success messages — if you don't see them, the write may have failed.

### Problem: Out of disk space

**Solution:** Remove unused Docker resources:

```bash
docker system prune -a --volumes
```

**⚠️ Warning:** This removes ALL unused Docker data, not just Cassandra.

---

## What's Next?

After completing this guide, you have:

- ✅ A working 3-node Cassandra cluster
- ✅ A complete schema with 7 tables
- ✅ Test data loaded via Spark
- ✅ Verified everything works end-to-end

**Next steps in the project:**

1. **Integrate Kafka** — Stream real-time flight data from OpenSky Network API
2. **Build Spark streaming pipeline** — Process Kafka data and write to Cassandra in real-time
3. **Add derived fields** — Calculate geo-cells, time buckets, departure/arrival detection
4. **Create dashboards** — Visualize the data using tools like Grafana or custom web apps

---

## Additional Resources

- **Cassandra Documentation**: https://cassandra.apache.org/doc/latest/
- **CQL Reference**: https://cassandra.apache.org/doc/latest/cql/
- **Spark-Cassandra Connector**: https://github.com/datastax/spark-cassandra-connector
- **Docker Compose Documentation**: https://docs.docker.com/compose/

---

## Getting Help

If you encounter issues:

1. Check the **Troubleshooting** section above
2. Review Docker logs: `docker logs cassandra-1`
3. Verify cluster status: `docker exec cassandra-1 nodetool status`
4. Ask your team or instructor for assistance

Happy testing! 🚀
