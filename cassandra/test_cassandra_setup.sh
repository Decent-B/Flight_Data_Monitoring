#!/bin/bash

# Cassandra End-to-End Test Script
# This script tests the complete Cassandra setup from scratch

set -e  # Exit on any error

echo "=========================================="
echo "Cassandra End-to-End Test Script"
echo "=========================================="
echo ""

# 1. Create network
echo "Step 1: Creating Docker network..."
docker network create docker_flight-network 2>/dev/null || echo "Network already exists, skipping..."
echo "✓ Network ready"
echo ""

# 2. Start Cassandra cluster
echo "Step 2: Starting Cassandra cluster..."
cd docker
docker-compose -f docker-cassandra.yml up -d
cd ..
echo "✓ Cassandra cluster starting..."
echo ""

# 3. Check cluster health
echo "Step 3: Checking cluster health..."
docker exec cassandra-1 nodetool status
echo ""

# 4. Create keyspace and tables
echo "Step 4: Creating keyspace and tables..."
docker exec -i cassandra-1 cqlsh < cassandra/schema/init_keyspace.cql
echo "✓ Keyspace created"
docker exec -i cassandra-1 cqlsh < cassandra/schema/create_tables.cql
echo "✓ Tables created"
echo ""

# 5. Verify tables exist
echo "Step 5: Verifying tables..."
docker exec -i cassandra-1 cqlsh -e "DESCRIBE TABLES;"
echo ""

# 6. Build Spark image
echo "Step 6: Building Spark image..."
docker-compose -f docker/docker-spark.yml build
echo "✓ Spark image built"
echo ""

# 7. Run dummy data loader
echo "Step 7: Running dummy data loader..."
docker run --rm --network docker_flight-network flight-data-spark:latest \
    /opt/spark/bin/spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
    --conf spark.cassandra.connection.host=cassandra-1,cassandra-2,cassandra-3 \
    --conf spark.cassandra.connection.port=9042 \
    --conf spark.cassandra.connection.keepAliveMS=60000 \
    /app/spark_dummy_loader.py
echo ""

# 8. Verify data was written
echo "Step 8: Verifying data..."
echo "Record count in aircrafts_by_icao24:"
docker exec -i cassandra-1 cqlsh -e "SELECT COUNT(*) FROM flight_analytics.aircrafts_by_icao24;"
echo ""
echo "Sample records:"
docker exec -i cassandra-1 cqlsh -e "SELECT * FROM flight_analytics.aircrafts_by_icao24 LIMIT 3;"
echo ""

echo "=========================================="
echo "✓✓✓ End-to-end test complete! ✓✓✓"
echo "=========================================="
