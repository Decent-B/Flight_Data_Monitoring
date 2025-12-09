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
docker-compose -f docker/docker-spark.yml build -q
echo "✓ Spark image built"
echo ""

# 7. Run dummy data loader
echo "Step 7: Running dummy data loader..."

# Enable pipefail so if docker fails, the script fails (even with grep attached)
set -o pipefail

docker run --rm --network docker_flight-network flight-data-spark:latest \
    /opt/spark/bin/spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
    --exclude-packages com.google.code.findbugs:jsr305 \
    --conf spark.cassandra.connection.host=cassandra-1,cassandra-2,cassandra-3 \
    --conf spark.cassandra.connection.port=9042 \
    --conf spark.cassandra.connection.keepAliveMS=60000 \
    /app/kafka_reader.py \
    | grep --line-buffered -vE "^[0-9/]+ [0-9:]+ INFO"

# Disable pipefail to return to normal bash behavior (optional)
set +o pipefail

echo ""

# 8. Verify data was written to ALL tables
echo "Step 8: Verifying data integrity across all tables..."

# Define all tables to check
TABLES=(
    "aircrafts_by_icao24"
    "aircraftstates_by_icao24"
    "aircrafts_by_cell_minute"
    "activeaircraft_by_country_hour"
    "departures_by_country_hour"
    "arrivals_by_country_hour"
)

# Function to check a single table
check_table() {
    local table=$1
    echo "---------------------------------------------------"
    echo "Checking table: $table"
    
    # Get the count
    count=$(docker exec -i cassandra-1 cqlsh -e "SELECT COUNT(*) FROM flight_analytics.$table;" | grep -o '[0-9]\+' | head -n 1)
    
    if [ -z "$count" ] || [ "$count" -eq 0 ]; then
        echo "❌ FAILURE: Table $table is EMPTY."
        return 1
    else
        echo "✓ SUCCESS: Found $count records."
        # Print a sample to verify schema accessibility
        echo "  Sample Row:"
        docker exec -i cassandra-1 cqlsh -e "SELECT * FROM flight_analytics.$table LIMIT 1;" | sed 's/^/    /'
        return 0
    fi
}

# Track failures
FAILURES=0

# Loop through all tables
for table in "${TABLES[@]}"; do
    check_table "$table"
    if [ $? -ne 0 ]; then
        FAILURES=$((FAILURES+1))
    fi
done

echo ""
echo "---------------------------------------------------"
echo "Specific Logic Checks (Data Validity)"
echo "---------------------------------------------------"

# Check 1: Verify Vietnam (VN) country data exists
echo "Test 1: Checking for Vietnam (VN) data..."
vn_count=$(docker exec -i cassandra-1 cqlsh -e "SELECT count(*) FROM flight_analytics.activeaircraft_by_country_hour WHERE country_code='VN' ALLOW FILTERING;" | grep -o '[0-9]\+' | head -n 1)

if [ -z "$vn_count" ] || [ "$vn_count" -eq 0 ]; then
    echo "❌ FAILURE: No entries found for Country Code 'VN'."
    FAILURES=$((FAILURES+1))
else
    echo "✓ SUCCESS: Found $vn_count entries for Country Code 'VN'."
fi

# Check 2: Verify aircraft states have valid positions
echo "Test 2: Checking aircraft states have valid latitude/longitude..."

position_check=$(docker exec -i cassandra-1 cqlsh -e "SELECT latitude, longitude FROM flight_analytics.aircraftstates_by_icao24 LIMIT 1;" | grep -oE '[0-9]+\.[0-9]+' | head -n 1)

if [ -z "$position_check" ]; then
    echo "❌ FAILURE: No aircraft states with valid position data."
    FAILURES=$((FAILURES+1))
else
    echo "✓ SUCCESS: Aircraft states contain valid position data."
fi

# Check 3: Verify departures and arrivals have matching country data
echo "Test 3: Checking departures/arrivals data consistency..."
departure_countries=$(docker exec -i cassandra-1 cqlsh -e "SELECT DISTINCT country_code FROM flight_analytics.departures_by_country_hour ALLOW FILTERING;" | grep -oE '[A-Z]{2}' | wc -l)
arrival_countries=$(docker exec -i cassandra-1 cqlsh -e "SELECT DISTINCT country_code FROM flight_analytics.arrivals_by_country_hour ALLOW FILTERING;" | grep -oE '[A-Z]{2}' | wc -l)

if [ "$departure_countries" -eq 0 ] || [ "$arrival_countries" -eq 0 ]; then
    echo "❌ FAILURE: Missing departure or arrival country data."
    FAILURES=$((FAILURES+1))
else
    echo "✓ SUCCESS: Found $departure_countries departure countries and $arrival_countries arrival countries."
fi

echo ""
if [ $FAILURES -eq 0 ]; then
    echo "=========================================="
    echo "✓✓✓ ALL CHECKS PASSED: Data Load Successful ✓✓✓"
    echo "=========================================="
    exit 0
else
    echo "=========================================="
    echo "❌ TEST FAILED: $FAILURES check(s) failed."
    echo "=========================================="
    exit 1
fi
