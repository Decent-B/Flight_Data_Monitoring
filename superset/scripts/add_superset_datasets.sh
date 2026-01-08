#!/bin/bash
# Add datasets from Trino tables to Superset

set -e

echo "=========================================="
echo "Adding Datasets to Superset"
echo "=========================================="
echo ""

SUPERSET_POD=$(kubectl get pods -n superset -l app=superset -o jsonpath='{.items[0].metadata.name}')

# List of tables to add as datasets
TABLES=(
    "aircrafts_by_icao24"
    "activeaircraft_by_country_hour"
    "departureaircraft_by_icao24_day"
    "arrivalaircraft_by_icao24_day"
    "activeaircraft_by_country_minute"
    "aircrafts_by_cell_minute"
)

echo "Adding datasets from Trino tables..."
echo ""

for table in "${TABLES[@]}"; do
    echo "Adding dataset: $table"
    kubectl exec -n superset $SUPERSET_POD -- python3 << EOF
from superset import db
from superset.models.core import Database
from superset.connectors.sqla.models import SqlaTable
from sqlalchemy import Column, String, Integer, BigInteger, Float, Boolean

# Get Trino database
trino_db = db.session.query(Database).filter_by(database_name='Trino').first()
if not trino_db:
    print("ERROR: Trino database not found!")
    exit(1)

# Check if dataset already exists
existing = db.session.query(SqlaTable).filter_by(
    database_id=trino_db.id,
    table_name='$table'
).first()

if existing:
    print("Dataset '$table' already exists, skipping...")
else:
    # Create dataset
    dataset = SqlaTable(
        table_name='$table',
        database_id=trino_db.id,
        schema='flight_analytics',
    )
    db.session.add(dataset)
    db.session.commit()
    print("✓ Added dataset: $table")
EOF
done

echo ""
echo "=========================================="
echo "✓ Datasets Added Successfully!"
echo "=========================================="
echo ""
echo "Datasets added:"
for table in "${TABLES[@]}"; do
    echo "  - $table"
done
echo ""
echo "You can now create charts from these datasets in Superset!"
echo ""
