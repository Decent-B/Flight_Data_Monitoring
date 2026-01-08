#!/bin/bash
# Configure Trino database connection in Superset

set -e

echo "=========================================="
echo "Configuring Trino Connection in Superset"
echo "=========================================="
echo ""

# Get the Superset pod
SUPERSET_POD=$(kubectl get pods -n superset -l app=superset -o jsonpath='{.items[0].metadata.name}')

echo "Superset pod: $SUPERSET_POD"
echo ""

# Add Trino database connection via Python script
echo "Adding Trino database connection..."
kubectl exec -n superset $SUPERSET_POD -- python3 << 'EOF'
from superset import db
from superset.models.core import Database

# Check if Trino database already exists
existing = db.session.query(Database).filter_by(database_name='Trino').first()

if existing:
    print("Trino database connection already exists")
    db.session.delete(existing)
    db.session.commit()
    print("Deleted existing Trino connection")

# Create new Trino database connection
trino_db = Database(
    database_name='Trino',
    sqlalchemy_uri='trino://admin@trino.trino.svc.cluster.local:8080/cassandra.properties/flight_analytics',
    expose_in_sqllab=True,
    allow_csv_upload=False,
    allow_ctas=False,
    allow_cvas=False,
    allow_dml=False,
)

db.session.add(trino_db)
db.session.commit()

print("✓ Trino database connection created successfully!")
print(f"  Database ID: {trino_db.id}")
print(f"  Database Name: {trino_db.database_name}")
print(f"  SQLAlchemy URI: {trino_db.sqlalchemy_uri}")
EOF

echo ""
echo "=========================================="
echo "✓ Trino Connection Configured!"
echo "=========================================="
echo ""
echo "Access Superset at: http://127.0.0.1:46524"
echo "Login with: admin / admin"
echo ""
echo "Next steps:"
echo "1. Go to Data > Datasets"
echo "2. Add datasets from Trino tables"
echo "3. Create charts and dashboards"
echo ""
