# Apache Superset Deployment Guide

## Overview

Apache Superset has been successfully deployed to your Minikube cluster with Trino as the ONLY data source.

## Architecture

```
Cassandra (Storage)
    ↓
Trino (Query Engine)
    ↓
Superset (Visualization)
```

## Deployment Details

### Components Deployed

1. **PostgreSQL 14** - Superset metadata database
   - Service: `superset-postgres.superset.svc.cluster.local:5432`
   - Database: `superset`
   - User: `superset`

2. **Redis 7** - Superset caching layer
   - Service: `superset-redis.superset.svc.cluster.local:6379`

3. **Apache Superset** - Business Intelligence platform
   - Service: `superset.superset.svc.cluster.local:8088` (NodePort: 30088)
   - Image: `apache/superset:latest-dev`
   - Includes Trino and sqlalchemy-trino drivers

4. **Trino Database Connection**
   - Database Name: `Trino`
   - SQLAlchemy URI: `trino://admin@trino.trino.svc.cluster.local:8080/cassandra.properties/flight_analytics`
   - Catalog: `cassandra.properties`
   - Schema: `flight_analytics`

### Datasets Available

The following datasets have been automatically added from your Cassandra tables:

1. `aircrafts_by_icao24` - Aircraft master data
2. `activeaircraft_by_country_hour` - Active aircraft by country and hour
3. `departureaircraft_by_icao24_day` - Departure data by aircraft and day
4. `arrivalaircraft_by_icao24_day` - Arrival data by aircraft and day
5. `activeaircraft_by_country_minute` - Active aircraft by country and minute (real-time)
6. `aircrafts_by_cell_minute` - Aircraft by geographic cell and minute

## Access Superset

### Option 1: Minikube Service (Recommended)

```bash
minikube service -n superset superset --url
```

Then open the URL in your browser (e.g., `http://<minikube-ip>:<nodeport>`)

### Option 2: Port Forward

```bash
kubectl port-forward --address 0.0.0.0 -n superset svc/superset 8088:8088
```

Then access: http://<host-ip>:8088

### Login Credentials

- **Username**: `admin`
- **Password**: `admin`

## Creating Visualizations

### 1. Verify Data Connection

1. Go to **Settings** → **Database Connections**
2. You should see "Trino" as the only database
3. Click **Edit** and test the connection

### 2. View Datasets

1. Go to **Data** → **Datasets**
2. You should see 6 datasets from the `flight_analytics` schema

### 3. Create Your First Chart

#### Example 1: Aircraft Distribution by Country (Pie Chart)

1. Go to **Charts** → **+ Chart**
2. Choose dataset: `activeaircraft_by_country_hour`
3. Choose chart type: **Pie Chart**
4. Configuration:
   - **Dimension**: `country_code`
   - **Metric**: `COUNT(*)`
5. Click **Update Chart**
6. Save the chart as "Aircraft by Country"

#### Example 2: Aircraft Activity Timeline (Line Chart)

1. Create new chart with dataset: `activeaircraft_by_country_hour`
2. Choose chart type: **Time-series Line Chart**
3. Configuration:
   - **Time Column**: `hour_bucket`
   - **Metric**: `COUNT(*)`
   - **Dimensions**: `country_code`
4. Click **Update Chart**
5. Save as "Aircraft Activity Over Time"

#### Example 3: Departures vs Arrivals (Bar Chart)

1. Create two separate charts or use **Mixed Chart** type
2. Datasets: `departureaircraft_by_icao24_day` and `arrivalaircraft_by_icao24_day`
3. Compare departure and arrival counts by day

### 4. Create a Dashboard

1. Go to **Dashboards** → **+ Dashboard**
2. Name it "Flight Monitoring Dashboard"
3. Add the charts you created
4. Arrange them in a layout
5. Save and share!

## Sample Data

Your Superset instance is connected to the following sample data:

- **5 aircraft**: ABC123 (VN), DEF456 (SG), GHI789 (US), JKL012 (GB), MNO345 (VN)
- **Countries**: Vietnam (VN), Singapore (SG), United States (US), United Kingdom (GB)
- **Time range**: Recent data with hourly and minute-level granularity

## Advanced Configuration

### Adding More Data

To add more data to Cassandra:

```bash
# Edit the seed script
vim k8s/seed-cassandra-data.sh

# Run the script
./k8s/seed-cassandra-data.sh
```

### Querying Data Directly via Trino

```bash
# Port forward to Trino
kubectl port-forward -n trino svc/trino 8080:8080

# Use Trino CLI or connect from your SQL client
# Connection: trino://<trino-host>:8080
# Catalog: cassandra.properties
# Schema: flight_analytics
```

### Refreshing Datasets

If you add new data or modify tables:

1. Go to **Data** → **Datasets**
2. Select the dataset
3. Click **Edit** → **Sync columns from source**
4. Save

## Troubleshooting

### Check Superset Logs

```bash
kubectl logs -n superset -l app=superset --tail=100 -f
```

### Check Trino Connection

```bash
# Get Superset pod name
SUPERSET_POD=$(kubectl get pods -n superset -l app=superset -o jsonpath='{.items[0].metadata.name}')

# Test Trino connection
kubectl exec -n superset $SUPERSET_POD -- python3 << 'EOF'
from sqlalchemy import create_engine
engine = create_engine('trino://admin@trino.trino.svc.cluster.local:8080/cassandra.properties/flight_analytics')
with engine.connect() as conn:
    result = conn.execute("SELECT * FROM aircrafts_by_icao24 LIMIT 5")
    for row in result:
        print(row)
EOF
```

### Restart Superset

```bash
kubectl rollout restart deployment/superset -n superset
kubectl rollout status deployment/superset -n superset
```

### Check All Components

```bash
kubectl get pods -n superset
kubectl get svc -n superset
```

Expected output:
```
NAME                              READY   STATUS      RESTARTS   AGE
superset-584c44587c-xnxxq         1/1     Running     0          5m
superset-init-dhw8t               0/1     Completed   0          5m
superset-postgres-0               1/1     Running     0          5m
superset-redis-665d855f96-76cjf   1/1     Running     0          5m
```

## Configuration Files

- **Deployment YAML**: `superset/k8s/superset-deployment.yaml`
- **Deploy Script**: `superset/scripts/deploy_superset.sh`
- **Configure Trino**: `superset/scripts/configure_superset_trino.sh`
- **Add Datasets**: `superset/scripts/add_superset_datasets.sh`

## Security Notes

⚠️ **Important**: This deployment uses default credentials for demo purposes:

- Superset: admin/admin
- PostgreSQL: superset/superset

**For production**:
1. Change all default passwords
2. Enable HTTPS/TLS
3. Configure proper authentication (LDAP, OAuth, etc.)
4. Set up proper RBAC
5. Use secrets for sensitive data

## Data Source Restriction

✅ **Verified**: Superset is configured with Trino as the ONLY data source. No other database connections can be added without explicit configuration.

To verify:
1. Log into Superset
2. Go to **Settings** → **Database Connections**
3. You should only see "Trino"

## Next Steps

1. **Explore the UI**: Familiarize yourself with Superset's interface
2. **Create Charts**: Use the datasets to create various visualizations
3. **Build Dashboards**: Combine charts into comprehensive dashboards
4. **Set Up Alerts**: Configure SQL Lab and set up alerts (optional)
5. **Share**: Share dashboards with your team

## Support

For issues or questions:
- Check Superset logs: `kubectl logs -n superset -l app=superset`
- Check Trino connectivity from Superset pod
- Verify Cassandra data is accessible via Trino
- Review this guide for configuration details

---

**Deployment Date**: $(date)
**Superset Version**: latest-dev (includes PostgreSQL and Trino drivers)
**Kubernetes Cluster**: Minikube
