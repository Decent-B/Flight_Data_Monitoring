#!/bin/bash
# Deploy Apache Superset to Kubernetes (Minikube)

set -e

echo "=========================================="
echo "Deploying Apache Superset"
echo "=========================================="
echo ""

# Clean up old deployment
echo "Cleaning up old Superset deployment..."
kubectl delete namespace superset --force --grace-period=0 2>/dev/null || true
sleep 5

# Create namespace
echo "Creating superset namespace..."
kubectl create namespace superset

# Deploy PostgreSQL, Redis, ConfigMap, and Init Job first
echo "Deploying Superset infrastructure..."
kubectl apply -f superset/k8s/superset-deployment.yaml -n superset

# Wait for init job separately
echo ""
echo "Waiting for PostgreSQL to be ready..."
kubectl wait --for=condition=ready pod -l app=superset-postgres -n superset --timeout=5m

echo ""
echo "Waiting for Redis to be ready..."
kubectl wait --for=condition=ready pod -l app=superset-redis -n superset --timeout=3m

echo ""
echo "Waiting for initialization job to complete (this may take 3-5 minutes)..."
kubectl wait --for=condition=complete job/superset-init -n superset --timeout=10m

# Now that init is done, make sure the main deployment is applied
echo ""
echo "Ensuring Superset application deployment..."
kubectl apply -f superset/k8s/superset-deployment.yaml -n superset

echo ""
echo "Waiting for Superset application to be ready..."
kubectl wait --for=condition=ready pod -l app=superset -n superset --timeout=5m

echo ""
echo "=========================================="
echo "✓✓✓ Superset Deployed Successfully! ✓✓✓"
echo "=========================================="
echo ""
echo "Access Superset:"
echo "  Command: minikube service -n superset superset --url"
echo "  Or:      kubectl port-forward -n superset svc/superset 8088:8088"
echo ""
echo "Login credentials:"
echo "  Username: admin"
echo "  Password: admin"
echo ""
echo "Next step: Configure Trino database connection"
echo "  SQLAlchemy URI: trino://admin@trino.trino.svc.cluster.local:8080/cassandra.properties/flight_analytics"
echo ""

# Show services
kubectl get pods -n superset
