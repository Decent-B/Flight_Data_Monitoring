# Flight Data Monitoring - GKE Deployment Runbook

A comprehensive, step-by-step guide to deploy the complete Flight Data Monitoring pipeline to Google Kubernetes Engine (GKE) from scratch. This runbook enables anyone to clone the repository and deploy the entire platform to Google Cloud.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Prerequisites](#prerequisites)
3. [Quick Start (Automated)](#quick-start-automated)
4. [Manual Deployment Guide](#manual-deployment-guide)
   - [Phase 1: GCP Setup](#phase-1-gcp-setup)
   - [Phase 2: GKE Cluster Creation](#phase-2-gke-cluster-creation)
   - [Phase 3: Build & Push Docker Images](#phase-3-build--push-docker-images)
   - [Phase 4: Deploy Kafka](#phase-4-deploy-kafka)
   - [Phase 5: Deploy Cassandra](#phase-5-deploy-cassandra)
   - [Phase 6: Deploy MinIO](#phase-6-deploy-minio)
   - [Phase 7: Deploy NiFi](#phase-7-deploy-nifi)
   - [Phase 8: Deploy Spark](#phase-8-deploy-spark)
   - [Phase 9: Deploy Trino](#phase-9-deploy-trino)
   - [Phase 10: Deploy Superset](#phase-10-deploy-superset)
5. [End-to-End Validation](#end-to-end-validation)
6. [Accessing Services](#accessing-services)
7. [Monitoring & Observability](#monitoring--observability)
8. [Data Retention Policies](#data-retention-policies)
9. [Cost Optimization](#cost-optimization)
10. [Troubleshooting](#troubleshooting)
11. [Cleanup](#cleanup)
12. [Appendix: Configuration Reference](#appendix-configuration-reference)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         GKE Cluster (flight-data-gke)                            │
│                         Region: asia-southeast1                                   │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                           DATA INGESTION LAYER                              │ │
│  │                                                                             │ │
│  │   [OpenSky API] ◄──────► [NiFi] ──────► [Kafka (3 Brokers)]                │ │
│  │   [Weather API] ◄──────►    │                   │                          │ │
│  │                             │                   │                          │ │
│  │                             ▼                   ▼                          │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
│                                    │                   │                         │
│  ┌─────────────────────────────────┴───────────────────┴───────────────────────┐ │
│  │                           DATA PROCESSING LAYER                             │ │
│  │                                                                             │ │
│  │                    [Spark Streaming] ──────► [Cassandra]                   │ │
│  │                           │                                                 │ │
│  │                           ▼                                                 │ │
│  │                       [MinIO]                                               │ │
│  │                   (S3-compatible checkpoints)                               │ │
│  │                                                                             │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                             │
│  ┌─────────────────────────────────┴───────────────────────────────────────────┐ │
│  │                          QUERY & VISUALIZATION LAYER                        │ │
│  │                                                                             │ │
│  │              [Trino] ◄────────────► [Superset Dashboard]                   │ │
│  │          (SQL Query Engine)            (MapBox Visualizations)              │ │
│  │                                                                             │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                  │
│  Node Pools:                                                                     │
│  ├── kafka-pool      (3× e2-standard-2, 1 per zone) - Kafka + NiFi              │
│  └── default-pool-v2 (3× e2-medium, autoscaling)    - All other workloads       │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Component Summary

| Component | Purpose | Namespace | Replicas |
|-----------|---------|-----------|----------|
| **Kafka** | Message broker for real-time data | `kafka` | 3 brokers |
| **NiFi** | Data ingestion from OpenSky API | `nifi` | 1 |
| **Cassandra** | Time-series database for flight data | `cassandra` | 1-3 |
| **MinIO** | S3-compatible storage for checkpoints | `minio` | 1 |
| **Spark** | Stream processing engine | `spark` | 1 |
| **Trino** | SQL query engine over Cassandra | `trino` | 1 coordinator + 3 workers |
| **Superset** | Data visualization dashboard | `superset` | 1 |

---

## Prerequisites

### Required Software

| Tool | Version | Purpose | Installation |
|------|---------|---------|--------------|
| **gcloud CLI** | 450+ | GCP management | [Install Guide](https://cloud.google.com/sdk/docs/install) |
| **kubectl** | 1.28+ | Kubernetes management | `gcloud components install kubectl` |
| **helm** | 3.12+ | Package manager | `curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 \| bash` |
| **docker** | 24+ | Container builds | [Install Guide](https://docs.docker.com/get-docker/) |

### Verify Installations

```bash
# Run these commands to verify all tools are installed
gcloud version                    # Should show version 450+
kubectl version --client          # Should show v1.28+
helm version                      # Should show v3.12+
docker --version                  # Should show 24+
```

### GCP Account Requirements

- Active GCP account with billing enabled
- Project Owner or Editor role
- Sufficient quota for:
  - 6 Compute Engine instances (e2-standard-2, e2-medium)
  - 6 persistent disks (100GB total)
  - 1 LoadBalancer external IP

### Estimated Costs

| Configuration | Monthly Cost (USD) | Notes |
|---------------|-------------------|-------|
| **Development** | ~$150 | Minimal replicas, small disks |
| **Production** | ~$350 | Full HA, larger resources |

---

## Quick Start (Automated)

For experienced users, run the automated deployment script:

```bash
# Clone the repository
git clone https://github.com/YOUR_ORG/Flight_Data_Monitoring.git
cd Flight_Data_Monitoring

# Configure GCP project
gcloud auth login
gcloud config set project YOUR_PROJECT_ID

# Run the automated deployment script
chmod +x deploy-gke.sh
./deploy-gke.sh
```

The script handles:
- Prerequisites verification
- API enablement
- Cluster creation
- All component deployments
- Validation

For manual, step-by-step deployment with explanations, continue below.

---

## Manual Deployment Guide

### Phase 1: GCP Setup

#### 1.1 Authenticate with Google Cloud

```bash
# Login to your Google Cloud account
gcloud auth login

# Set your project (create one first if needed)
export GCP_PROJECT_ID="your-project-id"
gcloud config set project $GCP_PROJECT_ID

# Verify authentication
gcloud auth list --filter=status:ACTIVE
```

**✅ Verification:**
```bash
# Should show your project ID
gcloud config get-value project
```

#### 1.2 Set Environment Variables

Create a configuration file for consistent values:

```bash
# Create config file
cat > ~/.flight-data-config << 'EOF'
export GCP_PROJECT_ID=$(gcloud config get-value project)
export GCP_REGION="asia-southeast1"
export GCP_ZONE="asia-southeast1-a"
export CLUSTER_NAME="flight-data-gke"
export AR_REPO="flight-data"
export AR_LOCATION="${GCP_REGION}"
export IMAGE_REGISTRY="${AR_LOCATION}-docker.pkg.dev/${GCP_PROJECT_ID}/${AR_REPO}"
EOF

# Source the config
source ~/.flight-data-config
echo "Project: $GCP_PROJECT_ID, Region: $GCP_REGION, Registry: $IMAGE_REGISTRY"
```

#### 1.3 Enable Required APIs

```bash
# Enable all required Google Cloud APIs
gcloud services enable \
    container.googleapis.com \
    compute.googleapis.com \
    storage.googleapis.com \
    iam.googleapis.com \
    artifactregistry.googleapis.com \
    cloudresourcemanager.googleapis.com

# Wait for APIs to propagate
echo "Waiting for APIs to propagate..."
sleep 15
```

**✅ Verification:**
```bash
# List enabled APIs (should include all above)
gcloud services list --enabled | grep -E "container|compute|storage|iam|artifact"
```

#### 1.4 Create Artifact Registry

```bash
# Create Docker repository for our images
gcloud artifacts repositories create $AR_REPO \
    --repository-format=docker \
    --location=$AR_LOCATION \
    --description="Flight Data Monitoring container images"

# Configure Docker authentication
gcloud auth configure-docker ${AR_LOCATION}-docker.pkg.dev --quiet
```

**✅ Verification:**
```bash
# Should show the flight-data repository
gcloud artifacts repositories list --location=$AR_LOCATION
```

---

### Phase 2: GKE Cluster Creation

#### 2.1 Create the GKE Cluster

```bash
# Create regional GKE cluster with minimal default pool
gcloud container clusters create $CLUSTER_NAME \
    --region $GCP_REGION \
    --num-nodes 1 \
    --machine-type e2-small \
    --node-locations ${GCP_ZONE},${GCP_REGION}-b,${GCP_REGION}-c \
    --enable-ip-alias \
    --enable-autorepair \
    --enable-autoupgrade \
    --enable-autoscaling \
    --min-nodes 1 \
    --max-nodes 1 \
    --release-channel regular \
    --addons HorizontalPodAutoscaling,HttpLoadBalancing,GcePersistentDiskCsiDriver \
    --workload-pool=${GCP_PROJECT_ID}.svc.id.goog \
    --logging=SYSTEM,WORKLOAD \
    --monitoring=SYSTEM \
    --disk-type pd-standard \
    --disk-size 50

echo "⏳ Cluster creation takes 5-10 minutes..."
```

**✅ Verification:**
```bash
# Get cluster credentials
gcloud container clusters get-credentials $CLUSTER_NAME --region $GCP_REGION

# Verify cluster access
kubectl cluster-info
kubectl get nodes
```

#### 2.2 Create Kafka Node Pool

The Kafka pool uses dedicated nodes with taints for isolation:

```bash
gcloud container node-pools create kafka-pool \
    --cluster $CLUSTER_NAME \
    --region $GCP_REGION \
    --machine-type e2-standard-2 \
    --num-nodes 1 \
    --node-locations ${GCP_ZONE},${GCP_REGION}-b,${GCP_REGION}-c \
    --enable-autorepair \
    --enable-autoupgrade \
    --disk-type pd-ssd \
    --disk-size 50 \
    --node-labels=workload=kafka \
    --node-taints=workload=kafka:NoSchedule
```

**✅ Verification:**
```bash
# Should show 3 kafka-pool nodes with label workload=kafka
kubectl get nodes -l workload=kafka
```

#### 2.3 Create Default Pool (Autoscaling)

For other workloads, create an autoscaling pool:

```bash
gcloud container node-pools create default-pool-v2 \
    --cluster $CLUSTER_NAME \
    --region $GCP_REGION \
    --machine-type e2-medium \
    --num-nodes 0 \
    --node-locations ${GCP_ZONE},${GCP_REGION}-b,${GCP_REGION}-c \
    --enable-autorepair \
    --enable-autoupgrade \
    --enable-autoscaling \
    --min-nodes 0 \
    --max-nodes 3 \
    --disk-type pd-standard \
    --disk-size 50

# Delete the original default pool (optional, saves cost)
# gcloud container node-pools delete default-pool --cluster $CLUSTER_NAME --region $GCP_REGION --quiet
```

**✅ Verification:**
```bash
# List all node pools
gcloud container node-pools list --cluster $CLUSTER_NAME --region $GCP_REGION

# Should show kafka-pool + default-pool-v2
```

#### 2.4 Apply Storage Classes

```bash
# Apply GKE storage classes
kubectl apply -f k8s/gke/storage-classes.yaml
```

**✅ Verification:**
```bash
# Should show pd-ssd, pd-balanced, pd-standard
kubectl get storageclasses
```

---

### Phase 3: Build & Push Docker Images

#### 3.1 Build Spark Image

```bash
# Navigate to project root
cd /path/to/Flight_Data_Monitoring

# Build Spark image
docker build -f docker/Dockerfile.spark -t ${IMAGE_REGISTRY}/spark:v3 .

# Push to Artifact Registry
docker push ${IMAGE_REGISTRY}/spark:v3
```

**✅ Verification:**
```bash
# List images in registry
gcloud artifacts docker images list ${IMAGE_REGISTRY}
```

#### 3.2 Build Custom NiFi Image (Optional)

If using custom NiFi processors:

```bash
# Build custom NiFi image (if needed)
docker build -f docker/Dockerfile.nifi -t ${IMAGE_REGISTRY}/nifi:latest .
docker push ${IMAGE_REGISTRY}/nifi:latest
```

---

### Phase 4: Deploy Kafka

#### 4.1 Create Namespace and Deploy

```bash
# Create namespace
kubectl create namespace kafka

# Add Bitnami Helm repository
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

# Deploy Kafka with Helm
helm install kafka bitnami/kafka \
    -n kafka \
    -f k8s/gke/kafka-values.yaml \
    --wait --timeout 10m
```

**⏳ Wait Time:** 5-8 minutes for all brokers to start

#### 4.2 Create Kafka Topics

```bash
# Wait for Kafka to be ready
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=kafka -n kafka --timeout=10m

# Create required topics
for topic in flights_raw flight_data flight_track; do
    echo "Creating topic: $topic"
    kubectl exec -n kafka kafka-controller-0 -- kafka-topics.sh \
        --bootstrap-server kafka.kafka.svc.cluster.local:9092 \
        --create --if-not-exists \
        --topic $topic \
        --partitions 3 \
        --replication-factor 3 \
        --config retention.ms=86400000 \
        --config retention.bytes=1073741824 \
        --config cleanup.policy=delete
done
```

**✅ Verification:**
```bash
# List topics
kubectl exec -n kafka kafka-controller-0 -- kafka-topics.sh \
    --bootstrap-server kafka.kafka.svc.cluster.local:9092 --list

# Test produce/consume
echo "test-message-$(date +%s)" | kubectl exec -i -n kafka kafka-controller-0 -- \
    kafka-console-producer.sh --bootstrap-server kafka.kafka.svc.cluster.local:9092 --topic flights_raw

kubectl exec -n kafka kafka-controller-0 -- kafka-console-consumer.sh \
    --bootstrap-server kafka.kafka.svc.cluster.local:9092 \
    --topic flights_raw --from-beginning --max-messages 1 --timeout-ms 10000

# Expected: See the test message printed
```

---

### Phase 5: Deploy Cassandra

#### 5.1 Deploy Cassandra StatefulSet

```bash
# Create namespace
kubectl create namespace cassandra

# Deploy Cassandra
kubectl apply -f k8s/gke/cassandra-statefulset.yaml

# Wait for pods (takes 5-10 minutes)
kubectl wait --for=condition=ready pod -l app=cassandra -n cassandra --timeout=15m
```

#### 5.2 Initialize Schema

```bash
# Wait for cluster to stabilize
echo "Waiting for Cassandra cluster to stabilize..."
sleep 60

# Check cluster status
kubectl exec -n cassandra cassandra-0 -- nodetool status

# Initialize keyspace and tables
kubectl exec -n cassandra -i cassandra-0 -- cqlsh << 'EOF'
CREATE KEYSPACE IF NOT EXISTS flight_analytics 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
EOF

kubectl exec -n cassandra -i cassandra-0 -- cqlsh < cassandra/schema/create_tables.cql
```

**✅ Verification:**
```bash
# Verify tables exist
kubectl exec -n cassandra cassandra-0 -- cqlsh -e "USE flight_analytics; DESCRIBE TABLES;"

# Test write/read
kubectl exec -n cassandra cassandra-0 -- cqlsh -e "
INSERT INTO flight_analytics.aircrafts_by_icao24 (icao24, callsign, origin_country, last_contact) 
VALUES ('test001', 'TEST01', 'TestCountry', toTimestamp(now()));
SELECT * FROM flight_analytics.aircrafts_by_icao24 WHERE icao24 = 'test001';
"

# Expected: See the test row returned
```

---

### Phase 6: Deploy MinIO

MinIO provides S3-compatible storage for Spark checkpoints.

#### 6.1 Deploy MinIO

```bash
# Create namespace and deploy
kubectl apply -f k8s/gke/minio.yaml

# Wait for MinIO to start
kubectl wait --for=condition=ready pod -l app=minio -n minio --timeout=5m

# Wait for bucket initialization job
kubectl wait --for=condition=complete job/minio-init -n minio --timeout=2m || true
```

**✅ Verification:**
```bash
# Check MinIO pod
kubectl get pods -n minio

# Port-forward to access console (optional)
# kubectl port-forward -n minio svc/minio 9090:9090 &
# Access at: http://localhost:9090
# Credentials: minioadmin / minioadmin123

# Verify buckets were created
kubectl logs -n minio job/minio-init
```

---

### Phase 7: Deploy NiFi

NiFi handles data ingestion from OpenSky Network API.

#### 7.1 Deploy NiFi

```bash
# Deploy NiFi (includes namespace, secrets, configmap, service, deployment)
kubectl apply -f k8s/gke/nifi.yaml

# Wait for NiFi to start (takes 3-5 minutes for first startup)
kubectl wait --for=condition=ready pod -l app=nifi -n nifi --timeout=10m
```

#### 7.2 Get NiFi Access URL

```bash
# Get NodePort
NIFI_PORT=$(kubectl get svc nifi -n nifi -o jsonpath='{.spec.ports[0].nodePort}')
NIFI_NODE=$(kubectl get nodes -l workload=kafka -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}')

echo "NiFi URL: https://${NIFI_NODE}:${NIFI_PORT}/nifi"
echo "Credentials: admin / Utlinhsaygex123."
```

**Note:** If nodes don't have external IPs, use port-forwarding:
```bash
kubectl port-forward -n nifi svc/nifi 8443:8443 &
# Access at: https://localhost:8443/nifi
```

#### 7.3 Deploy NiFi Flow (Automated)

```bash
# Apply the flow deployer job
kubectl apply -f k8s/gke/nifi-flow-deployer.yaml

# Wait for flow deployment
kubectl wait --for=condition=complete job/nifi-flow-deployer -n nifi --timeout=10m

# Check deployment logs
kubectl logs -n nifi job/nifi-flow-deployer
```

**✅ Verification:**
```bash
# Check if NiFi is producing to Kafka
# Wait a minute for flow to start
sleep 60

# Check message count in flights_raw topic
kubectl exec -n kafka kafka-controller-0 -- kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list kafka.kafka.svc.cluster.local:9092 \
    --topic flights_raw

# Expected: Offsets should be increasing
```

---

### Phase 8: Deploy Spark

Spark processes streaming data from Kafka and writes to Cassandra.

#### 8.1 Deploy Spark

```bash
# Deploy Spark (includes namespace, configmap, secret, deployment)
kubectl apply -f k8s/gke/spark.yaml

# Wait for Spark to start
kubectl wait --for=condition=ready pod -l app=spark-flight-reader -n spark --timeout=5m
```

#### 8.2 Verify Spark Processing

```bash
# Check Spark logs for streaming activity
kubectl logs -n spark -l app=spark-flight-reader --tail=50

# Look for lines like:
# - "Batch X completed"
# - "Writing to Cassandra"
# - "Processed X rows"
```

**✅ Verification:**
```bash
# Check if data is being written to Cassandra
kubectl exec -n cassandra cassandra-0 -- cqlsh -e \
    "SELECT COUNT(*) FROM flight_analytics.aircrafts_by_icao24;"

# Run multiple times - count should increase
sleep 30
kubectl exec -n cassandra cassandra-0 -- cqlsh -e \
    "SELECT COUNT(*) FROM flight_analytics.aircrafts_by_icao24;"
```

---

### Phase 9: Deploy Trino

Trino provides SQL query interface over Cassandra data.

#### 9.1 Deploy Trino

```bash
# Create namespace
kubectl create namespace trino

# Add Trino Helm repository
helm repo add trino https://trinodb.github.io/charts
helm repo update

# Deploy Trino
helm install trino trino/trino \
    -n trino \
    -f k8s/gke/trino-values.yaml \
    --wait --timeout 10m
```

#### 9.2 Scale Workers (Optional)

```bash
# Scale to 3 workers for better query performance
kubectl scale deployment trino-worker -n trino --replicas=3
```

**✅ Verification:**
```bash
# Check Trino pods
kubectl get pods -n trino

# Test Trino CLI
kubectl exec -n trino deploy/trino-coordinator -- trino --execute "SHOW CATALOGS"

# Query Cassandra through Trino
kubectl exec -n trino deploy/trino-coordinator -- trino --execute \
    "SELECT COUNT(*) FROM cassandra.flight_analytics.aircrafts_by_icao24"
```

---

### Phase 10: Deploy Superset

Superset provides data visualization with MapBox map support.

#### 10.1 Deploy Superset

```bash
# Create namespace
kubectl create namespace superset

# Deploy Superset (includes Postgres, Redis, main app)
kubectl apply -f k8s/gke/superset.yaml

# Wait for Superset initialization
kubectl wait --for=condition=complete job/superset-init -n superset --timeout=10m

# Wait for main pod
kubectl wait --for=condition=ready pod -l app=superset -n superset --timeout=5m
```

#### 10.2 Get Superset URL

```bash
# Get LoadBalancer external IP
SUPERSET_IP=$(kubectl get svc superset -n superset -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

echo "Superset URL: http://${SUPERSET_IP}"
echo "Credentials: admin / admin (change on first login!)"
```

**Note:** LoadBalancer IP may take 1-2 minutes to be assigned.

#### 10.3 Configure Trino Data Source

1. Access Superset at the URL above
2. Go to **Settings** → **Database Connections** → **+ Database**
3. Select **Trino** as database type
4. Use connection string:
   ```
   trino://trino-coordinator.trino.svc.cluster.local:8080/cassandra
   ```
5. Click **Test Connection** then **Connect**

**✅ Verification:**
```bash
# Check all Superset pods are running
kubectl get pods -n superset

# Test health endpoint
curl -s http://${SUPERSET_IP}/health
# Expected: "OK"
```

---

## End-to-End Validation

Run this complete validation script after deployment:

```bash
#!/bin/bash
echo "======================================"
echo "Flight Data Monitoring - E2E Validation"
echo "======================================"

# 1. Kafka Check
echo -e "\n[1/6] Checking Kafka..."
TOPICS=$(kubectl exec -n kafka kafka-controller-0 -- kafka-topics.sh \
    --bootstrap-server kafka.kafka.svc.cluster.local:9092 --list 2>/dev/null)
echo "Topics: $TOPICS"
[[ "$TOPICS" == *"flights_raw"* ]] && echo "✅ Kafka: OK" || echo "❌ Kafka: FAILED"

# 2. NiFi Check
echo -e "\n[2/6] Checking NiFi..."
NIFI_STATUS=$(kubectl get pods -n nifi -l app=nifi -o jsonpath='{.items[0].status.phase}')
[[ "$NIFI_STATUS" == "Running" ]] && echo "✅ NiFi: OK" || echo "❌ NiFi: FAILED"

# 3. Cassandra Check
echo -e "\n[3/6] Checking Cassandra..."
CASS_COUNT=$(kubectl exec -n cassandra cassandra-0 -- cqlsh -e \
    "SELECT COUNT(*) FROM flight_analytics.aircrafts_by_icao24;" 2>/dev/null | grep -E '^\s*[0-9]+' | tr -d ' ')
echo "Rows in aircrafts_by_icao24: $CASS_COUNT"
[[ -n "$CASS_COUNT" && "$CASS_COUNT" -gt 0 ]] && echo "✅ Cassandra: OK (data flowing)" || echo "⚠️  Cassandra: No data yet"

# 4. Spark Check
echo -e "\n[4/6] Checking Spark..."
SPARK_STATUS=$(kubectl get pods -n spark -l app=spark-flight-reader -o jsonpath='{.items[0].status.phase}')
[[ "$SPARK_STATUS" == "Running" ]] && echo "✅ Spark: OK" || echo "❌ Spark: FAILED"

# 5. Trino Check
echo -e "\n[5/6] Checking Trino..."
TRINO_CATALOGS=$(kubectl exec -n trino deploy/trino-coordinator -- trino --execute "SHOW CATALOGS" 2>/dev/null)
[[ "$TRINO_CATALOGS" == *"cassandra"* ]] && echo "✅ Trino: OK" || echo "❌ Trino: FAILED"

# 6. Superset Check
echo -e "\n[6/6] Checking Superset..."
SUPERSET_IP=$(kubectl get svc superset -n superset -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
if [[ -n "$SUPERSET_IP" ]]; then
    HEALTH=$(curl -s http://${SUPERSET_IP}/health 2>/dev/null)
    [[ "$HEALTH" == "OK" ]] && echo "✅ Superset: OK at http://${SUPERSET_IP}" || echo "❌ Superset: FAILED"
else
    echo "⚠️  Superset: LoadBalancer IP not assigned yet"
fi

echo -e "\n======================================"
echo "Validation Complete"
echo "======================================"
```

---

## Accessing Services

### Service URLs

| Service | Type | Access Method |
|---------|------|---------------|
| **Superset** | LoadBalancer | `http://<EXTERNAL-IP>` |
| **NiFi** | NodePort | `https://<NODE-IP>:<NodePort>/nifi` |
| **Trino** | ClusterIP | `kubectl port-forward -n trino svc/trino 8080:8080` |
| **MinIO** | ClusterIP | `kubectl port-forward -n minio svc/minio 9090:9090` |
| **Spark UI** | ClusterIP | `kubectl port-forward -n spark svc/spark-ui 4040:4040` |

### Quick Access Commands

```bash
# Get all external URLs
echo "=== Service Access ==="

# Superset (external)
SUPERSET_IP=$(kubectl get svc superset -n superset -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "Superset: http://${SUPERSET_IP} (admin/admin)"

# NiFi (NodePort)
NIFI_PORT=$(kubectl get svc nifi -n nifi -o jsonpath='{.spec.ports[0].nodePort}')
echo "NiFi: Run 'kubectl port-forward -n nifi svc/nifi 8443:8443' then https://localhost:8443/nifi"
echo "      Credentials: admin / Utlinhsaygex123."

# Port-forward for internal services
echo ""
echo "=== Port Forwarding Commands ==="
echo "Trino:    kubectl port-forward -n trino svc/trino 8080:8080"
echo "MinIO:    kubectl port-forward -n minio svc/minio 9090:9090"
echo "Spark UI: kubectl port-forward -n spark svc/spark-ui 4040:4040"
```

---

## Monitoring & Observability

### Built-in GKE Monitoring

GKE integrates with Google Cloud Operations (Stackdriver). Access at:

```bash
# Open monitoring dashboard
echo "Monitoring: https://console.cloud.google.com/monitoring?project=${GCP_PROJECT_ID}"
echo "Logs: https://console.cloud.google.com/logs?project=${GCP_PROJECT_ID}"
```

### Key Metrics to Monitor

| Component | Metric | Threshold |
|-----------|--------|-----------|
| **Kafka** | Consumer lag | < 10,000 messages |
| **Spark** | Batch processing time | < 30 seconds |
| **Cassandra** | Read/Write latency | < 50ms |
| **Trino** | Query execution time | < 10 seconds |

### Useful Monitoring Commands

```bash
# Pod resource usage
kubectl top pods --all-namespaces

# Node resource usage
kubectl top nodes

# Check for pod issues
kubectl get pods --all-namespaces | grep -v Running

# Recent events
kubectl get events --all-namespaces --sort-by='.lastTimestamp' | tail -20
```

---

## Data Retention Policies

### Kafka Topics

All topics configured with:
- **Retention Time:** 24 hours (`retention.ms=86400000`)
- **Retention Size:** 1GB per partition (`retention.bytes=1073741824`)
- **Cleanup Policy:** Delete (`cleanup.policy=delete`)

### Cassandra Tables

Apply TTL for automatic data expiration:

```sql
-- Set 7-day TTL on insert
INSERT INTO aircrafts_by_icao24 (...) VALUES (...) USING TTL 604800;

-- Or alter table default TTL
ALTER TABLE flight_analytics.aircrafts_by_icao24 
WITH default_time_to_live = 604800;
```

### MinIO (Checkpoints)

Configure lifecycle policies for checkpoint cleanup:

```bash
# Access MinIO console and set lifecycle rule:
# - Prefix: checkpoints/
# - Expiration: 7 days
```

---

## Cost Optimization

### Current Cost Breakdown

| Resource | Count | Monthly Cost |
|----------|-------|--------------|
| e2-standard-2 nodes (kafka-pool) | 3 | ~$100 |
| e2-medium nodes (default-pool) | 0-3 | ~$0-75 |
| Persistent Disks | ~100GB | ~$10 |
| LoadBalancer | 1 | ~$20 |
| **Total** | | **~$130-205** |

### Cost-Saving Tips

1. **Use Autoscaling**: default-pool-v2 scales to 0 when not needed
2. **Spot VMs**: Enable for non-critical workloads
3. **Right-size Resources**: Monitor and adjust based on usage
4. **Scale Down After Hours**:
   ```bash
   # Scale down non-critical components
   kubectl scale deploy -n trino trino-worker --replicas=1
   kubectl scale deploy -n superset superset --replicas=0
   ```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Pods stuck in `Pending` | Insufficient resources | Check node capacity, scale node pool |
| Kafka connection timeout | Wrong bootstrap servers | Verify ConfigMap: `kafka-headless.kafka.svc.cluster.local:9092` |
| Cassandra OOMKilled | Heap too large | Reduce `MAX_HEAP_SIZE` to 2G |
| NiFi can't produce to Kafka | Network policy or wrong topic | Check NiFi flow configuration |
| Trino can't connect to Cassandra | Wrong contact points | Verify: `cassandra.cassandra.svc.cluster.local` |
| Superset charts not loading | Missing MapBox API key | Add `MAPBOX_API_KEY` to ConfigMap |
| Spark Kerberos error | Missing user context | Ensure Dockerfile has `useradd -u 185` |

### Debug Commands

```bash
# Check pod status and events
kubectl describe pod <pod-name> -n <namespace>

# View logs
kubectl logs -n <namespace> <pod-name> --tail=100 -f

# Exec into pod
kubectl exec -it -n <namespace> <pod-name> -- /bin/bash

# Check configmap
kubectl get configmap -n <namespace> <configmap-name> -o yaml

# Check secrets (base64 encoded)
kubectl get secret -n <namespace> <secret-name> -o yaml
```

### Emergency Recovery

```bash
# Restart a deployment
kubectl rollout restart deployment/<deployment-name> -n <namespace>

# Force delete stuck pod
kubectl delete pod <pod-name> -n <namespace> --force --grace-period=0

# Check cluster health
kubectl get nodes
kubectl get componentstatuses
```

---

## Cleanup

To completely remove the deployment:

```bash
# Delete all workloads (in reverse order of creation)
kubectl delete -f k8s/gke/superset.yaml
helm uninstall trino -n trino
kubectl delete -f k8s/gke/spark.yaml
kubectl delete -f k8s/gke/nifi-flow-deployer.yaml
kubectl delete -f k8s/gke/nifi.yaml
kubectl delete -f k8s/gke/minio.yaml
kubectl delete -f k8s/gke/cassandra-statefulset.yaml
helm uninstall kafka -n kafka

# Delete namespaces
kubectl delete namespace superset trino spark nifi minio cassandra kafka

# Delete GKE cluster
gcloud container clusters delete $CLUSTER_NAME --region $GCP_REGION --quiet

# Delete Artifact Registry
gcloud artifacts repositories delete $AR_REPO --location=$AR_LOCATION --quiet

# Delete remaining resources (optional)
# Check for orphaned disks
gcloud compute disks list --filter="name~flight-data"
```

---

## Appendix: Configuration Reference

### Key Configuration Files

| File | Purpose |
|------|---------|
| `k8s/gke/storage-classes.yaml` | GKE storage class definitions |
| `k8s/gke/kafka-values.yaml` | Kafka Helm chart values |
| `k8s/gke/cassandra-statefulset.yaml` | Cassandra deployment |
| `k8s/gke/minio.yaml` | MinIO deployment |
| `k8s/gke/nifi.yaml` | NiFi deployment |
| `k8s/gke/nifi-flow-deployer.yaml` | NiFi flow automation |
| `k8s/gke/spark.yaml` | Spark deployment |
| `k8s/gke/trino-values.yaml` | Trino Helm values |
| `k8s/gke/superset.yaml` | Superset deployment |
| `cassandra/schema/create_tables.cql` | Cassandra table definitions |

### Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `GCP_PROJECT_ID` | (required) | Your GCP project ID |
| `GCP_REGION` | `asia-southeast1` | GCP region for deployment |
| `CLUSTER_NAME` | `flight-data-gke` | GKE cluster name |
| `AR_REPO` | `flight-data` | Artifact Registry repository |

### Service Ports

| Service | Internal Port | NodePort/LoadBalancer |
|---------|---------------|----------------------|
| Kafka | 9092 | - |
| Cassandra | 9042 | - |
| NiFi | 8443 | 31810 (NodePort) |
| Trino | 8080 | - |
| Superset | 8088 | 80 (LoadBalancer) |
| MinIO API | 9000 | - |
| MinIO Console | 9090 | - |

### Credentials Reference

| Service | Username | Password | Notes |
|---------|----------|----------|-------|
| NiFi | admin | Utlinhsaygex123. | Change in production |
| Superset | admin | admin | Change on first login |
| MinIO | minioadmin | minioadmin123 | Change in production |
| Cassandra | cassandra | cassandra | Default Cassandra auth |

---

## Deployment Checklist

Use this checklist to track your progress:

- [ ] **Phase 1: GCP Setup**
  - [ ] Authenticated with `gcloud auth login`
  - [ ] Set project with `gcloud config set project`
  - [ ] Enabled all required APIs
  - [ ] Created Artifact Registry repository

- [ ] **Phase 2: GKE Cluster**
  - [ ] Created GKE cluster
  - [ ] Created kafka-pool node pool
  - [ ] Created default-pool-v2 node pool
  - [ ] Applied storage classes

- [ ] **Phase 3: Docker Images**
  - [ ] Built and pushed Spark image

- [ ] **Phase 4: Kafka**
  - [ ] Deployed Kafka via Helm
  - [ ] Created topics (flights_raw, flight_data, flight_track)
  - [ ] Verified produce/consume

- [ ] **Phase 5: Cassandra**
  - [ ] Deployed Cassandra StatefulSet
  - [ ] Initialized keyspace and tables
  - [ ] Verified read/write operations

- [ ] **Phase 6: MinIO**
  - [ ] Deployed MinIO
  - [ ] Verified buckets created

- [ ] **Phase 7: NiFi**
  - [ ] Deployed NiFi
  - [ ] Deployed NiFi flow
  - [ ] Verified data flowing to Kafka

- [ ] **Phase 8: Spark**
  - [ ] Deployed Spark
  - [ ] Verified data flowing to Cassandra

- [ ] **Phase 9: Trino**
  - [ ] Deployed Trino via Helm
  - [ ] Verified Cassandra connectivity

- [ ] **Phase 10: Superset**
  - [ ] Deployed Superset
  - [ ] Configured Trino data source
  - [ ] Verified dashboard access

- [ ] **End-to-End Validation**
  - [ ] Ran E2E validation script
  - [ ] All components showing ✅

---

*Last Updated: January 11, 2026*
