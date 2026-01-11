# Google Cloud GKE Deployment Runbook

This runbook documents the complete migration from AWS EKS to **Google Kubernetes Engine (GKE)** for the Flight Data Monitoring platform. It covers resource planning, step-by-step deployment, data retention policies, and validation procedures.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Resource Planning & Cluster Sizing](#resource-planning--cluster-sizing)
3. [Prerequisites](#prerequisites)
4. [Step 1: Create GKE Cluster](#step-1-create-gke-cluster)
5. [Step 2: Deploy Kafka](#step-2-deploy-kafka)
6. [Step 3: Deploy Cassandra](#step-3-deploy-cassandra)
7. [Step 4: Deploy GCS Storage](#step-4-deploy-gcs-storage)
8. [Step 5: Deploy Spark Streaming](#step-5-deploy-spark-streaming)
9. [Step 6: Deploy Trino (Presto)](#step-6-deploy-trino-presto)
10. [Step 7: Deploy Superset](#step-7-deploy-superset)
11. [Step 8: Deploy Kafka Producer (Data Ingestion)](#step-8-deploy-kafka-producer-data-ingestion)
12. [Validation & Testing](#validation--testing)
13. [Data Retention Policies](#data-retention-policies)
14. [Monitoring & Alerts](#monitoring--alerts)
15. [Cost Optimization](#cost-optimization)
16. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              GKE Cluster                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         Data Flow Pipeline                          │   │
│  │                                                                     │   │
│  │  [OpenSky API] ──► [Kafka Producer] ──► [Kafka] ──► [Spark] ──►    │   │
│  │                                             │           │           │   │
│  │                                             │           ▼           │   │
│  │                                             │     [Cassandra]       │   │
│  │                                             │           │           │   │
│  │                                             ▼           ▼           │   │
│  │                                          [GCS]     [Trino/Presto]   │   │
│  │                                                         │           │   │
│  │                                                         ▼           │   │
│  │                                                    [Superset]       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Node Pools:                                                                │
│  ├── kafka-pool      (3x e2-standard-2)  - Kafka brokers                   │
│  ├── data-pool       (3x e2-standard-4)  - Cassandra + Spark               │
│  └── app-pool        (2x e2-standard-2)  - Trino, Superset, Producer       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Resource Planning & Cluster Sizing

### Component Resource Requirements

| Component | CPU Request | CPU Limit | Memory Request | Memory Limit | Storage | Replicas | Notes |
|-----------|-------------|-----------|----------------|--------------|---------|----------|-------|
| **Kafka Broker** | 500m | 1 | 2Gi | 3Gi | 10Gi | 3 | KRaft mode, log retention 24h |
| **Cassandra** | 1 | 2 | 4Gi | 6Gi | 20Gi | 3 | Heap 2G, GC tuned |
| **Spark Driver** | 500m | 2 | 2Gi | 4Gi | - | 1 | Structured Streaming |
| **Spark Executors** | 500m | 1 | 2Gi | 3Gi | - | 2 | Dynamic allocation |
| **Trino Coordinator** | 500m | 1 | 2Gi | 3Gi | - | 1 | Query engine |
| **Trino Worker** | 500m | 1 | 2Gi | 3Gi | - | 2 | Parallel queries |
| **Superset** | 250m | 500m | 1Gi | 2Gi | - | 1 | Web UI + Redis + Postgres |
| **Kafka Producer** | 100m | 250m | 256Mi | 512Mi | - | 1 | Python script |

### Node Pool Sizing

| Node Pool | Machine Type | vCPUs | Memory | Count | Purpose |
|-----------|--------------|-------|--------|-------|---------|
| **kafka-pool** | e2-standard-2 | 2 | 8GB | 3 | Kafka brokers (1 per node) |
| **data-pool** | e2-standard-4 | 4 | 16GB | 3 | Cassandra + Spark workloads |
| **app-pool** | e2-standard-2 | 2 | 8GB | 2 | Trino, Superset, monitoring |

### Total Cluster Resources

- **Total Nodes**: 8 nodes
- **Total vCPUs**: 22 vCPUs
- **Total Memory**: 72GB
- **Total Storage**: ~100GB PD-SSD
- **Estimated Monthly Cost**: ~$250-350 USD (with Spot VMs for data-pool)

### Why These Sizes?

1. **Kafka**: 3 nodes for HA, e2-standard-2 provides adequate CPU/memory for moderate throughput
2. **Cassandra**: 3 nodes for RF=3, needs 4Gi+ heap; e2-standard-4 provides 16GB memory
3. **Spark**: Single driver + 2 executors can handle ~10k events/sec
4. **Trino**: 1 coordinator + 2 workers for parallel query execution
5. **Superset**: Lightweight, shares nodes with Trino

---

## Prerequisites

### Required Tools

```bash
# Install Google Cloud SDK
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
gcloud init

# Install kubectl (if not already installed with gcloud)
gcloud components install kubectl

# Install Helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# Verify installations
gcloud version
kubectl version --client
helm version
```

### GCP Configuration

```bash
# Authenticate
gcloud auth login

# Set project
export GCP_PROJECT_ID="your-project-id"
gcloud config set project $GCP_PROJECT_ID

# Set default region and zone
export GCP_REGION="us-central1"
export GCP_ZONE="us-central1-a"
gcloud config set compute/region $GCP_REGION
gcloud config set compute/zone $GCP_ZONE

# Verify configuration
gcloud config list
```

### Enable Required APIs

```bash
# Enable necessary Google Cloud APIs
gcloud services enable container.googleapis.com
gcloud services enable compute.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable iam.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
gcloud services enable artifactregistry.googleapis.com
```

### Environment Variables

```bash
export GCP_PROJECT_ID=$(gcloud config get-value project)
export GCP_REGION="us-central1"
export GCP_ZONE="us-central1-a"
export CLUSTER_NAME="flight-data-gke"
export GCS_BUCKET="flight-data-${GCP_PROJECT_ID}"
```

---

## Step 1: Create GKE Cluster

### 1.1 Create GKE Cluster with Multiple Node Pools

```bash
# Create the GKE Standard cluster with minimal default node pool
# (we'll use custom node pools for our workloads)
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
  --enable-stackdriver-kubernetes \
  --logging=SYSTEM,WORKLOAD \
  --monitoring=SYSTEM \
  --disk-type pd-standard \
  --disk-size 50

echo "✅ Cluster created successfully"
```

### 1.2 Create Kafka Node Pool

```bash
gcloud container node-pools create kafka-pool \
  --cluster $CLUSTER_NAME \
  --region $GCP_REGION \
  --machine-type e2-standard-2 \
  --num-nodes 3 \
  --node-locations ${GCP_ZONE},${GCP_REGION}-b,${GCP_REGION}-c \
  --enable-autorepair \
  --enable-autoupgrade \
  --disk-type pd-ssd \
  --disk-size 50 \
  --node-labels=workload=kafka \
  --node-taints=workload=kafka:NoSchedule

echo "✅ Kafka node pool created"
```

### 1.3 Create Data Node Pool (Cassandra + Spark)

```bash
gcloud container node-pools create data-pool \
  --cluster $CLUSTER_NAME \
  --region $GCP_REGION \
  --machine-type e2-standard-4 \
  --num-nodes 3 \
  --node-locations ${GCP_ZONE},${GCP_REGION}-b,${GCP_REGION}-c \
  --enable-autorepair \
  --enable-autoupgrade \
  --disk-type pd-ssd \
  --disk-size 100 \
  --node-labels=workload=data \
  --spot

echo "✅ Data node pool created (using Spot VMs for cost savings)"
```

### 1.4 Create App Node Pool (Trino, Superset)

```bash
gcloud container node-pools create app-pool \
  --cluster $CLUSTER_NAME \
  --region $GCP_REGION \
  --machine-type e2-standard-2 \
  --num-nodes 2 \
  --node-locations ${GCP_ZONE},${GCP_REGION}-b \
  --enable-autorepair \
  --enable-autoupgrade \
  --disk-type pd-standard \
  --disk-size 50 \
  --node-labels=workload=app \
  --spot

echo "✅ App node pool created (using Spot VMs for cost savings)"
```

### 1.5 Get Cluster Credentials

```bash
# Configure kubectl to use the new cluster
gcloud container clusters get-credentials $CLUSTER_NAME --region $GCP_REGION

# Verify connection
kubectl cluster-info
kubectl get nodes -o wide
```

### 1.6 Create Storage Class

```bash
# Apply GKE-specific storage classes
kubectl apply -f k8s/gke/storage-classes.yaml
kubectl get sc
```

### 1.7 Validation

```bash
# Check all nodes are ready
kubectl get nodes --show-labels

# Check node pools
gcloud container node-pools list --cluster $CLUSTER_NAME --region $GCP_REGION

# Check storage classes
kubectl get sc

# Expected: 3 kafka-pool nodes, 3 data-pool nodes, 2 app-pool nodes, 1 default pool node
```

**Status**: [ ] Not Started | [ ] In Progress | [ ] Completed | [ ] Verified

---

## Step 2: Deploy Kafka

### 2.1 Create Kafka Namespace

```bash
kubectl create namespace kafka
```

### 2.2 Deploy Kafka with Helm (with node affinity for kafka-pool)

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

helm upgrade --install kafka bitnami/kafka \
  -n kafka \
  -f k8s/gke/kafka-values.yaml \
  --wait --timeout 10m
```

### 2.3 Create Topics with Retention Policies

```bash
# Wait for Kafka to be ready
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=kafka -n kafka --timeout=10m

# Create topics with retention settings
kubectl exec -n kafka kafka-controller-0 -- kafka-topics.sh \
  --bootstrap-server kafka.kafka.svc.cluster.local:9092 \
  --create --if-not-exists \
  --topic flights_raw \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=86400000 \
  --config retention.bytes=1073741824 \
  --config cleanup.policy=delete

kubectl exec -n kafka kafka-controller-0 -- kafka-topics.sh \
  --bootstrap-server kafka.kafka.svc.cluster.local:9092 \
  --create --if-not-exists \
  --topic flight_data \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=86400000 \
  --config retention.bytes=1073741824

kubectl exec -n kafka kafka-controller-0 -- kafka-topics.sh \
  --bootstrap-server kafka.kafka.svc.cluster.local:9092 \
  --create --if-not-exists \
  --topic flight_track \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=86400000 \
  --config retention.bytes=1073741824
```

### 2.4 Validation

```bash
# List topics
kubectl exec -n kafka kafka-controller-0 -- kafka-topics.sh \
  --bootstrap-server kafka.kafka.svc.cluster.local:9092 --list

# Describe topic
kubectl exec -n kafka kafka-controller-0 -- kafka-topics.sh \
  --bootstrap-server kafka.kafka.svc.cluster.local:9092 \
  --describe --topic flights_raw

# Test produce/consume
kubectl exec -n kafka kafka-controller-0 -- bash -c \
  'echo "test message" | kafka-console-producer.sh --bootstrap-server kafka.kafka.svc.cluster.local:9092 --topic flights_raw'

kubectl exec -n kafka kafka-controller-0 -- kafka-console-consumer.sh \
  --bootstrap-server kafka.kafka.svc.cluster.local:9092 \
  --topic flights_raw --from-beginning --max-messages 1 --timeout-ms 10000
```

**Status**: [ ] Not Started | [ ] In Progress | [ ] Completed | [ ] Verified

---

## Step 3: Deploy Cassandra

### 3.1 Create Cassandra Namespace

```bash
kubectl create namespace cassandra
```

### 3.2 Deploy Cassandra (with node affinity for data-pool)

```bash
kubectl apply -f k8s/gke/cassandra-statefulset.yaml
kubectl wait --for=condition=ready pod -l app=cassandra -n cassandra --timeout=15m
```

### 3.3 Initialize Schema

```bash
# Wait for all nodes to join
kubectl exec -n cassandra cassandra-0 -- nodetool status

# Create keyspace and tables
kubectl exec -n cassandra -i cassandra-0 -- cqlsh < cassandra/schema/init_keyspace.cql
kubectl exec -n cassandra -i cassandra-0 -- cqlsh < cassandra/schema/create_tables.cql
```

### 3.4 Validation

```bash
# Check cluster status
kubectl exec -n cassandra cassandra-0 -- nodetool status

# Verify tables
kubectl exec -n cassandra cassandra-0 -- cqlsh -e "USE flight_analytics; DESCRIBE TABLES;"

# Test write/read
kubectl exec -n cassandra cassandra-0 -- cqlsh -e "
  INSERT INTO flight_analytics.aircrafts_by_icao24 (icao24, callsign, origin_country, last_contact) 
  VALUES ('test123', 'TEST001', 'Test Country', toTimestamp(now()));
  SELECT * FROM flight_analytics.aircrafts_by_icao24 WHERE icao24 = 'test123';
"
```

**Status**: [ ] Not Started | [ ] In Progress | [ ] Completed | [ ] Verified

---

## Step 4: Deploy GCS Storage

For GKE, we'll use **Google Cloud Storage (GCS)** for cold storage and Spark checkpoints.

### 4.1 Create GCS Bucket

```bash
# Create bucket (globally unique name)
gsutil mb -p ${GCP_PROJECT_ID} -c STANDARD -l ${GCP_REGION} gs://${GCS_BUCKET}/

# Create folder structure
gsutil -h "Content-Type:application/x-www-form-urlencoded" cp /dev/null gs://${GCS_BUCKET}/checkpoints/
gsutil -h "Content-Type:application/x-www-form-urlencoded" cp /dev/null gs://${GCS_BUCKET}/archives/
gsutil -h "Content-Type:application/x-www-form-urlencoded" cp /dev/null gs://${GCS_BUCKET}/raw-data/

# Verify bucket
gsutil ls gs://${GCS_BUCKET}/
```

### 4.2 Create Service Account for GCS Access (Workload Identity)

```bash
# Create Google Service Account
gcloud iam service-accounts create spark-gcs-sa \
  --display-name="Spark GCS Service Account" \
  --project=${GCP_PROJECT_ID}

# Grant Storage permissions
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} \
  --member="serviceAccount:spark-gcs-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# Create Kubernetes namespace for Spark
kubectl create namespace spark

# Create Kubernetes Service Account
kubectl create serviceaccount spark-sa -n spark

# Bind Kubernetes SA to Google SA (Workload Identity)
gcloud iam service-accounts add-iam-policy-binding \
  spark-gcs-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:${GCP_PROJECT_ID}.svc.id.goog[spark/spark-sa]"

# Annotate Kubernetes SA
kubectl annotate serviceaccount spark-sa -n spark \
  iam.gke.io/gcp-service-account=spark-gcs-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com
```

### 4.3 Validation

```bash
# Test GCS access
gsutil ls gs://${GCS_BUCKET}/

# Test from pod
kubectl run -n spark test-gcs --rm -it --restart=Never \
  --image=google/cloud-sdk:slim \
  --serviceaccount=spark-sa \
  -- gsutil ls gs://${GCS_BUCKET}/
```

**Status**: [ ] Not Started | [ ] In Progress | [ ] Completed | [ ] Verified

---

## Step 5: Deploy Spark Streaming

### 5.1 Create Spark Resources

```bash
kubectl apply -f k8s/gke/spark-configmap.yaml
kubectl apply -f k8s/gke/spark-secret.yaml
```

### 5.2 Build and Push Spark Image to Artifact Registry

```bash
# Create Artifact Registry repository
gcloud artifacts repositories create flight-data \
  --repository-format=docker \
  --location=${GCP_REGION} \
  --description="Flight Data Monitoring images"

# Configure Docker for Artifact Registry
gcloud auth configure-docker ${GCP_REGION}-docker.pkg.dev

# Build and push
docker build -f docker/Dockerfile.spark -t flight-data-spark:latest .
docker tag flight-data-spark:latest ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/flight-data/spark:latest
docker push ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/flight-data/spark:latest
```

### 5.3 Deploy Spark

```bash
kubectl apply -f k8s/gke/spark-deployment.yaml
kubectl wait --for=condition=ready pod -l app=spark-stream -n spark --timeout=10m
```

### 5.4 Validation

```bash
# Check Spark logs
kubectl logs -n spark -l app=spark-stream --tail=100

# Verify Kafka connection
kubectl logs -n spark -l app=spark-stream | grep -i "kafka\|connected\|streaming"

# Check if processing is happening
kubectl logs -n spark -l app=spark-stream | grep -i "batch\|trigger\|processed"
```

**Status**: [ ] Not Started | [ ] In Progress | [ ] Completed | [ ] Verified

---

## Step 6: Deploy Trino (Presto)

### 6.1 Create Trino Namespace

```bash
kubectl create namespace trino
```

### 6.2 Deploy Trino (with node affinity for app-pool)

```bash
helm repo add trino https://trinodb.github.io/charts
helm repo update

helm upgrade --install trino trino/trino \
  -n trino \
  -f k8s/gke/trino-values.yaml \
  --wait --timeout 10m
```

### 6.3 Validation

```bash
# Check pods
kubectl get pods -n trino

# Test Trino CLI
kubectl exec -n trino deploy/trino-coordinator -- trino --execute "SHOW CATALOGS"

# Test Cassandra connection
kubectl exec -n trino deploy/trino-coordinator -- trino --execute \
  "SELECT * FROM cassandra.flight_analytics.aircrafts_by_icao24 LIMIT 5"
```

**Status**: [ ] Not Started | [ ] In Progress | [ ] Completed | [ ] Verified

---

## Step 7: Deploy Superset

### 7.1 Create Superset Namespace

```bash
kubectl create namespace superset
```

### 7.2 Deploy Superset (with node affinity for app-pool)

```bash
helm repo add superset https://apache.github.io/superset
helm repo update

helm upgrade --install superset superset/superset \
  -n superset \
  -f k8s/gke/superset-values.yaml \
  --wait --timeout 10m
```

### 7.3 Configure Trino Connection

```bash
# Port-forward Superset
kubectl port-forward -n superset svc/superset 8088:8088 &

# Access UI at http://localhost:8088
# Default credentials: admin / admin

# Add Trino database connection:
# SQLAlchemy URI: trino://trino-coordinator.trino.svc.cluster.local:8080/cassandra
```

### 7.4 Validation

```bash
# Check pods
kubectl get pods -n superset

# Test connectivity
curl -s http://localhost:8088/health
```

**Status**: [ ] Not Started | [ ] In Progress | [ ] Completed | [ ] Verified

---

## Step 8: Deploy Kafka Producer (Data Ingestion)

### 8.1 Build and Push Producer Image

```bash
# Build and push to Artifact Registry
docker build -f docker/Dockerfile.producer -t flight-data-producer:latest .
docker tag flight-data-producer:latest ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/flight-data/producer:latest
docker push ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/flight-data/producer:latest
```

### 8.2 Deploy Producer

```bash
kubectl create namespace producer
kubectl apply -f k8s/gke/producer-deployment.yaml
```

### 8.3 Validation

```bash
# Check producer logs
kubectl logs -n producer -l app=kafka-producer --tail=50

# Verify messages in Kafka
kubectl exec -n kafka kafka-controller-0 -- kafka-console-consumer.sh \
  --bootstrap-server kafka.kafka.svc.cluster.local:9092 \
  --topic flights_raw --max-messages 5 --timeout-ms 30000
```

**Status**: [ ] Not Started | [ ] In Progress | [ ] Completed | [ ] Verified

---

## Validation & Testing

### End-to-End Pipeline Test

```bash
# 1. Verify Kafka is receiving data
kubectl exec -n kafka kafka-controller-0 -- kafka-consumer-groups.sh \
  --bootstrap-server kafka.kafka.svc.cluster.local:9092 --list

# 2. Check Spark is processing
kubectl logs -n spark -l app=spark-stream --tail=20 | grep -i "batch\|processed"

# 3. Verify Cassandra has data
kubectl exec -n cassandra cassandra-0 -- cqlsh -e \
  "SELECT COUNT(*) FROM flight_analytics.aircrafts_by_icao24;"

# 4. Query via Trino
kubectl exec -n trino deploy/trino-coordinator -- trino --execute \
  "SELECT COUNT(*) FROM cassandra.flight_analytics.aircrafts_by_icao24"

# 5. Access Superset dashboard
kubectl port-forward -n superset svc/superset 8088:8088
# Open http://localhost:8088
```

---

## Data Retention Policies

### Kafka Topic Retention

All topics configured with:
- `retention.ms=86400000` (24 hours)
- `retention.bytes=1073741824` (1GB per partition)
- `cleanup.policy=delete`

### Cassandra TTL

Tables should use TTL for automatic expiration:
```sql
-- Example: 7-day TTL for real-time data
INSERT INTO aircrafts_by_icao24 (...) VALUES (...) USING TTL 604800;
```

### GCS Lifecycle Policy

```bash
# Create lifecycle policy JSON
cat > /tmp/gcs-lifecycle.json << 'EOF'
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {
          "age": 7,
          "matchesPrefix": ["checkpoints/"]
        }
      },
      {
        "action": {
          "type": "SetStorageClass",
          "storageClass": "NEARLINE"
        },
        "condition": {
          "age": 30,
          "matchesPrefix": ["archives/"]
        }
      },
      {
        "action": {"type": "Delete"},
        "condition": {
          "age": 365,
          "matchesPrefix": ["archives/"]
        }
      }
    ]
  }
}
EOF

# Apply lifecycle policy
gsutil lifecycle set /tmp/gcs-lifecycle.json gs://${GCS_BUCKET}/
gsutil lifecycle get gs://${GCS_BUCKET}/
```

---

## Monitoring & Alerts

### Enable GKE Monitoring (Google Cloud Operations)

GKE clusters automatically integrate with Google Cloud Operations (formerly Stackdriver). The cluster was created with `--enable-stackdriver-kubernetes` flag.

```bash
# View cluster metrics in Google Cloud Console
echo "View metrics at: https://console.cloud.google.com/monitoring/dashboards?project=${GCP_PROJECT_ID}"

# View logs
echo "View logs at: https://console.cloud.google.com/logs/query?project=${GCP_PROJECT_ID}"
```

### Deploy Prometheus + Grafana (Optional)

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace \
  -f k8s/gke/monitoring-values.yaml
```

### Key Metrics to Monitor

- Kafka: Consumer lag, throughput, partition count
- Spark: Batch processing time, records processed
- Cassandra: Read/write latency, heap usage
- Trino: Query latency, failed queries
- GKE: Node CPU/Memory utilization, Pod restarts

---

## Cost Optimization

### Use Spot VMs (Preemptible VMs)

The cluster uses Spot VMs for data-pool and app-pool to reduce costs by ~60-70%.

```bash
# Check spot instance usage
kubectl get nodes -o custom-columns=NAME:.metadata.name,SPOT:.metadata.labels.'cloud\\.google\\.com/gke-spot'
```

### Right-size Resources

Monitor actual usage and adjust:
```bash
kubectl top pods --all-namespaces
kubectl top nodes

# View in Cloud Console
echo "Resource usage: https://console.cloud.google.com/kubernetes/workload?project=${GCP_PROJECT_ID}"
```

### Scale Down When Not Needed

```bash
# Scale down non-critical workloads
kubectl scale deploy -n trino trino-worker --replicas=0
kubectl scale deploy -n superset superset --replicas=0

# Scale down node pools
gcloud container clusters resize $CLUSTER_NAME \
  --node-pool app-pool \
  --num-nodes 1 \
  --region $GCP_REGION
```

### Enable Cluster Autoscaler

```bash
# Enable autoscaling on node pools
gcloud container clusters update $CLUSTER_NAME \
  --enable-autoscaling \
  --node-pool data-pool \
  --min-nodes 2 \
  --max-nodes 5 \
  --region $GCP_REGION
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Pods stuck in Pending | Insufficient resources or taints | Check node capacity, scale node pool, verify tolerations |
| Kafka timeout | Network/memory issues | Check broker logs, increase timeout |
| Cassandra OOMKilled | Heap too large | Reduce MAX_HEAP_SIZE to 50% of memory limit |
| Spark fails to connect | Wrong bootstrap servers | Verify ConfigMap values |
| Trino can't reach Cassandra | DNS resolution | Use FQDN: cassandra.cassandra.svc.cluster.local |
| Workload Identity not working | Missing annotation | Verify KSA annotation and GSA binding |
| GCS access denied | Missing IAM permissions | Check service account roles |

### Debug Commands

```bash
# Check pod events
kubectl describe pod <pod-name> -n <namespace>

# Check resource usage
kubectl top pods -n <namespace>

# Check logs
kubectl logs -n <namespace> <pod-name> --tail=100

# Exec into pod
kubectl exec -it -n <namespace> <pod-name> -- /bin/bash

# Check node pool status
gcloud container node-pools describe <pool-name> \
  --cluster $CLUSTER_NAME \
  --region $GCP_REGION

# View GKE cluster events
kubectl get events --all-namespaces --sort-by='.lastTimestamp'
```

---

## Cleanup

To delete the entire cluster and resources:

```bash
# Delete all workloads first
helm uninstall kafka -n kafka
helm uninstall trino -n trino
helm uninstall superset -n superset
kubectl delete namespace kafka cassandra spark trino superset producer monitoring

# Delete GCS bucket
gsutil -m rm -r gs://${GCS_BUCKET}

# Delete node pools (optional - will be deleted with cluster)
gcloud container node-pools delete kafka-pool --cluster $CLUSTER_NAME --region $GCP_REGION --quiet
gcloud container node-pools delete data-pool --cluster $CLUSTER_NAME --region $GCP_REGION --quiet
gcloud container node-pools delete app-pool --cluster $CLUSTER_NAME --region $GCP_REGION --quiet

# Delete GKE cluster
gcloud container clusters delete $CLUSTER_NAME --region $GCP_REGION --quiet

# Delete Artifact Registry repository
gcloud artifacts repositories delete flight-data --location $GCP_REGION --quiet

# Delete Service Account
gcloud iam service-accounts delete spark-gcs-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com --quiet
```

---

## Deployment Checklist

| Step | Component | Status | Verified | Notes |
|------|-----------|--------|----------|-------|
| 1 | GKE Cluster | ⬜ | ⬜ | |
| 2 | Kafka | ⬜ | ⬜ | |
| 3 | Cassandra | ⬜ | ⬜ | |
| 4 | GCS Storage | ⬜ | ⬜ | |
| 5 | Spark | ⬜ | ⬜ | |
| 6 | Trino | ⬜ | ⬜ | |
| 7 | Superset | ⬜ | ⬜ | |
| 8 | Producer | ⬜ | ⬜ | |
| 9 | E2E Test | ⬜ | ⬜ | |

---

## Key Differences from AWS EKS

| Feature | AWS EKS | Google GKE |
|---------|---------|------------|
| **CLI Tool** | `eksctl`, `aws` | `gcloud` |
| **Cluster Creation** | YAML config file | CLI commands |
| **Node Groups** | Managed Node Groups | Node Pools |
| **Storage** | EBS CSI Driver + S3 | Persistent Disk + GCS |
| **IAM** | IRSA (IAM Roles for Service Accounts) | Workload Identity |
| **Container Registry** | ECR | Artifact Registry |
| **Spot Instances** | Spot Instances | Spot VMs (Preemptible) |
| **Machine Types** | t3.medium, t3.large | e2-standard-2, e2-standard-4 |
| **Monitoring** | CloudWatch | Cloud Operations/Stackdriver |
| **Regions** | us-east-1 | us-central1 |

---

*Last Updated: January 11, 2026*
