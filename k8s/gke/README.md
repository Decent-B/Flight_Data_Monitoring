# Migration from AWS EKS to Google Cloud GKE

This directory contains the complete migration from AWS EKS to Google Kubernetes Engine (GKE) for the Flight Data Monitoring platform.

## 📋 Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Directory Structure](#directory-structure)
- [Step-by-Step Migration](#step-by-step-migration)
- [Key Differences](#key-differences)
- [Cost Comparison](#cost-comparison)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

This migration moves the entire Flight Data Monitoring pipeline from AWS EKS to Google Cloud GKE, providing:

- ✅ **Better regional availability** (no quota issues like ap-east-1)
- ✅ **Integrated monitoring** with Cloud Operations (Stackdriver)
- ✅ **Workload Identity** for secure service account management
- ✅ **Spot VMs** for 60-70% cost savings
- ✅ **Persistent Disk CSI** built-in (no separate driver installation)
- ✅ **Google Cloud Storage** integration for cold storage

## 🚀 Quick Start

### Prerequisites

```bash
# Install Google Cloud SDK (if not already installed)
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Authenticate
gcloud auth login

# Set your project
gcloud config set project YOUR_PROJECT_ID

# Install kubectl and helm
gcloud components install kubectl
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

### Automated Deployment

```bash
# Run the automated deployment script
./deploy-gke.sh
```

This script will:
1. ✅ Validate prerequisites
2. ✅ Enable required GCP APIs
3. ✅ Create GKE cluster with multiple node pools
4. ✅ Configure storage classes
5. ✅ Create GCS bucket with folder structure
6. ✅ Setup Workload Identity for Spark
7. ✅ Deploy Kafka with 3 brokers
8. ✅ Deploy Cassandra cluster (3 nodes)
9. ✅ Initialize schemas and test connectivity

### Manual Deployment

Follow the comprehensive guide in [`system_docs/gke_runbook.md`](../system_docs/gke_runbook.md)

## 📁 Directory Structure

```
k8s/gke/
├── storage-classes.yaml          # PD-SSD, PD-Balanced, PD-Standard
├── kafka-values.yaml              # Kafka Helm values (3 brokers, node affinity)
├── cassandra-statefulset.yaml    # Cassandra StatefulSet (3 replicas)
├── spark-configmap.yaml           # Spark environment configuration
├── spark-secret.yaml              # Spark secrets (Cassandra credentials)
├── spark-deployment.yaml          # Spark Streaming application
├── trino-values.yaml              # Trino Helm values (Cassandra catalog)
├── superset-values.yaml           # Superset Helm values (with PostgreSQL/Redis)
├── producer-deployment.yaml       # Kafka producer (OpenSky API ingestion)
└── monitoring-values.yaml         # Prometheus + Grafana stack (optional)

system_docs/
└── gke_runbook.md                 # Complete deployment guide

deploy-gke.sh                      # Automated deployment script
```

## 📖 Step-by-Step Migration

### Phase 1: Infrastructure Setup (Automated)

```bash
# Run the deployment script
./deploy-gke.sh

# This takes approximately 20-30 minutes
```

**What gets deployed:**
- ✅ GKE cluster (1 default node)
- ✅ Kafka node pool (3x e2-standard-2)
- ✅ Data node pool (3x e2-standard-4, Spot VMs)
- ✅ App node pool (2x e2-standard-2, Spot VMs)
- ✅ Storage classes (pd-ssd, pd-balanced, pd-standard)
- ✅ GCS bucket with lifecycle policies
- ✅ Workload Identity binding
- ✅ Kafka (3 brokers, 3 topics)
- ✅ Cassandra (3 nodes, initialized schema)

### Phase 2: Application Deployment (Manual)

#### 1. Build and Push Docker Images

```bash
# Set environment variables
export GCP_PROJECT_ID=$(gcloud config get-value project)
export GCP_REGION="asia-southeast1"

# Create Artifact Registry repository
gcloud artifacts repositories create flight-data \
  --repository-format=docker \
  --location=${GCP_REGION} \
  --description="Flight Data Monitoring images"

# Configure Docker
gcloud auth configure-docker ${GCP_REGION}-docker.pkg.dev

# Build and push Spark image
docker build -f docker/Dockerfile.spark -t flight-data-spark:latest .
docker tag flight-data-spark:latest ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/flight-data/spark:latest
docker push ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/flight-data/spark:latest

# Build and push Producer image
docker build -f docker/Dockerfile.producer -t flight-data-producer:latest .
docker tag flight-data-producer:latest ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/flight-data/producer:latest
docker push ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/flight-data/producer:latest
```

#### 2. Deploy Spark Streaming

```bash
kubectl apply -f k8s/gke/spark-configmap.yaml
kubectl apply -f k8s/gke/spark-secret.yaml
kubectl apply -f k8s/gke/spark-deployment.yaml

# Verify
kubectl logs -n spark -l app=spark-stream --tail=100
```

#### 3. Deploy Trino

```bash
kubectl create namespace trino
helm repo add trino https://trinodb.github.io/charts
helm repo update
helm upgrade --install trino trino/trino -n trino -f k8s/gke/trino-values.yaml --wait

# Test
kubectl exec -n trino deploy/trino-coordinator -- trino --execute "SHOW CATALOGS"
```

#### 4. Deploy Superset

```bash
kubectl create namespace superset
helm repo add superset https://apache.github.io/superset
helm upgrade --install superset superset/superset -n superset -f k8s/gke/superset-values.yaml --wait

# Access UI
kubectl port-forward -n superset svc/superset 8088:8088
# Visit http://localhost:8088 (admin/admin)
```

#### 5. Deploy Kafka Producer

```bash
kubectl create namespace producer
kubectl apply -f k8s/gke/producer-deployment.yaml

# Verify
kubectl logs -n producer -l app=kafka-producer --tail=50
```

### Phase 3: End-to-End Testing

```bash
# 1. Check Kafka consumer groups
kubectl exec -n kafka kafka-controller-0 -- kafka-consumer-groups.sh \
  --bootstrap-server kafka.kafka.svc.cluster.local:9092 --list

# 2. Verify Spark is processing
kubectl logs -n spark -l app=spark-stream --tail=20 | grep -i "batch\|processed"

# 3. Query Cassandra
kubectl exec -n cassandra cassandra-0 -- cqlsh -e \
  "SELECT COUNT(*) FROM flight_analytics.aircrafts_by_icao24;"

# 4. Query via Trino
kubectl exec -n trino deploy/trino-coordinator -- trino --execute \
  "SELECT COUNT(*) FROM cassandra.flight_analytics.aircrafts_by_icao24"

# 5. Access Superset dashboard
kubectl port-forward -n superset svc/superset 8088:8088
# Open http://localhost:8088
```

## 🔄 Key Differences from AWS EKS

| Aspect | AWS EKS | Google Cloud GKE |
|--------|---------|------------------|
| **Cluster Creation** | `eksctl create cluster -f cluster-config.yaml` | `gcloud container clusters create` |
| **Node Management** | Managed Node Groups | Node Pools |
| **CLI Tool** | `eksctl`, `aws` | `gcloud` |
| **Storage** | EBS CSI Driver (separate installation) | Persistent Disk CSI (built-in) |
| **Object Storage** | S3 + IRSA | GCS + Workload Identity |
| **Container Registry** | ECR | Artifact Registry |
| **Spot Instances** | Spot Instances (~70% discount) | Spot VMs (~60-70% discount) |
| **Monitoring** | CloudWatch (separate setup) | Cloud Operations (integrated) |
| **IAM** | IRSA (IAM Roles for Service Accounts) | Workload Identity |
| **Machine Types** | t3.medium (2 vCPU, 4GB) | e2-standard-2 (2 vCPU, 8GB) |
|  | t3.large (2 vCPU, 8GB) | e2-standard-4 (4 vCPU, 16GB) |
| **Region Used** | us-east-1 (N. Virginia) | asia-southeast1 (Singapore) |
| **Cluster Type** | Regional (multi-AZ control plane) | Regional (multi-zone control plane) |

## 💰 Cost Comparison

### AWS EKS Costs (us-east-1)

```
Control Plane:                   $73/month
Node Pools:
  - 3x t3.medium (kafka):        $90/month
  - 3x t3.large (data, spot):    $67/month (70% discount)
  - 2x t3.medium (app, spot):    $27/month (70% discount)
Storage (EBS gp3):               ~$20/month
Total:                           ~$277/month
```

### Google Cloud GKE Costs (asia-southeast1)

```
Control Plane:                   $73/month (Standard cluster)
Node Pools:
  - 1x e2-small (default):       $13/month
  - 3x e2-standard-2 (kafka):    $90/month
  - 3x e2-standard-4 (data, spot): $102/month (60% discount)
  - 2x e2-standard-2 (app, spot):  $30/month (60% discount)
Storage (PD-SSD):                ~$25/month
Total:                           ~$333/month
```

**Note:** GKE provides:
- Integrated monitoring (no separate CloudWatch costs)
- Better memory allocation (e2-standard-2 has 8GB vs t3.medium 4GB)
- Simpler IAM with Workload Identity
- Built-in Persistent Disk CSI driver

### Cost Optimization Tips

1. **Use GKE Autopilot** (alternative):
   - No node management
   - Pay only for pod resources
   - Estimated: $200-250/month

2. **Committed Use Discounts**:
   - 1-year commitment: 25% discount
   - 3-year commitment: 52% discount

3. **Right-size workloads**:
   ```bash
   kubectl top pods --all-namespaces
   kubectl top nodes
   ```

4. **Scale down during off-hours**:
   ```bash
   gcloud container clusters resize flight-data-gke \
     --node-pool app-pool --num-nodes 1 --region asia-southeast1
   ```

## 🔧 Troubleshooting

### Common Issues

#### 1. Pods Stuck in Pending

**Symptom:** Pods remain in `Pending` state

**Solution:**
```bash
# Check node capacity
kubectl describe nodes | grep -A 5 "Allocated resources"

# Check pod events
kubectl describe pod <pod-name> -n <namespace>

# Scale node pool if needed
gcloud container clusters resize flight-data-gke \
  --node-pool data-pool --num-nodes 4 --region asia-southeast1
```

#### 2. Workload Identity Not Working

**Symptom:** Spark can't access GCS

**Solution:**
```bash
# Verify service account annotation
kubectl get sa spark-sa -n spark -o yaml | grep annotation

# Re-bind if needed
gcloud iam service-accounts add-iam-policy-binding \
  spark-gcs-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:${GCP_PROJECT_ID}.svc.id.goog[spark/spark-sa]"
```

#### 3. Kafka Connection Timeout

**Symptom:** Spark can't connect to Kafka

**Solution:**
```bash
# Check Kafka service
kubectl get svc -n kafka

# Test connectivity from Spark pod
kubectl exec -n spark <spark-pod> -- nc -zv kafka.kafka.svc.cluster.local 9092

# Check Kafka logs
kubectl logs -n kafka kafka-controller-0 --tail=100
```

#### 4. Cassandra OOMKilled

**Symptom:** Cassandra pods keep restarting with OOM error

**Solution:**
```bash
# Reduce heap size in cassandra-statefulset.yaml
# Change MAX_HEAP_SIZE from 2G to 1500M
kubectl edit statefulset cassandra -n cassandra
```

### Debug Commands

```bash
# View cluster info
gcloud container clusters describe flight-data-gke --region asia-southeast1

# Check node pool status
gcloud container node-pools list --cluster flight-data-gke --region asia-southeast1

# View pod resources
kubectl top pods --all-namespaces

# View node resources
kubectl top nodes

# Check all events
kubectl get events --all-namespaces --sort-by='.lastTimestamp'

# View GKE logs in Cloud Console
echo "https://console.cloud.google.com/logs/query?project=${GCP_PROJECT_ID}"
```

## 🧹 Cleanup

To completely remove all GKE resources:

```bash
# Delete workloads
helm uninstall kafka -n kafka
helm uninstall trino -n trino
helm uninstall superset -n superset
kubectl delete namespace kafka cassandra spark trino superset producer

# Delete GCS bucket
gsutil -m rm -r gs://flight-data-${GCP_PROJECT_ID}

# Delete GKE cluster (includes all node pools)
gcloud container clusters delete flight-data-gke --region asia-southeast1 --quiet

# Delete Artifact Registry repository
gcloud artifacts repositories delete flight-data --location asia-southeast1 --quiet

# Delete service account
gcloud iam service-accounts delete spark-gcs-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com --quiet
```

## 📚 Additional Resources

- [GKE Documentation](https://cloud.google.com/kubernetes-engine/docs)
- [Workload Identity Guide](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
- [GKE Best Practices](https://cloud.google.com/kubernetes-engine/docs/best-practices)
- [Cost Optimization](https://cloud.google.com/architecture/best-practices-for-running-cost-effective-kubernetes-applications-on-gke)
- [Complete Runbook](../system_docs/gke_runbook.md)

## 📝 Notes

- **Region Choice:** Using `asia-southeast1` (Singapore) for better latency from Asia
- **Node Pools:** Using 3 specialized node pools (kafka, data, app) for workload isolation
- **Spot VMs:** Enabled on data-pool and app-pool for 60-70% cost savings
- **Storage:** Using pd-ssd for Kafka and Cassandra, pd-balanced for others
- **Monitoring:** Google Cloud Operations provides integrated monitoring and logging

---

**Last Updated:** January 11, 2026

**Status:** ✅ Migration infrastructure ready - Application deployment in progress
