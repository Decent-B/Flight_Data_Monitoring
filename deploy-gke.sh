#!/bin/bash

# GKE Deployment Script for Flight Data Monitoring Pipeline
# This script implements the complete GKE deployment following gke_runbook.md
# Author: GitHub Copilot
# Date: January 11, 2026

set -e  # Exit on error
set -o pipefail  # Pipe failures propagate

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${CYAN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - ✅ $1"
}

# Progress tracking
STEP_COUNTER=0
TOTAL_STEPS=16

print_progress() {
    STEP_COUNTER=$((STEP_COUNTER + 1))
    echo ""
    echo "========================================="
    log_step "[$STEP_COUNTER/$TOTAL_STEPS] $1"
    echo "========================================="
    echo ""
}

# Error handler
error_handler() {
    local line_no=$1
    log_error "Script failed at line $line_no"
    log_error "Last command: $BASH_COMMAND"
    log_warn "Cleaning up partial deployment..."
    exit 1
}

trap 'error_handler ${LINENO}' ERR

# Check prerequisites
check_prerequisites() {
    print_progress "Checking prerequisites"
    
    log_info "Checking gcloud CLI..."
    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI not found. Please install Google Cloud SDK first."
        log_info "Visit: https://cloud.google.com/sdk/docs/install"
        exit 1
    fi
    local gcloud_version=$(gcloud version --format="value(Google Cloud SDK)" 2>/dev/null | head -1)
    log_success "gcloud CLI found: $gcloud_version"
    
    log_info "Checking kubectl..."
    if ! command -v kubectl &> /dev/null; then
        log_warn "kubectl not found. Installing via gcloud..."
        gcloud components install kubectl --quiet
    fi
    local kubectl_version=$(kubectl version --client --short 2>/dev/null | head -1 || echo "unknown")
    log_success "kubectl found: $kubectl_version"
    
    log_info "Checking helm..."
    if ! command -v helm &> /dev/null; then
        log_error "helm not found. Please install Helm 3 first."
        log_info "Run: curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash"
        exit 1
    fi
    local helm_version=$(helm version --short 2>/dev/null)
    log_success "helm found: $helm_version"
    
    log_info "Checking docker..."
    if ! command -v docker &> /dev/null; then
        log_warn "docker not found. You'll need it to build images later."
    else
        local docker_version=$(docker --version 2>/dev/null)
        log_success "docker found: $docker_version"
    fi
    
    log_success "All prerequisites met"
}

# Set environment variables
setup_environment() {
    print_progress "Setting up environment variables"
    
    export GCP_PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
    
    if [ -z "$GCP_PROJECT_ID" ] || [ "$GCP_PROJECT_ID" == "(unset)" ]; then
        log_error "No GCP project configured"
        log_info "Run: gcloud config set project YOUR_PROJECT_ID"
        exit 1
    fi
    
    export GCP_REGION="asia-southeast1"
    export GCP_ZONE="asia-southeast1-a"
    export CLUSTER_NAME="flight-data-gke"
    export GCS_BUCKET="flight-data-${GCP_PROJECT_ID}"
    export AR_REPO="flight-data"
    export AR_LOCATION="${GCP_REGION}"
    
    log_info "Project ID: $GCP_PROJECT_ID"
    log_info "Region: $GCP_REGION"
    log_info "Zone: $GCP_ZONE"
    log_info "Cluster Name: $CLUSTER_NAME"
    log_info "GCS Bucket: $GCS_BUCKET"
    log_info "Artifact Registry: ${AR_LOCATION}-docker.pkg.dev/${GCP_PROJECT_ID}/${AR_REPO}"
    
    # Set default region and zone
    gcloud config set compute/region $GCP_REGION --quiet
    gcloud config set compute/zone $GCP_ZONE --quiet
    
    # Verify authentication
    log_info "Verifying authentication..."
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
        log_error "No active authentication found"
        log_info "Run: gcloud auth login"
        exit 1
    fi
    
    local active_account=$(gcloud auth list --filter=status:ACTIVE --format="value(account)")
    log_success "Authenticated as: $active_account"
}

# Enable required APIs
enable_apis() {
    print_progress "Enabling required Google Cloud APIs"
    
    local apis=(
        "container.googleapis.com"
        "compute.googleapis.com"
        "storage.googleapis.com"
        "iam.googleapis.com"
        "artifactregistry.googleapis.com"
        "cloudresourcemanager.googleapis.com"
    )
    
    for api in "${apis[@]}"; do
        log_info "Enabling $api..."
        if gcloud services enable $api --quiet 2>&1 | tee /tmp/api_enable.log | grep -q "ERROR"; then
            log_warn "Issue enabling $api (may already be enabled)"
        else
            log_success "$api enabled"
        fi
    done
    
    # Wait for APIs to propagate
    log_info "Waiting for APIs to propagate (10 seconds)..."
    sleep 10
    
    log_success "All APIs enabled"
}

# Create GKE cluster
create_cluster() {
    print_progress "Creating GKE cluster: $CLUSTER_NAME"
    
    # Check if cluster already exists
    if gcloud container clusters describe $CLUSTER_NAME --region $GCP_REGION &>/dev/null; then
        log_warn "Cluster $CLUSTER_NAME already exists in $GCP_REGION"
        read -p "Do you want to delete and recreate it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            log_info "Deleting existing cluster..."
            gcloud container clusters delete $CLUSTER_NAME --region $GCP_REGION --quiet
        else
            log_info "Using existing cluster"
            return 0
        fi
    fi
    
    log_warn "This will take approximately 5-10 minutes..."
    log_info "Creating cluster with default node pool..."
    
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
        --disk-size 50 \
        --quiet
    
    log_success "GKE cluster created successfully"
    
    # Verify cluster
    log_info "Verifying cluster status..."
    local cluster_status=$(gcloud container clusters describe $CLUSTER_NAME --region $GCP_REGION --format="value(status)")
    log_info "Cluster status: $cluster_status"
}

# Create node pools
create_node_pools() {
    print_progress "Creating specialized node pools"
    
    # Kafka pool
    log_info "Creating Kafka node pool (3x e2-standard-2, dedicated, 1 per zone)..."
    if gcloud container node-pools describe kafka-pool --cluster $CLUSTER_NAME --region $GCP_REGION &>/dev/null; then
        log_warn "Kafka pool already exists, skipping"
    else
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
            --node-taints=workload=kafka:NoSchedule \
            --quiet
        
        log_success "Kafka node pool created (dedicated for Kafka brokers)"
    fi
    
    # Data pool (Cassandra + Spark)
    log_info "Creating Data node pool (3x e2-standard-4, Spot VMs, 1 per zone)..."
    if gcloud container node-pools describe data-pool --cluster $CLUSTER_NAME --region $GCP_REGION &>/dev/null; then
        log_warn "Data pool already exists, skipping"
    else
        gcloud container node-pools create data-pool \
            --cluster $CLUSTER_NAME \
            --region $GCP_REGION \
            --machine-type e2-standard-4 \
            --num-nodes 1 \
            --node-locations ${GCP_ZONE},${GCP_REGION}-b,${GCP_REGION}-c \
            --enable-autorepair \
            --enable-autoupgrade \
            --disk-type pd-ssd \
            --disk-size 100 \
            --node-labels=workload=data \
            --spot \
            --quiet
        
        log_success "Data node pool created (Spot VMs, 60% cost savings)"
    fi
    
    # App pool (Trino, Superset)
    log_info "Creating App node pool (2x e2-standard-2, Spot VMs)..."
    if gcloud container node-pools describe app-pool --cluster $CLUSTER_NAME --region $GCP_REGION &>/dev/null; then
        log_warn "App pool already exists, skipping"
    else
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
            --spot \
            --quiet
        
        log_success "App node pool created (Spot VMs, 60% cost savings)"
    fi
    
    # List all node pools
    log_info "Node pools summary:"
    gcloud container node-pools list --cluster $CLUSTER_NAME --region $GCP_REGION --format="table(name,status,machineType,diskSizeGb,nodeCount)"
}

# Get cluster credentials
get_credentials() {
    print_progress "Configuring kubectl access"
    
    log_info "Getting cluster credentials..."
    gcloud container clusters get-credentials $CLUSTER_NAME --region $GCP_REGION --quiet
    
    log_info "Verifying cluster access..."
    kubectl cluster-info | head -3
    
    log_info "Node status:"
    kubectl get nodes -o wide
    
    log_info "Waiting for all nodes to be Ready..."
    local max_wait=300
    local elapsed=0
    while [ $elapsed -lt $max_wait ]; do
        local not_ready=$(kubectl get nodes --no-headers | grep -v "Ready" | wc -l)
        if [ $not_ready -eq 0 ]; then
            break
        fi
        log_info "Waiting for $not_ready nodes to become Ready... ($elapsed/$max_wait seconds)"
        sleep 10
        elapsed=$((elapsed + 10))
    done
    
    local total_nodes=$(kubectl get nodes --no-headers | wc -l)
    local ready_nodes=$(kubectl get nodes --no-headers | grep "Ready" | wc -l)
    log_success "Cluster accessible: $ready_nodes/$total_nodes nodes Ready"
}

# Apply storage classes
apply_storage_classes() {
    print_progress "Applying storage classes"
    
    log_info "Applying GKE storage classes..."
    kubectl apply -f k8s/gke/storage-classes.yaml
    
    log_info "Storage classes available:"
    kubectl get sc
    
    log_success "Storage classes configured"
}

# Create GCS bucket
create_gcs_bucket() {
    print_progress "Creating GCS bucket for cold storage"
    
    log_info "Creating GCS bucket: gs://${GCS_BUCKET}"
    
    if gsutil ls gs://${GCS_BUCKET}/ &> /dev/null; then
        log_warn "Bucket gs://${GCS_BUCKET} already exists"
    else
        gsutil mb -p ${GCP_PROJECT_ID} -c STANDARD -l ${GCP_REGION} gs://${GCS_BUCKET}/
        log_success "GCS bucket created"
    fi
    
    log_info "Creating folder structure..."
    echo "" | gsutil cp - gs://${GCS_BUCKET}/checkpoints/.keep 2>/dev/null || true
    echo "" | gsutil cp - gs://${GCS_BUCKET}/archives/.keep 2>/dev/null || true
    echo "" | gsutil cp - gs://${GCS_BUCKET}/raw-data/.keep 2>/dev/null || true
    
    log_info "Setting up lifecycle policy for cost optimization..."
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
    
    gsutil lifecycle set /tmp/gcs-lifecycle.json gs://${GCS_BUCKET}/ 2>/dev/null || true
    
    log_info "Bucket contents:"
    gsutil ls gs://${GCS_BUCKET}/
    
    log_success "GCS bucket configured with lifecycle policies"
}

# Setup Workload Identity for Spark
setup_workload_identity() {
    print_progress "Setting up Workload Identity for Spark"
    
    local GSA_NAME="spark-gcs-sa"
    local GSA_EMAIL="${GSA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
    local KSA_NAME="spark-sa"
    local NAMESPACE="spark"
    
    # Create Google Service Account
    log_info "Creating Google Service Account: $GSA_NAME..."
    if gcloud iam service-accounts describe $GSA_EMAIL &>/dev/null; then
        log_warn "Service account $GSA_NAME already exists"
    else
        gcloud iam service-accounts create $GSA_NAME \
            --display-name="Spark GCS Service Account" \
            --project=${GCP_PROJECT_ID} \
            --quiet
        log_success "Google Service Account created"
    fi
    
    # Grant Storage permissions
    log_info "Granting Storage Object Admin role..."
    gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} \
        --member="serviceAccount:${GSA_EMAIL}" \
        --role="roles/storage.objectAdmin" \
        --quiet
    
    # Create Kubernetes namespace and service account
    log_info "Creating Kubernetes namespace: $NAMESPACE..."
    kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
    
    log_info "Creating Kubernetes service account: $KSA_NAME..."
    kubectl create serviceaccount $KSA_NAME -n $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
    
    # Bind Kubernetes SA to Google SA (Workload Identity)
    log_info "Binding Workload Identity..."
    gcloud iam service-accounts add-iam-policy-binding \
        $GSA_EMAIL \
        --role roles/iam.workloadIdentityUser \
        --member "serviceAccount:${GCP_PROJECT_ID}.svc.id.goog[${NAMESPACE}/${KSA_NAME}]" \
        --quiet
    
    # Annotate Kubernetes SA
    log_info "Annotating Kubernetes service account..."
    kubectl annotate serviceaccount $KSA_NAME -n $NAMESPACE \
        iam.gke.io/gcp-service-account=$GSA_EMAIL \
        --overwrite
    
    # Verify setup
    log_info "Verifying Workload Identity configuration..."
    local annotation=$(kubectl get sa $KSA_NAME -n $NAMESPACE -o jsonpath='{.metadata.annotations.iam\.gke\.io/gcp-service-account}')
    if [ "$annotation" == "$GSA_EMAIL" ]; then
        log_success "Workload Identity configured successfully"
    else
        log_warn "Workload Identity annotation may not be set correctly"
    fi
}

# Deploy Kafka
deploy_kafka() {
    print_progress "Deploying Kafka cluster (3 brokers)"
    
    local NAMESPACE="kafka"
    
    # Create namespace
    log_info "Creating namespace: $NAMESPACE..."
    kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
    
    # Add Bitnami repo
    log_info "Adding Bitnami Helm repository..."
    helm repo add bitnami https://charts.bitnami.com/bitnami
    helm repo update
    
    # Check if Kafka is already installed
    if helm list -n $NAMESPACE | grep -q "kafka"; then
        log_warn "Kafka already installed, upgrading..."
        helm upgrade kafka bitnami/kafka \
            -n $NAMESPACE \
            -f k8s/gke/kafka-values.yaml \
            --wait --timeout 10m
    else
        log_info "Installing Kafka (this may take 5-10 minutes)..."
        helm install kafka bitnami/kafka \
            -n $NAMESPACE \
            -f k8s/gke/kafka-values.yaml \
            --wait --timeout 10m
    fi
    
    log_success "Kafka Helm chart deployed"
    
    # Wait for Kafka pods to be ready
    log_info "Waiting for Kafka pods to be ready..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=kafka -n $NAMESPACE --timeout=10m
    
    # Verify Kafka deployment
    log_info "Kafka pods status:"
    kubectl get pods -n $NAMESPACE -l app.kubernetes.io/name=kafka
    
    log_success "Kafka cluster is ready"
}

# Create Kafka topics
create_kafka_topics() {
    print_progress "Creating Kafka topics with retention policies"
    
    local NAMESPACE="kafka"
    local BOOTSTRAP_SERVER="kafka.kafka.svc.cluster.local:9092"
    
    # Wait for Kafka to be fully initialized
    log_info "Waiting for Kafka to be fully initialized (30 seconds)..."
    sleep 30
    
    # Define topics
    local topics=("flights_raw" "flight_data" "flight_track")
    
    for topic in "${topics[@]}"; do
        log_info "Creating topic: $topic..."
        kubectl exec -n $NAMESPACE kafka-controller-0 -- kafka-topics.sh \
            --bootstrap-server $BOOTSTRAP_SERVER \
            --create --if-not-exists \
            --topic $topic \
            --partitions 3 \
            --replication-factor 3 \
            --config retention.ms=86400000 \
            --config retention.bytes=1073741824 \
            --config cleanup.policy=delete
    done
    
    log_info "Topic list:"
    kubectl exec -n $NAMESPACE kafka-controller-0 -- kafka-topics.sh \
        --bootstrap-server $BOOTSTRAP_SERVER --list
    
    log_success "All Kafka topics created with 24h retention"
}

# Verify Kafka
verify_kafka() {
    print_progress "Verifying Kafka deployment and functionality"
    
    local NAMESPACE="kafka"
    local BOOTSTRAP_SERVER="kafka.kafka.svc.cluster.local:9092"
    
    log_info "Listing Kafka topics:"
    kubectl exec -n $NAMESPACE kafka-controller-0 -- kafka-topics.sh \
        --bootstrap-server $BOOTSTRAP_SERVER --list
    
    log_info "Describing flights_raw topic:"
    kubectl exec -n $NAMESPACE kafka-controller-0 -- kafka-topics.sh \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --describe --topic flights_raw
    
    log_info "Testing produce/consume..."
    local test_message="Test from deployment - $(date '+%Y-%m-%d %H:%M:%S')"
    
    echo "$test_message" | kubectl exec -i -n $NAMESPACE kafka-controller-0 -- \
        kafka-console-producer.sh \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic flights_raw
    
    local consumed=$(kubectl exec -n $NAMESPACE kafka-controller-0 -- \
        kafka-console-consumer.sh \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic flights_raw \
        --from-beginning \
        --max-messages 1 \
        --timeout-ms 10000 2>/dev/null | tail -1)
    
    if [ -n "$consumed" ]; then
        log_success "Kafka verification passed"
    else
        log_warn "Could not verify Kafka produce/consume"
    fi
}

# Deploy Cassandra
deploy_cassandra() {
    print_progress "Deploying Cassandra cluster (3 nodes)"
    
    local NAMESPACE="cassandra"
    
    # Create namespace
    log_info "Creating namespace: $NAMESPACE..."
    kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
    
    # Deploy Cassandra
    log_info "Applying Cassandra StatefulSet..."
    kubectl apply -f k8s/gke/cassandra-statefulset.yaml
    
    log_info "Waiting for Cassandra pods to be ready (this may take 10-15 minutes)..."
    kubectl wait --for=condition=ready pod -l app=cassandra -n $NAMESPACE --timeout=15m
    
    log_success "Cassandra cluster deployed"
    
    # Wait for cluster to stabilize
    log_info "Waiting for Cassandra cluster to stabilize (60 seconds)..."
    sleep 60
    
    # Check cluster status
    log_info "Cassandra cluster status:"
    kubectl exec -n $NAMESPACE cassandra-0 -- nodetool status
    
    log_success "Cassandra is ready"
}

# Initialize Cassandra schema
initialize_cassandra_schema() {
    print_progress "Initializing Cassandra schema"
    
    local NAMESPACE="cassandra"
    
    log_info "Creating keyspace..."
    kubectl exec -n $NAMESPACE -i cassandra-0 -- cqlsh < cassandra/schema/init_keyspace.cql
    
    log_info "Creating tables..."
    kubectl exec -n $NAMESPACE -i cassandra-0 -- cqlsh < cassandra/schema/create_tables.cql
    
    log_success "Cassandra schema initialized"
}

# Verify Cassandra
verify_cassandra() {
    print_progress "Verifying Cassandra deployment and functionality"
    
    local NAMESPACE="cassandra"
    
    log_info "Checking cluster status:"
    kubectl exec -n $NAMESPACE cassandra-0 -- nodetool status
    
    log_info "Listing keyspaces:"
    kubectl exec -n $NAMESPACE cassandra-0 -- cqlsh -e "DESCRIBE KEYSPACES;"
    
    log_info "Verifying tables in flight_analytics:"
    kubectl exec -n $NAMESPACE cassandra-0 -- cqlsh -e "USE flight_analytics; DESCRIBE TABLES;"
    
    log_info "Testing write/read operations..."
    kubectl exec -n $NAMESPACE cassandra-0 -- cqlsh -e "
      INSERT INTO flight_analytics.aircrafts_by_icao24 (icao24, callsign, origin_country, last_contact) 
      VALUES ('test-$(date +%s)', 'TEST001', 'Test Country', toTimestamp(now()));
    "
    
    local count=$(kubectl exec -n $NAMESPACE cassandra-0 -- cqlsh -e \
      "SELECT COUNT(*) FROM flight_analytics.aircrafts_by_icao24;" 2>/dev/null | grep -E "^\s*[0-9]+" | tr -d ' ')
    
    if [ -n "$count" ]; then
        log_success "Cassandra verification passed - $count rows in aircrafts_by_icao24"
    else
        log_warn "Could not verify Cassandra row count"
    fi
}

# Update ConfigMaps with actual values
update_configs() {
    print_progress "Updating configuration files with project-specific values"
    
    log_info "Updating Spark ConfigMap..."
    sed -i "s|gs://REPLACE_WITH_YOUR_BUCKET|gs://${GCS_BUCKET}|g" k8s/gke/spark-configmap.yaml
    
    log_info "Updating Spark Deployment..."
    sed -i "s|REPLACE_WITH_GCP_REGION|${GCP_REGION}|g" k8s/gke/spark-deployment.yaml
    sed -i "s|REPLACE_WITH_PROJECT_ID|${GCP_PROJECT_ID}|g" k8s/gke/spark-deployment.yaml
    
    log_info "Updating Producer Deployment..."
    sed -i "s|REPLACE_WITH_GCP_REGION|${GCP_REGION}|g" k8s/gke/producer-deployment.yaml
    sed -i "s|REPLACE_WITH_PROJECT_ID|${GCP_PROJECT_ID}|g" k8s/gke/producer-deployment.yaml
    
    log_success "Configuration files updated"
}

# Print deployment summary
print_summary() {
    echo ""
    echo "========================================="
    echo "  ✅ Infrastructure Deployment Complete!"
    echo "========================================="
    echo ""
    log_info "Cluster Information:"
    echo "  - Cluster Name: $CLUSTER_NAME"
    echo "  - Region: $GCP_REGION"
    echo "  - Project: $GCP_PROJECT_ID"
    echo ""
    log_info "Deployed Components:"
    echo "  ✅ GKE Cluster (4 node pools)"
    echo "  ✅ Storage Classes (PD-SSD, PD-Balanced, PD-Standard)"
    echo "  ✅ GCS Bucket: gs://${GCS_BUCKET}"
    echo "  ✅ Workload Identity (spark-gcs-sa)"
    echo "  ✅ Kafka (3 brokers, 3 topics)"
    echo "  ✅ Cassandra (3 nodes, schema initialized)"
    echo ""
    log_info "Resource Summary:"
    kubectl get nodes -o wide
    echo ""
    log_info "Namespaces created:"
    kubectl get namespaces | grep -E "(kafka|cassandra|spark)" || true
    echo ""
    log_warn "Next Steps:"
    echo "  1. Build and push Docker images:"
    echo "     ./scripts/build-and-push-images.sh"
    echo ""
    echo "  2. Deploy remaining services:"
    echo "     - Spark Streaming (data processing)"
    echo "     - Trino (SQL query engine)"
    echo "     - Superset (data visualization)"
    echo "     - Kafka Producer (data ingestion)"
    echo ""
    echo "  3. Refer to the detailed runbook:"
    echo "     cat system_docs/gke_runbook.md"
    echo ""
    log_info "View GKE Console:"
    echo "  https://console.cloud.google.com/kubernetes/clusters/details/${GCP_REGION}/${CLUSTER_NAME}/details?project=${GCP_PROJECT_ID}"
    echo ""
}

# Main deployment function
main() {
    echo ""
    log_step "🚀 Starting GKE Infrastructure Deployment"
    echo ""
    echo "This script will deploy the following:"
    echo "  - GKE Cluster with 4 node pools (default, kafka, data, app)"
    echo "  - Kafka (3 brokers with 3 topics)"
    echo "  - Cassandra (3-node cluster with schema)"
    echo "  - Storage classes and GCS bucket"
    echo "  - Workload Identity for Spark"
    echo ""
    log_warn "⚠️  Estimated cost: ~\$333/month (with Spot VM discounts)"
    echo ""
    
    # Skip confirmation if SKIP_CONFIRM is set (for automated runs)
    if [ "${SKIP_CONFIRM}" != "true" ]; then
        read -t 30 -p "Continue with deployment? (yes/no): " confirm || confirm="timeout"
        if [ "$confirm" == "timeout" ]; then
            log_info "No response received, proceeding with deployment..."
        elif [ "$confirm" != "yes" ]; then
            log_error "Deployment cancelled by user"
            exit 1
        fi
    else
        log_info "Skipping confirmation (SKIP_CONFIRM=true), proceeding with deployment..."
    fi
    
    # Step 1-3: Prerequisites and setup
    check_prerequisites
    setup_environment
    enable_apis
    
    # Step 4-6: Cluster creation
    create_cluster
    create_node_pools
    get_credentials
    
    # Step 7-9: Storage and IAM
    apply_storage_classes
    create_gcs_bucket
    setup_workload_identity
    
    # Step 10: Update configs
    update_configs
    
    # Step 11-13: Kafka deployment
    deploy_kafka
    create_kafka_topics
    verify_kafka
    
    # Step 14-16: Cassandra deployment
    deploy_cassandra
    initialize_cassandra_schema
    verify_cassandra
    
    # Final summary
    print_summary
    
    log_success "🎉 Infrastructure deployment completed successfully!"
}

# Run main function
main "$@"
