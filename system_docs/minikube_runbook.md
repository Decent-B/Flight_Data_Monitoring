# Minikube Runbook (Local NiFi -> Kafka in Cluster)

This runbook documents the full flow using **local NiFi** for API ingestion and **Minikube** for Kafka, Spark, Cassandra, MinIO, Trino, and Superset. It includes .env preparation, ConfigMap/Secret usage, and verification steps.

## Why ConfigMaps and Secrets

- ConfigMaps store non-sensitive configuration (service endpoints, feature flags, bucket names). They let you change behavior without rebuilding images.
- Secrets store sensitive values (credentials, access keys). They avoid hardcoding secrets in images or Git.
- In this project, Spark needs both kinds of configuration (Kafka/Cassandra endpoints vs. credentials), so ConfigMaps + Secrets are the cleanest path to support Minikube now and EKS later.

---

## Step 0: Prerequisites

- `minikube`, `kubectl`, and `helm` installed
- Docker installed (for local NiFi + building Spark image)
- From repo root: `/home/binh/Coding/Flight_Data_Monitoring`

---

## Step 1: Local NiFi setup (.env + Docker)

### 1.1 Create the `.env` file for NiFi

Create `docker/.env` with the required credentials:

```env
NIFI_SINGLE_USER_USERNAME=admin
NIFI_SINGLE_USER_PASSWORD=YourSecurePassword123!
```

### 1.2 Start NiFi locally

```bash
# From repo root
cd docker

docker network create docker_flight-network

docker-compose -f docker-nifi.yml up -d --build
```

### 1.3 Access NiFi UI

- URL: `https://localhost:8443/nifi/`
- Login with `NIFI_SINGLE_USER_USERNAME` / `NIFI_SINGLE_USER_PASSWORD`

### 1.4 Configure OpenSky OAuth2

In the NiFi UI (OpenSky flow):

1. Open Controller Services.
2. Edit `StandardOauth2AccessTokenProvider`.
3. Set **Client secret** for `trnkh02-api-client`.
4. Enable the service.

### 1.5 Set Kafka bootstrap servers in NiFi

We will expose Kafka from Minikube via NodePort. After Kafka is up, update:

- `Kafka3ConnectionService` → `bootstrap.servers` = `<minikube-ip>:<nodeport>`

---

## Step 2: Start Minikube

```bash
minikube start --cpus=4 --memory=8192 --disk-size=40g
kubectl config current-context
```

---

## Step 3: Deploy Cassandra to Minikube

```bash
kubectl apply -f k8s/cassandra-statefulset.yaml
kubectl wait --for=condition=ready pod -l app=cassandra --timeout=10m
```

Initialize schema:

```bash
kubectl exec -i cassandra-0 -- cqlsh < cassandra/schema/init_keyspace.cql
kubectl exec -i cassandra-0 -- cqlsh < cassandra/schema/create_tables.cql
```

Verify:

```bash
kubectl exec -i cassandra-0 -- cqlsh -e "DESCRIBE TABLES IN flight_analytics;"
```

---

## Step 4: Deploy MinIO to Minikube

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm upgrade --install minio bitnami/minio \
  -n minio --create-namespace \
  --set auth.rootUser=minioadmin \
  --set auth.rootPassword=minioadmin123 \
  --set mode=standalone \
  --set defaultBuckets="flight-raw,flight-data,flight-tracks,checkpoints"

kubectl get pods -n minio
```

---

## Step 5: Deploy Kafka to Minikube (external access for local NiFi)

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm upgrade --install kafka bitnami/kafka \
  -n kafka --create-namespace \
  --set replicaCount=1 \
  --set listeners.client.protocol=PLAINTEXT \
  --set externalAccess.enabled=true \
  --set externalAccess.service.type=NodePort \
  --set externalAccess.autoDiscovery.enabled=true
```

Get the NodePort for external access:

```bash
minikube ip
kubectl get svc -n kafka
```

Create Kafka topics:

```bash
kubectl exec -n kafka -it $(kubectl get pods -n kafka -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}') -- \
  kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic flights_raw --partitions 3 --replication-factor 1

kubectl exec -n kafka -it $(kubectl get pods -n kafka -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}') -- \
  kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic flight_data --partitions 3 --replication-factor 1

kubectl exec -n kafka -it $(kubectl get pods -n kafka -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}') -- \
  kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic flight_track --partitions 3 --replication-factor 1
```

---

## Step 6: Configure Spark for Minikube (Option B)

### 6.1 Update Spark ConfigMap/Secret

Edit the values if your service names differ:

- `k8s/spark/spark-configmap.yaml`
- `k8s/spark/spark-secret.yaml`

Apply them:

```bash
kubectl create namespace spark
kubectl apply -f k8s/spark/spark-configmap.yaml
kubectl apply -f k8s/spark/spark-secret.yaml
```

### 6.2 Build and load Spark image into Minikube

```bash
docker build -f docker/Dockerfile.spark -t flight-data-spark:latest .
minikube image load flight-data-spark:latest
```

### 6.3 Deploy Spark streaming

```bash
kubectl apply -f k8s/spark/spark-deployment.yaml
kubectl get pods -n spark
```

---

## Step 7: Deploy Trino + Superset

### 7.1 Trino

```bash
helm repo add trino https://trinodb.github.io/charts
helm upgrade --install trino trino/trino -n trino --create-namespace -f trino/trino-values.yaml
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=trino --timeout=10m -n trino
```

### 7.2 Superset

```bash
./superset/scripts/deploy_superset.sh
./superset/scripts/configure_superset_trino.sh
./superset/scripts/add_superset_datasets.sh
```

---

## Step 8: Validation Checklist (must run)

### Kafka topics exist

```bash
kubectl exec -n kafka -it $(kubectl get pods -n kafka -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}') -- \
  kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Spark streaming is running

```bash
kubectl logs -n spark -l app=spark-stream --tail=200
```

### Cassandra has data

```bash
kubectl exec -i cassandra-0 -- cqlsh -e "SELECT COUNT(*) FROM flight_analytics.aircrafts_by_icao24;"
```

### Trino can read Cassandra

```bash
kubectl exec -n superset $(kubectl get pods -n superset -l app=superset -o jsonpath='{.items[0].metadata.name}') -- \
  python3 -c "from sqlalchemy import create_engine; e=create_engine('trino://admin@trino.trino.svc.cluster.local:8080/cassandra.properties/flight_analytics'); print(list(e.connect().execute('SELECT COUNT(*) FROM aircrafts_by_icao24')));"
```

### Superset datasets exist

- Open Superset UI and confirm the datasets list is populated from Trino.

---

## Notes for Future EKS Migration (high-level)

- Replace MinIO with S3 and use IAM Roles for Service Accounts (IRSA) for Spark.
- Replace NodePort exposure with LoadBalancer or Ingress.
- Move images to ECR and pin versions instead of `latest`.
- Consider MSK for Kafka and managed Cassandra alternatives if ops overhead grows.
