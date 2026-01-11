# Minikube Runbook (NiFi + Kafka in Cluster)

This runbook documents the full flow using **NiFi in Kubernetes** for API ingestion and **Minikube** for Kafka, Spark, Cassandra, MinIO, Trino, and Superset. It includes ConfigMap/Secret usage, flow deployment, and verification steps.

## Why ConfigMaps and Secrets

- ConfigMaps store non-sensitive configuration (service endpoints, feature flags, bucket names). They let you change behavior without rebuilding images.
- Secrets store sensitive values (credentials, access keys). They avoid hardcoding secrets in images or Git.
- In this project, Spark needs both kinds of configuration (Kafka/Cassandra endpoints vs. credentials), so ConfigMaps + Secrets are the cleanest path to support Minikube now and EKS later.

---

## Step 0: Prerequisites

- `minikube`, `kubectl`, and `helm` installed
- Docker installed (for building NiFi/Spark images)
- From repo root: `/home/binh/Coding/Flight_Data_Monitoring`

---

## Step 1: Start Minikube

```bash
minikube start --cpus=4 --memory=8192 --disk-size=40g
kubectl config current-context
```

---

## Step 2: Deploy NiFi + Registry in Minikube

Build the NiFi image and load it into Minikube:

```bash
docker build -f docker/Dockerfile.nifi -t custom-nifi-secure:latest .
minikube image load custom-nifi-secure:latest
```

Update the NiFi credentials, OpenSky OAuth2 secret, and Kafka bootstrap servers:

- Edit `k8s/nifi/nifi-secret.yaml` for `NIFI_SINGLE_USER_USERNAME` / `NIFI_SINGLE_USER_PASSWORD`.
- Set `OPENSKY_CLIENT_SECRET` in `k8s/nifi/nifi-secret.yaml`.
- Set `NIFI_SENSITIVE_PROPS_KEY` in `k8s/nifi/nifi-secret.yaml` to a stable value; NiFi encrypts sensitive properties with this key, so changing it will invalidate stored secrets and you must re-run the flow deployer.
- Edit `k8s/nifi/nifi-configmap.yaml` for `KAFKA_BOOTSTRAP_SERVERS` (use `kafka-broker-headless.kafka.svc.cluster.local:9092` so clients hit brokers, not the controller).
- TLS keystore/truststore passwords are generated at runtime; if you need stable values, set `KEYSTORE_PASS` and `TRUSTSTORE_PASS` via a Secret/env (do not bake them into the Dockerfile).
- TLS is not required for data flow, but this runbook keeps HTTPS enabled to preserve NiFi authentication. If you want HTTP-only, switch the NiFi service/deployment and the flow deployer `NIFI_API_URL` to port 8080.

Apply the NiFi stack:

```bash
kubectl kustomize --load-restrictor LoadRestrictionsNone k8s/nifi | kubectl apply -f -
kubectl wait --for=condition=available deploy/nifi -n nifi --timeout=10m
kubectl wait --for=condition=available deploy/nifi-registry -n nifi --timeout=10m
```

NiFi runs with Minikube-friendly memory defaults (requests 2Gi, limits 3Gi). If it OOMKills, raise these values in `k8s/nifi/nifi-deployment.yaml`.
Run the flow deployer job (re-run by deleting it first):

```bash
kubectl delete job -n nifi nifi-flow-deployer --ignore-not-found
kubectl kustomize --load-restrictor LoadRestrictionsNone k8s/nifi | kubectl apply -f -
kubectl wait --for=condition=complete job/nifi-flow-deployer -n nifi --timeout=10m
```

The deployer replaces any existing process groups named `OpenSkyAPI` or `AviationWeatherAPI` to ensure the flow definition stays in sync with the repo. It will drop queued FlowFiles before deletion if needed.

Access NiFi UI:

```bash
kubectl port-forward -n nifi svc/nifi 8443:8443
```

- URL: `https://<your-hostname>:8443/nifi/`
- Login with `NIFI_SINGLE_USER_USERNAME` / `NIFI_SINGLE_USER_PASSWORD`

The flow deployer will apply the OpenSky OAuth2 client secret automatically and enable the controller service when `OPENSKY_CLIENT_SECRET` is set.

If this controller service stays disabled, the OpenSky processors will remain invalid and **no data** will reach Kafka/Spark/Cassandra/Superset.

Kafka endpoints are parameterized via `KAFKA_BOOTSTRAP_SERVERS` in `k8s/nifi/nifi-configmap.yaml`. No hard-coded endpoints remain in the flows.

---

## Step 3: Deploy Cassandra to Minikube

```bash
kubectl apply -f k8s/cassandra-statefulset.yaml
kubectl wait --for=condition=ready pod -l app=cassandra --timeout=10m
```

If Cassandra hits OOMKilled in Minikube, adjust heap/requests in `k8s/cassandra-statefulset.yaml` before retrying.
Current Minikube-friendly defaults use `MAX_HEAP_SIZE=1G`, `HEAP_NEWSIZE=256M`, and memory requests/limits of 2Gi/3Gi.

Initialize schema:

```bash
kubectl exec -i cassandra-0 -- cqlsh < cassandra/schema/init_keyspace.cql
kubectl exec -i cassandra-0 -- cqlsh < cassandra/schema/create_tables.cql
```

Verify:

```bash
kubectl exec -i cassandra-0 -- cqlsh -e "USE flight_analytics; DESCRIBE TABLES;"
```

---

## Step 4: Deploy MinIO to Minikube (official chart)

The Bitnami MinIO images referenced by the chart are not publicly available, so use the official MinIO Helm chart instead.

```bash
helm repo add minio https://charts.min.io/
helm repo update

# If you already installed the Bitnami release, remove it first
helm uninstall minio -n minio || true
kubectl delete namespace minio --wait || true

# Install MinIO using the project values file
helm upgrade --install minio minio/minio \
  -n minio --create-namespace \
  -f k8s/minio/minio-values.yaml

kubectl get pods -n minio
```

If the pod is Pending due to memory, lower the resource requests in `k8s/minio/minio-values.yaml` and re-run the upgrade. The values file in this repo already targets Minikube-friendly defaults.

Port-forward console (optional):

```bash
kubectl port-forward -n minio svc/minio-console 9001:9001
```

---

## Step 5: Deploy Kafka to Minikube (in-cluster access)

Bitnami moved the Debian-based images used by this chart to `bitnamilegacy/*`, so pin those repositories/tags to avoid image pull failures.

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm upgrade --install kafka bitnami/kafka \
  -n kafka --create-namespace \
  --set controller.replicaCount=1 \
  --set broker.replicaCount=1 \
  --set listeners.client.protocol=PLAINTEXT \
  --set rbac.create=true \
  --set controller.automountServiceAccountToken=true \
  --set broker.automountServiceAccountToken=true \
  --set defaultInitContainers.autoDiscovery.enabled=true \
  --set defaultInitContainers.autoDiscovery.image.repository=bitnamilegacy/kubectl \
  --set defaultInitContainers.autoDiscovery.image.tag=1.33.4-debian-12-r0 \
  --set image.repository=bitnamilegacy/kafka \
  --set image.tag=4.0.0-debian-12-r10 \
  --wait --timeout 10m
```

Confirm the service is up:

```bash
kubectl get svc -n kafka
```

Use the broker headless service (`kafka-broker-headless.kafka.svc.cluster.local:9092`) for client bootstrap to avoid hitting controller pods.

Create Kafka topics:

```bash
kubectl exec -n kafka -it $(kubectl get pods -n kafka -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}') -- \
  kafka-topics.sh --bootstrap-server kafka-broker-headless.kafka.svc.cluster.local:9092 --create --if-not-exists --topic flights_raw --partitions 1 --replication-factor 1

kubectl exec -n kafka -it $(kubectl get pods -n kafka -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}') -- \
  kafka-topics.sh --bootstrap-server kafka-broker-headless.kafka.svc.cluster.local:9092 --create --if-not-exists --topic flight_data --partitions 1 --replication-factor 1

kubectl exec -n kafka -it $(kubectl get pods -n kafka -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}') -- \
  kafka-topics.sh --bootstrap-server kafka-broker-headless.kafka.svc.cluster.local:9092 --create --if-not-exists --topic flight_track --partitions 1 --replication-factor 1
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

If the Spark pod CrashLoopBackOffs with `KerberosAuthException` or OOMKilled, the deployment already runs as root and has increased memory limits in `k8s/spark/spark-deployment.yaml`. Re-apply the file and wait for the pod to stabilize.

---

## Step 7: Deploy Trino + Superset

### 7.1 Trino

```bash
helm repo add trino https://trinodb.github.io/charts
helm upgrade --install trino trino/trino -n trino --create-namespace -f trino/trino-values.yaml
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=trino --timeout=10m -n trino
```

The values file uses Minikube-friendly Trino CPU/memory defaults; increase them later for EKS.
It also caps `query.max-memory` and `query.max-memory-per-node` to stay within the smaller JVM heap.

### 7.2 Superset

```bash
./superset/scripts/deploy_superset.sh
./superset/scripts/configure_superset_trino.sh
./superset/scripts/add_superset_datasets.sh
```

---

## Step 8: Validation Checklist (must run)

### NiFi flows deployed

```bash
kubectl get jobs -n nifi nifi-flow-deployer
kubectl logs -n nifi job/nifi-flow-deployer --tail=200
```

### Kafka topics exist

```bash
kubectl exec -n kafka -it $(kubectl get pods -n kafka -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}') -- \
  kafka-topics.sh --bootstrap-server kafka-broker-headless.kafka.svc.cluster.local:9092 --list
```

### Spark streaming is running

```bash
kubectl logs -n spark -l app=spark-stream --tail=200
```

### Kafka has data (NiFi -> Kafka)

```bash
kubectl exec -n kafka -i $(kubectl get pods -n kafka -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}') -- \
  timeout 10 /opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-broker-headless.kafka.svc.cluster.local:9092 --topic flights_raw --from-beginning --max-messages 3
```

If this times out with no messages, re-check the OpenSky OAuth2 controller service in NiFi.

### Cassandra has data

```bash
kubectl exec -i cassandra-0 -- cqlsh -e "SELECT COUNT(*) FROM flight_analytics.aircrafts_by_icao24;"
```

### Trino can read Cassandra

```bash
kubectl exec -n trino $(kubectl get pods -n trino -l app.kubernetes.io/name=trino -o jsonpath='{.items[0].metadata.name}') -- \
  trino --server http://trino.trino.svc.cluster.local:8080 --execute "SELECT COUNT(*) FROM \"cassandra.properties\".flight_analytics.aircrafts_by_icao24"
```

### Superset datasets exist

- Open Superset UI and confirm the datasets list is populated from Trino.

---

## Notes for Future EKS Migration (high-level)

- Replace MinIO with S3 and use IAM Roles for Service Accounts (IRSA) for Spark.
- Replace NodePort exposure with LoadBalancer or Ingress.
- Move images to ECR and pin versions instead of `latest`.
- Convert the NiFi and NiFi Registry PVCs to EBS-backed StorageClasses and use an Ingress/ALB for the NiFi UI.
- Keep Kafka endpoints parameterized via `KAFKA_BOOTSTRAP_SERVERS` so switching to MSK only requires a ConfigMap update.
- Consider MSK for Kafka and managed Cassandra alternatives if ops overhead grows.
