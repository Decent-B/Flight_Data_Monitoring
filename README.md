# ✈️ Flight Data Monitoring

A real-time flight tracking and analytics platform built on Google Kubernetes Engine (GKE).

![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5.svg?style=for-the-badge&logo=kubernetes&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000000.svg?style=for-the-badge&logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-%23E25A1C.svg?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Cassandra](https://img.shields.io/badge/Apache%20Cassandra-1287B1.svg?style=for-the-badge&logo=apachecassandra&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google%20Cloud-4285F4.svg?style=for-the-badge&logo=googlecloud&logoColor=white)

---

## Overview

This project implements a **Kappa Architecture** streaming pipeline that:

- **Ingests** real-time flight data from [OpenSky Network API](https://opensky-network.org/)
- **Processes** streams using Apache Spark Structured Streaming
- **Stores** data in Apache Cassandra for fast querying
- **Visualizes** flight positions and analytics in Apache Superset with MapBox maps

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         GKE Cluster                                     │
│                                                                         │
│   [OpenSky API] → [NiFi] → [Kafka] → [Spark] → [Cassandra]             │
│                                                        ↓                │
│                                    [Trino] ← [Superset Dashboard]       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Data Ingestion** | Apache NiFi | Fetch data from OpenSky API |
| **Message Broker** | Apache Kafka (3 brokers) | Real-time event streaming |
| **Stream Processing** | Apache Spark | Transform and aggregate data |
| **Database** | Apache Cassandra | Time-series data storage |
| **Query Engine** | Trino | SQL queries over Cassandra |
| **Visualization** | Apache Superset | Dashboards with MapBox maps |
| **Object Storage** | MinIO | S3-compatible checkpoints |
| **Orchestration** | Kubernetes (GKE) | Container orchestration |

---

## Quick Start

### Prerequisites

- Google Cloud account with billing enabled
- `gcloud`, `kubectl`, `helm`, `docker` installed

### Deploy to GKE

```bash
# Clone the repository
git clone https://github.com/YOUR_ORG/Flight_Data_Monitoring.git
cd Flight_Data_Monitoring

# Authenticate with GCP
gcloud auth login
gcloud config set project YOUR_PROJECT_ID

# Run automated deployment
chmod +x deploy-gke.sh
./deploy-gke.sh
```

📖 **For detailed step-by-step instructions, see the [GKE Deployment Runbook](system_docs/gke_runbook.md).**

---

## Documentation

| Document | Description |
|----------|-------------|
| [GKE Runbook](system_docs/gke_runbook.md) | **Complete deployment guide** - Start here! |
| [Minikube Runbook](system_docs/minikube_runbook.md) | Local development setup |
| [Data Sources](system_docs/data_source_readme.md) | API documentation and data schemas |
| [Local Setup](system_docs/setup_local_readme.md) | Development environment setup |

---

## Project Structure

```
Flight_Data_Monitoring/
├── cassandra/              # Cassandra schema definitions
├── config/                 # Python configuration modules
├── data/                   # Sample data files
├── docker/                 # Dockerfiles for Spark, NiFi
├── k8s/
│   └── gke/               # GKE Kubernetes manifests
├── kafka/                  # Kafka utilities and producers
├── nifi/                   # NiFi flows and scripts
├── spark/                  # Spark streaming applications
├── superset/               # Superset deployment and config
├── system_docs/            # Documentation and runbooks
├── trino/                  # Trino Helm values
├── deploy-gke.sh           # Automated GKE deployment script
└── README.md               # This file
```

---

## Key Features

- **Real-time Processing**: Sub-second latency from API to dashboard
- **Scalable**: Horizontal scaling via Kubernetes node pools
- **Cost-Optimized**: Spot VMs and autoscaling for ~60% cost savings
- **Production-Ready**: Includes monitoring, logging, and troubleshooting guides

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

*Built for the Big Data Storage and Processing course • Last Updated: January 2026*
