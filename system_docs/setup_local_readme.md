# 🐋 Docker Setup Guide - Flight Data Monitoring System

This guide provides step-by-step instructions to set up and run the Flight Data Monitoring system using Docker containers.

---

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [Project Architecture](#project-architecture)
3. [Initial Setup](#initial-setup)
4. [Starting Services](#starting-services)
5. [Service-Specific Instructions](#service-specific-instructions)
6. [Verification Steps](#verification-steps)
7. [Stopping Services](#stopping-services)
8. [Troubleshooting](#troubleshooting)

---

## ✅ Prerequisites

Before starting, ensure you have the following installed:

- **Docker Desktop** (version 20.10 or higher)
  - Download from: https://www.docker.com/products/docker-desktop
  - Ensure Docker Desktop is running
- **Docker Compose** (included with Docker Desktop)
- **Git** (to clone the repository)
- **Minimum System Requirements:**
  - RAM: 8GB (16GB recommended)
  - CPU: 4 cores
  - Disk Space: 20GB free

### Verify Installation

```powershell
docker --version
docker-compose --version
```

---

## 🏗️ Project Architecture

The system consists of the following Docker services:

| Service | Purpose | Ports |
|---------|---------|-------|
| **Zookeeper** | Kafka coordination | 2181 |
| **Kafka Brokers (3)** | Message streaming platform | 29092, 29093, 29094 |
| **Cassandra (3 nodes)** | Distributed database | 9042, 9043, 9044 |
| **MinIO** | S3-compatible object storage | 9000 (API), 9090 (Console) |
| **NiFi** | Data orchestration | 8443 (HTTPS) |
| **NiFi Registry** | Flow versioning | 18080 |
| **Spark** | Stream processing | 4040 (UI) |

All services communicate through a shared Docker network: `docker_flight-network`

---

## 🚀 Initial Setup

### Step 1: Navigate to Project Directory

```powershell
cd C:\Users\ADMIN\Desktop\Workspace\Flight_Data_Monitoring
```

### Step 2: Set Up Environment Variables

Create a `.env` file in the `docker/` directory:

```powershell
cd docker
```

Create `.env` file with the following content:

```env
# NiFi Credentials
NIFI_SINGLE_USER_USERNAME=admin
NIFI_SINGLE_USER_PASSWORD=YourSecurePassword123!

# MinIO Credentials (already in docker-compose but can be overridden)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
```

### Step 3: Create Docker Network

Create the shared network that all services will use:

```powershell
docker network create docker_flight-network
```

> **Note:** If the network already exists, you'll see a message saying so. This is fine—just proceed to the next step.

---

## 🎯 Starting Services

Services should be started in a specific order to ensure proper dependency management.

### Order of Service Startup

1. **MinIO** (Data storage - no dependencies)
2. **Zookeeper & Kafka** (Message broker)
3. **Cassandra** (Database cluster)
4. **NiFi** (Data ingestion)
5. **Spark** (Stream processing)

### Step 1: Start MinIO

```powershell
docker-compose -f docker-minio.yml up -d
```

**Wait for MinIO to be healthy:**
```powershell
docker logs minio-init
```

You should see: `Buckets created successfully`

### Step 2: Start Kafka Cluster

```powershell
docker-compose -f docker-kafka.yml up -d
```

**Wait 30-60 seconds for Kafka brokers to initialize:**
```powershell
docker-compose -f docker-kafka.yml logs -f kafka-broker-1
```

Look for: `[KafkaServer id=1] started`

Press `Ctrl+C` to exit logs.

### Step 3: Start Cassandra Cluster

```powershell
docker-compose -f docker-cassandra.yml up -d
```

**This takes 2-3 minutes. Monitor the startup:**
```powershell
docker-compose -f docker-cassandra.yml logs -f cassandra-1
```

Look for: `Created default superuser role 'cassandra'` or `Node ... state jump to NORMAL`

Press `Ctrl+C` to exit logs.

**Verify Cassandra cluster status:**
```powershell
docker exec -it cassandra-1 nodetool status
```

You should see all 3 nodes in state `UN` (Up/Normal).

### Step 4: Initialize Cassandra Schema

```powershell
# Return to project root
cd ..

# Create keyspace
docker exec -i cassandra-1 cqlsh < cassandra/schema/init_keyspace.cql

# Create tables
docker exec -i cassandra-1 cqlsh < cassandra/schema/create_tables.cql
```

**Verify schema creation:**
```powershell
docker exec -it cassandra-1 cqlsh -e "DESCRIBE KEYSPACE flight_data;"
```

### Step 5: Start NiFi

```powershell
cd docker
docker-compose -f docker-nifi.yml up -d
```

**Wait 2-3 minutes for NiFi to start:**
```powershell
docker-compose -f docker-nifi.yml logs -f nifi
```

Look for: `NiFi has started`

Access NiFi UI: **https://<nifi-host>:8443/nifi/**

> **Note:** You'll see a security warning (self-signed certificate). This is expected—click "Advanced" and proceed.

**Login Credentials:**
- Username: Value from `NIFI_SINGLE_USER_USERNAME` in `.env` (default: `admin`)
- Password: Value from `NIFI_SINGLE_USER_PASSWORD` in `.env`

### Step 6: Start Spark

```powershell
docker-compose -f docker-spark.yml up -d
```

**Monitor Spark application:**
```powershell
docker logs -f spark
```

Access Spark UI: **http://<spark-host>:4040**

---

## 📦 Service-Specific Instructions

### MinIO Console

**Access:** http://<minio-host>:9090

**Login:**
- Username: `minioadmin`
- Password: `minioadmin123`

**Buckets created automatically:**
- `flight-raw` - Raw ingested data
- `flight-data` - Processed data
- `flight-tracks` - Flight trajectory data
- `checkpoints` - Spark streaming checkpoints

### Kafka Management

**List topics:**
```powershell
docker exec -it kafka-broker-1 kafka-topics --bootstrap-server kafka-broker-1:9092 --list
```

**Create a topic manually:**
```powershell
docker exec -it kafka-broker-1 kafka-topics --bootstrap-server kafka-broker-1:9092 --create --topic flight-events --partitions 3 --replication-factor 3
```

**Check topic details:**
```powershell
docker exec -it kafka-broker-1 kafka-topics --bootstrap-server kafka-broker-1:9092 --describe --topic flight-events
```

### Cassandra Management

**Access CQL shell:**
```powershell
docker exec -it cassandra-1 cqlsh
```

**Inside CQL shell:**
```cql
USE flight_data;
DESCRIBE TABLES;
SELECT * FROM flights LIMIT 10;
```

Type `exit` to leave CQL shell.

---

## ✔️ Verification Steps

### Check All Containers Are Running

```powershell
docker ps
```

You should see all containers with status "Up".

### Check Docker Network

```powershell
docker network inspect docker_flight-network
```

This shows all connected containers.

### Health Check Summary

```powershell
# MinIO health
docker exec minio mc admin info local

# Kafka health
docker exec kafka-broker-1 kafka-broker-api-versions --bootstrap-server kafka-broker-1:9092

# Cassandra health
docker exec cassandra-1 nodetool status

# NiFi health (should return 200)
curl -k https://<nifi-host>:8443/nifi/

# Spark health (check if logs are flowing)
docker logs spark --tail 50
```

---

## 🛑 Stopping Services

### Stop All Services

Stop in reverse order (Spark → NiFi → Cassandra → Kafka → MinIO):

```powershell
cd docker

docker-compose -f docker-spark.yml down
docker-compose -f docker-nifi.yml down
docker-compose -f docker-cassandra.yml down
docker-compose -f docker-kafka.yml down
docker-compose -f docker-minio.yml down
```

### Stop and Remove All Data (⚠️ Destructive)

```powershell
docker-compose -f docker-spark.yml down -v
docker-compose -f docker-nifi.yml down -v
docker-compose -f docker-cassandra.yml down -v
docker-compose -f docker-kafka.yml down -v
docker-compose -f docker-minio.yml down -v
```

> **Warning:** The `-v` flag removes all volumes, deleting all stored data permanently.

---

## 🔧 Troubleshooting

### Problem: "Network docker_flight-network not found"

**Solution:**
```powershell
docker network create docker_flight-network
```

### Problem: Port Already in Use

**Find what's using the port (example for port 9042):**
```powershell
netstat -ano | findstr :9042
```

**Kill the process or stop the conflicting container:**
```powershell
docker stop <container-name>
```

### Problem: Cassandra Node Won't Start

**Check logs:**
```powershell
docker logs cassandra-1
```

**Common issues:**
- Not enough memory (increase Docker Desktop memory to 8GB+)
- Previous data corruption (remove volumes and restart)

**Solution - Clean restart:**
```powershell
docker-compose -f docker-cassandra.yml down -v
docker-compose -f docker-cassandra.yml up -d
```

### Problem: Kafka Broker Connection Errors

**Check Zookeeper is running:**
```powershell
docker logs zookeeper
```

**Restart Kafka cluster:**
```powershell
docker-compose -f docker-kafka.yml restart
```

### Problem: NiFi Won't Start (Memory Issues)

**Increase Docker Desktop Memory:**
1. Open Docker Desktop
2. Settings → Resources → Memory
3. Increase to at least 8GB
4. Apply & Restart

### Problem: Spark Keeps Restarting

**Check dependencies:**
```powershell
# Ensure Kafka is up
docker exec kafka-broker-1 kafka-broker-api-versions --bootstrap-server kafka-broker-1:9092

# Ensure Cassandra is up
docker exec cassandra-1 nodetool status

# Ensure MinIO is up
docker exec minio mc admin info local
```

**Check Spark logs for specific errors:**
```powershell
docker logs spark --tail 100
```

### View Real-Time Logs for Any Service

```powershell
docker-compose -f docker-<service>.yml logs -f
```

Press `Ctrl+C` to stop following logs.

---

## 📝 Quick Reference Commands

### Start Everything (After Initial Setup)

```powershell
cd C:\Users\ADMIN\Desktop\Workspace\Flight_Data_Monitoring\docker

docker-compose -f docker-minio.yml up -d
timeout /t 10
docker-compose -f docker-kafka.yml up -d
timeout /t 30
docker-compose -f docker-cassandra.yml up -d
timeout /t 120
docker-compose -f docker-nifi.yml up -d
docker-compose -f docker-spark.yml up -d
```

### Stop Everything

```powershell
cd C:\Users\ADMIN\Desktop\Workspace\Flight_Data_Monitoring\docker

docker-compose -f docker-spark.yml down
docker-compose -f docker-nifi.yml down
docker-compose -f docker-cassandra.yml down
docker-compose -f docker-kafka.yml down
docker-compose -f docker-minio.yml down
```

### Check Status

```powershell
docker ps -a
docker network ls
docker volume ls
```

---

## 🎉 Success Indicators

You'll know the system is fully operational when:

✅ All containers show status "Up" in `docker ps`  
✅ MinIO Console is accessible at http://<minio-host>:9090  
✅ NiFi UI is accessible at https://<nifi-host>:8443/nifi/  
✅ Spark UI is accessible at http://<spark-host>:4040  
✅ Cassandra cluster shows 3 nodes in UN (Up/Normal) state  
✅ Kafka has all 3 brokers connected  
✅ No repeated error messages in any container logs  

---

## 📚 Additional Resources

- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Cassandra Documentation](https://cassandra.apache.org/doc/)
- [Apache NiFi Documentation](https://nifi.apache.org/docs.html)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [MinIO Documentation](https://min.io/docs/)

---

## 🆘 Need Help?

If you encounter issues not covered in this guide:

1. Check container logs: `docker logs <container-name>`
2. Verify network connectivity: `docker network inspect docker_flight-network`
3. Ensure adequate system resources (RAM, CPU, disk)
4. Review Docker Desktop logs
5. Restart Docker Desktop and try again

---

**Last Updated:** January 7, 2026  
**Project:** Flight Data Monitoring System  
**Docker Version:** 20.10+  
**Platform:** Windows with Docker Desktop
