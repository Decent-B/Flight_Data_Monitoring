# ✈️ Real-Time Global Flight Data Streaming & Analytics

![Architecture Banner](https://img.shields.io/badge/Architecture-Kappa-blue?style=for-the-badge)
![Spark](https://img.shields.io/badge/Apache%20Spark-%23E25A1C.svg?style=for-the-badge&logo=apachespark&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-000000.svg?style=for-the-badge&logo=apachekafka&logoColor=white)
![Cassandra](https://img.shields.io/badge/Apache%20Cassandra-1287B1.svg?style=for-the-badge&logo=apachecassandra&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5.svg?style=for-the-badge&logo=kubernetes&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB.svg?style=for-the-badge&logo=python&logoColor=white)

---

## 🧩 Problem Definition

### 🛫 Selected Problem
The project builds a **real-time flight monitoring and analytics system** that continuously processes live flight data from a global tracking API (e.g., [OpenSky Network](https://opensky-network.org/api/states/all)).  
Incoming data includes aircraft identifiers, positions, altitudes, speeds, and timestamps.  
We process, store, and visualize these streams to answer operational questions like:

- Which regions currently have the highest flight density?  
- What is the average flight altitude by airline or country?  
- How are real-time trajectories changing over time?

---

### 🌍 Suitability for Big Data
The system exhibits all **five Vs** of big data:

| 💡 Aspect | ✈️ In this project |
|------------|--------------------|
| **Volume** | Tens of thousands of flights generate millions of events daily |
| **Velocity** | Updates every 1–5 seconds per flight — demands real-time streaming |
| **Variety** | JSON data containing structured & semi-structured fields |
| **Veracity** | Noisy or missing coordinates require cleaning & validation |
| **Value** | Aggregated insights improve air traffic awareness and efficiency |

Hence, this problem is an ideal fit for distributed, **stream-oriented big data processing** using technologies like Kafka and Spark.

---

### ⚙️ Scope and Limitations

**Scope**
- Real-time ingestion of flight telemetry via APIs  
- Stream processing using Spark Structured Streaming  
- Distributed storage in Cassandra and HDFS  
- Visualization dashboard for real-time analytics  

**Limitations**
- Depends on API rate limits and data availability  
- Focuses on *live* streams; historical replays are optional  
- Limited to descriptive analytics; predictive models (e.g., delay prediction) are future work  

---

## 🏗️ Architecture and Design

### 🧠 Overall Architecture
We adopt a **Kappa Architecture** — a unified, stream-based design suitable for continuous event processing.  
Unlike Lambda, Kappa eliminates the need for separate batch and stream layers, simplifying development and ensuring consistency.

📊 **Layers Overview**

1. **Data Ingestion Layer** → Fetches flight data via API and pushes to Kafka  
2. **Stream Transport Layer** → Kafka brokers manage and distribute messages  
3. **Processing Layer** → Spark Structured Streaming cleans, aggregates, and transforms data  
4. **Storage Layer** → Cassandra for hot data, HDFS for cold archival data  
5. **Visualization Layer** → Dashboard visualizes flight density and metrics  
6. **Deployment Layer** → Kubernetes orchestrates services for scalability and fault tolerance  

<p align="center">
  <img src="https://www.interviewbit.com/blog/wp-content/uploads/2022/06/Kappa-Architecture-2048x1435.png" alt="Kappa Architecture (Image from InterviewBit)" width="500"/>
</p>

---

### 🧩 Detailed Components and Roles

| 🧱 Component | 🛠️ Technology | 🎯 Role |
|--------------|----------------|--------|
| **Data Collector** | Python Script | Fetches data from flight API and publishes to Kafka topic `flights_raw` |
| **Message Broker** | Apache Kafka | Manages streams, ensures ordering, supports exactly-once processing |
| **Stream Processor** | Apache Spark Structured Streaming | Applies transformations, windowed aggregations, joins, and writes results |
| **Hot Storage** | Apache Cassandra | Stores aggregated results for fast querying by region/time |
| **Cold Storage** | HDFS / MinIO | Archives raw JSON data for replay or offline analysis |
| **Dashboard** | Flask + Leaflet / Grafana | Displays flight positions, heatmaps, and real-time metrics |
| **Cluster Orchestration** | Kubernetes | Manages all components as scalable, fault-tolerant pods |

---

### 🔁 Data Flow Overview

```text
[Flight API]
     ↓
[Kafka Producer] → publishes to → [Kafka Topic: flights_raw]
     ↓
[Spark Structured Streaming]
     ↓
 ├── cleans, aggregates, joins (windowed)
 ├── writes results → [Cassandra] (hot)
 └── archives → [HDFS] (cold)
     ↓
[Dashboard Service] → queries Cassandra or Kafka summary topic
     ↓
[End User Visualization]
```


---

### 🧭 Key Advantages
- Pure streaming architecture — no separate batch layer  
- Exactly-once semantics for accurate analytics  
- Scalable deployment using Kubernetes  
- Real-time visualization of global flight activity  

---

> 🪶 *This README reflects the architecture and design plan for the milestone project in the course **Big Data Storage and Processing**.  
> The next stages will include detailed implementation steps, code modules, and deployment scripts.*
