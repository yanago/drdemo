# Disaster Recovery Replay System

A disaster recovery system built with **core Java + Apache Pekko Actors** that reads historical security event data from a simulated Apache Iceberg data lake and replays it to downstream consumers (Kafka topics and REST endpoints).

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        Kubernetes Cluster                        │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │  DR Replay   │    │    Kafka     │    │  Mock Downstream │  │
│  │  Service     │───▶│  (Kafka +   │    │  REST Consumer   │  │
│  │  (Pekko)     │    │  Zookeeper) │    │  (HTTP Server)   │  │
│  │              │    └──────────────┘    └──────────────────┘  │
│  │  - REST API  │                                               │
│  │  - Iceberg   │    ┌──────────────┐                          │
│  │    Reader    │───▶│  REST POST   │                          │
│  │  - Job Mgmt  │    │  Endpoint    │                          │
│  └──────────────┘    └──────────────┘                          │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │              MinIO (Iceberg Data Lake Storage)          │    │
│  └────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

## Components

- **DR Replay Service**: Core Java + Pekko Actors — manages replay jobs, reads from Iceberg, writes to Kafka/REST
- **Iceberg Data Lake**: Simulated via MinIO (S3-compatible) + in-process Hadoop catalog with 50,000+ events
- **Kafka**: Event streaming output destination
- **Mock REST Consumer**: Simple HTTP server to receive replayed events

## Quick Start

### Prerequisites
- Docker + kubectl + minikube (or kind/k3s)
- Java 17+, Maven 3.8+

### 1. Start Kubernetes
```bash
minikube start --memory=6144 --cpus=4
```

### 2. Build & Deploy
```bash
chmod +x scripts/*.sh
./scripts/build-and-deploy.sh
```

### 3. Seed the Data Lake
```bash
./scripts/seed-data.sh
```

### 4. Run the Demo
```bash
./scripts/demo.sh
```

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/v1/replay/jobs | Create new replay job |
| GET | /api/v1/replay/jobs | List all jobs |
| GET | /api/v1/replay/jobs/{id} | Get job details |
| POST | /api/v1/replay/jobs/{id}/start | Start replay |
| POST | /api/v1/replay/jobs/{id}/pause | Pause replay |
| POST | /api/v1/replay/jobs/{id}/resume | Resume replay |
| POST | /api/v1/replay/jobs/{id}/cancel | Cancel replay |
| GET | /api/v1/replay/jobs/{id}/status | Get job status |
| GET | /api/v1/replay/jobs/{id}/metrics | Get job metrics |
| GET | /health | Health check |
| GET | /metrics | Prometheus metrics |

## Event Schema

```json
{
  "cid": "customer-a1b2c3",
  "event_timestamp": "2024-10-15T14:23:45.123Z",
  "event_time": 1729004625123,
  "event_type": "ProcessStart",
  "event_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

## Project Structure

```
drdemo/
├── src/main/java/com/drdemo/
│   ├── Main.java                    # Entry point
│   ├── actors/                      # Pekko actors
│   │   ├── JobSupervisorActor.java
│   │   ├── ReplayJobActor.java
│   │   ├── IcebergReaderActor.java
│   │   ├── KafkaWriterActor.java
│   │   └── RestWriterActor.java
│   ├── api/
│   │   ├── HttpServer.java
│   │   └── JobRoutes.java
│   ├── model/
│   │   ├── SecurityEvent.java
│   │   ├── ReplayJob.java
│   │   └── JobMetrics.java
│   ├── iceberg/
│   │   ├── IcebergCatalogManager.java
│   │   └── IcebergDataSeeder.java
│   └── kafka/
│       └── KafkaProducerManager.java
├── k8s/                             # Kubernetes manifests
├── docker/                          # Dockerfiles
├── scripts/                         # Helper scripts
└── pom.xml
```
