# Disaster Recovery Replay System

A disaster recovery system that reads historical security event data from an Apache Iceberg data lake and replays it to downstream consumers (Kafka or REST). Built with **core Java (no Spring)** and **Pekko Actor** framework.

## Features

- **Data source**: Apache Iceberg (Hadoop catalog, local or HDFS warehouse)
- **Outputs**: Apache Kafka topics and REST API (HTTP POST)
- **Replay job API**: Create, list, get, start, pause, resume, cancel; status and metrics
- **Deployment**: Containerized, Kubernetes-ready (minikube, kind, k3s)
- **Test data**: Generator for 50,000+ events with required schema

## Event schema

Each event has:

| Field            | Type      | Description                    |
|-----------------|-----------|--------------------------------|
| `cid`           | string    | Customer identifier            |
| `event_timestamp` | string  | ISO 8601 timestamp              |
| `event_time`    | long      | Unix epoch milliseconds        |
| `event_type`    | string    | e.g. ProcessStart, NetworkConnect |
| `event_id`      | string    | Unique UUID                    |

## Quick start (local)

### Prerequisites

- Java 17+
- Maven 3.8+
- (Optional) Docker, minikube/kind/k3s for K8s

### Build

```bash
mvn clean package -DskipTests
```

### Run

1. **Replay service** (API + Iceberg reader + Pekko):

   ```bash
   export ICEBERG_WAREHOUSE=file:///tmp/warehouse
   export PORT=8080
   java -jar target/disaster-recovery-replay-1.0.0-SNAPSHOT-all.jar
   ```

   On first run, it creates the Iceberg table `security.events` and generates 50,000+ test events if needed.

2. **Mock REST sink** (for testing REST destination):

   ```bash
   java -cp target/disaster-recovery-replay-1.0.0-SNAPSHOT-all.jar com.crowdstrike.dr.replay.sink.MockRestSink
   ```
   Listens on port 8081, accepts `POST /events` and exposes `GET /events/count`.

### Replay API (documented operations)

Base URL: `http://localhost:8080`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/metrics` | Service metrics |
| POST | `/api/v1/replay/jobs` | Create replay job |
| GET | `/api/v1/replay/jobs` | List all jobs |
| GET | `/api/v1/replay/jobs/{id}` | Get job details |
| POST | `/api/v1/replay/jobs/{id}/start` | Start replay |
| POST | `/api/v1/replay/jobs/{id}/pause` | Pause replay |
| POST | `/api/v1/replay/jobs/{id}/resume` | Resume replay |
| POST | `/api/v1/replay/jobs/{id}/cancel` | Cancel replay |
| GET | `/api/v1/replay/jobs/{id}/status` | Get job status |
| GET | `/api/v1/replay/jobs/{id}/metrics` | Get job metrics |

#### Create job (POST /api/v1/replay/jobs)

Body (JSON):

```json
{
  "table_path": "security.events",
  "destination_type": "REST",
  "destination_config": "http://localhost:8081",
  "speed_multiplier": 2.0,
  "start_event_time": null,
  "end_event_time": null
}
```

- `destination_type`: `"KAFKA"` or `"REST"`
- `destination_config`: Kafka topic name (for KAFKA) or base URL (for REST, e.g. `http://localhost:8081`)
- `speed_multiplier`: replay speed (1.0 = real-time)
- `start_event_time` / `end_event_time`: optional Unix ms filter

## Demo flow (20 min)

1. Start mock sink: `java -cp target/...-all.jar com.crowdstrike.dr.replay.sink.MockRestSink`
2. Start replay service (see above).
3. Create job:
   ```bash
   curl -s -X POST http://localhost:8080/api/v1/replay/jobs \
     -H "Content-Type: application/json" \
     -d '{"table_path":"security.events","destination_type":"REST","destination_config":"http://localhost:8081","speed_multiplier":10}'
   ```
4. Note `id` from response; start replay:
   ```bash
   curl -s -X POST http://localhost:8080/api/v1/replay/jobs/<id>/start
   ```
5. Monitor: `GET /api/v1/replay/jobs/<id>/status` and `GET /api/v1/replay/jobs/<id>/metrics`.
6. Pause: `POST /api/v1/replay/jobs/<id>/pause`; resume: `POST /api/v1/replay/jobs/<id>/resume`.
7. Verify events at mock sink: `curl http://localhost:8081/events/count`.

## Kubernetes

```bash
# Build image (minikube: eval $(minikube docker-env) first if using local image)
docker build -t disaster-recovery-replay:1.0.0 .

# Deploy
kubectl apply -f kubernetes/namespace.yaml
kubectl apply -f kubernetes/configmap.yaml
kubectl apply -f kubernetes/mock-sink-deployment.yaml
kubectl apply -f kubernetes/replay-deployment.yaml

# Port-forward for local access
kubectl -n replay-demo port-forward svc/replay-service 8080:8080
kubectl -n replay-demo port-forward svc/mock-sink 8081:8081
```

For Kafka in-cluster, deploy Kafka (e.g. Strimzi) and set `KAFKA_BOOTSTRAP_SERVERS` in the ConfigMap.

## Configuration (env)

| Variable | Default | Description |
|----------|---------|-------------|
| `ICEBERG_WAREHOUSE` | `file:///data/warehouse` | Iceberg warehouse path |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `REST_DESTINATION_BASE_URL` | `http://localhost:8081` | Default REST sink URL |
| `PORT` | `8080` | API port |

## Project layout

- `src/main/java/com/crowdstrike/dr/replay/`
  - `Main.java` – entry point
  - `model/` – SecurityEvent, ReplayJob, ReplayJobRequest
  - `iceberg/` – catalog, schema, reader, test data generator
  - `destination/` – KafkaDestination, RestDestination
  - `actor/` – ReplayGuardian, ReplayJobActor (Pekko typed)
  - `api/` – Javalin REST API
  - `config/` – ReplayConfig
  - `sink/` – MockRestSink (demo)
- `kubernetes/` – namespace, configmap, deployments, services
- `Dockerfile` – single-stage image

## License

Internal / demo use.
