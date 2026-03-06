# DR Demo — Disaster Recovery System

A full-stack disaster recovery orchestration platform for replaying, managing, and monitoring data recovery jobs across distributed systems.

## Architecture

- **API Server** — REST API for job management (Node.js / Express)
- **Worker Engine** — Partition-aware job executor
- **Storage Layer** — PostgreSQL for job state, Parquet for data partitions
- **Streaming** — Kafka producer for downstream consumers

## Getting Started

```bash
npm install
cp .env.example .env
docker-compose up -d
npm run dev
```

## API Overview

See [docs/API.md](docs/API.md) for full endpoint documentation.
