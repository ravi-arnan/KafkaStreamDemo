# KafkaStream — Real-Time Log Processing Demo

Apache Kafka producer-consumer demo dengan real-time log processing dashboard.

## Arsitektur

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Network (kafka-net)               │
│                                                                 │
│  ┌──────────┐    ┌─────────────────────────────────────┐       │
│  │Zookeeper │───▶│         Apache Kafka Broker          │       │
│  └──────────┘    │  Topics: app-logs, error-logs,       │       │
│                  │          audit-events (3 partitions) │       │
│                  └───────────┬──────────────┬───────────┘       │
│                              │              │                   │
│             ┌────────────────┘    ┌─────────┘                  │
│             ▼                     ▼                             │
│  ┌──────────────────┐   ┌──────────────────┐                   │
│  │  Node Producer   │   │  Node Consumer   │                   │
│  │  (kafka-producer)│   │  (log-aggregator)│                   │
│  │  500ms / 5 msgs  │   │  Filter + Alert  │                   │
│  └──────────────────┘   └──────────────────┘                   │
│                                                                 │
│  ┌──────────────────────────────────────────┐                   │
│  │  Express Dashboard (port 3000)           │                   │
│  │  - SSE real-time stream                  │                   │
│  │  - Topic management                      │                   │
│  │  - Consumer group monitoring             │                   │
│  │  - Producer controls                     │                   │
│  └──────────────────────────────────────────┘                   │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Opsi 1: Dashboard saja (tanpa Kafka broker)
```bash
npm install
npm start
# Buka: http://localhost:3000
```
Dashboard menggunakan in-memory Kafka simulation (tidak perlu Kafka broker).

### Opsi 2: Full Docker Compose (dengan Kafka broker nyata)
```bash
# Start semua services
docker compose up -d

# Dengan Kafka UI
docker compose --profile tools up -d

# Lihat logs
docker compose logs -f producer
docker compose logs -f consumer

# Stop
docker compose down
```

**URL:**
- Dashboard:   http://localhost:3000
- Kafka UI:    http://localhost:8080 (jika pakai `--profile tools`)

### Opsi 3: Kafka Producer/Consumer standalone (connect ke broker lokal)
```bash
npm install kafkajs

# Start Kafka dulu (via docker compose atau lokal)
docker compose up -d zookeeper kafka

# Jalankan producer
KAFKA_BROKERS=localhost:29092 node kafka-producer.js

# Jalankan consumer (terminal baru)
KAFKA_BROKERS=localhost:29092 \
CONSUMER_GROUP_ID=my-group \
CONSUMER_TOPICS=app-logs,error-logs \
node kafka-consumer.js
```

## Struktur Project

```
kafka-demo/
├── server.js              # Express backend + SSE + in-memory Kafka sim
├── kafka-producer.js      # Real Kafka producer (pakai kafkajs)
├── kafka-consumer.js      # Real Kafka consumer (pakai kafkajs)
├── public/
│   ├── index.html         # Dashboard UI
│   ├── style.css          # Styling
│   └── app.js             # Frontend JS
├── docker-compose.yml     # Full stack deployment
├── Dockerfile             # Dashboard image
├── Dockerfile.producer    # Producer image
└── Dockerfile.consumer    # Consumer image
```

## Topics

| Topic | Partitions | Deskripsi |
|-------|-----------|-----------|
| `app-logs` | 3 | Semua log aplikasi (DEBUG, INFO, WARN) |
| `error-logs` | 3 | Error dan fatal logs khusus |
| `audit-events` | 3 | Audit trail untuk WARNING, ERROR, FATAL |

## Environment Variables

### Dashboard (server.js)
| Variable | Default | Deskripsi |
|---------|---------|-----------|
| `PORT` | `5000` | Server port |

### Producer (kafka-producer.js)
| Variable | Default | Deskripsi |
|---------|---------|-----------|
| `KAFKA_BROKERS` | `localhost:29092` | Kafka broker addresses |
| `PRODUCER_INTERVAL_MS` | `500` | Interval antara batch (ms) |
| `PRODUCER_BATCH_SIZE` | `5` | Messages per batch |

### Consumer (kafka-consumer.js)
| Variable | Default | Deskripsi |
|---------|---------|-----------|
| `KAFKA_BROKERS` | `localhost:29092` | Kafka broker addresses |
| `CONSUMER_GROUP_ID` | `log-aggregator` | Consumer group ID |
| `CONSUMER_TOPICS` | `app-logs,error-logs,audit-events` | Topics to subscribe |

## Fitur Dashboard

- **Overview**: KPI cards, throughput chart, log level distribution, top services
- **Live Stream**: Real-time log entries dengan filter topic + level
- **Topics**: Create/delete topics, lihat partition details
- **Consumer Groups**: Monitor consumer lag per group
- **Producer**: Control speed + batch size, manual produce

## Konsep Kafka yang Didemonstrasikan

1. **Topics & Partitions**: Messages didistribusi ke partisi berdasarkan key
2. **Producer**: Publish ke topik dengan key-based partitioning
3. **Consumer Groups**: Multiple consumers sharing topic partitions
4. **Offset Management**: Track posisi konsumsi per partition
5. **Replication Factor**: Konfigurasi replikasi data
6. **Real-time Streaming**: SSE untuk live dashboard updates
7. **Log Aggregation Pattern**: Producer → Topic → Consumer pipeline
