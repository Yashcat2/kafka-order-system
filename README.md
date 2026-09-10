# Kafka Order Processing System

Produces and consumes Avro-serialized order messages, with real-time price
aggregation, retry logic for transient failures, and a Dead Letter Queue
(DLQ) for permanently-failed messages.

## Architecture

```
producer.py --> [orders topic] --> consumer.py --> [orders-dlq topic] --> dlq_monitor.py
                (Avro, order.avsc)      |
                                        v
                          running avg (overall + per product)
```

- **Serialization**: Avro, via Confluent Schema Registry (`order.avsc` for
  live orders, `order_dlq.avsc` for DLQ records — it carries the original
  fields plus `errorReason`, `retryCount`, `failedAt`).
- **Retry logic**: on a simulated transient failure, the consumer retries
  up to `MAX_RETRIES` times with exponential backoff before giving up.
- **DLQ**: once retries are exhausted, the order is republished to
  `orders-dlq` with failure metadata, and the original offset is still
  committed (so the consumer doesn't get stuck reprocessing it forever).
- **Aggregation**: the consumer keeps a running (incremental) average price
  overall and per product — no need to store the full history in memory.

## 1. Get a Kafka broker — two options

**Option A — no Docker needed (recommended if Docker/your machine is a problem):**
use [Aiven's free Kafka tier](https://console.aiven.io) — no credit card required,
runs in their cloud, and includes the Karapace schema registry out of the box.

1. Sign up at console.aiven.io, create a project.
2. Create service -> **Apache Kafka** -> Service tier: **Free**. Give it a name,
   click Create. It takes a minute or two to go "Running".
3. Open the service -> **Quick connect** -> pick **Python**. This gives you the
   exact bootstrap server, ports, and a link to download the SSL cert bundle
   (`ca.pem`, `service.cert`, `service.key`).
4. Put those three files in a `certs/` folder here, copy `.env.example` to
   `.env` and fill in the values from Quick connect (bootstrap server, schema
   registry URL, and the Karapace username/password shown on the service's
   **Overview** page under "Schema Registry").
5. Create the two topics (`orders`, `orders-dlq`) from the Aiven Console's
   **Topics** tab — free tier caps you at 5 topics, 2 partitions each, which
   is plenty here.
6. Before running anything: `export $(grep -v '^#' .env | xargs)`

Note: a free-tier service auto-powers-off after ~24h idle (or within a few
hours if never used) — just hit "Power on" in the console before your demo.

**Option B — local Docker** (if/when Docker is working again):

```bash
docker-compose up -d
```

This brings up Zookeeper, Kafka (`localhost:9092`), and Schema Registry
(`localhost:8081`) — and none of the `.env` / SSL variables above are needed.

## 2. Install dependencies

```bash
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## 3. Run the demo

In three separate terminals (with the venv activated):

```bash
# Terminal 1 — consumer (processes orders, retries, aggregates, DLQs failures)
python consumer/consumer.py

# Terminal 2 — producer (streams random orders, ~1/sec)
python producer/producer.py

# Terminal 3 — DLQ monitor (optional, for showing the grader failed messages live)
python consumer/dlq_monitor.py
```

You should see:
- the producer logging each delivered order,
- the consumer logging processing attempts, retries, running averages,
  and any messages it routes to the DLQ,
- the DLQ monitor printing the same failed messages with their error
  reason and retry count.

## Tuning knobs (env vars)

| Variable                  | Default          | Purpose                                    |
|----------------------------|------------------|---------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`  | `localhost:9092` | Kafka broker address                        |
| `SCHEMA_REGISTRY_URL`      | `http://localhost:8081` | Schema Registry address              |
| `ORDERS_TOPIC`             | `orders`         | Topic for live order messages               |
| `ORDERS_DLQ_TOPIC`         | `orders-dlq`     | Topic for permanently-failed messages       |
| `MAX_RETRIES`              | `3`              | Retry attempts before sending to DLQ        |
| `BASE_BACKOFF_SECONDS`     | `1.0`            | Base for exponential backoff                |
| `SIMULATED_FAILURE_RATE`   | `0.3`            | Chance a given order "fails" (demo purposes)|
| `KAFKA_SECURITY_PROTOCOL`  | *(unset)*        | Set to `SSL` for Aiven; unset for local Docker |
| `KAFKA_SSL_CA_LOCATION`    | *(unset)*        | Path to `ca.pem` (Aiven only)                |
| `KAFKA_SSL_CERTFILE`       | *(unset)*        | Path to `service.cert` (Aiven only)          |
| `KAFKA_SSL_KEYFILE`        | *(unset)*        | Path to `service.key` (Aiven only)           |
| `SCHEMA_REGISTRY_USER`     | *(unset)*        | Karapace username, e.g. `avnadmin` (Aiven only) |
| `SCHEMA_REGISTRY_PASSWORD` | *(unset)*        | Karapace password (Aiven only)               |

To demonstrate the DLQ live, either leave `SIMULATED_FAILURE_RATE` as-is
(some orders will always be routed to the DLQ eventually) or temporarily
set it to `1.0` to force every order through retries into the DLQ.

## Project structure

```
kafka-order-system/
├── docker-compose.yml       # Kafka + Zookeeper + Schema Registry
├── requirements.txt
├── schemas/
│   ├── order.avsc           # live order schema
│   └── order_dlq.avsc       # DLQ record schema
├── producer/
│   └── producer.py
└── consumer/
    ├── consumer.py          # retry + DLQ + aggregation
    └── dlq_monitor.py       # optional live-demo helper
```

## Git repository

```bash
git init
git add .
git commit -m "Kafka order pipeline: Avro, retry, DLQ, real-time aggregation"
git remote add origin <your-repo-url>
git push -u origin main
```

## Notes on `process_order()`

`process_order()` in `consumer.py` is currently a stand-in that randomly
raises `TransientError` to simulate a flaky downstream dependency (a DB
write, an external pricing API, etc.). Swap in real logic there — the
retry/backoff/DLQ scaffolding around it doesn't need to change.
