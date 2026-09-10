# Kafka Order Processing System

Assignment 1 submission — a Kafka producer/consumer pair that streams order
messages with Avro serialization, does real-time price aggregation, retries
on failure, and sends permanently-failed messages to a dead letter queue.

Gunawardhana I.K.Y.K (EG/2021/4535)

## What it does

- Producer sends randomly generated orders (`orderId`, `product`, `price`)
  to a Kafka topic, Avro-encoded.
- Consumer reads them, simulates a transient failure some % of the time,
  retries with exponential backoff (3 attempts), and if it still fails
  sends the order to a DLQ topic with the error reason and retry count
  attached.
- Consumer also keeps a running average price — overall and per product —
  and prints it as each message comes in.
- `dlq_monitor.py` just tails the DLQ topic so you can see failed messages
  live, separately from the consumer's own logs.

## Setup

Using Aiven's free Kafka tier (no Docker — I had Docker issues on my
machine and didn't want to fight it during the assignment).

1. Create a Kafka service on [Aiven](https://console.aiven.io) (free tier).
2. Once it's running, go to **Quick connect** → Python, copy the bootstrap
   server address, and download the CA cert (`ca.pem`).
3. Create two topics manually from the Aiven console's **Topics** tab:
   `orders` and `orders-dlq`. (Aiven doesn't auto-create topics — my
   consumer hung for a full 60s timeout the first time I forgot this.)
4. Put `ca.pem` in a `certs/` folder, copy `.env.example` to `.env`, fill
   in the values (see table below).
5. `python -m venv .venv` then activate it, then `pip install -r requirements.txt`

### A note on `confluent-kafka`

I originally had `confluent-kafka[avro]` in requirements.txt, following
most Kafka+Python tutorials. It doesn't ship prebuilt wheels for Python
3.13 on Windows, and building it from source needs `librdkafka` + MSVC
build tools, which I didn't have set up. Rather than fight the C build
chain, I switched everything to `kafka-python` + `fastavro`, which are
pure Python and install cleanly. Schema validation happens against the
local `.avsc` files in `schemas/`, no Schema Registry involved — simpler
for what this assignment needs.

## Running it

Three terminals, venv activated in each:

```bash
python consumer/consumer.py
python producer/producer.py
python consumer/dlq_monitor.py
```

Order doesn't strictly matter but starting the consumer first means you
don't miss the earliest messages if you're not using `earliest` offset
reset (I am, so it's not critical, but habit).

## Env vars

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | — | required, no default — Aiven broker:port |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | `SASL_SSL` for Aiven |
| `KAFKA_SASL_MECHANISM` | — | `SCRAM-SHA-256` for Aiven |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | — | Aiven service user (`avnadmin` by default) |
| `KAFKA_SSL_CA_LOCATION` | — | path to `ca.pem` |
| `ORDERS_TOPIC` | `orders` | |
| `ORDERS_DLQ_TOPIC` | `orders-dlq` | |
| `MAX_RETRIES` | `3` | |
| `BASE_BACKOFF_SECONDS` | `1.0` | doubles each retry |
| `SIMULATED_FAILURE_RATE` | `0.3` | chance a single attempt "fails" — set higher (I used `1.0`) to force a DLQ hit while testing |

## Project structure
kafka-order-system/
├── requirements.txt
├── schemas/
│ ├── order.avsc
│ └── order_dlq.avsc
├── producer/
│ └── producer.py
└── consumer/
├── consumer.py # retry + DLQ + aggregation
└── dlq_monitor.py # tails the DLQ topic, for the demo


## Known issues / what I'd improve with more time

- `process_order()` just rolls a random number to decide whether to fail —
  fine for demonstrating the retry/DLQ mechanism, but a real system would
  have actual failure conditions (downstream service timeout, etc.)
- No persistence for the aggregation — it resets if the consumer restarts,
  since it's all in-memory. Would need to checkpoint it somewhere for a
  real deployment.
- `kafka-python`'s serializer/deserializer args throw deprecation warnings
  because they expect a class-based interface rather than a plain
  function — cosmetic, didn't fix it since it doesn't affect behavior.
- Aiven's free tier auto-pauses after ~24h idle, so if you're grading this
  from the repo later rather than the live demo, the service may need to
  be manually restarted from the console first.
