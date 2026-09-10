"""
Small standalone script to tail the DLQ topic — handy for the live demo,
to show the grader the permanently-failed messages as they land.
"""

import io
import json
import os

from dotenv import load_dotenv
from fastavro import schemaless_reader, parse_schema
from kafka import KafkaConsumer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM")
SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME")
SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD")
SSL_CAFILE = os.getenv("KAFKA_SSL_CA_LOCATION")

DLQ_TOPIC = os.getenv("ORDERS_DLQ_TOPIC", "orders-dlq")

SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "..", "schemas")
with open(os.path.join(SCHEMAS_DIR, "order_dlq.avsc")) as f:
    ORDER_DLQ_SCHEMA = parse_schema(json.load(f))


def deserialize(data: bytes, schema) -> dict:
    buf = io.BytesIO(data)
    return schemaless_reader(buf, schema)


def _security_kwargs() -> dict:
    kwargs = {"bootstrap_servers": BOOTSTRAP_SERVERS, "security_protocol": SECURITY_PROTOCOL}
    if SECURITY_PROTOCOL in ("SSL", "SASL_SSL"):
        kwargs["ssl_cafile"] = SSL_CAFILE
    if SECURITY_PROTOCOL == "SASL_SSL":
        kwargs["sasl_mechanism"] = SASL_MECHANISM
        kwargs["sasl_plain_username"] = SASL_USERNAME
        kwargs["sasl_plain_password"] = SASL_PASSWORD
    return kwargs


def build_dlq_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        DLQ_TOPIC,
        group_id="dlq-monitor",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: deserialize(v, ORDER_DLQ_SCHEMA),
        **_security_kwargs(),
    )


def main():
    consumer = build_dlq_consumer()
    print(f"[dlq-monitor] watching '{DLQ_TOPIC}'... Ctrl+C to stop")

    try:
        for msg in consumer:
            print(f"[dlq-monitor] {msg.value}")
    except KeyboardInterrupt:
        print("\n[dlq-monitor] stopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()