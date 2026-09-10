import os
import io
import random
import time
import uuid

from dotenv import load_dotenv
from fastavro import schemaless_writer, parse_schema
from kafka import KafkaProducer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM")
SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME")
SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD")
SSL_CAFILE = os.getenv("KAFKA_SSL_CA_LOCATION")
TOPIC = os.getenv("ORDERS_TOPIC", "orders")

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]

ORDER_SCHEMA = parse_schema({
    "namespace": "com.ruhuna.orders",
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "orderId", "type": "string"},
        {"name": "product", "type": "string"},
        {"name": "price", "type": "float"},
    ],
})


def serialize(record: dict, schema) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, schema, record)
    return buf.getvalue()


def build_producer() -> KafkaProducer:
    kwargs = dict(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol=SECURITY_PROTOCOL,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: serialize(v, ORDER_SCHEMA),
    )
    if SECURITY_PROTOCOL in ("SSL", "SASL_SSL"):
        kwargs["ssl_cafile"] = SSL_CAFILE
    if SECURITY_PROTOCOL == "SASL_SSL":
        kwargs["sasl_mechanism"] = SASL_MECHANISM
        kwargs["sasl_plain_username"] = SASL_USERNAME
        kwargs["sasl_plain_password"] = SASL_PASSWORD
    return KafkaProducer(**kwargs)


def generate_order() -> dict:
    return {
        "orderId": str(uuid.uuid4().int)[:8],
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def main(num_messages: int = 0, interval_seconds: float = 1.0):
    producer = build_producer()
    sent = 0
    try:
        while num_messages == 0 or sent < num_messages:
            order = generate_order()
            producer.send(TOPIC, key=order["orderId"], value=order)
            producer.flush()
            print(f"[producer] sent orderId={order['orderId']} "
                  f"product={order['product']} price={order['price']}")
            sent += 1
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("\n[producer] stopping...")
    finally:
        producer.close()


if __name__ == "__main__":
    main(num_messages=0, interval_seconds=1.0)