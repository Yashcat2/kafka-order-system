import os
import io
import random
import time
from collections import defaultdict
from datetime import datetime, timezone

from dotenv import load_dotenv
from fastavro import schemaless_writer, schemaless_reader, parse_schema
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM")
SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME")
SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD")
SSL_CAFILE = os.getenv("KAFKA_SSL_CA_LOCATION")

ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.getenv("ORDERS_DLQ_TOPIC", "orders-dlq")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "order-consumer-group")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_BACKOFF_SECONDS = float(os.getenv("BASE_BACKOFF_SECONDS", "1.0"))
SIMULATED_FAILURE_RATE = float(os.getenv("SIMULATED_FAILURE_RATE", "0.3"))

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

ORDER_DLQ_SCHEMA = parse_schema({
    "namespace": "com.ruhuna.orders",
    "type": "record",
    "name": "OrderDLQ",
    "fields": [
        {"name": "orderId", "type": "string"},
        {"name": "product", "type": "string"},
        {"name": "price", "type": "float"},
        {"name": "errorReason", "type": "string"},
        {"name": "retryCount", "type": "int"},
        {"name": "failedAt", "type": "string"},
    ],
})


class TransientError(Exception):
    pass


def serialize(record: dict, schema) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, schema, record)
    return buf.getvalue()


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


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        ORDERS_TOPIC,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: deserialize(v, ORDER_SCHEMA),
        **_security_kwargs(),
    )


def build_dlq_producer() -> KafkaProducer:
    return KafkaProducer(
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: serialize(v, ORDER_DLQ_SCHEMA),
        **_security_kwargs(),
    )


class RunningAggregator:
    def __init__(self):
        self.overall_count = 0
        self.overall_sum = 0.0
        self.per_product_count = defaultdict(int)
        self.per_product_sum = defaultdict(float)

    def add(self, product: str, price: float):
        self.overall_count += 1
        self.overall_sum += price
        self.per_product_count[product] += 1
        self.per_product_sum[product] += price

    def overall_average(self) -> float:
        return self.overall_sum / self.overall_count if self.overall_count else 0.0

    def product_average(self, product: str) -> float:
        c = self.per_product_count[product]
        return self.per_product_sum[product] / c if c else 0.0

    def print_snapshot(self, order: dict):
        print(
            f"[aggregate] orderId={order['orderId']} ({order['product']} @ {order['price']:.2f}) -> "
            f"overall_avg={self.overall_average():.2f} (n={self.overall_count}) | "
            f"{order['product']}_avg={self.product_average(order['product']):.2f} "
            f"(n={self.per_product_count[order['product']]})"
        )


def process_order(order: dict):
    if random.random() < SIMULATED_FAILURE_RATE:
        raise TransientError(f"simulated transient failure for {order['orderId']}")


def handle_with_retry(order: dict, dlq_producer: KafkaProducer) -> bool:
    attempt = 0
    last_error = None
    while attempt <= MAX_RETRIES:
        try:
            process_order(order)
            print(f"[consumer] processed orderId={order['orderId']} successfully")
            return True
        except TransientError as e:
            last_error = e
            attempt += 1
            if attempt > MAX_RETRIES:
                break
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            print(f"[consumer] attempt {attempt}/{MAX_RETRIES} failed for "
                  f"orderId={order['orderId']}: {e}. Retrying in {backoff:.1f}s...")
            time.sleep(backoff)

    dlq_message = {
        **order,
        "errorReason": str(last_error),
        "retryCount": attempt,
        "failedAt": datetime.now(timezone.utc).isoformat(),
    }
    dlq_producer.send(DLQ_TOPIC, key=order["orderId"], value=dlq_message)
    dlq_producer.flush()
    print(f"[dlq] sent orderId={order['orderId']} to DLQ after {attempt} attempts")
    return True


def main():
    consumer = build_consumer()
    dlq_producer = build_dlq_producer()
    aggregator = RunningAggregator()
    print(f"[consumer] subscribed to '{ORDERS_TOPIC}', DLQ='{DLQ_TOPIC}'")

    try:
        for msg in consumer:
            order = msg.value
            if handle_with_retry(order, dlq_producer):
                aggregator.add(order["product"], order["price"])
                aggregator.print_snapshot(order)
                consumer.commit()
    except KeyboardInterrupt:
        print("\n[consumer] stopping...")
    finally:
        dlq_producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()