"""
Builds Kafka client + Schema Registry config from environment variables,
supporting three modes:
  - local PLAINTEXT (no security env vars set — local Docker Kafka)
  - SSL client-certificate auth (KAFKA_SECURITY_PROTOCOL=SSL)
  - SASL_SSL username/password auth (KAFKA_SECURITY_PROTOCOL=SASL_SSL)
"""

import os

KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL")  # "SSL" or "SASL_SSL"
KAFKA_SSL_CA_LOCATION = os.getenv("KAFKA_SSL_CA_LOCATION")
KAFKA_SSL_CERTFILE = os.getenv("KAFKA_SSL_CERTFILE")
KAFKA_SSL_KEYFILE = os.getenv("KAFKA_SSL_KEYFILE")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM")  # e.g. "SCRAM-SHA-256"
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD")

SCHEMA_REGISTRY_USER = os.getenv("SCHEMA_REGISTRY_USER")
SCHEMA_REGISTRY_PASSWORD = os.getenv("SCHEMA_REGISTRY_PASSWORD")


def apply_kafka_security(conf: dict) -> dict:
    """Mutates and returns a confluent_kafka client config dict with
    whichever security settings are configured via env vars."""
    if not KAFKA_SECURITY_PROTOCOL:
        return conf  # local PLAINTEXT, nothing to add

    conf["security.protocol"] = KAFKA_SECURITY_PROTOCOL

    if KAFKA_SECURITY_PROTOCOL == "SASL_SSL":
        conf["sasl.mechanism"] = KAFKA_SASL_MECHANISM
        conf["sasl.username"] = KAFKA_SASL_USERNAME
        conf["sasl.password"] = KAFKA_SASL_PASSWORD
        if KAFKA_SSL_CA_LOCATION:
            conf["ssl.ca.location"] = KAFKA_SSL_CA_LOCATION
    else:  # plain "SSL" client-certificate auth
        conf["ssl.ca.location"] = KAFKA_SSL_CA_LOCATION
        conf["ssl.certificate.location"] = KAFKA_SSL_CERTFILE
        conf["ssl.key.location"] = KAFKA_SSL_KEYFILE

    return conf


def schema_registry_conf() -> dict:
    conf = {"url": os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")}
    if SCHEMA_REGISTRY_USER and SCHEMA_REGISTRY_PASSWORD:
        conf["basic.auth.user.info"] = f"{SCHEMA_REGISTRY_USER}:{SCHEMA_REGISTRY_PASSWORD}"
    return conf
