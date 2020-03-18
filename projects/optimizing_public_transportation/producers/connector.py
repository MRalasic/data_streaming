"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests
from datetime import datetime


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + " Creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + " Connector already created skipping recreation")
        return

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": "500",
                "connection.url": "jdbc:postgresql://localhost:5432/cta",
                "connection.user": "cta_admin",
                "connection.password": "chicago",
                "table.whitelist": "stations",
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": "org.chicago.cta.",
                "poll.interval.ms": "10000",
            }
        }),
    )

    resp.raise_for_status()
    logging.debug(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + " Connector created successfully")


if __name__ == "__main__":
    configure_connector()
