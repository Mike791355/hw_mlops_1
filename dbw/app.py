import os
import sys
import json
import time
import logging

import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("dbw")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "db-writer")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "fraud")
PG_USER = os.getenv("POSTGRES_USER", "fraud")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fraud")
PG_TABLE = os.getenv("POSTGRES_TABLE", "scores")

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {PG_TABLE} (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id TEXT             NOT NULL,
    score          DOUBLE PRECISION NOT NULL,
    fraud_flag     INTEGER          NOT NULL,
    created_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);
"""


def connect_postgres():
    while True:
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD,
            )
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as e:
            logger.warning("PostgreSQL: %s", e)
            time.sleep(3)


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)


def insert_score(conn, record):
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {PG_TABLE} (transaction_id, score, fraud_flag) VALUES (%s, %s, %s)",
            (str(record["transaction_id"]), float(record["score"]), int(record["fraud_flag"])),
        )


def parse_message(raw):
    data = json.loads(raw)
    if isinstance(data, list):
        return data
    return [data]


def main():
    conn = connect_postgres()
    ensure_table(conn)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([SCORES_TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka: %s", msg.error())
                continue
            try:
                for record in parse_message(msg.value().decode("utf-8")):
                    insert_score(conn, record)
            except Exception as e:
                logger.error("Message: %s", e)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
