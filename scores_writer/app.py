import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("scores_writer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")

PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "fraud_db"),
    "user": os.getenv("POSTGRES_USER", "fraud"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud"),
}

INSERT_SQL = (
    "INSERT INTO scores (transaction_id, score, fraud_flag) "
    "VALUES (%s, %s, %s) ON CONFLICT (transaction_id) DO NOTHING"
)


def connect():
    """Подключение к Postgres с повторами, пока база не поднимется."""
    while True:
        try:
            conn = psycopg2.connect(**PG_CONFIG)
            conn.autocommit = True
            log.info("Connected to Postgres")
            return conn
        except psycopg2.Error as e:
            log.warning("Postgres is not ready: %s", e)
            time.sleep(2)


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "scores-writer",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([SCORING_TOPIC])
    conn = connect()
    log.info("Scores writer started, topic: %s", SCORING_TOPIC)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            log.error("Kafka error: %s", msg.error())
            continue

        # fraud_detector пишет список с одним объектом: [{"score":..,"fraud_flag":..,"transaction_id":..}]
        try:
            payload = json.loads(msg.value().decode("utf-8"))
            records = payload if isinstance(payload, list) else [payload]
            rows = [
                (str(r["transaction_id"]), float(r["score"]), int(r["fraud_flag"]))
                for r in records
            ]
        except Exception as e:
            log.error("Bad message skipped: %s", e)
            continue

        while True:
            try:
                with conn.cursor() as cur:
                    cur.executemany(INSERT_SQL, rows)
                break
            except psycopg2.Error as e:
                log.error("DB error, reconnecting: %s", e)
                time.sleep(2)
                conn = connect()


if __name__ == "__main__":
    main()
