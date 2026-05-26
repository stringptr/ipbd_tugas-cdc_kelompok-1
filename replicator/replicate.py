import json
import os
import time

import psycopg2
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cdc.public.products")

TARGET_HOST = os.getenv("TARGET_HOST")
TARGET_PORT = int(os.getenv("TARGET_PORT", "5432"))
TARGET_DB = os.getenv("TARGET_DB", "appdb")
TARGET_USER = os.getenv("TARGET_USER", "postgres")
TARGET_PASSWORD = os.getenv("TARGET_PASSWORD", "postgres")


def connect_target():
    while True:
        try:
            conn = psycopg2.connect(
                host=TARGET_HOST,
                port=TARGET_PORT,
                dbname=TARGET_DB,
                user=TARGET_USER,
                password=TARGET_PASSWORD,
            )
            conn.autocommit = True
            print(f"Connected to target PostgreSQL B at {TARGET_HOST}:{TARGET_PORT}")
            return conn
        except Exception as e:
            print(f"Waiting for target PostgreSQL B... {e}")
            time.sleep(3)


def apply_event(conn, event):
    if event is None:
        return

    payload = event.get("payload", event)
    op = payload.get("op")

    before = payload.get("before")
    after = payload.get("after")

    with conn.cursor() as cur:
        if op in ("c", "r", "u") and after:
            cur.execute(
                """
                INSERT INTO products (id, name, stock, price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    stock = EXCLUDED.stock,
                    price = EXCLUDED.price;
                """,
                (
                    after["id"],
                    after["name"],
                    after["stock"],
                    after["price"],
                ),
            )
            print(f"UPSERT product id={after['id']} op={op}")

        elif op == "d" and before:
            cur.execute(
                "DELETE FROM products WHERE id = %s;",
                (before["id"],),
            )
            print(f"DELETE product id={before['id']}")


def main():
    conn = connect_target()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="pg-b-replicator",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else None,
    )

    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")

    for message in consumer:
        try:
            apply_event(conn, message.value)
        except Exception as e:
            print(f"Error applying event: {e}")
            conn = connect_target()


if __name__ == "__main__":
    main()
