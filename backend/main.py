import logging
import os
import secrets
import threading
import time

import psycopg
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from psycopg.rows import dict_row
from pydantic import BaseModel

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://keystack:keystack123@db:5432/keystack")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda:9092")
TOPIC = "key-events"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("keystack")

app = FastAPI(title="Keystack")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

producer = Producer({"bootstrap.servers": KAFKA_BROKERS})


def db():
    return psycopg.connect(DATABASE_URL, autocommit=True, row_factory=dict_row)


SCHEMA = """
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    created_at TIMESTAMP DEFAULT now()
);
CREATE TABLE IF NOT EXISTS api_keys (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    secret TEXT NOT NULL,
    prefix TEXT,
    label TEXT,
    description TEXT,
    status TEXT DEFAULT 'active',
    created_at TIMESTAMP DEFAULT now()
);
CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    message TEXT,
    created_at TIMESTAMP DEFAULT now()
);
"""


def publish(message: str):
    producer.produce(TOPIC, message.encode())
    producer.flush()


def audit_consumer():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": "audit",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        with db() as conn:
            conn.execute("INSERT INTO audit_log (message) VALUES (%s)", (msg.value().decode(),))


@app.on_event("startup")
def startup():
    for _ in range(30):
        try:
            with db() as conn:
                conn.execute(SCHEMA)
            break
        except psycopg.OperationalError:
            log.info("waiting for database...")
            time.sleep(2)
    try:
        AdminClient({"bootstrap.servers": KAFKA_BROKERS}).create_topics(
            [NewTopic(TOPIC, num_partitions=1, replication_factor=1)]
        )
    except Exception as e:
        log.warning("topic: %s", e)
    threading.Thread(target=audit_consumer, daemon=True).start()


class CustomerIn(BaseModel):
    name: str
    email: str | None = None


class KeyIn(BaseModel):
    label: str | None = None
    description: str | None = None


class KeyUpdate(BaseModel):
    label: str | None = None
    description: str | None = None
    secret: str | None = None


@app.post("/create_customer")
def create_customer(body: CustomerIn):
    with db() as conn:
        row = conn.execute(
            "INSERT INTO customers (name, email) VALUES (%s, %s) RETURNING *",
            (body.name, body.email),
        ).fetchone()
    return row


@app.get("/customers")
def list_customers():
    with db() as conn:
        return conn.execute("SELECT * FROM customers ORDER BY id").fetchall()


@app.get("/customers/{customer_id}")
def get_customer(customer_id: int):
    with db() as conn:
        row = conn.execute("SELECT * FROM customers WHERE id = %s", (customer_id,)).fetchone()
    if not row:
        return {"error": "customer not found"}
    return row


@app.put("/customers/{customer_id}")
def update_customer(customer_id: int, body: CustomerIn):
    with db() as conn:
        row = conn.execute(
            "UPDATE customers SET name = %s, email = %s WHERE id = %s RETURNING *",
            (body.name, body.email, customer_id),
        ).fetchone()
    if not row:
        raise HTTPException(404, "not found")
    return row


@app.post("/customers/{customer_id}/keys")
def create_key(customer_id: int, body: KeyIn):
    secret = "sk-" + secrets.token_hex(16)
    with db() as conn:
        row = conn.execute(
            "INSERT INTO api_keys (customer_id, secret, prefix, label, description) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (customer_id, secret, secret[:7], body.label, body.description),
        ).fetchone()
    log.info(f"created key {secret} for customer {customer_id}")
    publish(f"key created: {secret} customer={customer_id}")
    return {"id": row["id"], "secret": secret, "prefix": secret[:7]}


@app.get("/customers/{customer_id}/keys")
def list_keys(customer_id: int):
    with db() as conn:
        return conn.execute(
            "SELECT * FROM api_keys WHERE customer_id = %s ORDER BY id", (customer_id,)
        ).fetchall()


@app.patch("/keys/{key_id}")
def update_key(key_id: int, body: KeyUpdate):
    with db() as conn:
        row = conn.execute(
            "UPDATE api_keys SET label = COALESCE(%s, label), "
            "description = COALESCE(%s, description), secret = COALESCE(%s, secret) "
            "WHERE id = %s RETURNING id, prefix, label, description, status",
            (body.label, body.description, body.secret, key_id),
        ).fetchone()
    return row


@app.post("/keys/{key_id}/revoke")
def revoke_key(key_id: int):
    with db() as conn:
        conn.execute("UPDATE api_keys SET status = 'revoked' WHERE id = %s", (key_id,))
    publish(f"key revoked: {key_id}")
    return {"ok": True}


@app.post("/v1/inference")
def inference(authorization: str = Header(default="")):
    token = authorization.removeprefix("Bearer ").strip()
    with db() as conn:
        key = conn.execute(
            "SELECT * FROM api_keys WHERE secret = %s AND status = 'active'", (token,)
        ).fetchone()
    if not key:
        raise HTTPException(401, "invalid key")
    return {"model": "mock-1", "output": "Hello from the mock model!", "tokens": 42}
