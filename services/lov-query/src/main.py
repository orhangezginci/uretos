import os
import json
import time
import threading
import uuid
from typing import Dict
from datetime import datetime, timezone

import pika
from fastapi import FastAPI
from sqlalchemy import create_engine, Column, String, DateTime, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# --- Database & Config ---
POSTGRES_USER = os.getenv("POSTGRES_USER", "uretos")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "uretos_dev_pass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "lov-query-db")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "lov_query_db")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- PostgreSQL Entity with UUIDv4 Primary Key ---
class LovItemProjection(Base):
    __tablename__ = "lov_items"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    category = Column(String, index=True, nullable=False)
    code = Column(String, nullable=False)
    translations = Column(JSONB, nullable=False, default={})
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("category", "code", name="uq_category_code"),
    )

def init_db_with_retry(max_retries=15, delay=2):
    for i in range(max_retries):
        try:
            Base.metadata.create_all(bind=engine)
            print("[lov-query] Database tables initialized successfully.", flush=True)
            return
        except Exception as e:
            print(f"[lov-query] Waiting for DB connection ({i+1}/{max_retries}): {e}", flush=True)
            time.sleep(delay)
    raise RuntimeError("[lov-query] Could not establish connection to lov-query-db.")


# --- RabbitMQ Setup ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

EXCHANGE_EVENTS = "uretos_events"
EXCHANGE_COMMANDS = "uretos_commands"
QUEUE_EVENTS = "lov_query_events"
QUEUE_RPC_REQUESTS = "lov_query_rpc_requests"


# --- Background Worker (CloudEvent Consumer + RPC Server) ---
def start_query_worker():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
            channel = connection.channel()
            
            # Exchanges & Queues deklarieren
            channel.exchange_declare(exchange=EXCHANGE_EVENTS, exchange_type="topic", durable=True)
            channel.exchange_declare(exchange=EXCHANGE_COMMANDS, exchange_type="topic", durable=True)
            
            channel.queue_declare(queue=QUEUE_EVENTS, durable=True)
            channel.queue_declare(queue=QUEUE_RPC_REQUESTS, durable=True)

            # Bindings
            channel.queue_bind(exchange=EXCHANGE_EVENTS, queue=QUEUE_EVENTS, routing_key="uretos.lov.event.#")
            channel.queue_bind(exchange=EXCHANGE_EVENTS, queue=QUEUE_EVENTS, routing_key="uretos.lov.#")
            channel.queue_bind(exchange=EXCHANGE_COMMANDS, queue=QUEUE_RPC_REQUESTS, routing_key="uretos.lov.query.#")

            print("[lov-query] Connected to RabbitMQ (CloudEvent-aware). Ready for Events and RPC requests...", flush=True)

            def handle_message(ch, method, properties, body):
                try:
                    cloudevent = json.loads(body)
                    routing_key = method.routing_key
                    
                    msg_type = cloudevent.get("type") or routing_key
                    messagetype = cloudevent.get("messagetype", "event")
                    correlation_id = cloudevent.get("correlationid") or properties.correlation_id
                    data = cloudevent.get("data", cloudevent)

                    # --- 1. RPC REQUEST HANDLING (Query) ---
                    if routing_key.startswith("uretos.lov.query.") or messagetype == "query":
                        category = data.get("category")
                        lang = data.get("lang", "en-US")
                        response_data = []

                        db: Session = SessionLocal()
                        try:
                            if category:
                                items = db.query(LovItemProjection).filter(LovItemProjection.category == category).all()
                                for item in items:
                                    trans: Dict[str, str] = item.translations or {}
                                    resolved_value = (
                                        trans.get(lang) or
                                        trans.get("en-US") or
                                        (next(iter(trans.values())) if trans else item.code)
                                    )
                                    response_data.append({
                                        "id": str(item.id),
                                        "category": item.category,
                                        "code": item.code,
                                        "value": resolved_value
                                    })
                        finally:
                            db.close()

                        # Antwort via RPC an reply_to zurückschicken
                        if properties.reply_to and properties.correlation_id:
                            channel.basic_publish(
                                exchange='',
                                routing_key=properties.reply_to,
                                properties=pika.BasicProperties(
                                    correlation_id=properties.correlation_id,
                                    content_type="application/json"
                                ),
                                body=json.dumps(response_data)
                            )

                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    # --- 2. EVENT HANDLING (Projections via CloudEvent) ---
                    category = data.get("category")
                    code = data.get("code")
                    translations = data.get("translations", {})

                    if category and code:
                        db: Session = SessionLocal()
                        try:
                            item = db.query(LovItemProjection).filter_by(category=category, code=code).first()
                            if not item:
                                item = LovItemProjection(
                                    id=uuid.uuid4(),
                                    category=category,
                                    code=code,
                                    translations=translations
                                )
                                db.add(item)
                            else:
                                item.translations = translations

                            db.commit()
                            print(f"[lov-query] Projected CloudEvent entry to DB: {category}/{code} (CorrID: {correlation_id})", flush=True)
                        except Exception as ex:
                            db.rollback()
                            print(f"[lov-query] DB projection error: {ex}", flush=True)
                        finally:
                            db.close()

                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    print(f"[lov-query] Message processing failed: {e}", flush=True)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue=QUEUE_EVENTS, on_message_callback=handle_message)
            channel.basic_consume(queue=QUEUE_RPC_REQUESTS, on_message_callback=handle_message)
            channel.start_consuming()

        except Exception as e:
            print(f"[lov-query] RabbitMQ error: {e}. Retrying in 5s...", flush=True)
            time.sleep(5)


# --- FastAPI Health-Check Service ---
app = FastAPI(title="uRetOS LOV Query Service", version="0.1.0")

@app.on_event("startup")
def startup_event():
    init_db_with_retry()
    worker_thread = threading.Thread(target=start_query_worker, daemon=True)
    worker_thread.start()

@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "lov-query"}