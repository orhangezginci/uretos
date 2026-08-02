import os
import json
import time
import threading
from typing import List, Optional, Dict
from datetime import datetime

import pika
from fastapi import FastAPI, Query, Depends
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, String, Integer, DateTime, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
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

# --- PostgreSQL JSONB Projection Entity ---
class LovItemProjection(Base):
    __tablename__ = "lov_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
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


# --- Event Consumer ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

def start_event_consumer():
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
            
            channel.exchange_declare(exchange="uretos_events", exchange_type="topic", durable=True)
            channel.queue_declare(queue="lov_query_events", durable=True)
            channel.queue_bind(exchange="uretos_events", queue="lov_query_events", routing_key="uretos.lov.event.#")
            channel.queue_bind(exchange="uretos_events", queue="lov_query_events", routing_key="uretos.lov.#")

            print("[lov-query] Connected to RabbitMQ. Listening for events...", flush=True)

            def process_event(ch, method, properties, body):
                try:
                    payload = json.loads(body)
                    data = payload.get("data", payload)
                    
                    category = data.get("category")
                    code = data.get("code")
                    translations = data.get("translations", {})

                    if category and code:
                        db: Session = SessionLocal()
                        try:
                            item = db.query(LovItemProjection).filter_by(category=category, code=code).first()
                            if not item:
                                item = LovItemProjection(category=category, code=code, translations=translations)
                                db.add(item)
                            else:
                                item.translations = translations

                            db.commit()
                            print(f"[lov-query] Projected LOV entry (JSONB): {category}/{code}", flush=True)
                        except Exception as ex:
                            db.rollback()
                            print(f"[lov-query] DB projection error: {ex}", flush=True)
                        finally:
                            db.close()

                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    print(f"[lov-query] Failed to process message: {e}", flush=True)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue="lov_query_events", on_message_callback=process_event)
            channel.start_consuming()

        except Exception as e:
            print(f"[lov-query] RabbitMQ error: {e}. Retrying in 5s...", flush=True)
            time.sleep(5)


# --- FastAPI Application ---
class LovResponseDTO(BaseModel):
    code: str
    value: str

app = FastAPI(title="uRetOS LOV Query Service", version="0.1.0")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.on_event("startup")
def startup_event():
    init_db_with_retry()
    consumer_thread = threading.Thread(target=start_event_consumer, daemon=True)
    consumer_thread.start()

@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "lov-query"}

@app.get("/api/v1/lov/{category}", response_model=List[LovResponseDTO])
def get_lov_by_category(
    category: str,
    lang: str = Query(default="en-US", description="Locale key, e.g. de-DE, en-US, tr-TR"),
    db: Session = Depends(get_db)
):
    """
    Fetches LOV entries by category localized by `?lang=`.
    Fallback chain: Requested `lang` -> `en-US` -> First available key -> Raw `code`.
    """
    items = db.query(LovItemProjection).filter(LovItemProjection.category == category).all()

    result = []
    for item in items:
        trans: Dict[str, str] = item.translations or {}
        
        resolved_value = (
            trans.get(lang) or
            trans.get("en-US") or
            (next(iter(trans.values())) if trans else item.code)
        )
        
        result.append(LovResponseDTO(code=item.code, value=resolved_value))

    return result