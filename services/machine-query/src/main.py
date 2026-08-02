import os
import json
import time
import threading
import uuid
from typing import List, Optional
from datetime import datetime

import pika
import redis
from fastapi import FastAPI, Query, Depends
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# --- DB Setup ---
POSTGRES_USER = os.getenv("POSTGRES_USER", "uretos")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "uretos_dev_pass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "machine-query-db")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "machine_query_db")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Redis Setup ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)


# --- Read Model Entity ---
class MachineProjection(Base):
    __tablename__ = "machine_projections"

    id = Column(UUID(as_uuid=True), primary_key=True)
    name = Column(String, nullable=False)
    serial_number = Column(String, nullable=False, unique=True)
    machine_type_id = Column(UUID(as_uuid=True), nullable=False)
    manufacturer_id = Column(UUID(as_uuid=True), nullable=True)
    status_id = Column(UUID(as_uuid=True), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


def init_db_with_retry(max_retries=15, delay=2):
    for i in range(max_retries):
        try:
            Base.metadata.create_all(bind=engine)
            print("[machine-query] DB initialized successfully.", flush=True)
            return
        except Exception as e:
            print(f"[machine-query] Waiting for DB ({i+1}/{max_retries}): {e}", flush=True)
            time.sleep(delay)
    raise RuntimeError("[machine-query] Could not connect to machine-query-db.")


# --- RabbitMQ Event Consumer (Machine + LOV Events) ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

EXCHANGE_EVENTS = "uretos_events"
QUEUE_EVENTS = "machine_query_all_events"

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

            channel.exchange_declare(exchange=EXCHANGE_EVENTS, exchange_type="topic", durable=True)
            channel.queue_declare(queue=QUEUE_EVENTS, durable=True)
            
            # Bind to Machine and LOV Events
            channel.queue_bind(exchange=EXCHANGE_EVENTS, queue=QUEUE_EVENTS, routing_key="uretos.machine.event.#")
            channel.queue_bind(exchange=EXCHANGE_EVENTS, queue=QUEUE_EVENTS, routing_key="uretos.lov.event.#")

            print("[machine-query] Connected to RabbitMQ. Listening for Machine and LOV events...", flush=True)

            def process_event(ch, method, properties, body):
                try:
                    payload = json.loads(body)
                    routing_key = method.routing_key

                    # CASE 1: MACHINE EVENT -> Write to Postgres
                    if routing_key.startswith("uretos.machine.event."):
                        data = payload.get("data", payload)
                        m_id_str = data.get("id") or data.get("machine_id") or data.get("machineId")
                        if m_id_str:
                            db: Session = SessionLocal()
                            try:
                                m_id = uuid.UUID(m_id_str)
                                item = db.query(MachineProjection).filter_by(id=m_id).first()
                                
                                # Robust extraction supporting both snake_case and camelCase
                                raw_type_id = data.get("machine_type_id") or data.get("machineTypeId")
                                raw_manuf_id = (
                                    data.get("manufacturer_id") 
                                    or data.get("manufacturerId") 
                                    or data.get("manufacturer")
                                )
                                raw_status_id = data.get("status_id") or data.get("statusId")

                                m_type_id = uuid.UUID(raw_type_id) if raw_type_id else None
                                m_manuf_id = uuid.UUID(raw_manuf_id) if raw_manuf_id else None
                                m_status_id = uuid.UUID(raw_status_id) if raw_status_id else None

                                m_name = data.get("name")
                                m_serial = data.get("serial_number") or data.get("serialNumber")

                                if not item:
                                    item = MachineProjection(
                                        id=m_id,
                                        name=m_name,
                                        serial_number=m_serial,
                                        machine_type_id=m_type_id,
                                        manufacturer_id=m_manuf_id,
                                        status_id=m_status_id
                                    )
                                    db.add(item)
                                else:
                                    item.name = m_name if m_name else item.name
                                    item.serial_number = m_serial if m_serial else item.serial_number
                                    item.machine_type_id = m_type_id if m_type_id else item.machine_type_id
                                    item.manufacturer_id = m_manuf_id if m_manuf_id is not None else item.manufacturer_id
                                    item.status_id = m_status_id if m_status_id is not None else item.status_id

                                db.commit()
                                print(f"[machine-query] Projected Machine to DB: {m_id} (Manuf: {m_manuf_id})", flush=True)
                            except Exception as ex:
                                db.rollback()
                                print(f"[machine-query] Machine projection DB error: {ex}", flush=True)
                            finally:
                                db.close()

                    # CASE 2: LOV EVENT -> Write to Redis Cache
                    elif routing_key.startswith("uretos.lov.event."):
                        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                        
                        lov_id = data.get("lov_id") or data.get("id") or data.get("event_id") or data.get("code")
                        translations = data.get("translations", {})

                        if lov_id and translations:
                            for lang, text_val in translations.items():
                                redis_key = f"lov:{lov_id}:{lang}"
                                redis_client.set(redis_key, text_val)
                            print(f"[machine-query] Cached LOV translations in Redis for ID {lov_id}: {translations}", flush=True)
                        else:
                            print(f"[machine-query] LOV event received but missing lov_id or translations: {payload}", flush=True)

                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    print(f"[machine-query] Event processing failed: {e}", flush=True)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue=QUEUE_EVENTS, on_message_callback=process_event)
            channel.start_consuming()

        except Exception as e:
            print(f"[machine-query] RabbitMQ error: {e}. Retrying in 5s...", flush=True)
            time.sleep(5)


# --- DTOs & FastAPI ---
class MachineEnrichedDTO(BaseModel):
    id: str
    name: str
    serial_number: str
    machine_type_id: str
    machine_type_name: Optional[str] = "N/A"
    manufacturer_id: Optional[str] = None
    manufacturer_name: Optional[str] = None
    status_id: Optional[str] = None
    status_name: Optional[str] = None

app = FastAPI(title="uRetOS Machine Query Service", version="0.1.0")

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
    return {"status": "ok", "service": "machine-query"}

@app.get("/api/v1/machines", response_model=List[MachineEnrichedDTO])
def get_machines(lang: str = Query("de-DE", description="Language code"), db: Session = Depends(get_db)):
    machines = db.query(MachineProjection).all()
    result = []

    for m in machines:
        type_str = str(m.machine_type_id) if m.machine_type_id else None
        manuf_str = str(m.manufacturer_id) if m.manufacturer_id else None
        status_str = str(m.status_id) if m.status_id else None

        # Fetch resolved names from Redis with fallbacks (:lang -> :int -> Raw ID)
        type_name = (
            redis_client.get(f"lov:{type_str}:{lang}") 
            or redis_client.get(f"lov:{type_str}:int") 
            or type_str 
            if type_str else "N/A"
        )
        
        manuf_name = (
            redis_client.get(f"lov:{manuf_str}:{lang}") 
            or redis_client.get(f"lov:{manuf_str}:int") 
            or manuf_str 
            if manuf_str else None
        )

        status_name = (
            redis_client.get(f"lov:{status_str}:{lang}") 
            or redis_client.get(f"lov:{status_str}:int") 
            or status_str 
            if status_str else None
        )

        result.append(
            MachineEnrichedDTO(
                id=str(m.id),
                name=m.name,
                serial_number=m.serial_number,
                machine_type_id=type_str,
                machine_type_name=type_name,
                manufacturer_id=manuf_str,
                manufacturer_name=manuf_name,
                status_id=status_str,
                status_name=status_name
            )
        )

    return result