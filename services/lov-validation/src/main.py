import os
import json
import time
import threading
import uuid
from datetime import datetime

import pika
from fastapi import FastAPI
from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# --- Database & Config ---
POSTGRES_USER = os.getenv("POSTGRES_USER", "uretos")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "uretos_dev_pass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "lov-validation-db")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "lov_validation_db")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class ValidLovProjection(Base):
    __tablename__ = "valid_lov_items"

    id = Column(UUID(as_uuid=True), primary_key=True)
    category = Column(String, index=True, nullable=False)
    code = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

def init_db_with_retry(max_retries=15, delay=2):
    for i in range(max_retries):
        try:
            Base.metadata.create_all(bind=engine)
            print("[lov-validation] Database tables initialized successfully.", flush=True)
            return
        except Exception as e:
            print(f"[lov-validation] Waiting for DB connection ({i+1}/{max_retries}): {e}", flush=True)
            time.sleep(delay)
    raise RuntimeError("[lov-validation] Could not establish connection to DB.")

# --- RabbitMQ Setup ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

EXCHANGE_EVENTS = "uretos_events"
EXCHANGE_RPC = "uretos_rpc"
QUEUE_EVENTS = "lov_validation_events"
QUEUE_RPC = "lov_validation_rpc_requests"

def start_validation_worker():
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
            channel.exchange_declare(exchange=EXCHANGE_RPC, exchange_type="direct", durable=True)
            
            channel.queue_declare(queue=QUEUE_EVENTS, durable=True)
            channel.queue_declare(queue=QUEUE_RPC, durable=True)

            channel.queue_bind(exchange=EXCHANGE_EVENTS, queue=QUEUE_EVENTS, routing_key="uretos.lov.event.#")
            channel.queue_bind(exchange=EXCHANGE_RPC, queue=QUEUE_RPC, routing_key="lov.validate")

            print("[lov-validation] Connected to RabbitMQ. Ready for synchronization and validation RPC...", flush=True)

            def handle_message(ch, method, properties, body):
                try:
                    message = json.loads(body)
                    routing_key = method.routing_key

                    # --- 1. RPC VALIDATION REQUEST ---
                    if routing_key == "lov.validate":
                        data = message.get("data", {})
                        requested_ids = data.get("ids", [])
                        invalid_ids = []

                        if requested_ids:
                            db: Session = SessionLocal()
                            try:
                                uuid_list = [uuid.UUID(uid) for uid in requested_ids]
                                existing = db.query(ValidLovProjection.id).filter(ValidLovProjection.id.in_(uuid_list)).all()
                                existing_ids = {str(item.id) for item in existing}
                                invalid_ids = [uid for uid in requested_ids if uid not in existing_ids]
                            except Exception as ex:
                                print(f"[lov-validation] DB validation error: {ex}", flush=True)
                            finally:
                                db.close()

                        response_data = {
                            "valid": len(invalid_ids) == 0,
                            "invalid_ids": invalid_ids
                        }

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

                    # --- 2. EVENT SYNC (lov.created) ---
                    data = message.get("data", message)
                    lov_id = data.get("lov_id") or data.get("id")
                    category = data.get("category")
                    code = data.get("code")

                    if lov_id and category and code:
                        db: Session = SessionLocal()
                        try:
                            item_uuid = uuid.UUID(lov_id)
                            item = db.query(ValidLovProjection).filter_by(id=item_uuid).first()
                            if not item:
                                item = ValidLovProjection(
                                    id=item_uuid,
                                    category=category,
                                    code=code
                                )
                                db.add(item)
                                db.commit()
                                print(f"[lov-validation] Synced valid LOV ID: {lov_id}", flush=True)
                        except Exception as ex:
                            db.rollback()
                            print(f"[lov-validation] Sync DB error: {ex}", flush=True)
                        finally:
                            db.close()

                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    print(f"[lov-validation] Message processing failed: {e}", flush=True)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue=QUEUE_EVENTS, on_message_callback=handle_message)
            channel.basic_consume(queue=QUEUE_RPC, on_message_callback=handle_message)
            channel.start_consuming()

        except Exception as e:
            print(f"[lov-validation] RabbitMQ error: {e}. Retrying in 5s...", flush=True)
            time.sleep(5)

# --- FastAPI Health-Check Service ---
app = FastAPI(title="uRetOS LOV Validation Service", version="0.1.0")

@app.on_event("startup")
def startup_event():
    init_db_with_retry()
    worker_thread = threading.Thread(target=start_validation_worker, daemon=True)
    worker_thread.start()

@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "lov-validation"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)