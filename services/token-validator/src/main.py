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
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "token-validator-db")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "token_validator_db")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class ValidTokenProjection(Base):
    __tablename__ = "valid_tokens"

    id = Column(UUID(as_uuid=True), primary_key=True)
    client_id = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

def init_db_with_retry(max_retries=15, delay=2):
    for i in range(max_retries):
        try:
            Base.metadata.create_all(bind=engine)
            print("[token-validator] Database tables initialized successfully.", flush=True)
            return
        except Exception as e:
            print(f"[token-validator] Waiting for DB connection ({i+1}/{max_retries}): {e}", flush=True)
            time.sleep(delay)
    raise RuntimeError("[token-validator] Could not establish connection to DB.")

# --- RabbitMQ Setup ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

EXCHANGE_EVENTS = "uretos_events"
EXCHANGE_RPC = "uretos_rpc"
QUEUE_EVENTS = "token_validation_events"
QUEUE_RPC = "token_validation_rpc_requests"

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

            channel.queue_bind(exchange=EXCHANGE_EVENTS, queue=QUEUE_EVENTS, routing_key="uretos.token.event.#")
            channel.queue_bind(exchange=EXCHANGE_RPC, queue=QUEUE_RPC, routing_key="token.validate")

            print("[token-validator] Connected to RabbitMQ. Ready for token management and validation RPC...", flush=True)

            def handle_message(ch, method, properties, body):
                try:
                    message = json.loads(body)
                    routing_key = method.routing_key

                    # --- 1. RPC VALIDATION REQUEST ---
                    if routing_key == "token.validate":
                        data = message.get("data", {})
                        token_id = data.get("token_id")
                        is_valid = False
                        client_id = None

                        if token_id:
                            db: Session = SessionLocal()
                            try:
                                token_uuid = uuid.UUID(token_id)
                                item = db.query(ValidTokenProjection).filter_by(id=token_uuid).first()
                                if item:
                                    is_valid = True
                                    client_id = item.client_id
                            except Exception as ex:
                                print(f"[token-validator] DB validation error: {ex}", flush=True)
                            finally:
                                db.close()

                        response_data = {
                            "valid": is_valid,
                            "token_id": token_id,
                            "client_id": client_id
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

                    # --- 2. EVENT SYNC (token.created / generated) ---
                    data = message.get("data", message)
                    token_id = data.get("token_id") or data.get("id")
                    client_id = data.get("client_id", "unknown")

                    if token_id:
                        db: Session = SessionLocal()
                        try:
                            item_uuid = uuid.UUID(token_id)
                            item = db.query(ValidTokenProjection).filter_by(id=item_uuid).first()
                            if not item:
                                item = ValidTokenProjection(
                                    id=item_uuid,
                                    client_id=client_id
                                )
                                db.add(item)
                                db.commit()
                                print(f"[token-validator] Synced new token ID: {token_id}", flush=True)
                        except Exception as ex:
                            db.rollback()
                            print(f"[token-validator] Sync DB error: {ex}", flush=True)
                        finally:
                            db.close()

                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    print(f"[token-validator] Message processing failed: {e}", flush=True)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue=QUEUE_EVENTS, on_message_callback=handle_message)
            channel.basic_consume(queue=QUEUE_RPC, on_message_callback=handle_message)
            channel.start_consuming()

        except Exception as e:
            print(f"[token-validator] RabbitMQ error: {e}. Retrying in 5s...", flush=True)
            time.sleep(5)

# --- FastAPI Health-Check Service ---
app = FastAPI(title="uRetOS Token Validation Service", version="0.1.0")

@app.on_event("startup")
def startup_event():
    init_db_with_retry()
    worker_thread = threading.Thread(target=start_validation_worker, daemon=True)
    worker_thread.start()

@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "token-validator"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)