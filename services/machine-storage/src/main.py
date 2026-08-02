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

# --- Config & DB Setup ---
POSTGRES_USER = os.getenv("POSTGRES_USER", "uretos")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "uretos_dev_pass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "machine-db")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "machine_db")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# --- PostgreSQL Domain Entity ---
class Machine(Base):
    __tablename__ = "machines"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    serial_number = Column(String, nullable=False, unique=True)
    
    # LOV References saved strictly as UUIDs
    machine_type_id = Column(UUID(as_uuid=True), nullable=False)
    manufacturer_id = Column(UUID(as_uuid=True), nullable=True)
    status_id = Column(UUID(as_uuid=True), nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)


def init_db_with_retry(max_retries=15, delay=2):
    for i in range(max_retries):
        try:
            Base.metadata.create_all(bind=engine)
            print("[machine-storage] DB tables initialized successfully.", flush=True)
            return
        except Exception as e:
            print(f"[machine-storage] Waiting for DB connection ({i+1}/{max_retries}): {e}", flush=True)
            time.sleep(delay)
    raise RuntimeError("[machine-storage] Could not connect to machine-db.")


# --- RabbitMQ Setup & Consumer ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

EXCHANGE_COMMANDS = "uretos_commands"
EXCHANGE_EVENTS = "uretos_events"
QUEUE_COMMANDS = "machine_storage_commands"

def start_command_consumer():
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

            # Exchanges
            channel.exchange_declare(exchange=EXCHANGE_COMMANDS, exchange_type="topic", durable=True)
            channel.exchange_declare(exchange=EXCHANGE_EVENTS, exchange_type="topic", durable=True)

            # Command Queue Bindings
            channel.queue_declare(queue=QUEUE_COMMANDS, durable=True)
            channel.queue_bind(
                exchange=EXCHANGE_COMMANDS, 
                queue=QUEUE_COMMANDS, 
                routing_key="uretos.machine.command.#"
            )

            print("[machine-storage] Connected to RabbitMQ. Waiting for machine commands...", flush=True)

            def process_command(ch, method, properties, body):
                try:
                    payload = json.loads(body)
                    data = payload.get("data", {})
                    
                    name = data.get("name")
                    serial_number = data.get("serial_number")
                    machine_type_id_str = data.get("machine_type_id")
                    manufacturer_id_str = data.get("manufacturer_id")
                    status_id_str = data.get("status_id")

                    if name and serial_number and machine_type_id_str:
                        db: Session = SessionLocal()
                        try:
                            # Parse UUIDs
                            m_type_uuid = uuid.UUID(machine_type_id_str)
                            m_manufacturer_uuid = uuid.UUID(manufacturer_id_str) if manufacturer_id_str else None
                            m_status_uuid = uuid.UUID(status_id_str) if status_id_str else None

                            machine = Machine(
                                id=uuid.uuid4(),
                                name=name,
                                serial_number=serial_number,
                                machine_type_id=m_type_uuid,
                                manufacturer_id=m_manufacturer_uuid,
                                status_id=m_status_uuid
                            )
                            db.add(machine)
                            db.commit()
                            db.refresh(machine)

                            print(f"[machine-storage] Persisted Machine: {machine.id} (Type LOV UUID: {machine.machine_type_id}, Manufacturer LOV UUID: {machine.manufacturer_id})", flush=True)

                            # Publish Domain Event
                            event_payload = {
                                "event_id": str(uuid.uuid4()),
                                "event_type": "MACHINE_CREATED",
                                "timestamp": time.time(),
                                "data": {
                                    "id": str(machine.id),
                                    "name": machine.name,
                                    "serial_number": machine.serial_number,
                                    "machine_type_id": str(machine.machine_type_id),
                                    "manufacturer_id": str(machine.manufacturer_id) if machine.manufacturer_id else None,
                                    "status_id": str(machine.status_id) if machine.status_id else None
                                }
                            }

                            channel.basic_publish(
                                exchange=EXCHANGE_EVENTS,
                                routing_key="uretos.machine.event.created",
                                body=json.dumps(event_payload),
                                properties=pika.BasicProperties(
                                    content_type="application/json",
                                    delivery_mode=2
                                )
                            )
                            print(f"[machine-storage] Published Event 'uretos.machine.event.created' for ID {machine.id}", flush=True)

                        except Exception as ex:
                            db.rollback()
                            print(f"[machine-storage] DB Error while saving machine: {ex}", flush=True)
                        finally:
                            db.close()

                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    print(f"[machine-storage] Failed to process message: {e}", flush=True)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue=QUEUE_COMMANDS, on_message_callback=process_command)
            channel.start_consuming()

        except Exception as e:
            print(f"[machine-storage] RabbitMQ error: {e}. Retrying in 5s...", flush=True)
            time.sleep(5)


# --- FastAPI App ---
app = FastAPI(title="uRetOS Machine Storage Service", version="0.1.0")

@app.on_event("startup")
def startup_event():
    init_db_with_retry()
    consumer_thread = threading.Thread(target=start_command_consumer, daemon=True)
    consumer_thread.start()

@app.get("/healthy")
def healthy():
    return {"status": "ok", "service": "machine-storage"}