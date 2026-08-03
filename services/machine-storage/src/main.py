from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import os
import threading
import time
import uuid
import pika
from fastapi import FastAPI
from sqlalchemy import Column, String, DateTime, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# Database Setup
DB_USER = os.getenv("POSTGRES_USER", "uretos")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "uretos_dev_pass")
DB_HOST = os.getenv("POSTGRES_HOST", "machine-db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "machine_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class MachineModel(Base):
    __tablename__ = "machines"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    serial_number = Column(String, nullable=False, unique=True)
    machine_type_id = Column(String, nullable=False)
    manufacturer_id = Column(String, nullable=True)
    status_id = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    deleted_at = Column(DateTime, nullable=True)

Base.metadata.create_all(bind=engine)

# Transport Configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

EXCHANGE_COMMANDS = "uretos_commands"
EXCHANGE_EVENTS = "uretos_events"
QUEUE_COMMANDS = "machine_storage_commands"

def process_message(ch, method, properties, body):
    try:
        cloud_event = json.loads(body)
        routing_key = method.routing_key
        msg_type = cloud_event.get("type") or routing_key
        correlation_id = cloud_event.get("correlationid", str(uuid.uuid4()))
        data = cloud_event.get("data", {})

        db = SessionLocal()
        try:
            if routing_key == "uretos.machine.command.create" or msg_type == "uretos.machine.command.create":
                machine_id = data.get("id", str(uuid.uuid4()))
                machine = MachineModel(
                    id=machine_id,
                    name=data.get("name"),
                    serial_number=data.get("serial_number"),
                    machine_type_id=data.get("machine_type_id"),
                    manufacturer_id=data.get("manufacturer_id"),
                    status_id=data.get("status_id")
                )
                db.add(machine)
                db.commit()
                print(f"[machine-storage] Persisted Machine: {machine_id}", flush=True)

                event_payload = {
                    "specversion": "1.0",
                    "id": str(uuid.uuid4()),
                    "source": "uretos/services/machine-storage",
                    "type": "uretos.machine.event.created",
                    "datacontenttype": "application/json",
                    "time": datetime.now(timezone.utc).isoformat(),
                    "correlationid": correlation_id,
                    "messagetype": "event",
                    "data": {
                        "id": machine_id,
                        "machine_id": machine_id,
                        "name": data.get("name"),
                        "serial_number": data.get("serial_number"),
                        "machine_type_id": data.get("machine_type_id"),
                        "manufacturer_id": data.get("manufacturer_id"),
                        "status_id": data.get("status_id")
                    }
                }

                ch.basic_publish(
                    exchange=EXCHANGE_EVENTS,
                    routing_key="uretos.machine.event.created",
                    body=json.dumps(event_payload),
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        correlation_id=correlation_id,
                        delivery_mode=2
                    )
                )

            elif routing_key == "uretos.machine.command.update" or msg_type == "uretos.machine.command.update":
                machine_id = data.get("id")
                if machine_id:
                    machine = db.query(MachineModel).filter(MachineModel.id == machine_id).first()
                    if machine and not machine.deleted_at:
                        if "name" in data and data["name"] is not None:
                            machine.name = data["name"]
                        if "serial_number" in data and data["serial_number"] is not None:
                            machine.serial_number = data["serial_number"]
                        if "machine_type_id" in data and data["machine_type_id"] is not None:
                            machine.machine_type_id = data["machine_type_id"]
                        if "manufacturer_id" in data and data["manufacturer_id"] is not None:
                            machine.manufacturer_id = data["manufacturer_id"]
                        if "status_id" in data and data["status_id"] is not None:
                            machine.status_id = data["status_id"]

                        db.commit()
                        print(f"[machine-storage] Updated Machine: {machine_id}", flush=True)

                        event_payload = {
                            "specversion": "1.0",
                            "id": str(uuid.uuid4()),
                            "source": "uretos/services/machine-storage",
                            "type": "uretos.machine.event.updated",
                            "datacontenttype": "application/json",
                            "time": datetime.now(timezone.utc).isoformat(),
                            "correlationid": correlation_id,
                            "messagetype": "event",
                            "data": {
                                "id": machine_id,
                                "name": machine.name,
                                "serial_number": machine.serial_number,
                                "machine_type_id": machine.machine_type_id,
                                "manufacturer_id": machine.manufacturer_id,
                                "status_id": machine.status_id
                            }
                        }

                        ch.basic_publish(
                            exchange=EXCHANGE_EVENTS,
                            routing_key="uretos.machine.event.updated",
                            body=json.dumps(event_payload),
                            properties=pika.BasicProperties(
                                content_type="application/json",
                                correlation_id=correlation_id,
                                delivery_mode=2
                            )
                        )

            elif routing_key == "uretos.machine.command.delete" or msg_type == "uretos.machine.command.delete":
                machine_id = data.get("id")
                if machine_id:
                    machine = db.query(MachineModel).filter(MachineModel.id == machine_id).first()
                    if machine and not machine.deleted_at:
                        machine.deleted_at = datetime.utcnow()
                        db.commit()
                        print(f"[machine-storage] Soft-deleted Machine: {machine_id}", flush=True)

                        event_payload = {
                            "specversion": "1.0",
                            "id": str(uuid.uuid4()),
                            "source": "uretos/services/machine-storage",
                            "type": "uretos.machine.event.deleted",
                            "datacontenttype": "application/json",
                            "time": datetime.now(timezone.utc).isoformat(),
                            "correlationid": correlation_id,
                            "messagetype": "event",
                            "data": {
                                "id": machine_id
                            }
                        }

                        ch.basic_publish(
                            exchange=EXCHANGE_EVENTS,
                            routing_key="uretos.machine.event.deleted",
                            body=json.dumps(event_payload),
                            properties=pika.BasicProperties(
                                content_type="application/json",
                                correlation_id=correlation_id,
                                delivery_mode=2
                            )
                        )
        finally:
            db.close()

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[machine-storage] Error processing message: {e}", flush=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_consumer():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    
    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            channel.exchange_declare(exchange=EXCHANGE_COMMANDS, exchange_type="topic", durable=True)
            channel.exchange_declare(exchange=EXCHANGE_EVENTS, exchange_type="topic", durable=True)
            
            channel.queue_declare(queue=QUEUE_COMMANDS, durable=True)
            channel.queue_bind(
                queue=QUEUE_COMMANDS,
                exchange=EXCHANGE_COMMANDS,
                routing_key="uretos.machine.command.*"
            )

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(
                queue=QUEUE_COMMANDS,
                on_message_callback=process_message
            )
            print("[machine-storage] Connected to RabbitMQ. Waiting for machine commands...", flush=True)
            channel.start_consuming()
        except Exception as e:
            print(f"[machine-storage] Connection waiting... ({e})", flush=True)
            time.sleep(3)

@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer_thread = threading.Thread(target=start_consumer, daemon=True)
    consumer_thread.start()
    print("[machine-storage] DB tables initialized successfully.", flush=True)
    yield

app = FastAPI(title="uRetOS Machine Storage Service", version="0.1.0", lifespan=lifespan)

@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "machine-storage"}