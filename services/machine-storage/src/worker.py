from datetime import datetime, timezone
import json
import os
import time
import uuid
import pika
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
    machine_type = Column(String, nullable=False)
    machine_manufacturer = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

# Transport Configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

def process_message(ch, method, properties, body):
    try:
        cloud_event = json.loads(body)
        msg_type = cloud_event.get("type")
        correlation_id = cloud_event.get("correlationid", str(uuid.uuid4()))
        data = cloud_event.get("data", {})

        if msg_type == "uretos.machine.command.create":
            machine_id = str(uuid.uuid4())
            db = SessionLocal()
            try:
                # 1. Persist Record
                machine = MachineModel(
                    id=machine_id,
                    name=data["name"],
                    serial_number=data["serial_number"],
                    machine_type=data["machine_type"],
                    machine_manufacturer=data["machine_manufacturer"]
                )
                db.add(machine)
                db.commit()
            finally:
                db.close()

            # 2. Emit Event CloudEvent
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
                    "machine_id": machine_id,
                    "name": data["name"],
                    "serial_number": data["serial_number"],
                    "machine_type": data["machine_type"],
                    "machine_manufacturer": data["machine_manufacturer"]
                }
            }

            ch.basic_publish(
                exchange="uretos.events",
                routing_key="uretos.machine.event.created",
                body=json.dumps(event_payload),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    correlation_id=correlation_id,
                    delivery_mode=2
                )
            )

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[machine-storage] Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_consumer():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials
    )
    
    # Retry connection until RabbitMQ is up
    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            channel.queue_declare(queue="machine_storage_commands", durable=True)
            channel.queue_bind(
                queue="machine_storage_commands",
                exchange="uretos.events",
                routing_key="uretos.machine.command.*"
            )

            channel.basic_consume(
                queue="machine_storage_commands",
                on_message_callback=process_message
            )
            channel.start_consuming()
        except Exception as e:
            print(f"[machine-storage] Connection waiting... ({e})")
            time.sleep(3)