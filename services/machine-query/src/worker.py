from datetime import datetime
import json
import os
import time
import pika
from sqlalchemy import Column, String, DateTime, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

DB_USER = os.getenv("POSTGRES_USER", "uretos")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "uretos_dev_pass")
DB_HOST = os.getenv("POSTGRES_HOST", "machine-query-db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "machine_query_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class MachineReadModel(Base):
    __tablename__ = "machine_read_projections"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    serial_number = Column(String, nullable=False)
    machine_type = Column(String, nullable=False)
    machine_manufacturer = Column(String, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

def process_event(ch, method, properties, body):
    try:
        cloud_event = json.loads(body)
        msg_type = cloud_event.get("type")
        data = cloud_event.get("data", {})

        if msg_type == "uretos.machine.event.created":
            db = SessionLocal()
            try:
                # Upsert into Read Projection Table
                existing = db.query(MachineReadModel).filter(MachineReadModel.id == data["machine_id"]).first()
                if not existing:
                    projection = MachineReadModel(
                        id=data["machine_id"],
                        name=data["name"],
                        serial_number=data["serial_number"],
                        machine_type=data["machine_type"],
                        machine_manufacturer=data["machine_manufacturer"],
                        updated_at=datetime.utcnow()
                    )
                    db.add(projection)
                else:
                    existing.name = data["name"]
                    existing.serial_number = data["serial_number"]
                    existing.machine_type = data["machine_type"]
                    existing.machine_manufacturer = data["machine_manufacturer"]
                    existing.updated_at = datetime.utcnow()
                
                db.commit()
            finally:
                db.close()

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[machine-query] Error processing event projection: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_event_consumer():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials
    )
    
    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            channel.queue_declare(queue="machine_query_events", durable=True)
            channel.queue_bind(
                queue="machine_query_events",
                exchange="uretos.events",
                routing_key="uretos.machine.event.*"
            )

            channel.basic_consume(
                queue="machine_query_events",
                on_message_callback=process_event
            )
            channel.start_consuming()
        except Exception as e:
            print(f"[machine-query] Connection waiting... ({e})")
            time.sleep(3)