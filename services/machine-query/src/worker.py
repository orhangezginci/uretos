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
    machine_type_id = Column(String, nullable=False)
    manufacturer_id = Column(String, nullable=True)
    status_id = Column(String, nullable=True)
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

        # Unterstützt sowohl uretos.machine.event.created als auch direct event handling
        if msg_type == "uretos.machine.event.created" or "machine_type_id" in data:
            db = SessionLocal()
            try:
                machine_id = data.get("id") or data.get("machine_id")
                existing = db.query(MachineReadModel).filter(MachineReadModel.id == machine_id).first()
                
                if not existing:
                    projection = MachineReadModel(
                        id=machine_id,
                        name=data.get("name"),
                        serial_number=data.get("serial_number"),
                        machine_type_id=data.get("machine_type_id"),
                        manufacturer_id=data.get("manufacturer_id"),
                        status_id=data.get("status_id"),
                        updated_at=datetime.utcnow()
                    )
                    db.add(projection)
                else:
                    existing.name = data.get("name")
                    existing.serial_number = data.get("serial_number")
                    existing.machine_type_id = data.get("machine_type_id")
                    existing.manufacturer_id = data.get("manufacturer_id")
                    existing.status_id = data.get("status_id")
                    existing.updated_at = datetime.utcnow()
                
                db.commit()
            finally:
                db.close()

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[machine-query] Error processing event projection: {e}", flush=True)
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
            print(f"[machine-query] Connection waiting... ({e})", flush=True)
            time.sleep(3)

if __name__ == "__main__":
    start_event_consumer()