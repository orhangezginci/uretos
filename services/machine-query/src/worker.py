import os
import json
import time
import uuid
from datetime import datetime, timezone

import pika
from sqlalchemy import create_engine, Column, String, DateTime, text
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

# --- Read Model Entity ---
class MachineProjection(Base):
    __tablename__ = "machine_projections"

    id = Column(UUID(as_uuid=True), primary_key=True)
    name = Column(String, nullable=False)
    serial_number = Column(String, nullable=False)
    machine_type_id = Column(UUID(as_uuid=True), nullable=False)
    manufacturer_id = Column(UUID(as_uuid=True), nullable=True)
    status_id = Column(UUID(as_uuid=True), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    deleted_at = Column(DateTime, nullable=True)

def init_db_with_retry(max_retries=15, delay=2):
    for i in range(max_retries):
        try:
            Base.metadata.create_all(bind=engine)
            with engine.connect() as conn:
                conn.execute(text("ALTER TABLE machine_projections ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;"))
                conn.execute(text("ALTER TABLE machine_projections ADD COLUMN IF NOT EXISTS manufacturer_id UUID;"))
                conn.execute(text("ALTER TABLE machine_projections ADD COLUMN IF NOT EXISTS status_id UUID;"))
                conn.commit()
            print("[machine-query] DB initialized successfully.", flush=True)
            return
        except Exception as e:
            print(f"[machine-query] Waiting for DB ({i+1}/{max_retries}): {e}", flush=True)
            time.sleep(delay)
    raise RuntimeError("[machine-query] Could not connect to machine-query-db.")

# --- RabbitMQ Setup ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

EXCHANGE_EVENTS = "uretos_events"
EXCHANGE_COMMANDS = "uretos_commands"
QUEUE_EVENTS = "machine_query_all_events"
QUEUE_RPC_REQUESTS = "machine_query_rpc_requests"

# --- Background Worker ---
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

            channel.exchange_declare(exchange=EXCHANGE_EVENTS, exchange_type="topic", durable=True)
            channel.exchange_declare(exchange=EXCHANGE_COMMANDS, exchange_type="topic", durable=True)
            
            channel.queue_declare(queue=QUEUE_EVENTS, durable=True)
            channel.queue_declare(queue=QUEUE_RPC_REQUESTS, durable=True)

            # Bindings
            channel.queue_bind(exchange=EXCHANGE_EVENTS, queue=QUEUE_EVENTS, routing_key="uretos.machine.event.#")
            channel.queue_bind(exchange=EXCHANGE_COMMANDS, queue=QUEUE_RPC_REQUESTS, routing_key="uretos.machine.query.get_all")

            print("[machine-query] Connected. Ready for CloudEvent v1.0 Events and RPC requests...", flush=True)

            def handle_message(ch, method, properties, body):
                try:
                    payload = json.loads(body)
                    routing_key = method.routing_key

                    # 1. RPC Handling (Query)
                    if routing_key == "uretos.machine.query.get_all":
                        query_data = payload.get("data", payload) if payload.get("specversion") == "1.0" else payload
                        action = query_data.get("action") or payload.get("action")
                        
                        if not action or action == "get_machines" or "lang" in query_data or payload.get("messagetype") == "query":
                            response_data = []
                            db: Session = SessionLocal()
                            try:
                                machines = db.query(MachineProjection).filter(MachineProjection.deleted_at.is_(None)).all()
                                for m in machines:
                                    response_data.append({
                                        "id": str(m.id),
                                        "name": m.name,
                                        "serial_number": m.serial_number,
                                        "machine_type_id": str(m.machine_type_id),
                                        "manufacturer_id": str(m.manufacturer_id) if m.manufacturer_id else None,
                                        "status_id": str(m.status_id) if m.status_id else None
                                    })
                            finally:
                                db.close()

                            correlation_id = properties.correlation_id or payload.get("correlationid")
                            response_envelope = {
                                "specversion": "1.0",
                                "id": str(uuid.uuid4()),
                                "source": "uretos/services/machine-query",
                                "type": "uretos.machine.query.response",
                                "datacontenttype": "application/json",
                                "time": datetime.now(timezone.utc).isoformat(),
                                "correlationid": correlation_id,
                                "messagetype": "response",
                                "data": response_data
                            }

                            if properties.reply_to and correlation_id:
                                ch.basic_publish(
                                    exchange='',
                                    routing_key=properties.reply_to,
                                    properties=pika.BasicProperties(correlation_id=correlation_id),
                                    body=json.dumps(response_envelope)
                                )
                        
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    # 2. Event Handling (CloudEvent v1.0 konform)
                    if routing_key.startswith("uretos.machine.event."):
                        event_data = payload.get("data", payload) if payload.get("specversion") == "1.0" else payload
                        m_id_str = event_data.get("id") or event_data.get("machine_id")
                        
                        if m_id_str:
                            db: Session = SessionLocal()
                            try:
                                m_id = uuid.UUID(m_id_str)
                                item = db.query(MachineProjection).filter_by(id=m_id).first()
                                
                                if routing_key == "uretos.machine.event.deleted":
                                    if item:
                                        item.deleted_at = datetime.utcnow()
                                        db.commit()
                                else:
                                    m_name = event_data.get("name")
                                    m_serial = event_data.get("serial_number")
                                    m_type = uuid.UUID(event_data.get("machine_type_id")) if event_data.get("machine_type_id") else None
                                    m_manufacturer = uuid.UUID(event_data.get("manufacturer_id")) if event_data.get("manufacturer_id") else None
                                    m_status = uuid.UUID(event_data.get("status_id")) if event_data.get("status_id") else None
                                    
                                    if not item:
                                        item = MachineProjection(
                                            id=m_id, 
                                            name=m_name, 
                                            serial_number=m_serial, 
                                            machine_type_id=m_type,
                                            manufacturer_id=m_manufacturer,
                                            status_id=m_status
                                        )
                                        db.add(item)
                                    else:
                                        if m_name: item.name = m_name
                                        if m_serial: item.serial_number = m_serial
                                        if m_type: item.machine_type_id = m_type
                                        if m_manufacturer: item.manufacturer_id = m_manufacturer
                                        if m_status: item.status_id = m_status
                                    db.commit()
                            finally:
                                db.close()
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    print(f"[machine-query] Error processing message: {e}", flush=True)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_consume(queue=QUEUE_EVENTS, on_message_callback=handle_message)
            channel.basic_consume(queue=QUEUE_RPC_REQUESTS, on_message_callback=handle_message)
            channel.start_consuming()
        except Exception as e:
            print(f"[machine-query] Connection error: {e}. Retrying...", flush=True)
            time.sleep(5)