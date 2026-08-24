from datetime import datetime, timezone
import json
import os
import time
import pika
from sqlalchemy import Column, String, Boolean, DateTime, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

DB_USER = os.getenv("POSTGRES_USER", "uretos")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "uretos_dev_pass")
DB_HOST = os.getenv("POSTGRES_HOST", "gateway-token-db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "gateway_token_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class GatewayTokenModel(Base):
    __tablename__ = "gateway_tokens"

    id = Column(String, primary_key=True)
    tenant_id = Column(String, nullable=False, index=True)
    is_used = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

EXCHANGE_EVENTS = "uretos_events"


def publish_token_created_event(token_id: str, client_id: str):
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
        )
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_EVENTS, exchange_type="topic", durable=True)

        event_payload = {
            "specversion": "1.0",
            "id": str(time.time()),
            "source": "uretos/services/gateway-token-command",
            "type": "uretos.token.event.created",
            "data": {
                "token_id": token_id,
                "client_id": client_id
            }
        }

        channel.basic_publish(
            exchange=EXCHANGE_EVENTS,
            routing_key="uretos.token.event.created",
            body=json.dumps(event_payload),
            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json")
        )
        connection.close()
    except Exception as e:
        print(f"[gateway-token-command] Failed to publish token.created event: {e}", flush=True)


def process_command(ch, method, properties, body):
    try:
        # Empfang des CloudEvent-Envelopes
        cloudevent = json.loads(body)
        
        # Validierung des Messagetypes und der Spezifikation
        messagetype = cloudevent.get("messagetype")
        action = cloudevent.get("type") # z.B. "uretos.token.command.create"
        correlation_id = cloudevent.get("correlationid")
        data = cloudevent.get("data", {})

        print(f"[gateway-token-command] Processing [{messagetype}] {action} (Correlation-ID: {correlation_id})", flush=True)

        db = SessionLocal()
        try:
            if action == "uretos.token.command.create" or cloudevent.get("action") == "create_token":
                token_id = data.get("id")
                tenant_id = data.get("tenant_id")
                
                if not token_id or not tenant_id:
                    raise ValueError(f"Missing required token fields in payload: {data}")

                existing = db.query(GatewayTokenModel).filter(GatewayTokenModel.id == token_id).first()
                if not existing:
                    token = GatewayTokenModel(
                        id=token_id,
                        tenant_id=tenant_id,
                        is_used=False,
                        created_at=datetime.now(timezone.utc)
                    )
                    db.add(token)
                    db.commit()
                    print(f"[gateway-token-command] Token {token_id} successfully persisted for tenant {tenant_id}.", flush=True)
                    
                    # Event publizieren, damit der token-validator es synchronisiert
                    publish_token_created_event(token_id, tenant_id)

            elif action == "uretos.token.command.consume" or cloudevent.get("action") == "consume_token":
                token_id = data.get("id")
                token = db.query(GatewayTokenModel).filter(GatewayTokenModel.id == token_id).first()
                if token and not token.is_used:
                    token.is_used = True
                    db.commit()
                    print(f"[gateway-token-command] Token {token_id} marked as used.", flush=True)
        finally:
            db.close()

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[gateway-token-command] Error processing command: {e}", flush=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_command_consumer():
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

            channel.queue_declare(queue="gateway_token_commands", durable=True)
            channel.queue_bind(
                queue="gateway_token_commands",
                exchange="uretos_commands",
                routing_key="uretos.token.command.*"
            )

            channel.basic_consume(
                queue="gateway_token_commands",
                on_message_callback=process_command
            )
            print(" [*] Gateway Token Command Worker (CloudEvent-ready) waiting for messages...", flush=True)
            channel.start_consuming()
        except Exception as e:
            print(f"[gateway-token-command] Connection waiting... ({e})", flush=True)
            time.sleep(3)

if __name__ == "__main__":
    start_command_consumer()