from datetime import datetime, timezone
import json
import os
import time
import pika
from sqlalchemy import Column, String, Boolean, DateTime, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# --- Datenbank-Konfiguration ---
DB_USER = os.getenv("POSTGRES_USER", "uretos")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "uretos_dev_pass")
DB_HOST = os.getenv("POSTGRES_HOST", "gateway-token-db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "gateway_token_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class GatewayTokenReadModel(Base):
    __tablename__ = "gateway_tokens"
    id = Column(String, primary_key=True)
    tenant_id = Column(String, nullable=False, index=True)
    is_used = Column(Boolean, default=False)
    created_at = Column(DateTime)


# --- RabbitMQ-Konfiguration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")


def handle_rpc_request(ch, method, props, body):
    response = {}
    db = SessionLocal()
    correlation_id = props.correlation_id or "unknown-correlation-id"

    try:
        request = json.loads(body)

        # CloudEvent / RPC Action-Extraktion
        action = request.get("action") or request.get("type")
        payload = request.get("data", {})
        
        # Falls correlationid im CloudEvent-Body mitgeschickt wurde
        if isinstance(request, dict) and request.get("correlationid"):
            correlation_id = request.get("correlationid")

        print(f"[gateway-token-query] Handling RPC Query action='{action}' (Correlation-ID: {correlation_id})", flush=True)

        if action in ["get_token_by_id", "uretos.token.query.get"]:
            token_id = payload.get("id")
            token = db.query(GatewayTokenReadModel).filter(GatewayTokenReadModel.id == token_id).first()
            if token:
                created_at_iso = (
                    token.created_at.replace(tzinfo=timezone.utc).isoformat()
                    if token.created_at else None
                )
                response = {
                    "status": "success",
                    "data": {
                        "id": token.id,
                        "tenant_id": token.tenant_id,
                        "is_used": token.is_used,
                        "created_at": created_at_iso
                    }
                }
            else:
                response = {"status": "error", "message": f"Token with ID '{token_id}' not found"}

        elif action in ["list_tokens_by_tenant", "uretos.token.query.list"]:
            tenant_id = payload.get("tenant_id")
            query = db.query(GatewayTokenReadModel)
            
            # Wenn tenant_id angegeben ist, filtern; andernfalls alle abrufen
            if tenant_id and tenant_id != "default_tenant":
                tokens = query.filter(GatewayTokenReadModel.tenant_id == tenant_id).all()
            else:
                tokens = query.all()

            response = {
                "status": "success",
                "tokens": [
                    {
                        "id": t.id,
                        "tenant_id": t.tenant_id,
                        "is_used": t.is_used,
                        "created_at": t.created_at.replace(tzinfo=timezone.utc).isoformat() if t.created_at else None
                    } for t in tokens
                ]
            }
        else:
            response = {"status": "error", "message": f"Unknown query action: '{action}'"}

    except Exception as e:
        print(f"[gateway-token-query] RPC processing error: {e}", flush=True)
        response = {"status": "error", "message": str(e)}
    finally:
        db.close()

    # Antwort an die temporäre Reply-Queue zurücksenden
    if props.reply_to:
        ch.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(
                correlation_id=props.correlation_id,
                content_type="application/json"
            ),
            body=json.dumps(response)
        )
    
    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_query_rpc_server():
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
            
            channel.queue_declare(queue="gateway_token_queries", durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue="gateway_token_queries",
                on_message_callback=handle_rpc_request
            )
            
            print(" [*] Gateway Token Query RPC Server (CloudEvent-aware) waiting for requests...", flush=True)
            channel.start_consuming()
        except Exception as e:
            print(f"[gateway-token-query] Connection waiting... ({e})", flush=True)
            time.sleep(3)


if __name__ == "__main__":
    start_query_rpc_server()