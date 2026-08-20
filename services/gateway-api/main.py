import os
import json
import uuid
import time
from datetime import datetime, timezone
from typing import Optional

import pika
from fastapi import FastAPI, HTTPException, status, Query
from pydantic import BaseModel, Field

# --- RabbitMQ Setup ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

EXCHANGE_COMMANDS = "uretos_commands"
ROUTING_KEY_CREATE_TOKEN = "uretos.token.command.create"


def publish_command(routing_key: str, cloudevent_payload: dict) -> bool:
    """Publishes a CloudEvent-compliant command message to the RabbitMQ command exchange."""
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials,
                connection_attempts=3,
                retry_delay=2,
            )
        )
        channel = connection.channel()

        channel.exchange_declare(
            exchange=EXCHANGE_COMMANDS, exchange_type="topic", durable=True
        )

        channel.basic_publish(
            exchange=EXCHANGE_COMMANDS,
            routing_key=routing_key,
            body=json.dumps(cloudevent_payload),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Persistent
                content_type="application/json",
            ),
        )
        connection.close()
        return True
    except Exception as e:
        print(f"[gateway-api] Failed to publish CloudEvent: {e}", flush=True)
        return False


# --- RabbitMQ RPC Client for Query-Path ---
class GatewayTokenRPCClient:
    def __init__(self):
        self.host = RABBITMQ_HOST
        self.port = RABBITMQ_PORT
        self.user = RABBITMQ_USER
        self.password = RABBITMQ_PASS

    def call(self, queue_name: str, action: str, payload: dict = None) -> dict:
        self.correlation_id = str(uuid.uuid4())
        self.response = None

        credentials = pika.PlainCredentials(self.user, self.password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port, credentials=credentials)
        )
        channel = connection.channel()

        channel.queue_declare(queue=queue_name, durable=True)
        result = channel.queue_declare(queue='', exclusive=True)
        callback_queue = result.method.queue

        channel.basic_consume(
            queue=callback_queue,
            on_message_callback=lambda ch, m, p, b: setattr(self, 'response', b) if self.correlation_id == p.correlation_id else None,
            auto_ack=True
        )

        request_body = {"action": action, "data": payload or {}}

        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            properties=pika.BasicProperties(
                reply_to=callback_queue,
                correlation_id=self.correlation_id,
            ),
            body=json.dumps(request_body)
        )

        start_time = time.time()
        while self.response is None:
            connection.process_data_events(time_limit=0.1)
            if time.time() - start_time > 5.0:
                connection.close()
                raise TimeoutError("RPC call timed out.")

        connection.close()
        return json.loads(self.response)


# --- DTOs ---
class TokenCreateDTO(BaseModel):
    client_id: str = Field(..., min_length=1)
    permissions: Optional[list] = Field(default=[])


# --- FastAPI App ---
app = FastAPI(title="uRetOS Gateway API", version="0.1.0")


@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "gateway-api"}


@app.get("/api/v1/tokens")
def get_tokens(tenant_id: Optional[str] = Query(None)):
    try:
        rpc_client = GatewayTokenRPCClient()
        response = rpc_client.call(
            queue_name="gateway_token_queries",
            action="list_tokens_by_tenant",
            payload={"tenant_id": tenant_id or "default_tenant"}
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=504, detail=str(e))


@app.post("/api/v1/tokens", status_code=status.HTTP_202_ACCEPTED)
def create_token(payload: TokenCreateDTO):
    """Triggert Token-Erstellung via CloudEvent v1.0 Standard."""
    token_id = str(uuid.uuid4())
    correlation_id = str(uuid.uuid4())
    
    cloudevent = {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "source": "uretos/services/gateway-api",
        "type": "uretos.token.command.create",
        "datacontenttype": "application/json",
        "time": datetime.now(timezone.utc).isoformat(),
        "correlationid": correlation_id,
        "messagetype": "command",
        "subject": f"token/{token_id}",
        "data": {
            "id": token_id,
            "tenant_id": payload.client_id,
            "permissions": payload.permissions,
        },
    }

    if publish_command(ROUTING_KEY_CREATE_TOKEN, cloudevent):
        return {
            "status": "accepted",
            "correlation_id": correlation_id,
            "token_id": token_id,
            "message": "Token creation command queued successfully."
        }
    
    raise HTTPException(status_code=500, detail="Failed to queue command.")