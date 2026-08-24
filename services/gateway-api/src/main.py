import os
import json
import uuid
import time
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import pika
import aio_pika
from fastapi import FastAPI, HTTPException, status, Query
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# --- App Setup & Middleware ---
app = FastAPI(title="uRetOS Gateway API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway-api")

# --- Environment Variables ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")
RABBITMQ_URL = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"

EXCHANGE_COMMANDS = "uretos_commands"
EXCHANGE_RPC = "uretos_rpc"
EXCHANGE_TOPIC = "amq.topic"
ROUTING_KEY_CREATE_TOKEN = "uretos.token.command.create"

# Global In-Memory Cache für Metadata
LATEST_METADATA_CACHE: Dict[str, Any] = {}


def publish_command(exchange_name: str, routing_key: str, cloudevent_payload: dict) -> bool:
    """Publiziert ein CloudEvent-Command an ein angegebenes RabbitMQ Exchange."""
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

        channel.basic_publish(
            exchange=exchange_name,
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
        logger.error(f"Failed to publish CloudEvent to {exchange_name}: {e}")
        return False


# --- Background Listener für OPC Metadata Events ---
async def start_metadata_listener():
    """Liest opc.metadata.# Events aus RabbitMQ und aktualisiert LATEST_METADATA_CACHE."""
    connection = None
    while not connection:
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
        except Exception as e:
            logger.warning(f"RabbitMQ connection pending... Retrying in 3s ({e})")
            await asyncio.sleep(3)

    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(EXCHANGE_TOPIC, aio_pika.ExchangeType.TOPIC, passive=True)
        
        queue = await channel.declare_queue("gateway_api_metadata_listener", exclusive=True)
        await queue.bind(exchange, routing_key="opc.metadata.#")
        
        logger.info("[*] Gateway Metadata Listener listening on amq.topic -> opc.metadata.#")
        
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        raw_body = json.loads(message.body.decode("utf-8"))
                        
                        routing_key = message.routing_key
                        tenant_id = raw_body.get("tenant_id")
                        if not tenant_id and "." in routing_key:
                            tenant_id = routing_key.split(".")[-1]

                        payload_content = raw_body.get("payload") or raw_body.get("data") or {}

                        if tenant_id:
                            LATEST_METADATA_CACHE[tenant_id] = {
                                "received_at": datetime.now(timezone.utc).isoformat(),
                                "event_id": raw_body.get("event_id") or raw_body.get("id"),
                                "event_type": raw_body.get("event_type") or raw_body.get("type"),
                                "payload": payload_content
                            }
                            logger.info(f"Updated LATEST_METADATA_CACHE for tenant '{tenant_id}'")
                    except Exception as err:
                        logger.error(f"Error parsing metadata event: {err}")


# --- RPC Clients ---
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


class TokenValidatorRpcClient:
    def __init__(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials
            )
        )
        self.channel = self.connection.channel()
        
        self.channel.exchange_declare(exchange=EXCHANGE_RPC, exchange_type="direct", durable=True)
        
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )
        self.response = None
        self.correlation_id = None

    def on_response(self, ch, method, properties, body):
        if self.correlation_id == properties.correlation_id:
            self.response = json.loads(body)

    def call(self, token_id: str):
        self.response = None
        self.correlation_id = str(uuid.uuid4())
        
        payload = {
            "data": {
                "token_id": token_id
            }
        }

        self.channel.basic_publish(
            exchange=EXCHANGE_RPC,
            routing_key="token.validate",
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.correlation_id,
                content_type="application/json"
            ),
            body=json.dumps(payload)
        )

        start_time = time.time()
        while self.response is None:
            self.connection.process_data_events(time_limit=0.1)
            if time.time() - start_time > 5.0:
                self.connection.close()
                raise TimeoutError("Token validation RPC call timed out.")
            
        return self.response


# --- DTOs ---
class TokenCreateDTO(BaseModel):
    client_id: str = Field(..., min_length=1)
    permissions: Optional[list] = Field(default=[])


class OPCConnectDTO(BaseModel):
    tenant_id: str = Field(..., min_length=1)
    opc_url: str = Field(..., example="opc.tcp://opc-server:4840")


# --- Lifecycle Events ---
@app.on_event("startup")
async def startup_event():
    """Startet den RabbitMQ Background Listener für OPC Metadata."""
    asyncio.create_task(start_metadata_listener())


@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "gateway-api"}


# --- OPC Endpoints ---
@app.post("/api/v1/opc/connect", status_code=status.HTTP_202_ACCEPTED)
def connect_opc_machine(payload: OPCConnectDTO):
    """Triggert die Maschinen-Koppelung und publiziert den Command-Event an den Harvester."""
    event_id = str(uuid.uuid4())
    routing_key = f"opc.command.{payload.tenant_id}"

    cloudevent = {
        "specversion": "1.0",
        "id": event_id,
        "type": "uretos.opc.command.connect",
        "source": "uretos/gateway-api",
        "tenant_id": payload.tenant_id,
        "time": datetime.now(timezone.utc).isoformat(),
        "data": {
            "tenant_id": payload.tenant_id,
            "opc_url": payload.opc_url
        }
    }

    if publish_command(EXCHANGE_TOPIC, routing_key, cloudevent):
        return {
            "status": "accepted",
            "tenant_id": payload.tenant_id,
            "message": f"Connect command published for OPC server {payload.opc_url}"
        }

    raise HTTPException(status_code=500, detail="Failed to publish connect command to broker.")


@app.get("/api/v1/opc/latest-metadata/{tenant_id}")
async def get_latest_metadata(tenant_id: str):
    """Liefert die zuletzt empfangenen Stammdaten per REST-Polling."""
    data = LATEST_METADATA_CACHE.get(tenant_id)
    if not data:
        return {"status": "pending", "message": "No metadata received yet"}
    return data


@app.get("/api/v1/opc/stream/{tenant_id}")
async def stream_opc_events(tenant_id: str):
    """Streamt empfangene OPC-Stammdaten/Telemetrie live an das Frontend per SSE."""
    async def event_generator():
        last_event_id = None
        while True:
            data = LATEST_METADATA_CACHE.get(tenant_id)
            if data and data.get("event_id") != last_event_id:
                last_event_id = data.get("event_id")
                yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# --- Token Endpoints ---
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


@app.get("/api/v1/tokens/{token_id}")
def validate_token(token_id: str):
    try:
        rpc_client = TokenValidatorRpcClient()
        response = rpc_client.call(token_id)
        
        if hasattr(rpc_client, 'connection') and rpc_client.connection.is_open:
            rpc_client.connection.close()

        if response and response.get("valid"):
            return {
                "status": "valid",
                "token_id": response.get("token_id"),
                "client_id": response.get("client_id"),
                "message": "Token successfully validated via Message Broker RPC."
            }
        else:
            raise HTTPException(status_code=401, detail="Token invalid or not found")

    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Validation broker communication failed: {str(e)}")


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

    if publish_command(EXCHANGE_COMMANDS, ROUTING_KEY_CREATE_TOKEN, cloudevent):
        return {
            "status": "accepted",
            "correlation_id": correlation_id,
            "token_id": token_id,
            "message": "Token creation command queued successfully."
        }
    
    raise HTTPException(status_code=500, detail="Failed to queue command.")