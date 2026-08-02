from datetime import datetime, timezone
import json
import os
import uuid
from typing import Dict
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import pika

app = FastAPI(title="uretOS LOV API Service", version="0.1.0")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.1.0")

# RabbitMQ Config
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")
COMMAND_EXCHANGE = "uretos_commands"

# --- Models ---
class CreateLOVCommand(BaseModel):
    category: str = Field(..., example="machine_type")
    code: str = Field(..., example="MILLING_CNC_5AXIS")
    translations: Dict[str, str] = Field(
        ...,
        example={
            "en-US": "5-Axis CNC Milling",
            "de-DE": "5-Achs-CNC-Fräse",
            "tr-TR": "5 Eksenli CNC İşleme"
        }
    )

class CommandAcceptedResponse(BaseModel):
    status: str = "command_accepted"
    correlation_id: str
    event_id: str


# --- Publisher Helper ---
def publish_command(routing_key: str, payload: dict):
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    try:
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
        channel.exchange_declare(exchange=COMMAND_EXCHANGE, exchange_type="topic", durable=True)

        channel.basic_publish(
            exchange=COMMAND_EXCHANGE,
            routing_key=routing_key,
            body=json.dumps(payload),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Persistent message
                content_type="application/json",
            ),
        )
        connection.close()
        print(f"[lov-api] Published command [{routing_key}]: correlation_id={payload.get('correlation_id')}", flush=True)
    except Exception as e:
        print(f"[lov-api] Failed to publish command to RabbitMQ: {e}", flush=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Message broker unavailable: {str(e)}"
        )


# --- Endpoints ---
@app.get("/healthy")
def healthy():
    return {"status": "healthy", "service": "lov-api", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.get("/version")
def version():
    return {"service": "lov-api", "version": SERVICE_VERSION}

@app.post(
    "/api/v1/lov",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=CommandAcceptedResponse
)
def create_lov_entry(cmd: CreateLOVCommand):
    correlation_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())

    envelope = {
        "event_id": event_id,
        "correlation_id": correlation_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "command_type": "CreateLOV",
        "category": cmd.category,
        "code": cmd.code,
        "translations": cmd.translations
    }

    routing_key = f"uretos.lov.command.create.{cmd.category}"
    publish_command(routing_key=routing_key, payload=envelope)

    return CommandAcceptedResponse(
        correlation_id=correlation_id,
        event_id=event_id
    )