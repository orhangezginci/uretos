import os
import json
import uuid
import time
from typing import Optional

import pika
import redis
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

# --- Redis Setup ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# --- RabbitMQ Setup ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

EXCHANGE_COMMANDS = "uretos_commands"
ROUTING_KEY_CREATE_MACHINE = "uretos.machine.command.create"
ROUTING_KEY_UPDATE_MACHINE = "uretos.machine.command.update"
ROUTING_KEY_DELETE_MACHINE = "uretos.machine.command.delete"


def publish_command(routing_key: str, payload: dict) -> bool:
    """Publishes a command message to the RabbitMQ command exchange."""
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

        message_body = json.dumps(payload)
        channel.basic_publish(
            exchange=EXCHANGE_COMMANDS,
            routing_key=routing_key,
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Persistent
                content_type="application/json",
            ),
        )
        connection.close()
        return True
    except Exception as e:
        print(f"[machine-api] Failed to publish command to RabbitMQ: {e}", flush=True)
        return False


# --- DTOs ---
class MachineCreateDTO(BaseModel):
    name: str = Field(..., min_length=1, description="Name of the machine")
    serial_number: str = Field(..., min_length=1, description="Unique serial number")
    machine_type_id: str = Field(..., description="Valid UUIDv4 of an existing machine_type LOV")
    manufacturer_id: Optional[str] = Field(None, description="Valid UUIDv4 of an existing manufacturer LOV")
    status_id: Optional[str] = Field(None, description="Valid UUIDv4 of an existing status LOV")


class MachineUpdateDTO(BaseModel):
    name: Optional[str] = Field(None, min_length=1, description="Name of the machine")
    serial_number: Optional[str] = Field(None, min_length=1, description="Unique serial number")
    machine_type_id: Optional[str] = Field(None, description="Valid UUIDv4 of an existing machine_type LOV")
    manufacturer_id: Optional[str] = Field(None, description="Valid UUIDv4 of an existing manufacturer LOV")
    status_id: Optional[str] = Field(None, description="Valid UUIDv4 of an existing status LOV")


# --- FastAPI App ---
app = FastAPI(title="uRetOS Machine API Gateway", version="0.1.0")


@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "machine-api"}


@app.post("/api/v1/machines", status_code=status.HTTP_202_ACCEPTED)
def create_machine(payload: MachineCreateDTO):
    # 1. Strikte Domain-Validierung: Prüfe machine_type_id gegen Redis
    type_keys = redis_client.keys(f"lov:{payload.machine_type_id}:*")
    if not type_keys:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid machine_type_id: '{payload.machine_type_id}' does not exist in LOV storage."
        )

    # 2. Strikte Domain-Validierung: Prüfe manufacturer_id (falls übergeben) gegen Redis
    if payload.manufacturer_id:
        manuf_keys = redis_client.keys(f"lov:{payload.manufacturer_id}:*")
        if not manuf_keys:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid manufacturer_id: '{payload.manufacturer_id}' does not exist in LOV storage."
            )

    # 3. Validierung bestanden -> Machine Creation Command bauen
    machine_id = str(uuid.uuid4())
    command_id = str(uuid.uuid4())

    command_payload = {
        "command_id": command_id,
        "timestamp": time.time(),
        "data": {
            "id": machine_id,
            "name": payload.name,
            "serial_number": payload.serial_number,
            "machine_type_id": payload.machine_type_id,
            "manufacturer_id": payload.manufacturer_id,
            "status_id": payload.status_id,
        },
    }

    # 4. Command auf Message Broker publishen
    published = publish_command(ROUTING_KEY_CREATE_MACHINE, command_payload)
    if not published:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to queue machine creation command due to message broker error."
        )

    return {
        "status": "accepted",
        "command_id": command_id,
        "machine_id": machine_id,
        "message": "Machine creation command validated and queued for processing."
    }


@app.put("/api/v1/machines/{machine_id}", status_code=status.HTTP_202_ACCEPTED)
@app.patch("/api/v1/machines/{machine_id}", status_code=status.HTTP_202_ACCEPTED)
def update_machine(machine_id: str, payload: MachineUpdateDTO):
    try:
        uuid.UUID(machine_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid machine_id format. Must be a valid UUIDv4."
        )

    # Optional: Validierung falls IDs aktualisiert werden sollen
    if payload.machine_type_id:
        type_keys = redis_client.keys(f"lov:{payload.machine_type_id}:*")
        if not type_keys:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid machine_type_id: '{payload.machine_type_id}' does not exist in LOV storage."
            )

    if payload.manufacturer_id:
        manuf_keys = redis_client.keys(f"lov:{payload.manufacturer_id}:*")
        if not manuf_keys:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid manufacturer_id: '{payload.manufacturer_id}' does not exist in LOV storage."
            )

    command_id = str(uuid.uuid4())
    command_payload = {
        "command_id": command_id,
        "timestamp": time.time(),
        "data": {
            "id": machine_id,
            "name": payload.name,
            "serial_number": payload.serial_number,
            "machine_type_id": payload.machine_type_id,
            "manufacturer_id": payload.manufacturer_id,
            "status_id": payload.status_id,
        },
    }

    published = publish_command(ROUTING_KEY_UPDATE_MACHINE, command_payload)
    if not published:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to queue machine update command due to message broker error."
        )

    return {
        "status": "accepted",
        "command_id": command_id,
        "machine_id": machine_id,
        "message": "Machine update command validated and queued for processing."
    }


@app.delete("/api/v1/machines/{machine_id}", status_code=status.HTTP_202_ACCEPTED)
def delete_machine(machine_id: str):
    try:
        uuid.UUID(machine_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid machine_id format. Must be a valid UUIDv4."
        )

    command_id = str(uuid.uuid4())
    command_payload = {
        "command_id": command_id,
        "timestamp": time.time(),
        "data": {
            "id": machine_id
        },
    }

    published = publish_command(ROUTING_KEY_DELETE_MACHINE, command_payload)
    if not published:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to queue machine deletion command due to message broker error."
        )

    return {
        "status": "accepted",
        "command_id": command_id,
        "machine_id": machine_id,
        "message": "Machine deletion command queued for processing."
    }