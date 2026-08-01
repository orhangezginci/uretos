from datetime import datetime, timezone
import os
import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, UUID4
import pika
import json

app = FastAPI(title="uretOS Machine API", version="0.1.0")

# Transport & Infrastructure Configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.1.0")

class CreateMachineRequest(BaseModel):
    name: str
    serial_number: str
    machine_type: UUID4
    machine_manufacturer: UUID4

# --- Standard System Endpoints ---

@app.get("/healthy")
def healthy():
    return {
        "status": "healthy",
        "service": "machine-api",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/version")
def version():
    return {
        "service": "machine-api",
        "version": SERVICE_VERSION
    }

# --- Domain API Endpoints ---

@app.post("/api/v1/machines", status_code=202)
def create_machine(request: CreateMachineRequest):
    correlation_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())
    
    cloud_event = {
        "specversion": "1.0",
        "id": event_id,
        "source": "uretos/services/machine-api",
        "type": "uretos.machine.command.create",
        "datacontenttype": "application/json",
        "time": datetime.now(timezone.utc).isoformat(),
        "correlationid": correlation_id,
        "messagetype": "command",
        "data": {
            "name": request.name,
            "serial_number": request.serial_number,
            "machine_type": str(request.machine_type),
            "machine_manufacturer": str(request.machine_manufacturer)
        }
    }

    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        channel.basic_publish(
            exchange="uretos.events",
            routing_key="uretos.machine.command.create",
            body=json.dumps(cloud_event),
            properties=pika.BasicProperties(
                content_type="application/json",
                correlation_id=correlation_id,
                delivery_mode=2
            )
        )
        connection.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to publish command: {str(e)}")

    return {
        "status": "command_accepted",
        "correlation_id": correlation_id,
        "event_id": event_id
    }