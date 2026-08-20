import os
import json
import uuid
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
EXCHANGE_RPC = "uretos_rpc"
ROUTING_KEY_CREATE_MACHINE = "uretos.machine.command.create"
ROUTING_KEY_UPDATE_MACHINE = "uretos.machine.command.update"
ROUTING_KEY_DELETE_MACHINE = "uretos.machine.command.delete"
ROUTING_KEY_CONNECT_MACHINE = "uretos.machine.command.connect"


def publish_cloudevent_command(routing_key: str, event_type: str, subject: str, data_payload: dict, correlation_id: str) -> bool:
    """Publishes a CloudEvent v1.0 compliant command message to the RabbitMQ command exchange."""
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

        cloudevent = {
            "specversion": "1.0",
            "id": str(uuid.uuid4()),
            "source": "uretos/services/machine-api",
            "type": event_type,
            "datacontenttype": "application/json",
            "time": datetime.now(timezone.utc).isoformat(),
            "correlationid": correlation_id,
            "messagetype": "command",
            "subject": subject,
            "data": data_payload
        }

        channel.basic_publish(
            exchange=EXCHANGE_COMMANDS,
            routing_key=routing_key,
            body=json.dumps(cloudevent),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Persistent
                content_type="application/json",
            ),
        )
        connection.close()
        print(f"[machine-api] Published CloudEvent command [{routing_key}]: correlation_id={correlation_id}", flush=True)
        return True
    except Exception as e:
        print(f"[machine-api] Failed to publish CloudEvent to RabbitMQ: {e}", flush=True)
        return False


# --- RabbitMQ RPC Client for Read-Path / Validation ---
class MachineRPCClient:
    def __init__(self):
        self.host = RABBITMQ_HOST
        self.port = RABBITMQ_PORT
        self.user = RABBITMQ_USER
        self.password = RABBITMQ_PASS
        self.exchange = EXCHANGE_COMMANDS
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.response = None
        self.correlation_id = None

    def call(self, exchange: str, routing_key: str, payload: dict, timeout: float = 5.0) -> dict:
        self.correlation_id = payload.get("correlationid") or str(uuid.uuid4())
        self.response = None

        credentials = pika.PlainCredentials(self.user, self.password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port, credentials=credentials)
        )
        channel = connection.channel()

        result = channel.queue_declare(queue='', exclusive=True)
        callback_queue = result.method.queue

        channel.basic_consume(
            queue=callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            properties=pika.BasicProperties(
                reply_to=callback_queue,
                correlation_id=self.correlation_id,
                content_type="application/json",
            ),
            body=json.dumps(payload)
        )

        start_time = __import__('time').time()
        while self.response is None:
            connection.process_data_events(time_limit=0.1)
            if __import__('time').time() - start_time > timeout:
                connection.close()
                raise TimeoutError("RPC call timed out waiting for service response.")

        connection.close()
        
        if isinstance(self.response, str):
            return json.loads(self.response)
        return self.response

    def on_response(self, ch, method, props, body):
        if self.correlation_id == props.correlation_id:
            try:
                self.response = json.loads(body)
            except Exception:
                self.response = body

    def validate_lov_ids(self, ids: list) -> bool:
        """Prüft via RPC beim lov-validation Service, ob alle angegebenen IDs existieren."""
        try:
            correlation_id = str(uuid.uuid4())
            rpc_payload = {
                "action": "validate_lov",
                "correlationid": correlation_id,
                "data": {
                    "ids": ids
                }
            }
            response = self.call(
                exchange=EXCHANGE_RPC,
                routing_key="lov.validate",
                payload=rpc_payload
            )
            return response.get("valid", False)
        except Exception as e:
            print(f"[machine-api] RPC validation failed for LOV IDs {ids}: {e}", flush=True)
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


class MachineConnectDTO(BaseModel):
    opc_url: str = Field(..., description="OPC-UA Endpoint URL of the machine")
    poll_interval: Optional[float] = Field(2.0, ge=0.5, le=60.0, description="Polling interval in seconds")


# --- FastAPI App ---
app = FastAPI(title="uRetOS Machine API Gateway", version="0.1.0")


@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "machine-api"}


@app.get("/api/v1/machines")
def get_machines(lang: str = Query("de-DE", description="Language code")):
    try:
        rpc_client = MachineRPCClient()
        correlation_id = str(uuid.uuid4())
        query_ce = {
            "specversion": "1.0",
            "id": str(uuid.uuid4()),
            "source": "uretos/services/machine-api",
            "type": "uretos.machine.query.get_all",
            "datacontenttype": "application/json",
            "time": datetime.now(timezone.utc).isoformat(),
            "correlationid": correlation_id,
            "messagetype": "query",
            "subject": "machines/all",
            "data": {"lang": lang}
        }
        response = rpc_client.call(
            exchange=EXCHANGE_COMMANDS,
            routing_key="uretos.machine.query.get_all",
            payload=query_ce
        )
        if isinstance(response, dict) and "data" in response:
            return response["data"]
        return response
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"Failed to fetch machines via RPC: {str(e)}"
        )


@app.post("/api/v1/machines", status_code=status.HTTP_202_ACCEPTED)
def create_machine(payload: MachineCreateDTO):
    rpc_client = MachineRPCClient()

    # 1. Sammeln aller zu prüfenden IDs
    ids_to_check = [payload.machine_type_id]
    if payload.manufacturer_id:
        ids_to_check.append(payload.manufacturer_id)
    if payload.status_id:
        ids_to_check.append(payload.status_id)

    # 2. Bulk-Validierung über den lov-validation Service
    if not rpc_client.validate_lov_ids(ids_to_check):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Validation failed: One or more provided LOV IDs do not exist."
        )

    # 3. Validierung bestanden -> CloudEvent Command bauen und publishen
    machine_id = str(uuid.uuid4())
    correlation_id = str(uuid.uuid4())

    data_payload = {
        "id": machine_id,
        "name": payload.name,
        "serial_number": payload.serial_number,
        "machine_type_id": payload.machine_type_id,
        "manufacturer_id": payload.manufacturer_id,
        "status_id": payload.status_id,
    }

    published = publish_cloudevent_command(
        routing_key=ROUTING_KEY_CREATE_MACHINE,
        event_type="uretos.machine.command.create",
        subject=f"machine/{machine_id}",
        data_payload=data_payload,
        correlation_id=correlation_id
    )

    if not published:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to queue machine creation command due to message broker error."
        )

    return {
        "status": "accepted",
        "correlation_id": correlation_id,
        "machine_id": machine_id,
        "message": "Machine creation command validated and queued as CloudEvent."
    }


@app.post("/api/v1/machines/{machine_id}/connect", status_code=status.HTTP_202_ACCEPTED)
def connect_machine(machine_id: str, payload: MachineConnectDTO):
    try:
        uuid.UUID(machine_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid machine_id format. Must be a valid UUIDv4."
        )

    correlation_id = str(uuid.uuid4())
    data_payload = {
        "machine_id": machine_id,
        "opc_url": payload.opc_url,
        "poll_interval": payload.poll_interval
    }

    published = publish_cloudevent_command(
        routing_key=ROUTING_KEY_CONNECT_MACHINE,
        event_type="uretos.machine.command.connect",
        subject=f"machine/{machine_id}/connect",
        data_payload=data_payload,
        correlation_id=correlation_id
    )

    if not published:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to queue connection command due to message broker error."
        )

    return {
        "status": "accepted",
        "correlation_id": correlation_id,
        "machine_id": machine_id,
        "message": f"Connection command for machine {machine_id} queued as CloudEvent."
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

    rpc_client = MachineRPCClient()
    ids_to_check = []
    if payload.machine_type_id:
        ids_to_check.append(payload.machine_type_id)
    if payload.manufacturer_id:
        ids_to_check.append(payload.manufacturer_id)
    if payload.status_id:
        ids_to_check.append(payload.status_id)

    if ids_to_check:
        if not rpc_client.validate_lov_ids(ids_to_check):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Validation failed: One or more provided LOV IDs do not exist."
            )

    correlation_id = str(uuid.uuid4())
    data_payload = {
        "id": machine_id,
        "name": payload.name,
        "serial_number": payload.serial_number,
        "machine_type_id": payload.machine_type_id,
        "manufacturer_id": payload.manufacturer_id,
        "status_id": payload.status_id,
    }

    published = publish_cloudevent_command(
        routing_key=ROUTING_KEY_UPDATE_MACHINE,
        event_type="uretos.machine.command.update",
        subject=f"machine/{machine_id}",
        data_payload=data_payload,
        correlation_id=correlation_id
    )

    if not published:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to queue machine update command due to message broker error."
        )

    return {
        "status": "accepted",
        "correlation_id": correlation_id,
        "machine_id": machine_id,
        "message": "Machine update command queued as CloudEvent."
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

    correlation_id = str(uuid.uuid4())
    data_payload = {"id": machine_id}

    published = publish_cloudevent_command(
        routing_key=ROUTING_KEY_DELETE_MACHINE,
        event_type="uretos.machine.command.delete",
        subject=f"machine/{machine_id}",
        data_payload=data_payload,
        correlation_id=correlation_id
    )

    if not published:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to queue machine deletion command due to message broker error."
        )

    return {
        "status": "accepted",
        "correlation_id": correlation_id,
        "machine_id": machine_id,
        "message": "Machine deletion command queued as CloudEvent."
    }


@app.get("/api/v1/machines/{machine_id}/history")
def get_machine_history(
    machine_id: str,
    limit: int = Query(50, description="Max number of historical data points"),
    lang: str = Query("de-DE", description="Language code")
):
    try:
        uuid.UUID(machine_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid machine_id format. Must be a valid UUIDv4."
        )

    try:
        rpc_client = MachineRPCClient()
        correlation_id = str(uuid.uuid4())
        query_ce = {
            "specversion": "1.0",
            "id": str(uuid.uuid4()),
            "source": "uretos/services/machine-api",
            "type": "uretos.telemetry.query.get_history",
            "datacontenttype": "application/json",
            "time": datetime.now(timezone.utc).isoformat(),
            "correlationid": correlation_id,
            "messagetype": "query",
            "subject": f"machine/{machine_id}/history",
            "data": {"machine_id": machine_id, "limit": limit, "lang": lang}
        }
        response = rpc_client.call(
            exchange=EXCHANGE_COMMANDS,
            routing_key="uretos.telemetry.query.get_history",
            payload=query_ce
        )
        if isinstance(response, dict) and "data" in response:
            return response["data"]
        return response
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"Failed to fetch machine history via RPC: {str(e)}"
        )