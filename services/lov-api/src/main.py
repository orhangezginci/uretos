import os
import json
import uuid
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, status, Query
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

class LovResponseDTO(BaseModel):
    id: str
    category: str
    code: str
    value: str


# --- RPC Client for Read Path (CloudEvent-aware) ---
class LOVQueryRPCClient:
    def __init__(self):
        self.host = RABBITMQ_HOST
        self.port = RABBITMQ_PORT
        self.user = RABBITMQ_USER
        self.password = RABBITMQ_PASS
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.response = None
        self.correlation_id = None
        self._lock = threading.Lock()

    def _connect(self):
        credentials = pika.PlainCredentials(self.user, self.password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
        )
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self._on_response,
            auto_ack=True
        )

    def _on_response(self, ch, method, props, body):
        if self.correlation_id == props.correlation_id:
            self.response = json.loads(body)

    def call(self, cloudevent_payload: dict, routing_key: str, timeout: int = 5) -> Optional[list]:
        with self._lock:
            self.response = None
            self.correlation_id = cloudevent_payload.get("correlationid")

            try:
                if not self.connection or self.connection.is_closed:
                    self._connect()

                self.channel.basic_publish(
                    exchange=COMMAND_EXCHANGE,
                    routing_key=routing_key,
                    properties=pika.BasicProperties(
                        reply_to=self.callback_queue,
                        correlation_id=self.correlation_id,
                        content_type="application/json"
                    ),
                    body=json.dumps(cloudevent_payload)
                )

                start_time = __import__('time').time()
                while self.response is None:
                    self.connection.process_data_events(time_limit=0.1)
                    if __import__('time').time() - start_time > timeout:
                        raise TimeoutError("RPC call timed out waiting for query service response.")

                return self.response
            except Exception as e:
                try:
                    if self.connection and not self.connection.is_closed:
                        self.connection.close()
                except:
                    pass
                self.connection = None
                raise e

rpc_client = LOVQueryRPCClient()


# --- Publisher Helper (Write Path) ---
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
                delivery_mode=2,
                content_type="application/json",
            ),
        )
        connection.close()
        print(f"[lov-api] Published CloudEvent command [{routing_key}]: correlation_id={payload.get('correlationid')}", flush=True)
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

    # Konformes CloudEvent v1.0 für den Command (Write-Path)
    cloudevent = {
        "specversion": "1.0",
        "id": event_id,
        "source": "uretos/services/lov-api",
        "type": f"uretos.lov.command.create.{cmd.category}",
        "datacontenttype": "application/json",
        "time": datetime.now(timezone.utc).isoformat(),
        "correlationid": correlation_id,
        "messagetype": "command",
        "subject": f"lov/{cmd.category}/{cmd.code}",
        "data": {
            "category": cmd.category,
            "code": cmd.code,
            "translations": cmd.translations
        }
    }

    routing_key = f"uretos.lov.command.create.{cmd.category}"
    publish_command(routing_key=routing_key, payload=cloudevent)

    return CommandAcceptedResponse(
        correlation_id=correlation_id,
        event_id=event_id
    )

@app.get("/api/v1/lov/{category}", response_model=List[LovResponseDTO])
def get_lov_by_category(
    category: str,
    lang: str = Query(default="en-US", description="Locale key, e.g. de-DE, en-US, tr-TR")
):
    """
    Fetches localized LOV entries by category via RabbitMQ RPC CloudEvent from lov-query service.
    """
    correlation_id = str(uuid.uuid4())
    
    # Konformes CloudEvent v1.0 für den Query-Pfad (RPC)
    query_cloudevent = {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "source": "uretos/services/lov-api",
        "type": "uretos.lov.query.get_by_category",
        "datacontenttype": "application/json",
        "time": datetime.now(timezone.utc).isoformat(),
        "correlationid": correlation_id,
        "messagetype": "query",
        "subject": f"lov/category/{category}",
        "data": {
            "category": category,
            "lang": lang
        }
    }

    try:
        response = rpc_client.call(
            cloudevent_payload=query_cloudevent if 'query_cloudevent' in locals() else None,
            routing_key="uretos.lov.query.get_by_category",
            timeout=5
        )
        # Direkt übergeben falls korrekt formatiert
        response = rpc_client.call(
            cloudevent_payload=query_cloudevent,
            routing_key="uretos.lov.query.get_by_category",
            timeout=5
        )
        
        if response is None:
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="RPC call timed out waiting for query service response."
            )
            
        # Falls der Query-Dienst im CloudEvent-Format oder direkt als Liste antwortet:
        if isinstance(response, dict) and "data" in response:
            return response["data"]
        return response
        
    except TimeoutError as te:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"Failed to fetch LOV entries via RPC: {str(te)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"RPC communication failed: {str(e)}"
        )