import os
import uuid
import time
import json
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from asyncua import Client
import pika

app = FastAPI(
    title="uretOS Machine Management Service",
    description="Headless Event-Driven Microservice für OPC-UA Discovery und CloudEvent Publishing."
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# RabbitMQ Umgebungsvariablen
RMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

class OnboardRequest(BaseModel):
    endpoint_uri: str = Field(..., description="OPC-UA Endpoint URI, z.B. opc.tcp://localhost:4840")
    machine_type_id: str = Field(..., description="UUIDv4 des Maschinentyps (LOV)")
    manufacturer_id: str = Field(None, description="Optional: UUIDv4 des Herstellers (LOV)")

@app.get("/health")
async def health_check():
    return {"status": "running", "service": "machine-management", "mode": "event-driven"}

@app.post("/api/v1/machine-management/onboard")
async def onboard_machine(payload: OnboardRequest):
    """
    1. Verbindet sich per OPC-UA mit der Maschine.
    2. Liest Metadaten (Name, Serial/DeviceID, Variablen) aus.
    3. Publiziert ein CloudEvent direkt an RabbitMQ (keine HTTP-Abhängigkeit zu anderen APIs).
    """
    uri = payload.endpoint_uri
    machine_id = str(uuid.uuid4())
    machine_name = "Shopfloor Machine"
    serial_number = ""
    variables = []

    # 1. OPC-UA Verbindung & Discovery
    try:
        client = Client(url=uri, timeout=3.0)
        async with client:
            objects_node = await client.nodes.objects.get_children()
            
            for obj in objects_node:
                browse_name = await obj.read_browse_name()
                name_str = browse_name.Name
                
                if any(sys_folder in name_str for sys_folder in ["Server", "Aliases", "Views", "Types"]):
                    continue
                
                children = await obj.get_children()
                if not children:
                    continue
                
                machine_name = name_str
                
                for child in children:
                    try:
                        child_name = await child.read_browse_name()
                        child_str = child_name.Name
                        
                        if "DeviceID" in child_str:
                            serial_number = str(await child.read_value())
                        
                        value = await child.read_value()
                        variables.append({
                            "browse_name": child_str,
                            "node_id": str(child.nodeid),
                            "value": value,
                            "type": type(value).__name__
                        })
                    except Exception:
                        pass
                break

        if not serial_number:
            serial_number = f"SN-{uuid.uuid4().hex[:8].upper()}"

    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Timeout: OPC-UA Server antwortet nicht.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"OPC-UA Verbindungsfehler: {str(e)}")

    # 2. CloudEvent Payload zusammenbauen
    event_id = str(uuid.uuid4())
    cloudevent = {
        "specversion": "1.0",
        "type": "org.uretos.machine.registered",
        "source": "urn:uretos:service:machine-management",
        "id": event_id,
        "time": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        "datacontenttype": "application/json",
        "data": {
            "id": machine_id,
            "name": machine_name,
            "serial_number": serial_number,
            "machine_type_id": payload.machine_type_id,
            "manufacturer_id": payload.manufacturer_id,
            "endpoint_uri": uri,
            "variables": variables
        }
    }

    # 3. Direkt an RabbitMQ publishen (Vollständig entkoppelt)
    try:
        credentials = pika.PlainCredentials(RMQ_USER, RMQ_PASS)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RMQ_HOST,
                port=RMQ_PORT,
                credentials=credentials,
                connection_attempts=3,
                retry_delay=1
            )
        )
        channel = connection.channel()
        
        # Command/Event Exchange deklarieren (z.B. urets_commands oder shopfloor.events)
        channel.exchange_declare(exchange='uretos_commands', exchange_type='topic', durable=True)
        
        channel.basic_publish(
            exchange='uretos_commands',
            routing_key='uretos.machine.command.create',
            body=json.dumps(cloudevent),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json"
            )
        )
        connection.close()
    except Exception as rmq_err:
        raise HTTPException(
            status_code=500,
            detail=f"Fehler beim Publizieren des CloudEvents an RabbitMQ: {str(rmq_err)}"
        )

    return {
        "status": "success",
        "event_id": event_id,
        "machine_id": machine_id,
        "message": "Maschine erfolgreich entdeckt und Registrierungs-Event an RabbitMQ gesendet."
    }