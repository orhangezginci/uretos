import os
import json
import uuid
import logging
import asyncio
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("enricher-service")

# --- Configuration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")
RABBITMQ_URL = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"

EXCHANGE_TOPIC = "amq.topic"
EXCHANGE_COMMANDS = "uretos_commands"
EXCHANGE_EVENTS = "uretos_events"


class LovQueryClient:
    """RPC Client für die Kommunikation mit dem lov-query Dienst via RabbitMQ."""
    def __init__(self, channel: aio_pika.Channel):
        self.channel = channel
        self.callback_queue = None
        self.futures = {}

    async def init(self):
        # Temporäre Exclusive Queue für RPC Replies
        self.callback_queue = await self.channel.declare_queue(exclusive=True)
        await self.callback_queue.consume(self.on_response)

    async def on_response(self, message: aio_pika.IncomingMessage):
        async with message.process():
            corr_id = message.correlation_id
            if corr_id in self.futures:
                future = self.futures.pop(corr_id)
                data = json.loads(message.body.decode("utf-8"))
                future.set_result(data)

    async def resolve_or_create_lov_id(self, category: str, raw_code: str) -> str:
        correlation_id = str(uuid.uuid4())
        future = asyncio.get_event_loop().create_future()
        self.futures[correlation_id] = future

        # Helper zur String-Normalisierung (z.B. "DMG MORI" -> "dmgmori")
        def normalize(val: str) -> str:
            if not val:
                return ""
            return val.lower().replace(" ", "").replace("_", "").replace("-", "")

        norm_raw = normalize(raw_code)

        # 1. RPC Query Payload an lov-query senden
        query_payload = {
            "specversion": "1.0",
            "type": "uretos.lov.query.resolve",
            "messagetype": "query",
            "correlationid": correlation_id,
            "data": {
                "category": category,
                "lang": "en-US"
            }
        }

        commands_exchange = await self.channel.get_exchange(EXCHANGE_COMMANDS)
        await commands_exchange.publish(
            aio_pika.Message(
                body=json.dumps(query_payload).encode("utf-8"),
                content_type="application/json",
                correlation_id=correlation_id,
                reply_to=self.callback_queue.name
            ),
            routing_key=f"uretos.lov.query.{category.lower()}"
        )

        try:
            # 500ms Timeout für RPC Response
            response_data = await asyncio.wait_for(future, timeout=0.5)
            for item in response_data:
                item_code = item.get("code", "")
                
                # Match auf technischen Code
                if normalize(item_code) == norm_raw:
                    return item.get("id")

                # Fallback: Match auf vorhandene Translations (de-DE, en-US, etc.)
                translations = item.get("translations", {})
                if any(normalize(str(val)) == norm_raw for val in translations.values()):
                    return item.get("id")

        except asyncio.TimeoutError:
            logger.warning(f"Timeout querying lov-query for category '{category}'. Fallback triggered.")

        # 2. Falls nicht gefunden: Auto-Registration Event feuern (Auto-Create in lov-query)
        new_lov_id = str(uuid.uuid4())
        creation_event = {
            "specversion": "1.0",
            "type": "uretos.lov.event.created",
            "source": "uretos/enricher-service",
            "correlationid": correlation_id,
            "data": {
                "lov_id": new_lov_id,
                "category": category,
                "code": raw_code,
                "translations": {
                    "de-DE": raw_code,
                    "en-US": raw_code
                }
            }
        }

        events_exchange = await self.channel.get_exchange(EXCHANGE_EVENTS)
        await events_exchange.publish(
            aio_pika.Message(
                body=json.dumps(creation_event).encode("utf-8"),
                content_type="application/json"
            ),
            routing_key="uretos.lov.event.created"
        )
        logger.info(f"Published auto-creation event for LOV '{category}/{raw_code}' with UUID {new_lov_id}")
        return new_lov_id


async def start_enricher_worker():
    connection = None
    while not connection:
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
        except Exception as e:
            logger.warning(f"RabbitMQ Connection pending... Retrying in 3s ({e})")
            await asyncio.sleep(3)

    async with connection:
        channel = await connection.channel()

        # Exchanges sicherstellen
        topic_exchange = await channel.declare_exchange(EXCHANGE_TOPIC, aio_pika.ExchangeType.TOPIC, passive=True)
        await channel.declare_exchange(EXCHANGE_COMMANDS, aio_pika.ExchangeType.TOPIC, durable=True)
        await channel.declare_exchange(EXCHANGE_EVENTS, aio_pika.ExchangeType.TOPIC, durable=True)

        # RPC Client für LOV Query initialisieren
        lov_client = LovQueryClient(channel)
        await lov_client.init()

        queue = await channel.declare_queue("uretos_metadata_enricher_queue", durable=True)
        await queue.bind(topic_exchange, routing_key="opc.raw.metadata.#")

        logger.info("[*] Enricher Worker connected to lov-query RPC. Listening on amq.topic -> opc.raw.metadata.#")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        raw_event = json.loads(message.body.decode("utf-8"))
                        tenant_id = raw_event.get("tenant_id") or "tenant_default"
                        data = raw_event.get("data", {})

                        raw_manufacturer = data.get("raw_manufacturer", "UNKNOWN")
                        raw_device_class = data.get("raw_device_class", "UNKNOWN")

                        # Auflösung der UUIDs direkt über den echten lov-query Dienst (mit exakten DB-Kategorien)
                        manufacturer_lov_id = await lov_client.resolve_or_create_lov_id("manufacturer", raw_manufacturer)
                        device_class_lov_id = await lov_client.resolve_or_create_lov_id("machine_type", raw_device_class)

                        enriched_event = {
                            "specversion": "1.0",
                            "id": str(uuid.uuid4()),
                            "type": "uretos.opc.event.enriched-metadata",
                            "source": "uretos/services/metadata-enricher",
                            "tenant_id": tenant_id,
                            "time": datetime.now(timezone.utc).isoformat(),
                            "data": {
                                "tenant_id": tenant_id,
                                "opc_url": data.get("opc_url"),
                                "machine_id": data.get("machine_id"),
                                "manufacturer_lov_id": manufacturer_lov_id,
                                "device_class_lov_id": device_class_lov_id,
                                "model_name": data.get("raw_model", "N/A"),
                                "serial_number": data.get("serial_number", "N/A"),
                                "status": "connected"
                            }
                        }

                        # Angereichertes Event an Gateway publizieren
                        out_routing_key = f"opc.metadata.{tenant_id}"
                        await topic_exchange.publish(
                            aio_pika.Message(
                                body=json.dumps(enriched_event).encode("utf-8"),
                                content_type="application/json",
                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                            ),
                            routing_key=out_routing_key
                        )
                        logger.info(f"Enriched event routed to {out_routing_key} (LOVs: {manufacturer_lov_id}, {device_class_lov_id})")

                    except Exception as err:
                        logger.error(f"Failed to process metadata event: {err}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    worker_task = asyncio.create_task(start_enricher_worker())
    yield
    worker_task.cancel()

app = FastAPI(title="uRetOS Metadata Enricher", version="0.1.0", lifespan=lifespan)

@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "metadata-enricher"}

@app.get("/version")
def get_version():
    return {
        "service": "metadata-enricher",
        "version": "0.1.0",
        "environment": os.getenv("ENV", "development")
    }