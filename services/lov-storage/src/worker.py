from datetime import datetime, timezone
import json
import os
import time
import uuid
import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

def start_consumer():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    print("[lov-storage] Starting command consumer worker thread (CloudEvent-aware)...", flush=True)

    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
            channel = connection.channel()

            # Command Exchange & Queue setup
            channel.exchange_declare(exchange="uretos_commands", exchange_type="topic", durable=True)
            channel.queue_declare(queue="lov_storage_commands", durable=True)
            channel.queue_bind(exchange="uretos_commands", queue="lov_storage_commands", routing_key="uretos.lov.command.#")
            channel.queue_bind(exchange="uretos_commands", queue="lov_storage_commands", routing_key="uretos.lov.#")

            # Domain Event Exchange setup
            channel.exchange_declare(exchange="uretos_events", exchange_type="topic", durable=True)

            print("[lov-storage] Connected to RabbitMQ. Listening on 'lov_storage_commands'...", flush=True)

            def on_command_received(ch, method, properties, body):
                try:
                    payload = json.loads(body)
                    
                    # CloudEvent-Attribute robust extrahieren
                    correlation_id = payload.get("correlationid") or properties.correlation_id or str(uuid.uuid4())
                    event_id = payload.get("id") or str(uuid.uuid4())
                    
                    # Daten-Payload isolieren (unterstützt sowohl CloudEvent 'data' als auch flache Payloads)
                    data = payload.get("data", payload)
                    category = data.get("category")
                    code = data.get("code")
                    translations = data.get("translations", {})
                    
                    print(f"[lov-storage] Received command [{method.routing_key}]: {category}/{code}", flush=True)

                    # Konformes uRetOS CloudEvent v1.0 für das Domain-Event zusammenbauen
                    cloudevent_payload = {
                        "specversion": "1.0",
                        "id": str(uuid.uuid4()),
                        "type": "uretos.lov.event.created",
                        "source": "uretos.lov-storage",
                        "subject": f"lov:{category}:{code}",
                        "time": datetime.now(timezone.utc).isoformat(),
                        "datacontenttype": "application/json",
                        "correlationid": correlation_id,
                        "messagetype": "event",
                        "data": {
                            "lov_id": data.get("lov_id") or data.get("id") or str(uuid.uuid4()),
                            "category": category,
                            "code": code,
                            "translations": translations
                        }
                    }

                    channel.basic_publish(
                        exchange="uretos_events",
                        routing_key="uretos.lov.event.created",
                        body=json.dumps(cloudevent_payload),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            content_type="application/json",
                            correlation_id=correlation_id
                        )
                    )
                    print(f"[lov-storage] Published CloudEvent 'uretos.lov.event.created' for {category}/{code} (CorrID: {correlation_id})", flush=True)

                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as ex:
                    print(f"[lov-query / storage] Error processing message: {ex}", flush=True)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue="lov_storage_commands", on_message_callback=on_command_received)
            channel.start_consuming()

        except Exception as e:
            print(f"[lov-storage] Connection/Worker error: {e}. Retrying in 5s...", flush=True)
            time.sleep(5)