import os
import json
import time
import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

def start_consumer():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    print("[lov-storage] Starting command consumer worker thread...", flush=True)

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
                    data = payload.get("data", payload)
                    print(f"[lov-storage] Received command [{method.routing_key}]: {data.get('code')}", flush=True)

                    # Project Domain Event for lov-query
                    event_payload = {
                        "event_id": payload.get("event_id"),
                        "correlation_id": payload.get("correlation_id"),
                        "category": data.get("category"),
                        "code": data.get("code"),
                        "translations": data.get("translations", {})
                    }

                    channel.basic_publish(
                        exchange="uretos_events",
                        routing_key="uretos.lov.event.created",
                        body=json.dumps(event_payload),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            content_type="application/json"
                        )
                    )
                    print(f"[lov-storage] Published event 'uretos.lov.event.created' for {data.get('category')}/{data.get('code')}", flush=True)

                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as ex:
                    print(f"[lov-storage] Error processing message: {ex}", flush=True)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue="lov_storage_commands", on_message_callback=on_command_received)
            channel.start_consuming()

        except Exception as e:
            print(f"[lov-storage] Connection/Worker error: {e}. Retrying in 5s...", flush=True)
            time.sleep(5)