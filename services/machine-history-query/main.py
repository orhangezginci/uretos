import os
import json
import time
import pika
from fastapi import FastAPI
from sqlalchemy import create_engine, text

# --- DB Setup (TimescaleDB / PostgreSQL) ---
DB_USER = os.getenv("POSTGRES_USER", "uretos")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "uretos_secure_password")
DB_HOST = os.getenv("POSTGRES_HOST", "timescaledb")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "uretos_telemetry")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# --- RabbitMQ Setup ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

EXCHANGE_COMMANDS = "uretos_commands"
QUEUE_RPC_HISTORY = "machine_history_rpc_requests"
ROUTING_KEY_GET_HISTORY = "uretos.telemetry.query.get_history"


def start_history_query_worker():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

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

            channel.exchange_declare(exchange=EXCHANGE_COMMANDS, exchange_type="topic", durable=True)
            channel.queue_declare(queue=QUEUE_RPC_HISTORY, durable=True)
            channel.queue_bind(exchange=EXCHANGE_COMMANDS, queue=QUEUE_RPC_HISTORY, routing_key=ROUTING_KEY_GET_HISTORY)

            print("[machine-history-query] Connected. Ready to query TimescaleDB for history...", flush=True)

            def handle_rpc_request(ch, method, properties, body):
                try:
                    payload = json.loads(body)
                    action = payload.get("action")
                    data = payload.get("data", {})

                    response_data = {}

                    if action == "get_telemetry_history":
                        machine_id = data.get("machine_id")
                        limit = data.get("limit", 50)

                        query = text("""
                            SELECT time, machine_code, machine_id, status, temperature, rpm 
                            FROM machine_telemetry 
                            WHERE machine_id = :machine_id 
                            ORDER BY time DESC 
                            LIMIT :limit
                        """)

                        history_records = []
                        with engine.connect() as conn:
                            result = conn.execute(query, {"machine_id": machine_id, "limit": limit})
                            for row in result:
                                history_records.append({
                                    "timestamp": row.time.isoformat() if row.time else None,
                                    "machine_code": row.machine_code,
                                    "machine_id": str(row.machine_id) if row.machine_id else None,
                                    "status": row.status,
                                    "temperature": row.temperature,
                                    "rpm": row.rpm
                                })

                        response_data = {
                            "machine_id": machine_id,
                            "count": len(history_records),
                            "history": history_records
                        }

                    if properties.reply_to and properties.correlation_id:
                        ch.basic_publish(
                            exchange='',
                            routing_key=properties.reply_to,
                            properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                            body=json.dumps(response_data)
                        )

                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    print(f"[machine-history-query] Error querying TimescaleDB: {e}", flush=True)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_consume(queue=QUEUE_RPC_HISTORY, on_message_callback=handle_rpc_request)
            channel.start_consuming()

        except Exception as e:
            print(f"[machine-history-query] Connection error: {e}. Retrying in 5s...", flush=True)
            time.sleep(5)


# --- FastAPI ---
app = FastAPI(title="uRetOS Machine History Query Service", version="0.1.0")

@app.on_event("startup")
def startup():
    import threading
    threading.Thread(target=start_history_query_worker, daemon=True).start()

@app.get("/healthz")
def health():
    return {"status": "ok", "service": "machine-history-query"}