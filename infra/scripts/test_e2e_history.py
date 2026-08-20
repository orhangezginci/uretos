import os
import json
import uuid
import time
import pika

# Konfiguration (entsprechend deiner Umgebung)
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

EXCHANGE_COMMANDS = "uretos_commands"
ROUTING_KEY_GET_HISTORY = "uretos.telemetry.query.get_history"

def test_machine_history_rpc():
    print("[E2E-TEST] Starte End-to-End Test für machine-history-query...")

    # Verbindung zu RabbitMQ aufbauen
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    )
    channel = connection.channel()

    # Exklusive Antwort-Queue für den RPC-Call deklarieren (Direct Reply-To oder temporäre Queue)
    result = channel.queue_declare(queue='', exclusive=True)
    reply_queue = result.method.queue

    correlation_id = str(uuid.uuid4())
    test_machine_id = "11111111-2222-3333-4444-555555555555" # Test-UUID oder eine echte aus deiner DB einsetzen

    # Payload für den RPC-Request
    request_payload = {
        "action": "get_telemetry_history",
        "data": {
            "machine_id": test_machine_id,
            "limit": 5
        }
    }

    response_received = {"data": None}

    def on_response(ch, method, props, body):
        if props.correlation_id == correlation_id:
            response_received["data"] = json.loads(body)

    channel.basic_consume(
        queue=reply_queue,
        on_message_callback=on_response,
        auto_ack=True
    )

    print(f"[E2E-TEST] Sende RPC-Request an Routing Key: {ROUTING_KEY_GET_HISTORY} (Correlation ID: {correlation_id})")

    # Nachricht an den Command-Exchange senden
    channel.basic_publish(
        exchange=EXCHANGE_COMMANDS,
        routing_key=ROUTING_KEY_GET_HISTORY,
        body=json.dumps(request_payload),
        properties=pika.BasicProperties(
            reply_to=reply_queue,
            correlation_id=correlation_id,
            delivery_mode=2
        )
    )

    # Warten auf Antwort (Timeout nach 5 Sekunden)
    start_time = time.time()
    while response_received["data"] is None:
        connection.process_data_events(time_limit=1.0)
        if time.time() - start_time > 5.0:
            print("[E2E-TEST] TIMEOUT: Keine Antwort vom machine-history-query Service erhalten!")
            connection.close()
            return False

    connection.close()

    # Ergebnis auswerten
    res = response_received["data"]
    print("[E2E-TEST] Erfolgreich Antwort erhalten!")
    print(json.dumps(res, indent=2))

    assert "history" in res, "Antwort enthält kein 'history'-Feld!"
    assert "count" in res, "Antwort enthält kein 'count'-Feld!"
    print(f"[E2E-TEST] Test erfolgreich bestanden! {res['count']} Datensätze geladen.")
    return True

if __name__ == "__main__":
    test_machine_history_rpc()