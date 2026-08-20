import os
import json
import uuid
import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

def test_validation():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    )
    channel = connection.channel()

    result = channel.queue_declare(queue='', exclusive=True)
    callback_queue = result.method.queue

    response = None
    correlation_id = str(uuid.uuid4())

    def on_response(ch, method, props, body):
        nonlocal response
        if correlation_id == props.correlation_id:
            response = json.loads(body)

    channel.basic_consume(
        queue=callback_queue,
        on_message_callback=on_response,
        auto_ack=True
    )

    # Test mit einer gefakten (und somit ungültigen) UUID
    fake_uuid = str(uuid.uuid4())
    payload = {
        "action": "validate_lov",
        "correlationid": correlation_id,
        "data": {
            "ids": [fake_uuid]
        }
    }

    print(f"Sende Test-Validierung für ID: {fake_uuid}...")
    channel.basic_publish(
        exchange='uretos_rpc',
        routing_key='lov.validate',
        properties=pika.BasicProperties(
            reply_to=callback_queue,
            correlation_id=correlation_id,
            content_type="application/json",
        ),
        body=json.dumps(payload)
    )

    while response is None:
        connection.process_data_events(time_limit=0.1)

    connection.close()
    print("Antwort vom lov-validation Dienst:", json.dumps(response, indent=2))
    
    # Erwartung: valid muss False sein und die ID in invalid_ids auftauchen
    assert response.get("valid") is False
    assert fake_uuid in response.get("invalid_ids", [])
    print("Test erfolgreich: Unbekannte ID wurde korrekt als ungültig erkannt!")

if __name__ == "__main__":
    test_validation()