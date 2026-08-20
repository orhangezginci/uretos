import os
import json
import uuid
import pika

class LOVValidationRPCClient:
    def __init__(self):
        self.host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        self.port = int(os.getenv("RABBITMQ_PORT", "5672"))
        self.user = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
        self.password = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")
        
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.response = None
        self.correlation_id = None

    def _connect(self):
        credentials = pika.PlainCredentials(self.user, self.password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port, credentials=credentials)
        )
        self.channel = self.connection.channel()

        # Exklusive Reply-To Queue für die Antwort
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

    def validate_ids(self, ids: list) -> dict:
        self.response = None
        self.correlation_id = str(uuid.uuid4())

        if not self.connection or self.connection.is_closed:
            self._connect()

        message = {
            "action": "validate_lov",
            "data": {
                "ids": ids
            }
        }

        # Nachricht an den lov-validation Service senden (über Direct Exchange 'uretos_rpc')
        self.channel.basic_publish(
            exchange='uretos_rpc',
            routing_key='lov.validate',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.correlation_id,
                content_type="application/json"
            ),
            body=json.dumps(message)
        )

        # Warten auf die Antwort (blockiert kurz, bis RabbitMQ antwortet)
        while self.response is None:
            self.connection.process_data_events(time_limit=0.1)

        return self.response