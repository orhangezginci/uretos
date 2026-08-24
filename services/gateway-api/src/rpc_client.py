import os
import json
import uuid
import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

EXCHANGE_RPC = "uretos_rpc"

class TokenValidatorRpcClient:
    def __init__(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials
            )
        )
        self.channel = self.connection.channel()
        
        self.channel.exchange_declare(exchange=EXCHANGE_RPC, exchange_type="direct", durable=True)
        
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )
        self.response = None
        self.correlation_id = None

    def on_response(self, ch, method, properties, body):
        if self.correlation_id == properties.correlation_id:
            self.response = json.loads(body)

    def call(self, token_id: str):
        self.response = None
        self.correlation_id = str(uuid.uuid4())
        
        payload = {
            "data": {
                "token_id": token_id
            }
        }

        self.channel.basic_publish(
            exchange=EXCHANGE_RPC,
            routing_key="token.validate",
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.correlation_id,
                content_type="application/json"
            ),
            body=json.dumps(payload)
        )

        while self.response is None:
            self.connection.process_data_events(time_limit=1.0)
            
        return self.response