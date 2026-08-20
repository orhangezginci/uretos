import json
import os
import time
import pika
from sqlalchemy import Column, String, ForeignKey, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

DB_USER = os.getenv("POSTGRES_USER", "uretos")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "uretos_dev_pass")
DB_HOST = os.getenv("POSTGRES_HOST", "lov-query-db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "lov_query_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class LOVItemReadModel(Base):
    __tablename__ = "lov_read_projections"
    id = Column(String, primary_key=True)
    category = Column(String, nullable=False, index=True)
    code = Column(String, nullable=False, index=True)
    translations = relationship("LOVTranslationReadModel", back_populates="item", cascade="all, delete-orphan")

class LOVTranslationReadModel(Base):
    __tablename__ = "lov_translation_read_projections"
    id = Column(String, primary_key=True)
    lov_item_id = Column(String, ForeignKey("lov_read_projections.id"), nullable=False)
    locale = Column(String, nullable=False)
    value = Column(String, nullable=False)
    item = relationship("LOVItemReadModel", back_populates="translations")

Base.metadata.create_all(bind=engine)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER", "uretos")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "uretos_dev_pass")

def process_event(ch, method, properties, body):
    try:
        cloud_event = json.loads(body)
        msg_type = cloud_event.get("type", "")
        data = cloud_event.get("data", {})

        # CloudEvent-Unterstützung für Erstellungs-Events
        if "uretos.lov.event" in msg_type or msg_type.endswith(".created"):
            db = SessionLocal()
            try:
                lov_id = data.get("lov_id") or data.get("id")
                category = data.get("category")
                code = data.get("code")

                if lov_id and category and code:
                    existing = db.query(LOVItemReadModel).filter(LOVItemReadModel.id == lov_id).first()
                    if not existing:
                        item = LOVItemReadModel(
                            id=lov_id,
                            category=category,
                            code=code
                        )
                        db.add(item)
                        
                        translations = data.get("translations", [])
                        # Unterstützung für sowohl Listen von Translations als auch Dict-Werte (z.B. {"en-US": "..."})
                        if isinstance(translations, dict):
                            for locale, val in translations.items():
                                db.add(LOVTranslationReadModel(
                                    id=str(os.urandom(8).hex()),
                                    lov_item_id=lov_id,
                                    locale=locale,
                                    value=val
                                ))
                        elif isinstance(translations, list):
                            for tr in translations:
                                db.add(LOVTranslationReadModel(
                                    id=tr.get("id", str(os.urandom(8).hex())),
                                    lov_item_id=lov_id,
                                    locale=tr.get("locale"),
                                    value=tr.get("value")
                                ))
                        db.commit()
            finally:
                db.close()

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[lov-query-worker] Error processing event projection: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_event_consumer():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue="lov_query_events", durable=True)
            channel.queue_bind(queue="lov_query_events", exchange="uretos_events", routing_key="uretos.lov.event.*")
            channel.basic_consume(queue="lov_query_events", on_message_callback=process_event)
            channel.start_consuming()
        except Exception as e:
            print(f"[lov-query-worker] Connection waiting... ({e})")
            time.sleep(3)

if __name__ == "__main__":
    print("[lov-query-worker] Starting standalone event projection worker...", flush=True)
    start_event_consumer()