# services/gateway-api/src/consumers/opc_consumer.py
import json
import logging

logger = logging.getLogger(__name__)

# In-Memory Cache für Testzwecke (in Prod: Redis)
LATEST_METADATA_CACHE = {}

async def process_opc_ingest_message(message_body: bytes, routing_key: str):
    """Lauscht auf opc.ingest.{tenant_id}"""
    try:
        event = json.loads(message_body.decode("utf-8"))
        data = event.get("data", {})
        
        if data.get("payload_type") == "METADATA":
            tenant_id = data.get("tenant_id")
            payload = data.get("payload", {})
            
            # Stammdaten strukturieren
            metadata = {
                "tenant_id": tenant_id,
                "opc_url": payload.get("opc_url"),
                "nodes_found": len(payload.get("nodes", [])),
                "nodes": payload.get("nodes", []),
                "timestamp": event.get("time")
            }
            
            # Für Test-Abfrage im Cache ablegen
            LATEST_METADATA_CACHE[tenant_id] = metadata
            logger.info(f"[{tenant_id}] Stored raw METADATA in cache for UI rendering")

    except Exception as e:
        logger.error(f"Error handling opc ingest: {e}")