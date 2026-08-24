import asyncio
import json
import logging
import os
import socket
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

import aio_pika
from asyncua import Client

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("uretos-opc-harvester")

# Environment Variables
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://uretos:uretos_pass@rabbitmq:5672/")
EXCHANGE_NAME = "amq.topic"


async def browse_node_recursive(node, max_depth: int = 3, current_depth: int = 0) -> Dict[str, Any]:
    """
    Rekursiver OPC UA Node Browser mit Tiefenbegrenzung und expliziter Typ-Konvertierung.
    """
    if current_depth > max_depth:
        return None

    try:
        node_class = await node.read_node_class()
        browse_name = await node.read_browse_name()
        display_name = await node.read_display_name()
    except Exception as err:
        logger.warning(f"Could not read node attributes: {err}")
        return None

    node_class_str = str(node_class)
    
    # System-Nodes filtern (Server-Eigene Nodes überspringen)
    browse_name_str = str(browse_name.Name) if hasattr(browse_name, 'Name') else str(browse_name)
    if browse_name_str in ["Server", "ServerType", "ServerArray", "NamespaceArray"]:
        return None

    node_info = {
        "node_id": str(node.nodeid),
        "browse_name": browse_name_str,
        "display_name": str(display_name.Text) if hasattr(display_name, 'Text') else str(display_name),
        "node_class": node_class_str,
        "children": []
    }
    
    if "Variable" in node_class_str:
        try:
            val = await node.read_value()
            node_info["value"] = str(val) if val is not None else None
        except Exception:
            node_info["value"] = None

    if current_depth < max_depth:
        try:
            children = await node.get_children()
            for child in children:
                child_class = await child.read_node_class()
                if any(c in str(child_class) for c in ["Object", "Variable"]):
                    child_info = await browse_node_recursive(child, max_depth, current_depth + 1)
                    if child_info:
                        node_info["children"].append(child_info)
        except Exception as err:
            logger.warning(f"Failed to browse children for node {node.nodeid}: {err}")

    return node_info


async def handle_connect_command(command_data: Dict[str, Any], exchange: aio_pika.RobustExchange):
    """Verarbeitet den Connect/Harvest Command eines Tenants."""
    tenant_id = command_data.get("tenant_id")
    opc_url = command_data.get("opc_url")
    
    logger.info(f"[{tenant_id}] Connecting to OPC Server: {opc_url}")
    
    try:
        async with Client(url=opc_url) as client:
            objects = client.get_objects_node()
            
            # Traversiere gezielt unterhalb des Objects Folders
            children = await objects.get_children()
            nodes_tree: List[Dict[str, Any]] = []
            
            for child in children:
                node_data = await browse_node_recursive(child, max_depth=3)
                if node_data:
                    nodes_tree.append(node_data)
            
            event_id = str(uuid.uuid4())
            timestamp = datetime.now(timezone.utc).isoformat()
            
            payload_data = {
                "tenant_id": str(tenant_id),
                "opc_url": str(opc_url),
                "status": "connected",
                "nodes": nodes_tree
            }
            
            # Striktes CloudEvent v1.0 Payload
            cloudevent = {
                "specversion": "1.0",
                "id": event_id,
                "type": "uretos.opc.event.metadata",
                "source": f"uretos/opc-harvester/{tenant_id}",
                "tenant_id": str(tenant_id),
                "time": timestamp,
                "datacontenttype": "application/json",
                "data": payload_data
            }
            
            metadata_routing_key = f"opc.metadata.{tenant_id}"
            
            await exchange.publish(
                aio_pika.Message(
                    body=json.dumps(cloudevent, ensure_ascii=False).encode("utf-8"),
                    content_type="application/json",
                    headers={
                        "tenant_id": str(tenant_id),
                        "event_type": "uretos.opc.event.metadata",
                        "event_id": event_id
                    }
                ),
                routing_key=metadata_routing_key
            )
            logger.info(f"[{tenant_id}] Forwarded CloudEvent METADATA to '{metadata_routing_key}' on '{EXCHANGE_NAME}'")

    except Exception as e:
        logger.error(f"[{tenant_id}] Error harvesting OPC UA Server '{opc_url}': {e}", exc_info=True)


async def main():
    logger.info("[*] Starting Harvester service initialization...")
    
    connection = None
    while not connection:
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
        except (socket.gaierror, aio_pika.exceptions.AMQPConnectionError) as e:
            logger.warning(f"RabbitMQ DNS/Broker not ready yet ({e}). Retrying in 5 seconds...")
            await asyncio.sleep(5)

    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC, passive=True)
        
        queue = await channel.declare_queue("uretos_opc_harvester_commands", durable=True)
        command_routing_key = "opc.command.#"
        await queue.bind(exchange, routing_key=command_routing_key)
        
        logger.info(f"[*] Harvester online. Listening on '{EXCHANGE_NAME}' -> '{command_routing_key}'")
        
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        cloudevent = json.loads(message.body.decode("utf-8"))
                        
                        # CloudEvent v1.0 Validierung (Verwirft unvollständige Payloads)
                        if cloudevent.get("specversion") != "1.0" or "data" not in cloudevent:
                            logger.error(f"Rejected non-CloudEvent message: {cloudevent}")
                            continue

                        command_data = cloudevent["data"]
                        
                        if not command_data.get("tenant_id") or not command_data.get("opc_url"):
                            logger.error(f"Rejected CloudEvent missing required data attributes: {command_data}")
                            continue

                        logger.info(f"Received valid CloudEvent '{cloudevent.get('id')}' on '{message.routing_key}'")
                        await handle_connect_command(command_data, exchange)

                    except Exception as err:
                        logger.error(f"Failed to process message envelope: {err}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Harvester stopped.")