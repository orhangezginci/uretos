from datetime import datetime, timezone
import os
import threading
from fastapi import FastAPI
from src.worker import start_consumer

app = FastAPI(title="uretOS LOV Storage Service", version="0.1.0")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.1.0")

@app.on_event("startup")
def startup_event():
    consumer_thread = threading.Thread(target=start_consumer, daemon=True)
    consumer_thread.start()

@app.get("/healthy")
def healthy():
    return {"status": "healthy", "service": "lov-storage", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.get("/version")
def version():
    return {"service": "lov-storage", "version": SERVICE_VERSION}