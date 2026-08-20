import os
import threading
from fastapi import FastAPI

# Importiere den Worker-Prozess aus worker.py
from worker import init_db_with_retry, start_query_worker

app = FastAPI(title="uRetOS Machine Query Service", version="0.1.0")

@app.on_event("startup")
def startup():
    init_db_with_retry()
    threading.Thread(target=start_query_worker, daemon=True).start()

@app.get("/healthz")
def health():
    return {"status": "ok", "service": "machine-query"}