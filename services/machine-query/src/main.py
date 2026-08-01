from datetime import datetime, timezone
import os
import threading
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import Column, String, DateTime, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from src.worker import start_event_consumer

app = FastAPI(title="uretOS Machine Query Service", version="0.1.0")

SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.1.0")

# Read DB Setup
DB_USER = os.getenv("POSTGRES_USER", "uretos")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "uretos_dev_pass")
DB_HOST = os.getenv("POSTGRES_HOST", "machine-query-db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "machine_query_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class MachineReadModel(Base):
    __tablename__ = "machine_read_projections"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    serial_number = Column(String, nullable=False)
    machine_type = Column(String, nullable=False)
    machine_manufacturer = Column(String, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow)

class MachineResponse(BaseModel):
    id: str
    name: str
    serial_number: str
    machine_type: str
    machine_manufacturer: str
    updated_at: datetime

@app.on_event("startup")
def startup_event():
    consumer_thread = threading.Thread(target=start_event_consumer, daemon=True)
    consumer_thread.start()

# --- Standard Operational Endpoints ---

@app.get("/healthy")
def healthy():
    return {
        "status": "healthy",
        "service": "machine-query",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/version")
def version():
    return {
        "service": "machine-query",
        "version": SERVICE_VERSION
    }

# --- Read Query Endpoints ---

@app.get("/api/v1/machines", response_model=List[MachineResponse])
def list_machines():
    db = SessionLocal()
    try:
        machines = db.query(MachineReadModel).all()
        return machines
    finally:
        db.close()

@app.get("/api/v1/machines/{machine_id}", response_model=MachineResponse)
def get_machine(machine_id: str):
    db = SessionLocal()
    try:
        machine = db.query(MachineReadModel).filter(MachineReadModel.id == machine_id).first()
        if not machine:
            raise HTTPException(status_code=404, detail="Machine projection not found")
        return machine
    finally:
        db.close()