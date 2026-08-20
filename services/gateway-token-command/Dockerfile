FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kopiere den src-Ordner
COPY src/ src/

CMD ["python", "src/worker.py"]