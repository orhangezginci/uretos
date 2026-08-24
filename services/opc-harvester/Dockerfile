FROM python:3.11-slim

WORKDIR /app

# Copy dependency requirements first for caching
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code (including src/)
COPY . /app/

# Option B entrypoint
CMD ["python", "src/main.py"]