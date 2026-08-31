FROM python:3.10-slim

WORKDIR /app

# Install dependencies first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Wait for environment variables to be injected and run the generator
CMD ["python", "-u", "stream_to_pubsub.py"]
