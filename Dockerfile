FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libpq-dev \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

CMD ["bash"]

