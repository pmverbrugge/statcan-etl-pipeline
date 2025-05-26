FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libpq-dev \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ✅ This stays cached until requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 🔄 These change more often — placed after pip install
COPY ./scripts /app/scripts
COPY ./lib /app/lib

ENV PYTHONPATH=/app

CMD ["bash"]

