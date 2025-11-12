FROM python:3.12-slim

WORKDIR /app

# ---------- System dependencies ----------
# (optional: tzdata helps with datetime, curl helps with debugging)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8002

ENV PYTHONUNBUFFERED=1
ENV REFRESH_INTERVAL=900  

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
