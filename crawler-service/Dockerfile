FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Data dir for persistent state file
RUN mkdir -p /data

# CRAWLER_MODE env var controls the mode (live|backfill).
# Default: live.
# Override at runtime: docker run -e CRAWLER_MODE=backfill ...
# Or with CLI: docker run ... python main.py --mode backfill --limit 50
CMD ["python", "-u", "main.py"]
