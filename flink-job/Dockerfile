# Phase 2: Flink Job Dockerfile
# Uses official Flink image (amd64) — works on Apple Silicon via Rosetta 2
# Installs PyFlink and submits the job on startup

FROM flink:1.18-scala_2.12-java11

# Install Python 3 + pip inside the Flink container
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

WORKDIR /opt/flink/job

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

# The entrypoint submits the PyFlink job to the JobManager
CMD ["python3", "job.py"]
