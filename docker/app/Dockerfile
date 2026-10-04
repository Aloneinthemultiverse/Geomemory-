# GeoMemory application image: Python 3.10 + Java 17 (for Spark/Sedona).
# Ubuntu 22.04 provides both as distro packages on amd64 and arm64.
FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive PYTHONUNBUFFERED=1
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      python3 python3-pip python3-venv openjdk-17-jre-headless curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s /usr/bin/python3 /usr/local/bin/python
WORKDIR /app
COPY pyproject.toml README.md ./
COPY geomemory ./geomemory
RUN python -m pip install --no-cache-dir --upgrade pip \
 && python -m pip install --no-cache-dir -e ".[all]"
COPY . .
ENV GEOMEMORY_PG_DSN=postgresql://geomemory:geomemory@db:5432/geomemory \
    GEOMEMORY_KAFKA=kafka:19092
EXPOSE 8765 8000
CMD ["python", "-m", "geomemory.demo", "--host", "0.0.0.0", "--port", "8765"]
