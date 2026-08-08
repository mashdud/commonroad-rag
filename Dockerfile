# Mirrors Databricks Runtime 15.4 LTS (Python 3.11, Spark 3.5) for local
# dev/test parity. This is NOT what runs the production job — Databricks
# Jobs run on job clusters via the Asset Bundle. This image is for:
#   - local unit testing (pytest)
#   - linting
#   - CI pipeline (see .github/workflows/ci.yml)
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
WORKDIR /app

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt

COPY pyproject.toml ./
COPY src/ ./src/
COPY tests/ ./tests/

RUN pip install --no-cache-dir -e .

CMD ["pytest", "tests/", "-v"]
