FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-spark.txt ./
RUN python -m pip install --no-cache-dir --timeout 120 --retries 10 -r requirements.txt -r requirements-spark.txt

COPY app.py ./
COPY src ./src

EXPOSE 8501

CMD ["sh", "-c", "python -m src.spark_batch_job && python -m streamlit run app.py --server.address=0.0.0.0 --server.port=8501"]
