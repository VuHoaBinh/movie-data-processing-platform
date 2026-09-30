FROM python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir --timeout 120 --retries 10 -r requirements.txt

COPY app.py ./
COPY src ./src

EXPOSE 8501

CMD ["sh", "-c", "python -m src.batch_processing && python -m src.stream_processing && python -m streamlit run app.py --server.address=0.0.0.0 --server.port=8501"]
