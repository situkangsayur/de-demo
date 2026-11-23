# Ingestor NATS JetStream -> bronze Parquet di RustFS. PyArrow dipilih karena penulis Parquet
# paling matang (schema eksplisit, kompresi zstd) dan punya klien S3 bawaan (tanpa boto3).
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY ingestor ./ingestor
RUN useradd -u 10001 -m app
USER app
EXPOSE 8080
CMD ["python", "-m", "ingestor.main"]
