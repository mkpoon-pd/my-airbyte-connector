FROM python:3.9-slim

WORKDIR /app

COPY source_s3_postgres_loader ./source_s3_postgres_loader
COPY config ./config
COPY setup.py .
COPY MANIFEST.in .
COPY source_s3_postgres_loader/spec.yaml ./source_s3_postgres_loader/spec.yaml

RUN pip install .

ENTRYPOINT ["python", "source_s3_postgres_loader/main.py"]



