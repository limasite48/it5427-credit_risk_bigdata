# Home Credit Default Risk: Big Data pipeline

Ingestion layer (Kafka + MinIO + supplementary macro data): see **[docs/INGESTION_MINIO_KAFKA.md](docs/INGESTION_MINIO_KAFKA.md)**.

```
infra/                 docker-compose (Kafka KRaft + MinIO), topic, bucket and user/policy creation
ingestion/             stage -> gen_schemas -> backfill (batch) | producer -> kafka_to_bronze (stream)
schemas/               JSON Schema per Kafka topic + example events
data/supplementary/    regional GDP, unemployment, CPI (simulated) + region lookup
scripts/eda.py         dataset EDA
```

## Quick start

```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt   # Linux/macOS: .venv/bin/...
# Raw Kaggle CSVs are auto-detected from project root _data/ (or data/raw/)

docker compose -f infra/docker-compose.yml up -d        # Kafka + MinIO + topics/buckets/users
.venv/Scripts/python -I ingestion/gen_macro.py          # macro data -> data/supplementary
.venv/Scripts/python -I ingestion/stage.py              # assign event times + envelope (~3 min)
.venv/Scripts/python -I ingestion/gen_schemas.py        # JSON Schemas + examples, self-checked
LAKE_MODE=minio .venv/Scripts/python -I ingestion/backfill.py --landing   # history -> MinIO

# two terminals:
LAKE_MODE=minio .venv/Scripts/python -I ingestion/kafka_to_bronze.py      # Kafka -> bronze sink
.venv/Scripts/python -I ingestion/producer.py                             # all of 12/2025 in ~11 min
```
