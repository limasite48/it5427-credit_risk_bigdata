FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system app \
    && useradd --system --gid app --home-dir /app --shell /usr/sbin/nologin app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src /app/src
COPY ml-service /app/ml-service
COPY outputs/models /app/outputs/models
COPY outputs/metrics /app/outputs/metrics
COPY outputs/scored_applications_sample.csv /app/outputs/scored_applications_sample.csv
COPY outputs/figures /app/outputs/figures
COPY sample_full_applicant.json sample_subprime_applicant.json sample_applicant.json /app/

ENV MODEL_PATH=/app/outputs/models/credit_risk_model.joblib \
    PYTHONPATH=/app

USER app

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=5s --start-period=40s --retries=5 \
  CMD curl -fsS http://127.0.0.1:8000/health || exit 1

CMD ["uvicorn", "ml-service.app:app", "--host", "0.0.0.0", "--port", "8000"]
