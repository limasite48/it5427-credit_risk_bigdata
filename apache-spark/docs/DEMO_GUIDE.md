# DEMO GUIDE

Suggested duration: 10–15 minutes. Run all commands from the `apache-spark/` directory.

## 0. Preparation (Before the Demo)

```bash
pip install -r requirements.txt
# Kaggle dataset: extract all 10 files into data/raw/ (do not commit to GitHub)
#   kaggle competitions download -c home-credit-default-risk -p data/raw && unzip data/raw/*.zip -d data/raw
# For a fast demo: extract a real 5,000-customer subset
python -m scripts.make_dev_subset --src data/raw --out data/dev_subset/raw --customers 5000
docker compose up -d          # MinIO + Kafka + Kafka UI
```

Without the Kaggle dataset or Docker, you can still demo using the test fixture:
`python -m scripts.make_test_fixture --out data/fixture/raw`, then append `--raw-dir data/fixture/raw` to the commands below. Make sure to inform viewers that this is **simulated test fixture data**.

## 1. Profiling (1 minute)

```bash
python -m processing.profiling.profile_raw --raw-dir data/dev_subset/raw
```
Show viewers:
- `reports/data_profiling/PROFILING_REPORT.md`: file summary, `TARGET` class imbalance, relationship table (orphan rows, measured cardinality).
- `reports/architecture/ERD.md`: ERD annotated with empirically measured numbers.

## 2. End-to-End Batch Pipeline (3–5 minutes)

```bash
python -m scripts.run_pipeline --raw-dir data/dev_subset/raw --skip-profiling --sync-minio
```
What to observe:

| Location | Expected Result |
|---|---|
| `ingestion` logs | Each table `SUCCESS` with row count; re-running shows `skipped (same sha256…)` for all |
| `silver` logs | `reconcile=True` for every table (`rows_in = out + quarantined + dups`) |
| `gold` logs | All `check … PASS` lines, especially `join …: row count unchanged` and `one row per sk_id_curr` |
| http://localhost:9001 (`minioadmin`/`minioadmin`) | Buckets `bronze/original/…`, `bronze/synthetic/…`, `silver/…`, `gold/…` |
| `docs/DATA_QUALITY_REPORT.md` | Before/after cleaning, orphan counts, rows dated after application excluded |
| `reports/PIPELINE_STATS.md` | Sizes, record counts, ingestion throughput, per-stage duration, feature counts |

Demonstrate idempotency: re-run the exact same command, then compare Gold row counts in `reports/PIPELINE_STATS.md` with the previous run. Both runs must match.

## 3. Streaming with Kafka (4–5 minutes, 3 terminals)

```bash
# Terminal A - continuous consumer, 5-second trigger
python -m streaming.consumers.stream_processor --source kafka --trigger 5 --reset-checkpoint
# Terminal B - producer at 20 events/s with fault injection
python -m streaming.producers.replay_producer --sink kafka --rate 20 \
   --sources installment_payment_system credit_card_system loan_application_system \
   --dup-rate 0.05 --late-rate 0.02 --malformed-rate 0.01 --from-start
# Terminal C - producer control
python -m streaming.producers.control pause     # Terminal A: subsequent batches receive 0 events
python -m streaming.producers.control resume
python -m streaming.producers.control stop
python -m streaming.consumers.snapshot          # current aggregated state per SK_ID_CURR
```
What to observe:
- Kafka UI (http://localhost:8081): messages arriving in topics `installment_payment_events`, etc.
- Terminal A: `batch N: X events {...}, avg latency …s` on each trigger.
- `reports/streaming/query_progress.jsonl`: `numDroppedDuplicateRows` (duplicates dropped), `numRowsDroppedByWatermark` (late events dropped).
- `silver/stream_quarantine`: malformed JSON events landed here with `reject_reason`.
- **Recovery:** Ctrl-C Terminal A, then restart without `--reset-checkpoint`. Consumer resumes from committed offsets without reprocessing old events.
- **Replay:** re-run producer with `--from-start`. Deterministic `event_id` ensures previously seen events are deduplicated.

Without Kafka, use file replay mode with identical processing logic:
```bash
python -m streaming.producers.replay_producer --sink file --rate 0 --max-events 3000 --dup-rate 0.05 --late-rate 0.02 --malformed-rate 0.01 --from-start
python -m streaming.consumers.stream_processor --source file --trigger available-now --reset-checkpoint
```
Expected result: `stream summary` at the end shows `duplicates_dropped` matching total `duplicates_injected` printed by the producer, and `quarantined_rows` matching `malformed_injected`. Late events are dropped starting from the second micro-batch onward because the watermark is established after the first batch.

## 4. Tests (1 minute)

```bash
python -m pytest            # 39 tests, ~2 minutes
```
