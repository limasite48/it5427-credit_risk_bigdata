# DEMO GUIDE

Thời lượng gợi ý: 10–15 phút. Mọi lệnh chạy ở thư mục gốc project.

## 0. Chuẩn bị (làm trước buổi demo)

```bash
pip install -r requirements.txt
# Dataset Kaggle: giải nén 10 file vào data/raw/ (không commit lên GitHub)
#   kaggle competitions download -c home-credit-default-risk -p data/raw && unzip data/raw/*.zip -d data/raw
# Muốn demo nhanh: cắt subset thật 5.000 khách hàng
python -m scripts.make_dev_subset --src data/raw --out data/dev_subset/raw --customers 5000
docker compose up -d          # MinIO + Kafka + Kafka UI
```

Không có dataset hoặc Docker vẫn demo được trên fixture:
`python -m scripts.make_test_fixture --out data/fixture/raw`, sau đó thêm `--raw-dir data/fixture/raw` vào các lệnh dưới. Nhớ nói rõ với người xem đây là **dữ liệu giả lập để test**.

## 1. Profiling (1 phút)

```bash
python -m processing.profiling.profile_raw --raw-dir data/dev_subset/raw
```
Cho người xem:
- `reports/data_profiling/PROFILING_REPORT.md`: bảng file, mất cân bằng TARGET, bảng quan hệ (orphan, cardinality đo được).
- `reports/architecture/ERD.md`: ERD có số liệu thực đo.

## 2. Batch pipeline end-to-end (3–5 phút)

```bash
python -m scripts.run_pipeline --raw-dir data/dev_subset/raw --skip-profiling --sync-minio
```
Cần quan sát:

| Ở đâu | Kết quả mong đợi |
|---|---|
| log `ingestion` | mỗi bảng `SUCCESS` kèm số dòng; chạy lại thì tất cả thành `skipped (same sha256…)` |
| log `silver` | `reconcile=True` ở mọi bảng (rows_in = out + quarantined + dups) |
| log `gold` | toàn bộ dòng `check … PASS`, đặc biệt `join …: row count unchanged` và `one row per sk_id_curr` |
| http://localhost:9001 (minioadmin/minioadmin) | bucket `bronze/original/…`, `bronze/synthetic/…`, `silver/…`, `gold/…` |
| `docs/DATA_QUALITY_REPORT.md` | trước/sau cleaning, orphan, dòng sau ngày nộp hồ sơ bị loại |
| `reports/PIPELINE_STATS.md` | dung lượng, số bản ghi, tốc độ ingest, thời gian từng stage, số feature |

Chứng minh tính idempotent: chạy lại đúng lệnh trên, rồi so số dòng Gold trong `reports/PIPELINE_STATS.md` với lần trước. Hai lần phải bằng nhau.

## 3. Streaming với Kafka (4–5 phút, 3 terminal)

```bash
# Terminal A - consumer chạy liên tục, trigger 5 giây
python -m streaming.consumers.stream_processor --source kafka --trigger 5 --reset-checkpoint
# Terminal B - producer 20 event/s, có chèn lỗi
python -m streaming.producers.replay_producer --sink kafka --rate 20 \
   --sources installment_payment_system credit_card_system loan_application_system \
   --dup-rate 0.05 --late-rate 0.02 --malformed-rate 0.01 --from-start
# Terminal C - điều khiển producer
python -m streaming.producers.control pause     # Terminal A: các batch không còn event
python -m streaming.producers.control resume
python -m streaming.producers.control stop
python -m streaming.consumers.snapshot          # trạng thái hiện tại theo SK_ID_CURR
```
Cần quan sát:
- Kafka UI (http://localhost:8080): message đến các topic `installment_payment_events`, …
- Terminal A: `batch N: X events {...}, avg latency …s` sau mỗi trigger.
- `reports/streaming/query_progress.jsonl`: `numDroppedDuplicateRows` (bản trùng bị loại), `numRowsDroppedByWatermark` (event trễ bị loại).
- `silver/stream_quarantine`: event sai JSON nằm ở đây kèm `reject_reason`.
- **Khôi phục:** Ctrl-C Terminal A, chạy lại không kèm `--reset-checkpoint`. Consumer tiếp tục từ offset đã commit, không xử lý lại event cũ.
- **Phát lại:** chạy lại producer với `--from-start`. Event ID tất định nên các event đã thấy bị loại là trùng.

Không có Kafka thì dùng chế độ file replay, với cùng logic:
```bash
python -m streaming.producers.replay_producer --sink file --rate 0 --max-events 3000 --dup-rate 0.05 --late-rate 0.02 --malformed-rate 0.01 --from-start
python -m streaming.consumers.stream_processor --source file --trigger available-now --reset-checkpoint
```
Kết quả mong đợi: dòng `stream summary` ở cuối có `duplicates_dropped` bằng đúng tổng `duplicates_injected` mà producer in ra. `quarantined_rows` bằng tổng `malformed_injected`. Event trễ chỉ bị loại từ micro-batch thứ hai trở đi, vì watermark chỉ được đặt sau batch đầu tiên.

## 4. Test (1 phút)

```bash
python -m pytest            # 39 test, khoảng 2 phút
```
