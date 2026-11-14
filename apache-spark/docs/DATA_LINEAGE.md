# DATA LINEAGE

## 1. Từ nguồn đến Gold

| # | Từ | Đến | Biến đổi | Code |
|---|---|---|---|---|
| 1 | `data/raw/*.csv` (Kaggle) | `bronze/original/<source>/<table>/ingest_id=…/` | Copy nguyên trạng. Ghi sha256, số dòng, header, kết quả kiểm tra theo chunk. Bỏ qua nếu sha256 đã có. | `ingestion/<source>/ingest.py` → `ingestion/base.py` |
| 2 | SK_ID_CURR của application | `data/synthetic/synthetic_traffic_profile.csv` | Sinh SYNTHETIC có seed, mỗi SK_ID_CURR thật một dòng | `ingestion/synthetic_source/generator.py` |
| 3 | `data/synthetic/*` | `bronze/synthetic/…` | như bước 1 | `ingestion/synthetic_source/ingest.py` |
| 4 | Bronze (snapshot mới nhất theo manifest) | `silver/<table>` + `silver/_quarantine/<table>` | Ép kiểu, chuẩn hóa tên cột/giá trị, 365243 → NULL + flag, XNA → NULL, Y/N → 1/0, bỏ trùng tuyệt đối, PK dedup, flag âm/outlier/orphan/after_application, thêm cột lineage | `processing/cleaning/clean_silver.py` |
| 5 | `silver/*` | `gold/loan_credit_features` | Aggregate installments (qua mức kỳ trả góp), POS, card về SK_ID_PREV, rồi LEFT JOIN vào previous_application | `processing/integration/build_gold.py`, `processing/aggregation/aggregates.py` |
| 6 | `silver/*` | `gold/customer_credit_features` | Aggregate từng nguồn về SK_ID_CURR, rồi LEFT JOIN vào danh sách application. Thiếu bản ghi thì count = 0, ratio = NULL. | như trên |
| 7 | application + customer features | `gold/integrated_credit_profile` | LEFT JOIN + feature engineering, bỏ TARGET, partition theo `dataset_split` | `processing/feature_engineering/features.py` |
| 8 | `silver/application_train` | `gold/training_labels` | chỉ (sk_id_curr, target) | `build_gold.py` |
| 9 | metrics Silver + kiểm tra Gold + profiling | `gold/data_quality_report`, `docs/DATA_QUALITY_REPORT.md` | gộp dạng long format | `processing/integration/dq_report.py` |
| 10 | Bronze (snapshot mới nhất) | Kafka topic / `data/_stream_landing/*.jsonl` | mỗi dòng thành một event (envelope + payload), bỏ TARGET | `streaming/producers/replay_producer.py` |
| 11 | event | `silver/stream_events`, `silver/stream_quarantine`, `gold/stream_customer_activity_delta`, `gold/stream_batch_metrics` | parse, quarantine, watermark, dedup event_id, aggregate theo SK_ID_CURR từng micro-batch | `streaming/consumers/stream_processor.py` |
| 12 | `gold/stream_customer_activity_delta` | `gold/stream_customer_snapshot` | gộp lại (sum/max) theo SK_ID_CURR | `streaming/consumers/snapshot.py` |
| 13 | `data/{bronze,silver,gold}` | MinIO buckets `bronze`, `silver`, `gold` | đồng bộ incremental (size + mtime) | `storage/minio/sync_lake.py` |

Mỗi dòng Silver mang `_source_id`, `_origin`, `_ingest_id`, `_ingested_at`. Nhờ đó luôn truy được dòng đó đến từ snapshot Bronze nào, tức file Kaggle nào, với sha256 nào.

## 2. Quan hệ giữa các logical source

| Logical source | Bảng | Khóa sở hữu | Liên kết sang | Tần suất phát sinh (mô phỏng) |
|---|---|---|---|---|
| loan_application_system | application_train, application_test | SK_ID_CURR | — (gốc) | theo sự kiện nộp hồ sơ |
| external_credit_bureau | bureau, bureau_balance | SK_ID_BUREAU | bureau.SK_ID_CURR → application; bureau_balance.SK_ID_BUREAU → bureau | khi tra cứu + snapshot tháng |
| previous_loan_system | previous_application | SK_ID_PREV | SK_ID_CURR → application | theo quyết định từng hồ sơ cũ |
| installment_payment_system | installments_payments | — (sự kiện) | SK_ID_PREV → previous_application | theo kỳ |
| pos_cash_system | POS_CASH_balance | — (SK_ID_PREV × tháng) | SK_ID_PREV → previous_application | hằng tháng |
| credit_card_system | credit_card_balance | — (SK_ID_PREV × tháng) | SK_ID_PREV → previous_application | hằng tháng |
| synthetic_customer_behavior | synthetic_traffic_profile | SK_ID_CURR | SK_ID_CURR → application | làm mới định kỳ (DATA_UPDATED_AT) |

Chỉ dùng khóa định danh gốc. Không có khách hàng ngẫu nhiên nào được tạo rồi JOIN vào dữ liệu Home Credit.

## 3. Các bước tạo bảng Gold (thứ tự thực thi)

1. Đọc `silver/application_train` và `application_test`, thêm `dataset_split`, tách TARGET sang `training_labels`, kiểm tra SK_ID_CURR là duy nhất.
2. Với mỗi bảng con, áp dụng `usable()`: loại `_after_application` và các dòng orphan. Số dòng bị loại ghi vào `gold_checks.json`.
3. installments: gom dòng thanh toán thành **kỳ trả góp**. Một kỳ có thể có nhiều lần trả một phần.
4. Aggregate về SK_ID_PREV, kiểm tra duy nhất, LEFT JOIN vào previous_application, kiểm tra số dòng → `loan_credit_features`.
5. Aggregate từng nguồn về SK_ID_CURR, kiểm tra duy nhất, LEFT JOIN lần lượt vào danh sách application, kiểm tra số dòng sau mỗi lần → `customer_credit_features`.
6. LEFT JOIN application + customer features, thêm feature kỹ thuật, kiểm tra 1 dòng/SK_ID_CURR và không có TARGET → `integrated_credit_profile`.
7. Mọi kiểm tra đều đạt thì mới ghi Gold. Sau đó ghi `data_quality_report`.
