# PHASE LOG – mục tiêu, file, thiết kế, kết quả chạy thử, lỗi và cách khắc phục

Theo mục 16 của đề, mỗi giai đoạn được ghi lại. Mọi lần chạy thử dùng **test fixture** (3.000 khách hàng giả lập theo đúng schema Home Credit, xem `scripts/make_test_fixture.py`), vì môi trường build không truy cập được Kaggle.

## Giai đoạn 1 – Khảo sát môi trường, fixture, profiling

- **Mục tiêu:** kiểm tra môi trường; có dữ liệu để chạy; profiling + ERD được kiểm chứng trên dữ liệu.
- **File:** `common/{config,schemas,spark,lake,logging_utils}.py`, `configs/*.yaml`, `scripts/make_test_fixture.py`, `scripts/make_dev_subset.py`, `processing/profiling/profile_raw.py`.
- **Thiết kế:**
  - Một schema registry duy nhất (`common/schemas.py`) khai báo grain, PK, FK, kiểu, cột thời gian tương đối và sentinel của từng bảng. Mọi module dùng chung registry này.
  - Profiling đọc toàn bộ cột dạng string trước rồi mới cast, để thấy được lỗi cast.
  - Mọi quan hệ được **đo** (orphan, coverage, min/avg/max số con trên mỗi cha), không suy diễn từ việc trùng tên cột.
- **Môi trường:** Python 3.13, OpenJDK 21, PySpark 3.5.3 chạy local[2], 7 GB RAM. Kaggle, Docker Hub và Maven Central bị chặn (HTTP 403).
- **Kết quả:** profiling 8 file trong khoảng 50 giây. Phát hiện đủ các lỗi cố ý chèn: 2 dòng trùng ở application_train (PK không unique), 4 orphan ở bureau, 3 ở bureau_balance, 5 ở installments; dòng sau ngày nộp hồ sơ; train/test không trùng ID; SK_ID_CURR của bảng con khớp với previous_application (0 dòng lệch).
- **Lỗi và cách sửa:** khóa tra mô tả cột của application bị viết sai thứ tự ưu tiên toán tử, dẫn đến lấy nhầm từ điển. Đã sửa thành chọn khóa tường minh.

## Giai đoạn 2–3 – Logical sources, ingestion Bronze, MinIO

- **Mục tiêu:** nạp từng nguồn độc lập, giữ nguyên dữ liệu gốc, chạy lại được, có metadata; lưu trên MinIO.
- **File:** `ingestion/base.py`, `ingestion/*_source/ingest.py` (7 nguồn), `ingestion/run_ingestion.py`, `ingestion/synthetic_source/generator.py`, `storage/minio/{client,sync_lake}.py`, `docker-compose.yml`.
- **Thiết kế:**
  - Copy byte-identical kèm sha256. Gặp sha256 đã ingest thì bỏ qua.
  - Kiểm tra theo chunk 500k dòng (pandas). Mỗi lần thử, kể cả thất bại, đều được ghi vào `bronze/_manifests/<table>.jsonl`.
  - Phát hiện schema drift bằng cách so header với lần ingest thành công trước.
  - Một file lỗi không làm dừng các nguồn còn lại.
- **Kết quả:** 9 bảng `SUCCESS` (654k dòng, 25 MB) trong 1 giây. Chạy lại: 9/9 `SKIPPED_ALREADY_INGESTED`. Đồng bộ lên S3 server tương thích (moto): 27 object; chạy lại thì 0 upload, 27 không đổi. Ingestion ghi trực tiếp lên S3 (`CRB_LAKE_BACKEND=s3a`) cũng chạy được. `docker compose config` hợp lệ.
- **Giới hạn:** chưa chạy được MinIO/Kafka thật vì không pull được image.

## Giai đoạn 4 – Cleaning (Silver), integration và feature (Gold)

- **Mục tiêu:** làm sạch có quy tắc, ghi nhận lại, kiểm tra toàn vẹn; tích hợp đúng grain; tạo feature; báo cáo DQ.
- **File:** `processing/cleaning/{rules,clean_silver}.py`, `processing/aggregation/aggregates.py`, `processing/feature_engineering/features.py`, `processing/integration/{build_gold,dq_report}.py`.
- **Thiết kế:**
  - Chỉ xóa bản trùng tuyệt đối. Bản trùng PK nhưng khác nội dung thì vào quarantine. Orphan, outlier, giá trị âm, dòng sau ngày nộp hồ sơ: **gắn flag, giữ lại**.
  - Gold aggregate trước, JOIN sau, kiểm tra số dòng sau mỗi JOIN. Có bất kỳ kiểm tra nào sai thì pipeline dừng.
- **Kết quả:**
  - Silver: 9 bảng đều cân đối (`rows_in = out + quarantined + dups`). Phát hiện đúng 2 + 3 + 2 bản trùng, 1 dòng thiếu khóa (quarantine), toàn bộ orphan và dòng sau ngày nộp hồ sơ đã chèn.
  - Gold: 31/31 kiểm tra đạt; `integrated_credit_profile` có 3.000 dòng = 3.000 hồ sơ, 102 cột.
- **Lỗi và cách sửa:**
  1. *Lần chạy đầu quarantine 1.107/2.581 dòng application.* Nguyên nhân: danh sách "cột bắt buộc có trong header" bị dùng luôn làm "cột không được NULL", nên OCCUPATION_TYPE (thiếu khoảng 31%) và NAME_CONTRACT_TYPE='XNA' làm loại oan cả dòng. Sửa: tách thành `required` (kiểm tra header) và `not_null` (chỉ gồm khóa và vài trường thiết yếu). Sau khi sửa: 0 dòng application bị quarantine.
  2. *Outlier không bao giờ được gắn flag trên mẫu nhỏ.* Nguyên nhân: quantile 0.999 trùng với giá trị lớn nhất, mà điều kiện là `>`. Sửa thành `>=`.
- **Ghi chú:** 11 dòng installments có AMT_PAYMENT âm là do cách sinh fixture tạo ra (thanh toán từng phần). Pipeline vẫn gắn flag `_invalid_negative` đúng như thiết kế.

## Giai đoạn 5–6 – Synthetic, replay producer, Structured Streaming

- **Mục tiêu:** mô phỏng dữ liệu phát sinh liên tục; xử lý event lỗi, trùng, trễ; có checkpoint.
- **File:** `streaming/schemas/event_schema.py`, `streaming/producers/{replay_producer,sinks,control}.py`, `streaming/consumers/{stream_processor,snapshot}.py`.
- **Thiết kế:**
  - `event_id` tất định (`evt_<table>_<offset>`), nên replay lại tự bị dedup.
  - Hai query: quarantine và main. Query main gồm watermark → `dropDuplicatesWithinWatermark` → `foreachBatch`, ghi đè theo partition `batch_id` (idempotent khi chạy lại một batch).
  - Producer điều chỉnh được tốc độ, pause/resume/stop, lưu offset để resume, và chèn lỗi có seed.
- **Kết quả (3.000 event + 141 bản trùng + 38 bản lỗi + 64 event trễ):** 5 micro-batch; quarantine 38; loại 141 bản trùng; watermark loại 37 event trễ; ghi 2.963 event. Khởi động lại không có dữ liệu mới: 0 dòng. Phát lại 300 dòng cũ: 296 bị loại vì trùng; 4 được ghi là đúng 4 event lần trước bị loại vì trễ, nên chưa từng vào state.
- **Lỗi và cách sửa:**
  1. *Toàn bộ 18 file rơi vào 1 micro-batch, nên event trễ không bị loại.* Watermark chỉ được đặt sau batch đầu, đúng theo cách Spark hoạt động. Sửa: thêm cấu hình `files_per_trigger` (mặc định 4) để có nhiều micro-batch; đồng thời ghi rõ hành vi này trong tài liệu.
  2. *Lỗi py4j khi thoát.* Nguyên nhân: listener Python vẫn đăng ký khi JVM dừng. Sửa: `removeListener` rồi `stop_spark()`.
  3. *Số quarantine báo -5.* File sink trả `numOutputRows = -1`. Sửa: đếm trực tiếp bảng quarantine trước và sau khi chạy.
  4. Lần đầu tóm tắt báo `duplicates_dropped = 0`, vì metric nằm trong `stateOperators[].customMetrics`. Đã sửa listener và phần tóm tắt.

## Giai đoạn 7 – Test, tài liệu, thống kê

- **File:** `tests/*` (39 test), `scripts/run_pipeline.py`, `scripts/generate_data_dictionary.py`, `docs/*`, `README.md`.
- **Kết quả:** `pytest`: **39 passed** (khoảng 2 phút), chạy trên fixture 500 khách hàng trong thư mục tạm. Toàn pipeline trên fixture 3.000 khách hàng, gồm cả MinIO sync và streaming demo: khoảng 135 giây.
- **Lỗi và cách sửa:**
  - Fixture `read` của test giữ SparkSession đã bị consumer streaming dừng. Sửa: luôn lấy session hiện tại.
  - Thiếu gói `tzdata` cho `toPandas()` với múi giờ Asia/Saigon. Đã thêm vào requirements.
  - Một số test chưa khai báo phụ thuộc vào fixture pipeline nên fail khi chạy riêng lẻ. Đã thêm phụ thuộc.

## Việc còn lại phía người dùng

1. Đặt 10 file Kaggle vào `data/raw/`, chạy `python -m scripts.run_pipeline --sync-minio`. Đưa số liệu thật từ `reports/` và `docs/DATA_QUALITY_REPORT.md` vào báo cáo.
2. `docker compose up -d`, rồi chạy demo Kafka theo `docs/DEMO_GUIDE.md` mục 3. Lần đầu Spark cần internet để tải jar `spark-sql-kafka` (và `hadoop-aws` nếu dùng `s3a`).
3. Cập nhật bảng trạng thái ở README sau khi kiểm chứng được MinIO thật, Kafka và s3a.
