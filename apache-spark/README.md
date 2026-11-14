# Credit Risk Intelligence – Big Data Data Pipeline

Bài tập lớn môn **Big Data Integration and Processing (IT5427)**.
Đề tài: *Tích hợp và phân tích dữ liệu lớn đa nguồn cho đánh giá rủi ro tín dụng*, trên bộ dữ liệu **Home Credit Default Risk** (Kaggle).
Phạm vi: hai module **Data Collection & Storage** và **Data Integration & Processing**.

---

## 1. Dự án đã làm gì

Từ 10 file CSV của Home Credit, dự án dựng một hệ thống mô phỏng môi trường dữ liệu tài chính thực tế. Hệ thống có nhiều nguồn nghiệp vụ độc lập, dữ liệu đi qua Data Lake 3 tầng, và có cả luồng batch lẫn streaming.

```
 7 logical sources ──► Ingestion ──► BRONZE ──► Cleaning/DQ ──► SILVER ──► Integration ──► GOLD
 (6 gốc + 1 synthetic)  (Python)     (CSV gốc)    (Spark)        (Parquet)   (Spark)       (feature 1 dòng/khách hàng)
                                         │
                                         └──► Replay producer ──► Kafka ──► Spark Structured Streaming ──► SILVER/GOLD
                         Lưu trữ: MinIO (buckets bronze / silver / gold)
```

| # | Thành phần | Đã làm | File chính |
|---|---|---|---|
| 1 | **Khảo sát dữ liệu (profiling)** | Đo số dòng, kiểu, missing, trùng lặp, outlier, tỷ lệ TARGET. **Kiểm tra trên dữ liệu thật** từng quan hệ khóa (orphan, cardinality), không JOIN chỉ vì trùng tên cột. Sinh báo cáo và ERD có số liệu đo. | `processing/profiling/profile_raw.py` |
| 2 | **Mô phỏng nhiều nguồn** | 8 file được chia thành 6 hệ thống nghiệp vụ độc lập (Loan Application, Credit Bureau, Previous Loan, Installment, POS/Cash, Credit Card). Mỗi hệ thống có module ingestion, schema, metadata, tần suất và quy tắc kiểm tra riêng. | `configs/sources.yaml`, `common/schemas.py`, `ingestion/*_source/` |
| 3 | **Nguồn SYNTHETIC** | Hồ sơ giao thông giả lập (số vi phạm, vi phạm nặng, tiền phạt, tai nạn). Chỉ sinh cho SK_ID_CURR có thật, có seed, độc lập với TARGET, gắn nhãn `SYNTHETIC`, lưu tách khỏi dữ liệu gốc. | `ingestion/synthetic_source/generator.py`, `docs/SYNTHETIC_DATA.md` |
| 4 | **Ingestion → Bronze** | Chép file nguyên trạng (sha256), đọc theo chunk, ghi manifest cho mỗi lần nạp. **Idempotent:** nạp lại file cũ thì tự bỏ qua. Phát hiện schema drift; một nguồn lỗi không làm dừng nguồn khác. | `ingestion/base.py`, `ingestion/run_ingestion.py` |
| 5 | **Cleaning & Data Quality → Silver** | Ép kiểu, chuẩn hóa tên cột/giá trị (365243→NULL + cờ, XNA→NULL, Y/N→1/0). Bỏ bản trùng tuyệt đối; bản trùng khóa đưa vào quarantine. **Gắn cờ, không xóa:** orphan, outlier, giá trị âm, dòng có ngày sau ngày nộp hồ sơ. Đối soát: số dòng vào = ra + loại. | `processing/cleaning/` |
| 6 | **Integration → Gold** | **Gom từng bảng con về đúng grain trước** (bureau_balance→bureau→khách hàng; dòng thanh toán→kỳ trả góp→khoản vay→khách hàng), **rồi mới LEFT JOIN**. Kiểm tra số dòng sau **mỗi** JOIN; sai là dừng pipeline. | `processing/aggregation/`, `processing/integration/build_gold.py` |
| 7 | **Feature engineering** | 37 feature tổng hợp theo đề, cộng 13 feature kỹ thuật (debt-to-income, loan-to-income, payment delay ratio, credit utilization, total outstanding debt, has_previous_overdue, …) có công thức và ý nghĩa nghiệp vụ. | `processing/feature_engineering/features.py` |
| 8 | **Chống leakage** | TARGET chỉ nằm ở `gold/training_labels`. Loại dữ liệu xảy ra sau ngày nộp hồ sơ. Producer không phát TARGET; consumer đưa vào quarantine event nào có TARGET. | `build_gold.py`, `stream_processor.py` |
| 9 | **Data Lake trên MinIO** | 3 bucket bronze/silver/gold; Parquet, partition hợp lý; Delta Lake là tùy chọn. | `common/lake.py`, `storage/minio/`, `docker-compose.yml` |
| 10 | **Streaming** | Producer phát lại dữ liệu lịch sử thành event (đổi được tốc độ, pause/resume/stop, replay, chèn lỗi). Spark Structured Streaming xử lý: quarantine event lỗi, **dedup theo `event_id`**, **watermark** cho event trễ, **checkpoint** để khôi phục, gom theo SK_ID_CURR. | `streaming/` |
| 11 | **Test & tài liệu** | 39 test pytest phủ 12 yêu cầu của đề. Có README, kiến trúc, lineage, data dictionary (tự sinh), DQ report, hướng dẫn demo, nhật ký từng giai đoạn. | `tests/`, `docs/`, `reports/` |

**Đầu ra Gold**

| Bảng | Grain | Nội dung |
|---|---|---|
| `integrated_credit_profile` | 1 dòng / SK_ID_CURR | thông tin hồ sơ + feature tổng hợp + feature kỹ thuật, **không có TARGET** |
| `customer_credit_features` | 1 dòng / SK_ID_CURR | feature tổng hợp từ 7 nguồn |
| `loan_credit_features` | 1 dòng / SK_ID_PREV | hành vi trả nợ theo từng khoản vay cũ |
| `training_labels` | 1 dòng / SK_ID_CURR (chỉ train) | TARGET, chỉ dùng làm nhãn |
| `data_quality_report` | 1 dòng / chỉ số | kết quả kiểm tra chất lượng |

### Phân loại dữ liệu

| Loại | Ở đâu | Ghi chú |
|---|---|---|
| Original historical data | `data/raw/` → `bronze/original/` | 10 file Kaggle, không bao giờ bị sửa |
| Synthetic data | `data/synthetic/` → `bronze/synthetic/` | `DATA_ORIGIN = SYNTHETIC` |
| Simulated streaming data | Kafka topic hoặc `data/_stream_landing/` | `event_time` là thời gian **replay** (`time_semantics = SYNTHETIC_REPLAY`), không phải thời điểm giao dịch thật |
| Test fixture | sinh vào thư mục tạm khi chạy test | giá trị ngẫu nhiên theo schema Home Credit, **không phải dữ liệu thật** |

### Ánh xạ với môn học

| Bước | Bài | Khái niệm |
|---|---|---|
| Lấy subset theo khóa | 4 | AQP: lấy mẫu, vấn đề sample khi JOIN |
| Profiling, kiểm tra quan hệ khóa | 5, 2 | Source profiling (Bellman), instance-based matching |
| Ingestion theo từng nguồn | 1, 5, 2 | Wrapper, source description, nguồn tự trị (autonomous) |
| Thêm nguồn synthetic | 2 | GAV: chi phí thay đổi mapping khi thêm nguồn |
| Cleaning Silver | 1 | Heterogeneity cú pháp / ngữ nghĩa / dữ liệu, Veracity |
| Dedup, kiểm tra khóa ngoại | 3 | Entity Resolution (exact match bằng khóa chung SK_ID_*) |
| Gold | 1, 2, 4 | Mediated schema, GAV vật chất hóa (warehouse), data fusion, các thuật toán join, gom trước khi join |
| DQ, leakage | 5, 1 | Source quality, heterogeneity thời gian |
| Streaming | 4 | Adaptive processing, time-to-first-tuple, partial results, chịu lỗi |

---

## 2. Cài đặt

Yêu cầu: Python 3.11+ (đã chạy trên 3.13), **Java 17 hoặc 21**, Docker. Khuyến nghị RAM ≥ 8 GB khi chạy dữ liệu đầy đủ.

```bash
python -m venv .venv && source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
docker compose up -d          # MinIO :9000/:9001 · Kafka :9092 · Kafka UI :8080
docker compose ps             # minio, kafka, kafka-ui ở trạng thái running
```

Tải dataset từ https://www.kaggle.com/competitions/home-credit-default-risk/data, rồi giải nén **10 file CSV vào `data/raw/`**. Pipeline chỉ đọc thư mục này, không ghi vào và không tải lại dataset.

---

## 3. Cách chạy

Mọi lệnh chạy ở thư mục gốc dự án.

### 3.1. Chạy thử nhanh trên subset (khuyến nghị làm trước)
```bash
python -m scripts.make_dev_subset --customers 5000        # 5.000 khách hàng thật kèm toàn bộ dữ liệu liên quan
export CRB_PATHS_RAW_DIR=data/dev_subset/raw              # Windows PowerShell: $env:CRB_PATHS_RAW_DIR="data/dev_subset/raw"
python -m scripts.run_pipeline --sync-minio
```

### 3.2. Chạy toàn bộ dữ liệu
```bash
unset CRB_PATHS_RAW_DIR
CRB_SPARK_DRIVER_MEMORY=6g python -m scripts.run_pipeline --sync-minio
```
`run_pipeline` lần lượt chạy: profiling → sinh synthetic → ingestion (Bronze) → cleaning (Silver) → integration (Gold + DQ) → đồng bộ MinIO → data dictionary. Mỗi stage được đo thời gian; stage nào lỗi thì dừng.
Thêm `--streaming-demo` để chạy luôn một vòng streaming ở chế độ file; `--skip-profiling` để bỏ bước profiling.

### 3.3. Chạy từng bước (dễ quan sát dữ liệu chảy qua các tầng)

| # | Lệnh | Mục đích |
|---|---|---|
| 1 | `python -m processing.profiling.profile_raw` | khảo sát dữ liệu, kiểm tra quan hệ khóa |
| 2 | `python -m ingestion.synthetic_source.generator` | sinh nguồn SYNTHETIC |
| 3 | `python -m ingestion.run_ingestion` | nạp 7 nguồn vào Bronze (chạy lại sẽ thấy `skipped`) |
| 4 | `python -m processing.cleaning.clean_silver` | làm sạch, kiểm tra chất lượng → Silver |
| 5 | `python -m processing.integration.build_gold` | gom, join, tạo feature → Gold + DQ report |
| 6 | `python -m storage.minio.sync_lake` | đẩy 3 tầng lên MinIO |
| 7 | `python -m scripts.generate_data_dictionary` | sinh `docs/DATA_DICTIONARY.md` từ schema thật |

Có thể chạy riêng một nguồn: `python -m ingestion.run_ingestion --sources external_credit_bureau [--force]`, hoặc riêng vài bảng: `python -m processing.cleaning.clean_silver --tables bureau bureau_balance`.

### 3.4. Streaming (mở 3 terminal; phải chạy batch trước để có dữ liệu Bronze)
```bash
# Terminal A – consumer: Spark Structured Streaming, micro-batch mỗi 5 giây
python -m streaming.consumers.stream_processor --source kafka --trigger 5 --reset-checkpoint

# Terminal B – producer: phát lại dữ liệu lịch sử, 10 event/giây, có chèn bản trùng, event trễ, event lỗi
python -m streaming.producers.replay_producer --sink kafka --rate 10 \
    --dup-rate 0.05 --late-rate 0.02 --malformed-rate 0.01 --from-start

# Terminal C – điều khiển và xem kết quả
python -m streaming.producers.control pause | resume | stop | status
python -m streaming.consumers.snapshot          # trạng thái gom hiện tại theo SK_ID_CURR
```
Không dùng Kafka: thay `kafka` bằng `file` ở cả A và B. Logic xử lý giữ nguyên.
Thử khôi phục: Ctrl-C terminal A, rồi chạy lại lệnh A **không có** `--reset-checkpoint`. Consumer chạy tiếp, không xử lý lại event cũ.

### 3.5. Test
```bash
python -m pytest        # 39 test, khoảng 2 phút; chạy trên fixture trong thư mục tạm, không đụng data/
```

### 3.6. Cấu hình
Mọi tham số nằm trong `configs/pipeline.yaml` và ghi đè được bằng biến môi trường `CRB_<SECTION>_<KEY>`. Ví dụ:
- `CRB_PATHS_RAW_DIR=...`: đổi thư mục dữ liệu đầu vào.
- `CRB_SPARK_DRIVER_MEMORY=6g`: tăng RAM cho Spark.
- `CRB_LAKE_BACKEND=s3a`: ghi thẳng lên MinIO.
- `CRB_LAKE_FORMAT=delta`: dùng Delta Lake.

---

## 4. Theo dõi hệ thống

| Muốn xem | Ở đâu |
|---|---|
| Log khi chạy | terminal, `reports/logs/pipeline.log` |
| Kết quả khảo sát, ERD có số liệu | `reports/data_profiling/PROFILING_REPORT.md`, `reports/architecture/ERD.md` |
| Lịch sử nạp dữ liệu | `data/bronze/_manifests/*.jsonl` |
| Dòng bị loại và lý do | `data/silver/_quarantine/<bảng>/` |
| Chất lượng dữ liệu trước/sau xử lý | `docs/DATA_QUALITY_REPORT.md` |
| Thống kê: dung lượng, số dòng, tốc độ, thời gian, số feature | `reports/PIPELINE_STATS.md` |
| Kế hoạch thực thi Spark (thấy bước gom nằm trước JOIN) | http://localhost:4040 → tab **SQL / DataFrame** (chỉ có khi job đang chạy) |
| Biểu đồ tốc độ streaming | http://localhost:4040 → tab **Structured Streaming** |
| Event trong Kafka | Kafka UI http://localhost:8080 → Topics → Messages |
| Dữ liệu trong lake | MinIO http://localhost:9001 (đăng nhập `minioadmin` / `minioadmin`) |
| Streaming theo từng micro-batch | `reports/streaming/batch_metrics.jsonl`, `query_progress.jsonl` (bản trùng và event trễ bị loại) |

**Dấu hiệu chạy đúng:**
- Log `silver` có `reconcile=True` ở mọi bảng.
- Log `gold` toàn bộ `check … PASS`.
- Chạy lại ingestion thì các bảng báo `skipped`.
- Streaming: `duplicates_dropped` khớp với số bản trùng producer đã chèn.

Lần theo một khách hàng qua các tầng:
```bash
python -c "
import pandas as pd; ID=100002   # đổi thành một SK_ID_CURR có trong dữ liệu của bạn
print(len(pd.read_parquet('data/silver/installments_payments', filters=[('sk_id_curr','=',ID)])), 'dòng thanh toán ở Silver')
print(pd.read_parquet('data/gold/customer_credit_features', filters=[('sk_id_curr','=',ID)]).T)   # 1 dòng đã tổng hợp ở Gold
"
```

---

## 5. Cấu trúc thư mục

```
credit-risk-bigdata/
├── common/            config, Spark session, đọc/ghi lake, schema registry (khóa, grain, kiểu, quan hệ)
├── configs/           pipeline.yaml (tham số), sources.yaml (7 logical sources)
├── data/              raw | synthetic | bronze | silver | gold   (không commit lên GitHub)
├── ingestion/         base.py + 1 package cho mỗi nguồn + run_ingestion.py
├── processing/        profiling | cleaning | aggregation | feature_engineering | integration
├── streaming/         schemas (hợp đồng event) | producers | consumers
├── storage/minio/     client.py, sync_lake.py
├── scripts/           run_pipeline, make_dev_subset, make_test_fixture, generate_data_dictionary
├── tests/             39 test pytest
├── docs/              ARCHITECTURE, DATA_LINEAGE, DATA_DICTIONARY, DATA_QUALITY_REPORT, DEMO_GUIDE, SYNTHETIC_DATA
├── reports/           profiling, data_quality, architecture (ERD), streaming, PIPELINE_STATS, PHASE_LOG
├── docker-compose.yml
└── requirements.txt
```

---

## 6. Trạng thái kiểm chứng và giới hạn

Môi trường build không truy cập được Kaggle, Docker Hub và Maven Central. Vì vậy mọi kiểm chứng dưới đây chạy trên **test fixture**. Các file trong `docs/` và `reports/` kèm theo gói này cũng là số liệu của fixture. **Hãy chạy lại pipeline trên dữ liệu thật để có số liệu đưa vào báo cáo**; các file đó sẽ được ghi đè.

| Thành phần | Trạng thái |
|---|---|
| Profiling, ingestion, Silver, Gold, synthetic, feature, DQ | ✅ chạy được, có test |
| Streaming chế độ file (dedup, watermark, quarantine, checkpoint, khôi phục, replay) | ✅ chạy được, có test |
| Đẩy lên S3 / MinIO bằng Python (sync, ingestion ghi thẳng) | ✅ chạy được với S3 server tương thích; chưa thử với MinIO thật |
| `docker-compose.yml`, Kafka producer/consumer thật | 🟡 có code; chưa chạy trên máy build |
| Spark ghi thẳng `s3a://`, Delta Lake | 🟡 có code và cấu hình; lần đầu cần internet để tải jar |
| Chạy trên dữ liệu Home Credit thật | ⏳ chạy theo mục 3 |

**Giới hạn đã biết:**
- `event_time` của streaming là thời gian replay. Ngày trong Home Credit là ngày tương đối so với từng hồ sơ, nên không thể đặt chúng lên một lịch chung mà không bịa ngày.
- Producer phát theo thứ tự dòng trong file, không sắp xếp toàn cục theo thời gian.
- Dedup streaming chỉ có hiệu lực trong cửa sổ watermark (10 phút).
- `bureau_total_debt` cộng dư nợ của mọi loại tiền tệ.
- Không làm Entity Resolution mờ, vì nguồn có khóa chung. Cũng không làm virtual integration hay AQP (xem bảng ánh xạ môn học ở mục 1).

## 7. Tài liệu khác
[Kiến trúc](docs/ARCHITECTURE.md) · [Lineage](docs/DATA_LINEAGE.md) · [Data dictionary](docs/DATA_DICTIONARY.md) · [Chất lượng dữ liệu](docs/DATA_QUALITY_REPORT.md) · [Hướng dẫn demo](docs/DEMO_GUIDE.md) · [Dữ liệu synthetic](docs/SYNTHETIC_DATA.md) · [Nhật ký từng giai đoạn](reports/PHASE_LOG.md)
