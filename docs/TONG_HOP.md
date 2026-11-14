# Tổng hợp dự án — Credit Risk Intelligence (IT5427)

Môn **IT5427 – Big Data**. Đề tài: tích hợp dữ liệu lớn đa nguồn để đánh giá và dự đoán rủi ro tín dụng, trên bộ **Home Credit Default Risk**.

Mục tiêu nghiệp vụ: khi một khách hàng xin vay, hệ thống ước lượng **xác suất vỡ nợ (PD)**, xếp mức rủi ro, rồi gợi ý quyết định cho nhân viên tín dụng.

Luồng thiết kế:

```text
Nguồn CSV / macro / sự kiện
        →  lưu (MinIO)
        →  xử lý (Apache Spark: Bronze → Silver → Gold)
        →  chấm điểm (Logistic Regression + LightGBM)
        →  quyết định trên dashboard
```

Dashboard hiện đọc hai nhóm kết quả đã có sẵn trong repo:

- Model Track A trong `outputs/` (mẫu 5.000 hồ sơ application).
- Báo cáo lakehouse Spark trong `apache-spark/reports/` (lần chạy fixture, không phải full Home Credit).

Ghi chú đăng nhập (mặc định trong `.env.example`; nếu `.env` đã sửa thì dùng giá trị trong `.env`):

| Username | Password | Vai trò |
| --- | --- | --- |
| `analyst` | `AnalystDemo!234` | RISK_ANALYST |
| `underwriter` | `UnderwriterDemo!234` | UNDERWRITER |
| `admin` | `AdminDemo!234` | ADMIN |

Local: http://127.0.0.1:5173. Docker: http://localhost:8088. Muốn vừa xem báo cáo vừa chấm hồ sơ thì đăng nhập `underwriter` / `UnderwriterDemo!234`.

---

## 1. Các phần trong repo

| Phần | Thư mục | Vai trò |
| --- | --- | --- |
| Thu thập và lưu trữ | `ingestion-minio-kafka/` | Đưa CSV vào lake MinIO, phát sự kiện Kafka, sinh dữ liệu vĩ mô theo vùng (GDP, thất nghiệp, CPI, 82 vùng). |
| Xử lý Spark | `apache-spark/` | Profiling, ingestion Bronze, làm sạch Silver, gom và join thành Gold, feature, streaming, đồng bộ MinIO. |
| Phân tích và model | `src/`, `outputs/`, `run_track_a.py` | Tiền xử lý, Logistic Regression, LightGBM, ngưỡng chi phí, SHAP, chấm điểm. |
| Dashboard | `frontend/`, `backend/`, `ml-service/` | Giao diện React, API Spring Boot (JWT), dịch vụ chấm điểm FastAPI. |
| Tài liệu | `docs/`, `README.md` | Kiến trúc, demo, backlog, khoảng trống so với đề. |

### 1.1. Ingestion (MinIO + Kafka)

- Dataset gốc nằm ở `_data/` (không commit lên git).
- Staging mô phỏng dòng thời gian 104 tuần. Mốc “live” trong thiết kế là `2025-12-01`.
- Lake có các bucket landing, bronze, reference, quarantine.
- Kafka có topic theo từng nguồn và hàng đợi lỗi.
- Bảng vĩ mô bổ sung nằm ở `ingestion-minio-kafka/data/supplementary/` và được dashboard dùng cho trang **Macro & Regions**.

### 1.2. Apache Spark (đồng đội)

Bảy nguồn logic: hồ sơ vay, bureau, khoản vay cũ, trả góp, POS/cash, thẻ tín dụng, và hồ sơ giao thông **giả lập** (số vi phạm, vi phạm nặng, tiền phạt, tai nạn). Nguồn giả lập chỉ gắn với `SK_ID_CURR` có thật, có seed, và được đánh dấu `SYNTHETIC`.

```text
7 nguồn → Bronze (CSV gốc) → Silver (Parquet đã làm sạch) → Gold (1 dòng / khách hàng)
                └→ phát lại thành event → Kafka hoặc file → Spark Structured Streaming
```

Quy tắc join: không nối thẳng bảng nhiều dòng vào hồ sơ. Mỗi bảng con được gom về đúng khóa trước, rồi mới `LEFT JOIN`. Sau mỗi join, pipeline đếm số dòng. `TARGET` chỉ nằm ở `gold/training_labels`, không nằm trên bảng feature.

Bảng Gold:

| Bảng | Grain |
| --- | --- |
| `integrated_credit_profile` | 1 dòng / `SK_ID_CURR`, không có TARGET |
| `customer_credit_features` | Feature tổng hợp từ 7 nguồn |
| `loan_credit_features` | 1 dòng / khoản vay cũ (`SK_ID_PREV`) |
| `training_labels` | Nhãn TARGET, chỉ tập train |
| `data_quality_report` | Kết quả kiểm tra chất lượng |

Báo cáo đã commit (lần chạy **test fixture**, giá trị ngẫu nhiên, không phải Home Credit đầy đủ):

| Chỉ số | Giá trị |
| --- | ---: |
| Dòng Bronze | 654.308 |
| Dung lượng Bronze | 25,3 MB |
| Dòng Silver giữ lại | 654.300 |
| Hồ sơ Gold (`integrated_credit_profile`) | 3.000 |
| Khoản vay cũ (`loan_credit_features`) | 8.724 |
| Nhãn train | 2.579 |
| Số cột feature | 102 |
| Kiểm tra Gold | 31/31 đạt |
| Streaming (file replay) | 2.000 event gửi đi, 1.982 event ghi, 441 khách trong snapshot |

Feature Gold gồm cột hồ sơ, tổng hợp bureau (số khoản, dư nợ, quá hạn), khoản vay cũ, trả góp trễ, POS DPD, utilization thẻ, vi phạm giao thông, và các tỷ lệ như `debt_to_income`, `loan_to_income`.

### 1.3. Model (Track A)

Train trên **mẫu 5.000 dòng** của `application_train.csv`, chưa phải full khoảng 307.511 hồ sơ, và chưa train lại trên feature store Gold.

| Model | Validation ROC-AUC | Vai trò |
| --- | ---: | --- |
| Logistic Regression | 0,7168 | Scorecard tuyến tính |
| LightGBM | 0,7361 | Model đang dùng để chấm điểm |

- Ngưỡng chi phí giả định: từ chối nhầm người tốt rẻ hơn duyệt nhầm người vỡ nợ, tỷ lệ phạt **FN:FP = 5:1**. Trên mẫu hiện tại, ngưỡng chọn được vẫn là PD = 0,50.
- Mức rủi ro: **LOW** &lt; 5%, **MEDIUM** 5–15%, **HIGH** ≥ 15%.
- KPI danh mục trong `outputs/metrics/risk_summary.json`: NPL **7,72%**, 5.000 hồ sơ.
- Biểu đồ mức rủi ro và tra cứu hồ sơ dùng `outputs/scored_applications_sample.csv`: **500** dòng đã chấm.

Hai hồ sơ mẫu đã đo trên model thật:

| Hồ sơ | PD | Mức | Quyết định |
| --- | ---: | --- | --- |
| `sample_full_applicant.json` | 0,0417 | LOW | Auto-Approve |
| `sample_subprime_applicant.json` | 0,5470 | HIGH | Decline |

### 1.4. Dashboard

```text
React (Vite) → Spring Boot API (JWT) → FastAPI (nội bộ) → model joblib
                    ├─ đọc outputs/           (KPI, segment, chấm điểm, ROC)
                    ├─ đọc supplementary/     (macro theo vùng)
                    └─ đọc apache-spark/reports/   (trang Data Pipeline)
```

Không cần database ứng dụng. Tài khoản demo và refresh token nằm trong bộ nhớ.

Trang:

| Trang | Đường dẫn | Nội dung |
| --- | --- | --- |
| Overview | `/` | NPL, số hồ sơ, phân bố LOW/MEDIUM/HIGH, ROC, và một ô dẫn sang Spark |
| Segment Risk | `/segments` | Rủi ro theo nghề, loại thu nhập, học vấn, `EXT_SOURCE_2` |
| Macro & Regions | `/macro` | Cắt theo vùng, thất nghiệp, GDP; giả lập sốc thất nghiệp và CPI |
| Data Pipeline | `/pipeline` | Kết quả Bronze / Silver / Gold, thời gian từng bước, feature, streaming, 31 kiểm tra chất lượng |
| Underwriting | `/underwriting` | Chấm một hồ sơ mới, hoặc tra `SK_ID_CURR` trong file đã chấm |

API pipeline: `GET /api/insights/pipeline` (cần đăng nhập).

---

## 2. Cài đặt

Cần một trong hai cách: Docker, hoặc chạy trực tiếp trên máy.

### 2.1. Phần mềm

Chạy local:

- JDK **25**
- Python **≥ 3.10** (nên 3.12)
- Node.js **20+**
- macOS Apple Silicon: `brew install libomp` (LightGBM cần OpenMP)

Chạy bằng Docker: Docker Desktop. Image backend dùng Temurin 25, không cần cài JDK trên máy.

Dataset Kaggle đầy đủ (`_data/`) **không cần** để mở dashboard. Artifact model đã có trong `outputs/`. Báo cáo Spark đã có trong `apache-spark/reports/`.

### 2.2. Cấu hình môi trường

Ở thư mục gốc repo:

```bash
cp -n .env.example .env
```

`./scripts/setup.sh` cũng tự copy file này nếu chưa có. File `.env` không được commit.

Các biến quan trọng:

| Biến | Ý nghĩa | Mặc định trong `.env.example` |
| --- | --- | --- |
| `JWT_SECRET` | Ký access token, tối thiểu 32 ký tự | `change-me-jwt-secret-must-be-32-chars-min` |
| `ML_SERVICE_TOKEN` | Spring gọi FastAPI | `change-me-ml-internal-token-32chars` |
| `CORS_ALLOWED_ORIGINS` | Origin được phép | `localhost` và `127.0.0.1`, cổng 5173 và 8088 |
| `BACKEND_PORT` | API | 8080 |
| `FRONTEND_PORT` | UI khi chạy Docker | 8088 |
| `VITE_DEV_PORT` | UI khi chạy local | 5173 |
| `ML_SERVICE_PORT` | Model, chỉ máy local | 8000 |

Đổi `JWT_SECRET` và mật khẩu demo trước khi đưa máy này cho người khác dùng.

### 2.3. Setup local (một lần)

```bash
./scripts/setup.sh
```

Script kiểm tra Java 25, Python, Node, tạo `.env` nếu thiếu, tạo `.venv`, cài `requirements.txt`, `npm install` trong `frontend/`, và tải dependency Maven. Thiếu `outputs/models/credit_risk_model.joblib` hoặc các file KPI thì script dừng.

---

## 3. Khởi động dashboard

### 3.1. Docker (nên dùng khi demo)

Ở thư mục gốc:

```bash
cp -n .env.example .env
docker compose up --build -d
docker compose ps
```

Mở [http://localhost:8088](http://localhost:8088).

- UI: cổng `8088`
- API: cổng `8080`
- Dịch vụ model **không** mở ra ngoài máy. Chỉ backend trong mạng Docker gọi được.

Xem log và tắt:

```bash
docker compose logs -f
docker compose down
```

Image backend copy sẵn `outputs/`, bảng macro, và `apache-spark/reports/`. Trang Data Pipeline có số liệu ngay sau khi container lên.

### 3.2. Chạy trên máy (khi đang sửa code)

```bash
./scripts/setup.sh
./scripts/start.sh
./scripts/status.sh
```

Địa chỉ:

| Dịch vụ | URL |
| --- | --- |
| Giao diện | http://127.0.0.1:5173 |
| API | http://127.0.0.1:8080 |
| Model (chỉ local) | http://127.0.0.1:8000 |
| Swagger (profile `local`) | http://127.0.0.1:8080/swagger-ui.html |

Log nằm ở `.run/logs/`. Tắt cả ba tiến trình:

```bash
./scripts/stop.sh
```

`start.sh` tự trỏ `SPARK_REPORTS_DIR` tới `apache-spark/reports`, nên trang Data Pipeline đọc đúng báo cáo Spark.

Trên Windows có bộ script tương ứng: `scripts/setup.ps1`, `start.ps1`, `stop.ps1`, `status.ps1`.

---

## 4. Đăng nhập

Mật khẩu dưới đây là mặc định trong `.env.example`. Nếu `.env` đã bị sửa, dùng đúng giá trị trong file đó.

| Username | Vai trò | Mật khẩu mặc định | Dùng để |
| --- | --- | --- | --- |
| `analyst` | RISK_ANALYST | `AnalystDemo!234` | Xem Overview, Segment, Macro, Data Pipeline |
| `underwriter` | UNDERWRITER | `UnderwriterDemo!234` | Thêm quyền chấm hồ sơ và tra cứu |
| `admin` | ADMIN | `AdminDemo!234` | Thêm `/api/admin/users` và `/api/health` |

Cách vào:

1. Mở http://127.0.0.1:5173 (local) hoặc http://localhost:8088 (Docker).
2. Nhập username và password.
3. Bấm **Sign in**.

Access token nằm trong bộ nhớ trình duyệt, hết hạn sau 15 phút. Refresh token là cookie `HttpOnly`, hạn 7 ngày. Đăng nhập sai 5 lần thì bị khóa 60 giây.

Nút **Logout** ở góc trên bên phải.

Ba vai trò trên đều xem được các trang dashboard, gồm Data Pipeline. Chấm điểm (`/underwriting`) cũng mở cho cả ba vai trò đã đăng nhập. Endpoint admin thì chỉ `admin`.

---

## 5. Xem lần lượt trên dashboard

Đăng nhập bằng `underwriter` nếu muốn vừa xem báo cáo vừa chấm hồ sơ.

1. **Overview.** NPL **7,72%**, 5.000 hồ sơ, biểu đồ mức rủi ro trên 500 hồ sơ đã chấm, đường ROC (LightGBM validation AUC **0,7361**). Ô **Spark integration** dẫn sang Data Pipeline.
2. **Segment Risk.** So sánh tỷ lệ nợ theo nghề, thu nhập, học vấn và nhóm `EXT_SOURCE_2`.
3. **Macro & Regions.** Hồ sơ đã chấm được nối với bảng vùng. Có thể tăng thất nghiệp (điểm phần trăm) và CPI rồi xem tỷ lệ mức HIGH thay đổi. Đây là phép tính lại trên PD hiện có, không train lại model.
4. **Data Pipeline.** Số Bronze **654.308** dòng, Gold **3.000** khách, **102** feature, **31/31** kiểm tra đạt, thời gian từng stage, bảng làm sạch, và thống kê streaming. Banner màu vàng ghi rõ đây là **test fixture**, không phải full Home Credit.
5. **Underwriting.**
   - **Load Full Applicant** → **Assess Applicant**: kỳ vọng mức LOW, Auto-Approve.
   - **Load Subprime Applicant** → **Assess Applicant**: kỳ vọng mức HIGH, Decline.
   - Tra cứu mã `101676`: hồ sơ có trong file đã chấm, mức HIGH.

Số PD có thể lệch nhẹ so với bảng ở mục 1.3 nếu model hoặc payload đổi. Luôn đọc số trên màn hình sau khi bấm chấm, không chép cứng vào slide nếu chưa chạy lại.

### Số trên từng trang đang nói điều gì

Dashboard là màn hình ra quyết định. Năm trang trả lời năm câu hỏi: sổ vay rủi ro đến mức nào, rủi ro nằm ở nhóm nào, kinh tế vùng có làm xấu thêm không, đường dữ liệu đa nguồn có đáng tin không, và một hồ sơ cụ thể nên duyệt hay từ chối.

**Overview — sổ vay.** Trên mẫu 5.000 hồ sơ có **386** hồ sơ vỡ nợ, tỷ lệ **7,72%**. Khoản vay trung bình khoảng **598.000**, thu nhập trung bình khoảng **168.000**, dư nợ gấp khoảng **3,95 lần** thu nhập một năm. Trung bình khoản vay cao hơn trung vị (508.000): một nhóm vay lớn kéo mặt bằng lên. Trong 500 hồ sơ đã chấm, **246** hồ sơ ở mức HIGH (PD từ 15%). LightGBM đạt validation AUC **0,7361**, cao hơn hồi quy logistic **0,7168**. Ở ngưỡng 0,50 model ít từ chối nhầm người tốt (độ đặc hiệu **96,64%**) nhưng chỉ bắt khoảng **23%** người thực sự vỡ nợ. Tín hiệu mạnh nhất là `EXT_SOURCE`, thời gian đi làm và thời điểm đăng ký giấy tờ.

**Segment Risk — rủi ro không đều.** So với mặt bằng 7,72%: nhóm `EXT_SOURCE_2` thấp nhất vỡ nợ **15,35%**, nhóm cao nhất **3,41%** (gấp khoảng 4,5 lần). Lao động trình độ thấp **13,64%**, nhân viên vệ sinh **12,12%**, trung học cơ sở **12,28%**, lao động phổ thông **9,87%**, người đang đi làm **9,40%**, nam **9,59%** so với nữ **6,76%**. Đại học **4,00%**, kế toán **4,79%**, công chức **3,49%**. Nhóm rất ít người (IT, HR, thất nghiệp) có tỷ lệ 0% vì mẫu quá nhỏ, không kết luận được.

**Macro & Regions — kịch bản vùng xấu đi.** Mỗi hồ sơ đã chấm được gắn thất nghiệp, GDP và CPI của vùng. Cùng kiểu khách, vùng khác nhau thì tỷ lệ mức HIGH khác nhau. Phép thử sốc dùng công thức cố định: PD nhân với `1 + 0,06 × điểm phần trăm thất nghiệp + 0,04 × điểm phần trăm CPI`. Đây là kịch bản giả định, không phải số vỡ nợ đã xảy ra, và không train lại model.

**Data Pipeline — chất lượng đường tích hợp, không nói ai sẽ vỡ nợ.** 654.308 dòng Bronze, 3.000 hồ sơ Gold, 102 đặc trưng và 31/31 kiểm tra đạt có nghĩa là bảy nguồn đã gom thành một dòng mỗi khách, bản trùng bị loại, và nhãn `TARGET` không lọt vào bảng đặc trưng. Đây là lần chạy fixture (dữ liệu giả lập), không mô tả 5.000 khách Home Credit, và các đặc trưng này chưa đi vào PD trên trang Underwriting.

**Underwriting — một quyết định.** Hồ sơ mẫu đầy đủ ra PD khoảng **4,2%**, mức LOW, tự duyệt. Hồ sơ dưới chuẩn ra PD khoảng **54,7%**, mức HIGH, từ chối. Mã `101676` trong file đã chấm nằm ở mức HIGH. Nhân viên không xem từng bảng bureau hay trả góp; họ xem một PD, một mức rủi ro, một hành động, và các yếu tố đang kéo rủi ro lên hoặc xuống.

---

## 6. Chạy pipeline Spark (tùy chọn)

Dashboard **không cần** chạy lại Spark. Số trên trang Data Pipeline lấy từ báo cáo đã commit.

Khi muốn chạy lại pipeline (Java 17 hoặc 21, Python 3.11+, Docker):

```bash
cd apache-spark
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Hạ tầng MinIO và Kafka của pipeline nằm ở `apache-spark/docker-compose.yml`:

- MinIO: http://localhost:9001 (`minioadmin` / `minioadmin`), API cổng 9000
- Kafka: `localhost:9092`
- Kafka UI: http://localhost:8080

Kafka UI chiếm cổng **8080**, trùng API dashboard. Tắt một trong hai trước khi bật cái còn lại (`docker compose down` trong `apache-spark/`, hoặc `./scripts/stop.sh` ở gốc repo).

Chạy nhanh trên subset (cần CSV thật trong `apache-spark/data/raw/`, hoặc fixture):

```bash
cd apache-spark
python -m scripts.make_dev_subset --customers 5000
export CRB_PATHS_RAW_DIR=data/dev_subset/raw
python -m scripts.run_pipeline --sync-minio
```

Sau khi chạy xong, thay file trong `apache-spark/reports/` (đặc biệt `pipeline_stats.json` và `data_quality/`). Khởi động lại backend để trang Data Pipeline đọc báo cáo mới. Service nạp báo cáo lúc start.

Test pipeline, không đụng dữ liệu thật:

```bash
cd apache-spark
python -m pytest
```

---

## 7. Kiểm thử dashboard

Ở thư mục gốc, khi đã setup:

```bash
./scripts/test.sh
```

Gồm pytest của phần Python, Maven test của backend, và Vitest của frontend.

Khi dashboard đang chạy:

```bash
./scripts/smoke.sh
```

---

## 8. Giới hạn cần nói khi bảo vệ

- Model chấm trên mẫu **5.000** hồ sơ application. Feature bureau / trả góp / thẻ của Spark chưa được đưa vào lần train này.
- Số trên **Data Pipeline** là lần chạy fixture (3.000 khách, giá trị ngẫu nhiên). Full Home Credit cần CSV Kaggle và chạy lại `run_pipeline`.
- Tỷ lệ NPL 7,72% là tỷ lệ `TARGET` trên mẫu Track A. Tỷ lệ trả chậm trên dashboard segment vẫn dựa trên hồ sơ application, chưa đếm kỳ trễ từ `installments_payments`.
- Kafka và MinIO không nằm trong đường demo của dashboard. Streaming trên trang Data Pipeline là số của lần replay ghi trong báo cáo.
- Chưa có báo cáo Word/PDF và slide thuyết trình trong repo.
- Delta Lake có trong thiết kế (`CRB_LAKE_FORMAT=delta`) nhưng lần chạy đã lưu dùng Parquet.

---

## 9. Sự cố thường gặp

| Hiện tượng | Cách xử lý |
| --- | --- |
| Backend không lên | Kiểm tra `JWT_SECRET` và ba cặp user/password trong `.env`. Local cần JDK 25 (`JAVA_HOME`). |
| Chấm điểm báo 503 | `curl http://127.0.0.1:8000/health`. Token ML của backend và của dịch vụ model phải giống nhau. macOS thiếu `libomp` thì cài `brew install libomp` rồi start lại. |
| Trang Data Pipeline trống hoặc 503 | Phải có `apache-spark/reports/pipeline_stats.json`. Docker cần build lại image sau khi thêm thư mục reports. |
| Lỗi CORS | `CORS_ALLOWED_ORIGINS` phải chứa đúng origin đang mở, gồm cả `http://` và cổng. Không dùng `*`. |
| Overview trống | Thiếu `outputs/metrics/risk_summary.json` hoặc `outputs/scored_applications_sample.csv`. |
| Cổng 8080 bị chiếm | Kafka UI của `apache-spark/docker-compose.yml` cũng dùng 8080. Tắt stack kia trước. |
| Docker, model không khỏe | `docker compose logs ml-service`. Trong Compose, backend gọi `http://ml-service:8000`. |
| Test tra cứu hồ sơ gốc fail | Bình thường nếu máy không có `_data/`. Test chấm điểm và API dashboard vẫn chạy được. |
