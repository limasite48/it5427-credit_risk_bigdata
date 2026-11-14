# SYNTHETIC DATA – Synthetic Customer Behaviour (traffic profile)

> ⚠️ **SYNTHETIC – KHÔNG PHẢI DỮ LIỆU THẬT.** Đây không phải hồ sơ vi phạm giao thông của bất kỳ ai. Không được dùng hay trình bày như dữ liệu thật. Mục đích duy nhất là minh họa việc tích hợp thêm một nguồn dữ liệu ngoài vào pipeline.

## 1. Vì sao chỉ bổ sung 4 trường

Home Credit đã có sẵn thông tin nghề nghiệp, thu nhập, thâm niên, quy mô gia đình, khoản vay và toàn bộ lịch sử tín dụng, trả góp. Nguồn synthetic chỉ thêm một **loại dữ liệu mà Home Credit không có** (hành vi bên ngoài tài chính), ở mức tối thiểu đủ để demo:

| Trường | Kiểu | Ý nghĩa |
|---|---|---|
| TRAFFIC_VIOLATION_COUNT | int | tổng số lần vi phạm |
| MAJOR_VIOLATION_COUNT | int | số lần vi phạm nghiêm trọng (≤ tổng) |
| TOTAL_TRAFFIC_FINE | double | tổng tiền phạt (đơn vị giả lập kiểu VND) |
| ACCIDENT_COUNT | int | số vụ tai nạn |
| DATA_UPDATED_AT | timestamp | thời điểm "làm mới" giả lập |
| DATA_ORIGIN | string | luôn bằng `SYNTHETIC` |
| GENERATOR_VERSION, GENERATOR_SEED | | để tái lập kết quả |

## 2. Cách sinh và phân phối

| Trường | Phân phối | Ghi chú |
|---|---|---|
| TRAFFIC_VIOLATION_COUNT | NegativeBinomial(r=0.6, p=0.6), trung bình 0.4 | nhiều số 0, đuôi dài (over-dispersion) |
| MAJOR_VIOLATION_COUNT | Binomial(TRAFFIC_VIOLATION_COUNT, 0.12) | đảm bảo ≤ tổng |
| TOTAL_TRAFFIC_FINE | tổng các mức phạt chọn ngẫu nhiên từ bảng giá cố định: nhẹ {0.3; 0.5; 0.8; 1; 2} triệu, nặng {6; 8; 12; 18} triệu | nhất quán với số vi phạm |
| ACCIDENT_COUNT | Poisson(0.03 + 0.10 × MAJOR_VIOLATION_COUNT) | chỉ phụ thuộc trường synthetic khác |
| DATA_UPDATED_AT | đều trong năm 2025 | |

Đây là các tham số giả định do nhóm tự chọn, không dựa trên thống kê thật.

## 3. Quy tắc tuân thủ

- **Chỉ sinh cho SK_ID_CURR có thật** (application_train ∪ application_test), mỗi khách hàng một dòng. Có test kiểm tra.
- **Không đọc cột Home Credit nào ngoài SK_ID_CURR** và không sửa dữ liệu gốc.
- **Không tạo tương quan giả với rủi ro tín dụng.** Giá trị được sinh độc lập với TARGET và với mọi thuộc tính Home Credit. Ràng buộc duy nhất là giữa các trường synthetic với nhau (major ≤ total, tai nạn tăng nhẹ theo vi phạm nặng). Vì vậy mọi mô hình học trên nguồn này sẽ thấy **không có** tín hiệu dự báo: đó là hành vi đúng.
- **Tái lập được:** mỗi khách hàng dùng một bộ sinh số ngẫu nhiên có seed = (seed toàn cục, SK_ID_CURR). Chạy lại cho cùng kết quả, và thêm khách hàng mới không làm đổi giá trị của khách hàng cũ (có test kiểm tra tính tất định).
- **Lưu tách biệt:** `data/synthetic/` → `bronze/synthetic/…`, cột `_origin = SYNTHETIC` ở Silver, nhãn "(SYNTHETIC)" trong DATA_DICTIONARY. Gold có thêm cờ `has_synthetic_profile`.

## 4. Lệnh

```bash
python -m ingestion.synthetic_source.generator --raw-dir data/raw --out data/synthetic --seed 20261113
```
Kết quả gồm `synthetic_traffic_profile.csv` và `synthetic_traffic_profile.meta.json` (thống kê mô tả của lần sinh).
