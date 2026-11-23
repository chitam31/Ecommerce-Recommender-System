# Thời gian phản hồi của web (đo từ máy khách)

Thời điểm đo: 2026-11-15 23:39:49 UTC. Web: http://10.68.71.149:8000. Mỗi nhóm 20 yêu cầu GET liên tiếp, xoay vòng các URL trong nhóm.

| Nhóm yêu cầu | Số lần | Lần đầu (ms) | Trung vị (ms) | p95 (ms) | Lớn nhất (ms) | Dưới 3 s |
|---|---:|---:|---:|---:|---:|---:|
| Trang chủ (không chọn user) | 20 | 18 | 18 | 54 | 67 | 20/20 |
| GET /api/goi-y/{user_id} | 20 | 528 | 8907 | 10154 | 10424 | 1/20 |
| GET /api/goi-y/{user_id} (user mới, cold start) | 20 | 511 | 17 | 511 | 790 | 20/20 |
| GET /api/thinh-hanh | 20 | 14 | 16 | 22 | 24 | 20/20 |
| Trang sản phẩm /san-pham/{id} | 20 | 3847 | 409 | 3847 | 3886 | 18/20 |
| GET /api/thinh-hanh/{id} (không đệm) | 20 | 272 | 410 | 818 | 819 | 20/20 |
| Bảng điều khiển A/B /ab | 20 | 2669 | 21 | 34 | 2669 | 20/20 |
| GET /api/ab/ket-qua | 20 | 17 | 18 | 44 | 56 | 20/20 |
| Chất lượng mô hình /mo-hinh | 1 | 8698 | 8698 | 8698 | 8698 | 0/1 (19 lỗi) |
| GET /api/mo-hinh/chi-so | 0 | - | - | - | - | lỗi |
| Tái lập thí nghiệm /tai-lap | 0 | - | - | - | - | lỗi |

Tổng cộng 161 yêu cầu: p95 = 8964 ms, lớn nhất = 10424 ms, 86.3% dưới 3 giây.

## Số đo phía máy chủ (GET /api/hieu-nang, gồm cả lưu lượng thật từ lúc web khởi động)

| API | Số yêu cầu | Trung vị (ms) | p95 (ms) | Lớn nhất (ms) | Tỷ lệ dưới 3 s |
|---|---:|---:|---:|---:|---:|
| `GET /` | 21 | 2.2 | 2.7 | 19593.1 | 95.2% |
| `GET /ab` | 20 | 3.5 | 4.0 | 2655.4 | 100.0% |
| `GET /api/ab/ket-qua` | 20 | 3.6 | 3.8 | 4.2 | 100.0% |
| `GET /api/goi-y/{user_id}` | 41 | 541.4 | 10141.8 | 18878.9 | 51.2% |
| `GET /api/nguoi-dung-mau` | 1 | 8496.9 | 8496.9 | 8496.9 | 0.0% |
| `GET /api/thinh-hanh` | 20 | 1.6 | 1.7 | 2.1 | 100.0% |
| `GET /api/thinh-hanh/{ma_san_pham}` | 20 | 287.1 | 315.6 | 318.0 | 100.0% |
| `GET /favicon.ico` | 1 | 1.0 | 1.0 | 1.0 | 100.0% |
| `GET /mo-hinh` | 1 | 8426.2 | 8426.2 | 8426.2 | 0.0% |
| `GET /san-pham/{ma_san_pham}` | 20 | 284.6 | 3704.1 | 3831.8 | 90.0% |
