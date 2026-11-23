# Thời gian phản hồi của web sau khi sửa sự cố 32, 33 (đo từ máy khách)

Chép từ màn hình `python3 scripts/do_do_tre_web.py` trên labsoe-11, sau khi build lại web (bản `bababb2`)
và chạy `ha_tang/spark/submit.sh lo bao_tri/toi_uu_delta.py --chi-bang gold.goi_y_nguoi_dung --bo-qua-vacuum`
(kết quả: `[bao_tri] gold.goi_y_nguoi_dung {'so_tep_truoc': 1, 'so_tep_sau': 36, 'zorder': 'ma_nguoi_dung'}`).

Thời điểm đo: 2026-11-16 01:12:23 UTC. Web: http://10.68.71.149:8000. Mỗi nhóm 20 yêu cầu GET liên tiếp, xoay vòng các URL trong nhóm.

| Nhóm yêu cầu | Số lần | Lần đầu (ms) | Trung vị (ms) | p95 (ms) | Lớn nhất (ms) | Dưới 3 s |
|---|---:|---:|---:|---:|---:|---:|
| Trang chủ (không chọn user) | 20 | 33 | 18 | 30 | 33 | 20/20 |
| GET /api/goi-y/{user_id} | 20 | 426 | 28 | 531 | 586 | 20/20 |
| GET /api/goi-y/{user_id} (user mới, cold start) | 20 | 402 | 22 | 407 | 438 | 20/20 |
| GET /api/thinh-hanh | 20 | 49 | 21 | 48 | 49 | 20/20 |
| Trang sản phẩm /san-pham/{id} | 20 | 2586 | 338 | 2586 | 3448 | 19/20 |
| GET /api/thinh-hanh/{id} (không đệm) | 20 | 409 | 319 | 414 | 432 | 20/20 |
| Bảng điều khiển A/B /ab | 20 | 2433 | 15 | 68 | 2433 | 20/20 |
| GET /api/ab/ket-qua | 20 | 20 | 16 | 43 | 46 | 20/20 |
| Chất lượng mô hình /mo-hinh | 20 | 8529 | 28 | 58 | 8529 | 19/20 |
| GET /api/mo-hinh/chi-so | 20 | 29 | 29 | 36 | 76 | 20/20 |
| Tái lập thí nghiệm /tai-lap | 20 | 2663 | 22 | 47 | 2663 | 20/20 |

Tổng cộng 220 yêu cầu: p95 = 457 ms, lớn nhất = 8529 ms, 99.1% dưới 3 giây.

## Số đo phía máy chủ (GET /api/hieu-nang, gồm cả lưu lượng thật từ lúc web khởi động)

| API | Số yêu cầu | Trung vị (ms) | p95 (ms) | Lớn nhất (ms) | Tỷ lệ dưới 3 s |
|---|---:|---:|---:|---:|---:|
| `GET /` | 20 | 1.9 | 2.3 | 19.3 | 100.0% |
| `GET /ab` | 20 | 0.9 | 1.0 | 2414.8 | 100.0% |
| `GET /api/ab/ket-qua` | 20 | 1.0 | 1.1 | 1.2 | 100.0% |
| `GET /api/goi-y/{user_id}` | 41 | 2.0 | 517.2 | 10775.7 | 97.6% |
| `GET /api/mo-hinh/chi-so` | 20 | 10.4 | 11.0 | 11.2 | 100.0% |
| `GET /api/nguoi-dung-mau` | 1 | 19446.0 | 19446.0 | 19446.0 | 0.0% |
| `GET /api/thinh-hanh` | 20 | 1.6 | 1.8 | 2.1 | 100.0% |
| `GET /api/thinh-hanh/{ma_san_pham}` | 20 | 285.1 | 345.4 | 365.7 | 100.0% |
| `GET /mo-hinh` | 20 | 9.1 | 9.8 | 8492.0 | 95.0% |
| `GET /san-pham/{ma_san_pham}` | 20 | 294.3 | 2469.5 | 3414.3 | 95.0% |
| `GET /tai-lap` | 20 | 3.1 | 3.6 | 2642.3 | 100.0% |

Ghi chú: các giá trị lớn nhất phía máy chủ (`/api/nguoi-dung-mau` 19,4 s, `/api/goi-y` 10,8 s, `/mo-hinh` 8,5 s)
là yêu cầu đầu tiên sau khi web khởi động (nạp nhật ký Delta, quét bảng gợi ý lấy người dùng mẫu). Sau lần đo
này web được thêm bước làm nóng bộ đệm trong luồng nền khi khởi động (`KhoDelta.lam_nong`).

## Kiểm tra làm nóng bộ đệm (bản `3eee18b`, build lại web 09:31 giờ Việt Nam)

`docker exec bd03-web grep -c lam_nong ...` = 2 (`app.py`) và 1 (`kho_du_lieu.py`): image chạy đúng bản mới.
`docker logs bd03-web 2>&1 | grep "Làm nóng"` (giờ UTC trong container):

```
2026-11-16 02:31:31,334 INFO web: Làm nóng nguoi_dung_mau: 20.3 s
2026-11-16 02:31:35,794 INFO web: Làm nóng lich_su_mo_hinh: 4.5 s
2026-11-16 02:31:39,421 INFO web: Làm nóng tuning: 3.6 s
2026-11-16 02:31:41,982 INFO web: Làm nóng do_zorder: 2.6 s
2026-11-16 02:31:44,279 INFO web: Làm nóng ket_qua_ab: 2.3 s
2026-11-16 02:31:46,624 INFO web: Làm nóng ket_qua_tai_lap: 2.3 s
2026-11-16 02:31:48,717 INFO web: Làm nóng thinh_hanh: 2.1 s
2026-11-16 02:31:53,443 INFO web: Làm nóng danh_muc_san_pham: 4.7 s
2026-11-16 02:31:53,975 INFO web: Làm nóng cay_danh_muc: 0.5 s
```

Chín bước, khoảng 43 giây sau khi container khởi động. Sau đó lần mở `/mo-hinh` đầu tiên
(`curl -s -o /dev/null -w "%{time_total}" http://localhost:8000/mo-hinh`) mất 0,024 giây, so với 8,5 giây
ở lần đo trên (chưa làm nóng).
