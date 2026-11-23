# models/ — Mô hình của hệ khuyến nghị

Thư mục này **không chứa tệp mô hình**. Mô hình được sinh trên cụm và lưu trên HDFS cùng với dữ liệu, để mọi executor và web đọc được và để mỗi mô hình gắn với đúng phiên bản dữ liệu đã sinh ra nó.

## Vị trí trên HDFS

| Đường dẫn | Nội dung | Sinh bởi |
|---|---|---|
| `/lakehouse/bd03_khuyen_nghi/models/user_indexer/` | `StringIndexerModel` của `ma_nguoi_dung` (chỉ nối nhãn, không fit lại) | `jobs/ml/chuan_bi_ma_tran.py` |
| `/lakehouse/bd03_khuyen_nghi/models/item_indexer/` | `StringIndexerModel` của `ma_san_pham` | `jobs/ml/chuan_bi_ma_tran.py` |
| `/lakehouse/bd03_khuyen_nghi/models/als/<ma_chay>/` | `ALSModel` tốt nhất của lần tuning, học trên tập train (4 tháng đầu) — dùng để đánh giá | `jobs/ml/huan_luyen_als.py` |
| `/lakehouse/bd03_khuyen_nghi/models/als/<ma_chay>_phuc_vu/` | `ALSModel` fit lại trên train + test với cùng siêu tham số — dùng để sinh gợi ý | `jobs/ml/sinh_goi_y_toan_bo.py` |

`ma_chay` có dạng `rYYYYMMDDTHHMMSS` (giờ UTC lúc DAG hoặc script bắt đầu). Tên phiên bản ghi vào `gold.goi_y_nguoi_dung.phien_ban_mo_hinh` là `als_<ma_chay>` hoặc `als_v2_<ma_chay>` (mô hình v2 lọc theo tầm giá, xem MO_HINH.md mục 9).

## Tra cứu một mô hình

Mỗi lần đánh giá ghi một dòng vào `gold.lich_su_huan_luyen` với siêu tham số, `max_iter`, `hat_giong`, `phien_ban_features`, `phien_ban_silver`, metric, quyết định triển khai và `duong_dan_mo_hinh`. Màn hình **Chất lượng mô hình** trên web hiển thị bảng này.

## Lấy mô hình về máy (ví dụ để đưa vào báo cáo)

```bash
# Trên máy master
docker exec hadoop-master hdfs dfs -ls /lakehouse/bd03_khuyen_nghi/models/als
docker exec hadoop-master hdfs dfs -get /lakehouse/bd03_khuyen_nghi/models/als/<ma_chay>_phuc_vu /tmp/als_phuc_vu
docker cp hadoop-master:/tmp/als_phuc_vu ./models/
```

Nạp lại trong PySpark:

```python
from pyspark.ml.recommendation import ALSModel
mo_hinh = ALSModel.load("models/als_phuc_vu")
mo_hinh.rank, mo_hinh.userFactors.count(), mo_hinh.itemFactors.count()
```

Tệp mô hình không đưa lên Git (xem `.gitignore`); muốn tái lập thì chạy lại DAG `kn_tai_lap` với `phien_ban_features` tương ứng thay vì lưu tệp nhị phân.

## Dọn mô hình cũ

Mỗi đêm DAG 1 tạo hai thư mục mô hình mới. Khi cần dọn, giữ lại các `ma_chay` còn nằm trong khoảng lưu lịch sử Delta (30 ngày) để tái lập được:

```bash
docker exec hadoop-master hdfs dfs -du -h /lakehouse/bd03_khuyen_nghi/models/als
docker exec hadoop-master hdfs dfs -rm -r /lakehouse/bd03_khuyen_nghi/models/als/<ma_chay_cu> /lakehouse/bd03_khuyen_nghi/models/als/<ma_chay_cu>_phuc_vu
```
