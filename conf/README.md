# conf/ — cấu hình dùng chung (mục A.5 đề cương)  

| Tệp | Là gì | Nguồn thật |
|---|---|---|
| `spark-defaults.conf` | Cấu hình Spark chung của mọi job (Delta, múi giờ UTC, timeout chịu mạng ZeroTier) | `ha_tang/spark/submit.sh` truyền đúng các khóa này; kiểm thử bảo đảm hai nơi khớp nhau |
| `hdfs-site.xml`, `core-site.xml` | Liên kết tới cấu hình HDFS mà container NameNode và DataNode dùng | `ha_tang/hadoop/config/` |
| `.env.example` | Mọi biến môi trường của dự án ở một chỗ | Tệp `.env` thật được sinh từ `cau_hinh/cum.env` bởi `scripts/cum/khoi_dong_may.sh` và `tao_cau_hinh_airflow.sh` |

Vì sao cấu hình thật nằm ở `ha_tang/` và `cau_hinh/`: cụm có 6 máy, mỗi máy chạy một tập vai trò khác nhau
(NameNode, DataNode, Spark Master/Worker, Kafka, Airflow, web). Mỗi vai trò có thư mục compose riêng,
và địa chỉ của từng máy sinh từ một danh bạ duy nhất `cau_hinh/cum.env`.
