# docs/bang_chung — bằng chứng chạy thật

| Tệp | Nội dung | Mục đề cương |
|---|---|---|
| `kiem_chung_airflow_cuc_bo.md` | Deadline Alert và `airflow backfill create` chạy trên Airflow 3.3.1 thật (máy cục bộ) | A.6.5, A.8 |
| `cum/10_backfill_dag3.txt` | `airflow backfill create` cho DAG 3 trên cụm (16/11): 3 lượt, đúng lượt 00:00 UTC tính lại ngày 15/11 | A.8 |
| `cum/01_hdfs_du_h.txt` | `hdfs dfs -du -h /lakehouse/bd03_khuyen_nghi` | A.6.3 (bắt buộc nộp) |
| `cum/02_hdfs_cay.txt` | du, count, cây thư mục (`hdfs dfs -ls -R` đã lọc) | A.4 |
| `cum/03_quarantine.json` | Dựng `_quarantine.tuong_tac` từ bronze (nếu chưa có) | A.4 |
| `cum/04_delta.txt` | DESCRIBE HISTORY, số commit theo thao tác, VERSION AS OF, schema evolution, Z-ORDER, thống kê làm sạch | A.6.2, C.3 |
| `cum/05_airflow.txt` | 5 DAG, lỗi import, Deadline Alert và retries từng DAG, lượt chạy gần nhất | A.6.5 |
| `cum/06_streaming.txt` | Ứng dụng Spark đang chạy và thời gian đã chạy | A.6.1 |
| `cum/07_do_tre_web.md` | Thời gian phản hồi web | A.8 |
| `cum/eda.json` | Khảo sát dữ liệu gốc cho `notebooks/eda_retailrocket.ipynb` | A.5, A.7, C.3 |
| `cum/phan_tich_loi.json` | NDCG từng người gom theo nhóm: ALS so với baseline | C.3, C.5 |

Thư mục `cum/` sinh bởi `scripts/thu_bang_chung.sh` chạy trên master (docs/TRIEN_KHAI_CUM.md mục 8c).
