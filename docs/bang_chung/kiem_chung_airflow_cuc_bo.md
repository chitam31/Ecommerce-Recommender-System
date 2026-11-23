# Kiểm chứng Deadline Alert và backfill trên Airflow 3.3.1 thật (máy cục bộ, 15/11/2026)

Mục đích: chứng minh hai tính năng mới của 5 DAG chạy đúng trên chính phiên bản Airflow của cụm
(apache-airflow 3.3.1, LocalExecutor, `airflow standalone`), trước khi đồng bộ lên cụm.

Thư mục DAG thử gồm đúng tệp `airflow/dags/bd03_chung.py` của dự án và hai DAG thử:

- `thu_deadline`: `deadline=han_chot(1)` (1 phút từ lúc vào hàng đợi), một task `sleep 150`.
- `thu_backfill`: lịch `0 * * * *` múi giờ Asia/Ho_Chi_Minh, cùng cổng `cong_backfill`
  (`bd03_chung.ngay_backfill`) và cùng mẫu tham số Jinja với task `tinh_ket_qua_ab` của DAG 3,
  nhưng task cuối chỉ `echo` tham số thay vì nộp job Spark.

Kiểm thử tự động tương ứng: `tests/test_airflow_dags.py` (nạp 5 DAG thật bằng DagBag, tuần tự hóa,
dựng lệnh với tham số backfill).

## Kết quả (trích log)

```text
--- Deadline Alert (thu_deadline: han_chot(1), task sleep 150) ---
[CANH BAO TRE HAN] 2026-11-15T10:10:29 | dag=thu_deadline run=manual__2026-11-15T10:09:28.006157+00:00 han=1 phut, han_chot=2026-11-15 10:10:28.017539+00:00 trang_thai=running
scheduler  | 2026-11-15 10:10:29.455525+00:00 [info] Callback executed successfully [callback_runner] callback_path=bd03_chung.canh_bao_tre_han
scheduler  | 2026-11-15T10:10:30.399353Z [info] Callback 01a1deae-ec0b-7e43-8436-cfa260c47669 completed successfully

--- airflow backfill create --dag-id thu_backfill --from-date 2026-11-12T05:00:00+07:00 --to-date 2026-11-12T09:00:00+07:00 ---
 scheduled__2026-11-15T10:00:00+00:00  success
 backfill__2026-11-12T02:00:00+00:00   success
 backfill__2026-11-12T01:00:00+00:00   success
 backfill__2026-11-12T00:00:00+00:00   success
 backfill__2026-11-11T23:00:00+00:00   success
 backfill__2026-11-12T05:00:00+07:00   success
backfill__2026-11-12T00:00:00+00:00 -> THAM_SO=[--tu-ngay 2026-11-12 --den-ngay 2026-11-12]
scheduled__2026-11-15T10:00:00+00:00 -> THAM_SO=[]
backfill__2026-11-12T05:00:00+07:00 -> Backfill: bỏ qua lượt 2026-11-11T22:00:00+00:00 (chỉ tính lại ở 00:00 UTC)
backfill__2026-11-11T23:00:00+00:00 -> Backfill: bỏ qua lượt 2026-11-11T23:00:00+00:00 (chỉ tính lại ở 00:00 UTC)
backfill__2026-11-12T01:00:00+00:00 -> Backfill: bỏ qua lượt 2026-11-12T01:00:00+00:00 (chỉ tính lại ở 00:00 UTC)
backfill__2026-11-12T02:00:00+00:00 -> Backfill: bỏ qua lượt 2026-11-12T02:00:00+00:00 (chỉ tính lại ở 00:00 UTC)
```

## Đọc kết quả

- Hạn chót là 1 phút sau lúc lượt chạy vào hàng đợi (10:09:28 → 10:10:28). Scheduler gọi
  `canh_bao_tre_han` lúc 10:10:29, khi task vẫn còn chạy (`trang_thai=running`). Đây là điểm khác
  với `sla` của Airflow 2: SLA cũ chỉ được kiểm tra khi lượt chạy kết thúc.
- Callback chạy qua executor (SyncCallback), không cần triggerer. Đúng với cấu hình cụm, nơi
  triggerer mặc định không bật.
- Backfill 5 giờ tạo 5 lượt `backfill__...`. Chỉ lượt 00:00 UTC (07:00 giờ Việt Nam) truyền
  `--tu-ngay 2026-11-12` cho job. Bốn lượt còn lại bị `cong_backfill` bỏ qua (skipped), nên mỗi
  ngày được tính lại đúng một lần. Lượt theo lịch (`scheduled__`) không có tham số, vẫn tính tăng
  dần theo CDF như cũ.
