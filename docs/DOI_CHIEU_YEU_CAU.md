# Đối chiếu yêu cầu với mã nguồn: đề cương đồ án (Phần A, C) và đề tài 3 (mục 3.1–3.11)

Phần đầu đối chiếu các quy định chung của `de_cuong_do_an_big_data.pdf` (Phần A: môi trường, cây HDFS, cấu trúc
repo, 5 yêu cầu bắt buộc, thang điểm, chống sao chép; Phần C: báo cáo, demo, câu hỏi phản biện). Phần sau đối
chiếu đặc tả riêng của đề tài 3.

## Phần A — Quy định chung

| Mục | Yêu cầu | Hiện thực | Bằng chứng |
|---|---|---|---|
| A.2 | Nhóm 3–4 sinh viên, bảng phân công 4 vai trò, mỗi thành viên có commit ở ≥ 2 vai trò | GVHD (TS. Cao Tiến Dũng) cho phép nhóm 2 người và không yêu cầu khắt khe về commit của từng thành viên. Đủ 4 vai trò: Lê Chí Tâm (ML Engineer, DevOps/Orchestration), Lê Nguyễn Quốc Anh (Web Developer, Data Engineer) | Báo cáo chính mục 10, báo cáo đầy đủ phụ lục F, `docs/AI_USAGE.md` mục 5 |
| A.3 | Hadoop ≥ 3.3, Spark 3.5, Delta ≥ 3.1, Airflow ≥ 2.9, Kafka ≥ 3.6, FastAPI/Flask, Jinja2 + Chart.js, Git | Hadoop 3.5.0, Spark 3.5.3 Standalone, Delta 3.2.0, Airflow 3.3.1 (BashOperator), Kafka 3.7.2 KRaft, FastAPI + Jinja2 + Chart.js 4.6.1, Git; docker-compose cho mọi thành phần | `ha_tang/*`, `web/static/vendor/` |
| A.4 | `hdfs:///lakehouse/<ma_de_tai>/` với bronze, silver, gold, features, models, checkpoints, `_quarantine` | `LAKE_DUONG_DAN=/lakehouse/bd03_khuyen_nghi` (`jobs/common/cau_hinh.py`); `_quarantine.tuong_tac` ghi mọi dòng bị loại khỏi silver kèm `ly_do`; chuyển từ `/lake` bằng `scripts/cum/chuyen_lakehouse.sh` | `docs/bang_chung/cum/02_hdfs_cay.txt`, `tests/test_chuyen_lakehouse.py` |
| A.5 | Cấu trúc repo chuẩn | Bảng ánh xạ ở README mục 2.1; thêm `conf/`, `notebooks/eda_retailrocket.ipynb` | README |
| A.6.1 | Job lô + mô hình MLlib + streaming chạy ≥ 30 phút khi demo | `bronze_to_silver` và các job lô; ALS (`huan_luyen_als`); 3 luồng chạy liên tục (bật trước giờ demo, xem thời gian chạy ở Spark UI) | `docs/bang_chung/cum/06_streaming.txt` |
| A.6.2 | MERGE INTO, DESCRIBE HISTORY, VERSION AS OF, OPTIMIZE có số trước/sau | MERGE: `ghi_gold`, `tinh_thinh_hanh`, `ghi_hien_thi`, `tinh_ket_qua_ab`, `tien_hoa_luoc_do`; DESCRIBE HISTORY: `spark_utils.phien_ban_moi_nhat`/`doc_ghi_chu_*` và `scripts/demo/delta_sql.sh lich_su`; VERSION AS OF: DAG 5, màn hình Tái lập, `delta_sql.sh phien_ban`; OPTIMIZE/ZORDER 3 giai đoạn: `bao_tri/do_zorder.py` | `docs/bang_chung/cum/04_delta.txt` |
| A.6.3 | Mọi bảng Delta trên HDFS + nộp `hdfs dfs -du -h /lakehouse/<ma_de_tai>` | Mọi bảng dưới `hdfs://10.68.71.113:8020/lakehouse/bd03_khuyen_nghi/` | `docs/bang_chung/cum/01_hdfs_du_h.txt` |
| A.6.4 | ≥ 3 màn hình, ≥ 1 thao tác ghi ngược vào Delta | 5 màn hình; ghi trực tiếp: "Chạy lại" → `gold.ket_qua_tai_lap` (`loai='ab_web'`); ghi qua Kafka: impression/click/mua → `gold.nhat_ky_hien_thi` (MERGE) | `tests/test_web.py`, `delta_sql.sh demo` |
| A.6.5 | ≥ 3 DAG khác lịch, retries, sla, Branch hoặc Sensor | 5 DAG (02:00 hằng ngày, 15 phút, mỗi giờ, 04:00, thủ công); `retries=1`; **Deadline Alerts** thay `sla` (Airflow 3 đã gỡ `sla`) ở cả 5 DAG; BranchPythonOperator, PythonSensor, ShortCircuitOperator | `tests/test_airflow_dags.py`, `docs/bang_chung/kiem_chung_airflow_cuc_bo.md`, `05_airflow.txt` |
| A.7 | Mốc tiến độ (EDA tuần 2...) | Notebook EDA trên dữ liệu gốc | `notebooks/eda_retailrocket.ipynb` |
| A.8 Spark | Đúng, phân tán, không kéo dữ liệu về driver | Chỉ đưa về driver bảng nhỏ có chủ đích (KIEN_TRUC.md mục 9); `do_zorder` chọn mẫu trên cụm | KIEN_TRUC.md §9 |
| A.8 Delta | MERGE, time travel, schema evolution, OPTIMIZE đo lường | MO_HINH §11–12, `do_hieu_nang_zorder` | Báo cáo chương 6 |
| A.8 HDFS | Phân vùng hợp lý, small files, benchmark | `silver` theo ngày (140 tệp sau sửa lỗi tệp nhỏ), Z-ORDER 200 → 67 → 1,07 tệp đọc | Báo cáo chương 6 |
| A.8 Web | Đủ màn hình, ghi ngược, phản hồi < 3 s | Middleware đo thời gian, `/api/hieu-nang`, `scripts/do_do_tre_web.py` | `docs/bang_chung/cum/07_do_tre_web.md` |
| A.8 Airflow | DAG rõ ràng, xử lý lỗi, backfill | retries + callback + nhánh giữ mô hình cũ + tự khởi động lại luồng; backfill DAG 1 (`tu_ngay`/`den_ngay`) và DAG 3 (`airflow backfill create`) | KIEN_TRUC.md §8; `docs/bang_chung/cum/10_backfill_dag3.txt` (backfill DAG 3 trên cụm) |
| A.9 | `docs/AI_USAGE.md` | Khai báo prompt, phần AI sinh, phần nhóm làm | `docs/AI_USAGE.md` |

## Phần C — Báo cáo, demo, câu hỏi phản biện

| Mục | Yêu cầu | Hiện thực |
|---|---|---|
| C.3 | Báo cáo 15–20 trang theo 11 mục | Bản chính theo đúng 11 mục: `bao_cao/CS411V_REPORT Final Project_Group Chí Tâm & Quốc Anh_Viet.pdf` (bản nộp, tiếng Việt) và bản tiếng Anh `bao_cao/CS411V_REPORT Final Project_Group Chí Tâm & Quốc Anh_Eng.pdf`; slide: `bao_cao/CS411V_SLIDE Final Project_Group Chí Tâm & Quốc Anh_{Viet,Eng}.pdf` |
| C.4 | Demo 15 phút: Airflow chạy DAG trực tiếp, `hdfs dfs -ls -R`, DESCRIBE HISTORY, VERSION AS OF, producer → bronze, ghi ngược web rồi xác nhận trong Delta | `docs/HUONG_DAN_DEMO.md` mục B', `scripts/demo/` |
| C.5 | 8 câu hỏi phản biện mẫu | Trả lời kèm vị trí trong mã: `docs/CHUAN_BI_BAO_VE.md` mục "Câu hỏi mẫu C.5" |

---

Ký hiệu trạng thái cho các bảng đề tài 3 bên dưới:

- **T** — có kiểm thử tự động chạy trên Spark local (`pytest`), đã qua.
- **M** — đã viết mã và kiểm tra cú pháp/logic, nhưng phần chạy thật cần Delta Lake hoặc cụm (chạy `scripts/kiem_tra_cuc_bo.sh` hoặc trên cụm để xác nhận).
- **C** — chỉ kiểm chứng được trên cụm 8 máy với dữ liệu thật.

## 3.1 Bối cảnh nghiệp vụ

| Yêu cầu | Hiện thực | TT |
|---|---|---|
| Khối "Dành cho bạn" trên trang chủ | `web/app.py` (`/`), `web/dich_vu/goi_y.py`, `gold.goi_y_nguoi_dung` | T |
| Khối "Sản phẩm liên quan" | `jobs/ml/san_pham_lien_quan.py`, `web/templates/san_pham.html` | T |
| Khối "Đang thịnh hành" cửa sổ 15 phút | `jobs/streaming/tinh_thinh_hanh.py` | T (nguồn tệp), C (Kafka) |
| Đo CTR bằng A/B, tái lập được | `jobs/transform/tinh_ket_qua_ab.py`, `jobs/kiem_tra/tai_lap_ab.py`, màn hình Tái lập | T (logic), M (time travel) |
| Cold start, implicit feedback, popularity bias | MO_HINH.md §1 | T |

## 3.2 Nguồn dữ liệu

| Yêu cầu | Hiện thực | TT |
|---|---|---|
| RetailRocket (sự kiện, thuộc tính, cây danh mục) | `scripts/tai_du_lieu.sh`, `jobs/transform/nhap_danh_muc.py` | C |
| Bộ phát Kafka theo timestamp gốc, 1 ngày = 30 giây | `producer/phat_lai_su_kien.py --giay-moi-ngay 30` | T (chế độ tệp), C (Kafka) |
| Thêm 5% người dùng mới hoàn toàn | `producer/phat_lai_su_kien.py --ty-le-nguoi-moi 0.05` (mã `moi_<n>`) | T |

## 3.3 Kiến trúc

Đủ mọi thành phần trong sơ đồ: Kafka → streaming → `gold.thinh_hanh_15p`; Kafka → `bronze.su_kien` → `silver.tuong_tac` → `features.ma_tran_tuong_tac` + `gold.thong_ke_san_pham` → ALS → `gold.goi_y_nguoi_dung` → web + A/B → `gold.nhat_ky_hien_thi` → `gold.ket_qua_ab`. Xem docs/KIEN_TRUC.md.

## 3.4 Đặc tả bảng

| Bảng | Cột của đặc tả | Cột thêm | Tệp | TT |
|---|---|---|---|---|
| bronze.su_kien | đủ 9 cột, phân vùng `ngay` | `ma_giao_dich, kafka_ts, _nguon, _ingest_ts, _schema_ver` | `streaming/nap_su_kien.py` | T |
| silver.tuong_tac | đủ 6 cột; khử trùng 5 giây; lọc bot > 500/giờ; trọng số 1/3/5 | `_nguon` | `transform/bronze_to_silver.py` | T |
| features.ma_tran_tuong_tac | `user_idx, item_idx, diem`; suy giảm λ = 0,01 | `tap` | `ml/chuan_bi_ma_tran.py` | T |
| gold.goi_y_nguoi_dung | đủ 7 cột, `xep_hang` 1..20, `nguon` ∈ {als, pho_bien_danh_muc, thinh_hanh} | — | `ml/tron_goi_y.py`, `ml/ghi_gold.py` | T (trộn), M (MERGE) |
| gold.thinh_hanh_15p | đủ 6 cột, công thức điểm đúng đề | — | `streaming/tinh_thinh_hanh.py` | T |
| gold.nhat_ky_hien_thi | đủ 8 cột | `nguon_traffic, chien_luoc` | `streaming/ghi_hien_thi.py` | T (gộp), M (MERGE) |
| gold.ket_qua_ab | đủ 9 cột | `p_value_chuyen_doi, ty_le_mo_phong` | `transform/tinh_ket_qua_ab.py` | T (tổng hợp), M (CDF) |

Lý do từng cột thêm: docs/KIEN_TRUC.md §5.

## 3.5 Job Spark

| Job | Yêu cầu | Hiện thực | TT |
|---|---|---|---|
| streaming/nap_su_kien.py | Kafka → bronze, checkpoint HDFS | `readStream.format("kafka")`, file sink Parquet `partitionBy("ngay")`, checkpoint `/lakehouse/bd03_khuyen_nghi/checkpoints/nap_su_kien_v2` | T (nguồn tệp), C |
| streaming/tinh_thinh_hanh.py | Cửa sổ trượt + watermark 2 phút; `outputMode("update")` + MERGE | `window(kafka_ts, "15 minutes", "1 minute")`, `withWatermark("kafka_ts", "2 minutes")`, `foreachBatch` + `MERGE` | T (cửa sổ, watermark), M (MERGE) |
| transform/bronze_to_silver.py | Khử trùng, lọc bot, gán trọng số | `phan_loai` (bỏ trùng mã → bỏ trùng nội dung → lọc bot → nhấp lặp 5 giây → trọng số), dòng bị loại vào `_quarantine.tuong_tac` kèm `ly_do`; tăng dần theo ngày, `replaceWhere`; backfill `--tu-ngay/--den-ngay` | T |
| ml/chuan_bi_ma_tran.py | StringIndexer (lưu index để tái sử dụng), suy giảm thời gian | Nối nhãn bằng `StringIndexerModel.from_labels`, lưu `/lakehouse/bd03_khuyen_nghi/models/*_indexer` | T |
| ml/huan_luyen_als.py | ALS implicit, tuning, top-20 bằng `recommendForAllUsers` | Tuning 27/9 tổ hợp; top-20 sinh ở `ml/sinh_goi_y_toan_bo.py` bằng `recommendForUserSubset` cho user hoạt động 30 ngày (phương án của chính mục 3.10) | T |
| ml/tron_goi_y.py | Trộn ALS + thịnh hành + phổ biến danh mục; cold start | Mẫu khối A/B, 3 chiến lược cold start, item mới 20% cuối, v2 lọc tầm giá | T |
| transform/tinh_ket_qua_ab.py | Tổng hợp impression log, z-test hai tỷ lệ | `common/ab.py`, `common/thong_ke.py`, đọc CDF | T (logic), M (CDF) |

Job bổ sung: `streaming/ghi_hien_thi.py`, `transform/nhap_danh_muc.py`, `transform/thong_ke_san_pham.py`, `transform/mo_phong_hien_thi.py`, `transform/tien_hoa_luoc_do.py`, `ml/danh_gia.py`, `ml/sinh_goi_y_toan_bo.py`, `ml/ghi_gold.py`, `ml/san_pham_lien_quan.py`, `bao_tri/*`, `kiem_tra/*`.

## 3.6 Mô hình

| Yêu cầu | Hiện thực | TT |
|---|---|---|
| ALS `implicitPrefs=true`, `coldStartStrategy="drop"` khi đánh giá | `common/als_lib.tao_als` | T |
| Tuning `rank × regParam × alpha`, tối thiểu 9 tổ hợp | `LUOI_DAY_DU` (27), `LUOI_TOI_THIEU` (9) | T, C (số liệu) |
| Chia theo thời gian: train 4 tháng, test tháng cuối; ground truth tháng cuối | `chia_theo_thoi_gian` (30 ngày cuối), `doc_features` | T |
| Precision/Recall/NDCG/MAP@10 | `common/chi_so.py` (đối chiếu `RankingMetrics`) | T |
| Coverage, Novelty | `common/als_lib.tinh_chi_so` | T |
| Baseline toàn sàn, baseline danh mục | `baseline_toan_san`, `baseline_danh_muc` | T |
| Ngưỡng NDCG ≥ +30% so với baseline bán chạy, Coverage ≥ 10% | `ml/danh_gia.py` → `dat_nguong` | T (công thức), C (kết quả) |
| Cold start: user mới → thịnh hành 15 phút | `danh_sach_nguoi_moi`, web | T |
| Cold start: user < 5 tương tác → bán chạy danh mục đã xem, 50/50 thịnh hành | `danh_sach_it_tuong_tac`, `tron_nguoi_it_tuong_tac`, bộ nhớ phiên của web | T |
| Cold start: item mới → chèn theo danh mục vào 20% vị trí cuối | `item_moi_theo_danh_muc`, mẫu `...mm` | T |

## 3.7 Delta Lake

| Yêu cầu | Hiện thực | TT |
|---|---|---|
| MERGE `gold.goi_y_nguoi_dung` theo user, không ghi đè toàn bảng | `ml/ghi_gold.py` (xóa thứ hạng thừa bằng cờ `_xoa`) | T (nguồn MERGE), M |
| MERGE `gold.thinh_hanh_15p` theo cửa sổ | `streaming/tinh_thinh_hanh.ghi_lo` | M |
| Time travel: ghi `phien_ban_delta` của features mỗi lần huấn luyện; chạy lại trên `VERSION AS OF`; màn hình "Tái lập" | `gold.lich_su_huan_luyen.phien_ban_features`, `kiem_tra/tai_lap_huan_luyen.py`, `web/templates/tai_lap.html` | T (tất định), M |
| Schema evolution tuần 5: `gia_san_pham`, `xep_hang_sao`; mô hình v2 lọc tầm giá | `transform/tien_hoa_luoc_do.py`, `common/tron.loc_theo_tam_gia`, `als_lib.loc_tam_gia_tren_test` | T (lọc), M (ALTER/MERGE) |
| Change Data Feed trên `gold.nhat_ky_hien_thi` | `kiem_tra/khoi_tao_bang.py`, `transform/tinh_ket_qua_ab.py` | M |
| OPTIMIZE / ZORDER BY (ma_nguoi_dung), đo trước/sau | `bao_tri/do_zorder.py`, `bao_tri/toi_uu_delta.py` | M, C (số liệu) |
| Partition silver theo `ngay`, giải thích không theo user | `bronze_to_silver.py`; docs/KIEN_TRUC.md §5 | T |

## 3.8 Web

| Màn hình | API | TT |
|---|---|---|
| Trang chủ (dropdown user, 10 món + thịnh hành, ghi impression mỗi lần render) | `GET /api/goi-y/{user_id}`, `POST /api/hien-thi` | T |
| Trang sản phẩm (liên quan; Xem / Thêm giỏ / Mua → Kafka) | `POST /api/su-kien` | T (giả lập Kafka), C |
| Bảng điều khiển A/B (CTR, chuyển đổi, biểu đồ theo ngày, p-value, kết luận) | `GET /api/ab/ket-qua` | T |
| Chất lượng mô hình (metric theo phiên bản, phân phối độ phổ biến) | `GET /api/mo-hinh/chi-so` | T |
| Tái lập thí nghiệm (chọn ngày → phiên bản Delta → Chạy lại → so sánh) | `POST /api/tai-lap/{ngay}` | T (giả lập), M |
| Phân nhóm hash ổn định: A = ALS thuần, B = ALS + 30% thịnh hành | `common/tron.chon_nhom`, `MAU_KHOI` | T |

## 3.9 Airflow

| DAG | Lịch | Cấu trúc | TT |
|---|---|---|---|
| kn_pipeline_hang_ngay | `0 2 * * *` | Sensor → bronze_to_silver → thong_ke_san_pham; chuan_bi_ma_tran → huan_luyen_als → danh_gia → Branch (sinh_goi_y_toan_bo → tron_goi_y → ghi_gold / giu_mo_hinh_cu → canh_bao) | T (DagBag, nhánh) |
| kn_giam_sat_streaming | `*/15 * * * *` | Job thịnh hành còn sống + có dữ liệu 15 phút qua | T |
| kn_tinh_ab | `0 * * * *` | CDF → tổng hợp → z-test → ghi kèm phiên bản | T |
| kn_bao_tri | `0 4 * * *` | OPTIMIZE + ZORDER + VACUUM + xóa gợi ý > 7 ngày | T |
| kn_tai_lap | thủ công | `phien_ban_delta` qua `dag_run.conf`, chạy lại, so với lịch sử | T |

## 3.10 Rủi ro và phương án

| Rủi ro | Phương án trong mã | TT |
|---|---|---|
| ALS OOM với 1,4 triệu user | `--min-tuong-tac 3`; `--so-khoi` (numBlocks); hồ sơ `als` riêng | T |
| Không đủ traffic cho A/B | `transform/mo_phong_hien_thi.py` (phụ thuộc vị trí + độ liên quan), nhãn `mo_phong`, `ty_le_mo_phong` trên web | T |
| `recommendForAllUsers` quá chậm | `recommendForUserSubset` cho user hoạt động 30 ngày, còn lại fallback danh mục | T |
| Coverage quá thấp | Phân tích phân phối độ phổ biến + re-ranking novelty (β = 0,1; 0,3); quét β × số ứng viên (`kiem_tra/thu_xep_lai.py`); danh sách phục vụ xếp lại với 200 ứng viên, β = 1,0 (`sinh_goi_y_toan_bo.py`, MO_HINH.md mục 6, 12.2) | T, C (số liệu) |

## 3.11 Danh mục nghiệm thu

Kịch bản và bằng chứng từng mục: docs/HUONG_DAN_DEMO.md. Cả 8 mục đều cần chạy trên cụm với dữ liệu thật (**C**) để có số liệu đưa vào báo cáo.
