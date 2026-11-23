# Hệ khuyến nghị sản phẩm cho sàn thương mại điện tử

Đề tài 3, học phần **Dữ liệu lớn (Big Data)** — Trường Đại học Tân Tạo.

**Trọng tâm kỹ thuật:** MLlib ALS (implicit feedback) + kết hợp batch và streaming + thí nghiệm A/B tái lập được bằng Delta Lake time travel.

**Dữ liệu:** RetailRocket Recommender System Dataset (Kaggle): khoảng 2,7 triệu sự kiện `view / addtocart / transaction` của 1,4 triệu khách trong 4,5 tháng, 235.000 sản phẩm.

**Hạ tầng:** 6 máy phòng lab và 2 máy cá nhân, nối với nhau qua ZeroTier (mạng `ttunet`, `633e31d8a29b038c`). Dự án dùng tiếp cụm hiện có của nhóm: Spark 3.5.3 standalone, HDFS 3.5.0 và Kafka 3.7.2 KRaft, tất cả chạy bằng Docker.

---

## 1. Kiến trúc tổng quan

```
                       ┌──────────── labsit 10.68.71.204 ────────────┐
 producer/phat_lai ──► │ Kafka: retailrocket-events   hien-thi       │ ◄── web (Thêm giỏ / impression)
 (1 ngày = 30 giây,    └──────┬─────────────────────────┬────────────┘
  +5% user mới)               │                         │
             ┌────────────────┼──────────────┐          │
             ▼                ▼              │          ▼
  [streaming/nap_su_kien] [streaming/tinh_thinh_hanh]  [streaming/ghi_hien_thi]
             │                │ cửa sổ 15p/1p, watermark 2p        │ MERGE theo lô
             ▼                ▼ update + MERGE                     ▼
     bronze.su_kien    gold.thinh_hanh_15p              gold.nhat_ky_hien_thi (CDF)
       (Parquet)                                                   │
             │ transform/bronze_to_silver (khử trùng 5s, lọc bot)  │ transform/tinh_ket_qua_ab
             ▼                                                     ▼ (đọc CDF, z-test)
     silver.tuong_tac ──► gold.thong_ke_san_pham         gold.ket_qua_ab (+ phiên bản Delta)
             │
             ▼ ml/chuan_bi_ma_tran (StringIndexer nối nhãn, suy giảm thời gian)
     features.ma_tran_tuong_tac ──► [ALS tuning 27/9 tổ hợp] ──► danh_gia (2 baseline, coverage, novelty)
                                                                        │ NDCG có cải thiện?
                                       sinh_goi_y_toan_bo ◄── có ───────┤
                                               │                        └── không ──► giữ mô hình cũ + cảnh báo
                                               ▼
                         tron_goi_y (A/B, cold start, item mới, v2 lọc tầm giá) ──► ghi_gold (MERGE theo user)
                                                                                         │
                                                                                         ▼
                                                     gold.goi_y_nguoi_dung (ZORDER BY ma_nguoi_dung)
                                                                                         │
                                         Web giả lập sàn TMĐT (labsoe-18 10.68.71.149:8000) ◄─┘
```

Mọi bảng nằm trên HDFS dưới gốc `hdfs://10.68.71.113:8020/lakehouse/bd03_khuyen_nghi/` đúng quy ước mục A.4 của đề cương (bronze, silver, gold, features, models, checkpoints, `_quarantine`). Bronze là Parquet; silver, features, gold và `_quarantine` là Delta Lake 3.2.0. Năm DAG Airflow 3.3.1 trên máy master điều phối các job lô. Chi tiết ở [docs/KIEN_TRUC.md](docs/KIEN_TRUC.md).

### Phân vai 8 máy

| Máy | IP ZeroTier | Vai trò trong đề tài 3 |
|---|---|---|
| labsoe-11 | 10.68.71.113 | HDFS NameNode, Spark Master, Airflow (cổng 8088) |
| labsit12 | 10.68.71.198 | DataNode + Spark Worker |
| labsit | 10.68.71.204 | DataNode + Spark Worker + **Kafka** (bộ phát chạy ở đây) |
| labsit-may25 | 10.68.71.42 | DataNode + Spark Worker |
| labsit-may26 | 10.68.71.90 | DataNode + Spark Worker |
| labsoe-18 | 10.68.71.149 | DataNode + Spark Worker + **web** giả lập sàn TMĐT (cổng 8000) |
| Ctam (WSL2 trong laptop) | 10.68.71.205 | Dự phòng (chạy web đến 11/11, xem TRIEN_KHAI_CUM mục 5) |
| Máy Windows cá nhân | 10.68.71.59 | Máy trình chiếu: trình duyệt mở web và các trang giám sát |

Danh bạ này là nguồn sự thật duy nhất, nằm ở [cau_hinh/cum.env](cau_hinh/cum.env). Tài nguyên đã đo trên máy thật (10/11/2026): mỗi máy lab 12 lõi, 7 GB RAM; RAM giao cho Spark ở từng worker được tính theo số đo đó (docs/TRIEN_KHAI_CUM.md, mục 1.4). Laptop điều khiển `whitehathacker` (10.68.71.182) cũng có trong danh bạ.

---

## 2. Cấu trúc thư mục

```
bd03_khuyen_nghi/
├── README.md               Tệp này
├── MO_HINH.md              Thẻ mô hình: dữ liệu, ALS, metric, baseline, cold start, v2, A/B
├── NHAT_KY.md              Nhật ký thực hiện, lỗi đã sửa, trạng thái kiểm chứng
├── requirements.txt        Thư viện Python (job chạy cục bộ, web, producer, test)
├── requirements-airflow.txt  Chỉ cần khi muốn chạy test DAG ngoài container
├── pytest.ini
├── cau_hinh/cum.env        Danh bạ 8 máy (IP, vai trò, lõi, RAM)
├── conf/                   spark-defaults.conf, hdfs-site.xml, core-site.xml (liên kết), .env.example
├── jobs/                   Toàn bộ job PySpark, nộp bằng ha_tang/spark/submit.sh
│   ├── common/             Thư viện dùng chung (đóng gói thành common.zip gửi executor)
│   ├── streaming/          nap_su_kien, tinh_thinh_hanh, ghi_hien_thi
│   ├── transform/          nhap_danh_muc, bronze_to_silver, thong_ke_san_pham,
│   │                       tinh_ket_qua_ab, mo_phong_hien_thi, tien_hoa_luoc_do
│   ├── ml/                 chuan_bi_ma_tran, huan_luyen_als, danh_gia, sinh_goi_y_toan_bo,
│   │                       tron_goi_y, ghi_gold, san_pham_lien_quan
│   ├── bao_tri/            toi_uu_delta (OPTIMIZE/ZORDER/VACUUM), do_zorder (đo trước/sau)
│   └── kiem_tra/           khoi_tao_bang, tai_lap_huan_luyen, tai_lap_ab, kiem_tra_thinh_hanh, ...
├── producer/               Bộ phát Kafka (phat_lai_su_kien.py) và người dùng ảo (nguoi_dung_ao.py)
├── web/                    FastAPI + Jinja2: 5 màn hình và API theo mục 3.8
├── airflow/dags/           5 DAG theo mục 3.9
├── ha_tang/                Docker compose cho Hadoop, Spark, Kafka, Airflow, Web
├── scripts/                Khởi động từng máy, điều khiển cụm qua SSH, kiểm tra cụm, chạy lần đầu, demo
├── notebooks/              eda_retailrocket.ipynb: khảo sát dữ liệu gốc (số liệu do kiem_tra/eda.py tính trên cụm)
├── tests/                  Kiểm thử pytest trên Spark local thật và Airflow 3.3.1 thật
├── docs/                   KIEN_TRUC, TRIEN_KHAI_CUM, HUONG_DAN_DEMO, DOI_CHIEU_YEU_CAU, CHUAN_BI_BAO_VE,
│                           AI_USAGE (khai báo dùng AI, mục A.9), bang_chung/ (output thu trên cụm)
└── models/                 Ghi chú về mô hình (mô hình thật nằm trên HDFS /lakehouse/bd03_khuyen_nghi/models)
```

### 2.1. Ánh xạ với cấu trúc repository chuẩn (mục A.5 đề cương)

Tên thư mục giữ theo tiếng Việt và theo cụm nhiều máy, vì container của từng máy gắn đúng các đường dẫn
này (`/opt/spark/jobs`, `/opt/bd03/airflow/dags`). Bảng dưới chỉ ra mỗi mục của cấu trúc chuẩn nằm ở đâu.

| Cấu trúc chuẩn (A.5) | Trong repo này | Ghi chú |
|---|---|---|
| `docker-compose.yml` | `ha_tang/{hadoop,spark,kafka,airflow,web}/docker-compose*.yml` | Mỗi vai trò một tệp vì 6 máy chạy các tập vai trò khác nhau; `scripts/cum/khoi_dong_may.sh` bật đúng vai trò của từng máy |
| `conf/` | `conf/` (+ nguồn thật `ha_tang/hadoop/config/`, `cau_hinh/cum.env`) | `spark-defaults.conf` khớp `submit.sh` (có kiểm thử) |
| `spark_jobs/ingest/` | `jobs/streaming/nap_su_kien.py`, `jobs/transform/nhap_danh_muc.py` | Nạp bronze là luồng Kafka → Parquet |
| `spark_jobs/transform/` | `jobs/transform/` | bronze → silver → gold, A/B |
| `spark_jobs/streaming/` | `jobs/streaming/` | 3 luồng Structured Streaming |
| `spark_jobs/ml/` | `jobs/ml/` (+ `jobs/common/als_lib.py`) | train, evaluate, predict, trộn |
| (bảo trì, kiểm tra) | `jobs/bao_tri/`, `jobs/kiem_tra/` | OPTIMIZE/ZORDER/VACUUM; tái lập, thí nghiệm, EDA |
| `dags/` | `airflow/dags/` | 5 DAG |
| `webapp/backend/` | `web/app.py`, `web/dich_vu/` | FastAPI |
| `webapp/frontend/` | `web/templates/`, `web/static/` | Jinja2 + Chart.js (đóng gói sẵn trong `web/static/vendor/`) |
| `notebooks/` | `notebooks/eda_retailrocket.ipynb` | EDA |
| `tests/` | `tests/` | Kiểm thử chất lượng dữ liệu, logic job, DAG, web, script hạ tầng |
| `docs/` | `docs/`, `bao_cao/` | Sơ đồ kiến trúc, báo cáo, slide, AI_USAGE |
| `README.md` | `README.md` | Dựng lại hệ thống từ đầu: mục 3 và docs/TRIEN_KHAI_CUM.md |

---

## 3. Bắt đầu nhanh

### 3.1. Chạy kiểm thử trên một máy bất kỳ (không cần cụm)

Cần Python 3.10–3.11 và Java 17.

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
pytest                      # 143 test chạy trên Spark local; test DAG tự bỏ qua nếu thiếu Airflow
```

Test DAG Airflow (tùy chọn, nên dùng venv riêng vì Airflow kéo nhiều thư viện):

```bash
python3 -m venv .venv-airflow && . .venv-airflow/bin/activate
pip install -r requirements-airflow.txt
pytest tests/test_ha_tang.py
```

### 3.2. Xem giao diện web với dữ liệu giả lập

```bash
cd web
WEB_CHE_DO=gia PYTHONPATH=../jobs uvicorn app:app --port 8000
# mở http://localhost:8000
```

Chế độ `gia` chỉ để xem giao diện: mọi con số trên màn hình là dữ liệu giả lập, không phải kết quả thí nghiệm.

### 3.3. Chạy toàn bộ pipeline trên một máy với Delta thật

```bash
bash scripts/kiem_tra_cuc_bo.sh            # máy ít RAM, ví dụ WSL2: BO_NHO=2g bash scripts/kiem_tra_cuc_bo.sh
```

Trên Windows, chạy trong Ubuntu của WSL2 (PySpark ghi tệp cục bộ trên Windows thuần cần thêm winutils). Script sinh một bộ dữ liệu nhỏ đúng định dạng RetailRocket ([scripts/tao_du_lieu_mau.py](scripts/tao_du_lieu_mau.py)), rồi chạy lần lượt mọi job với Spark local, Delta Lake thật và nguồn luồng đọc từ tệp thay cho Kafka. Script cần tải Delta qua `--packages` (Internet tới Maven Central), hoặc đặt sẵn `DELTA_JARS=/duong/delta-spark_2.12-3.2.0.jar,/duong/delta-storage-3.2.0.jar`.

### 3.4. Triển khai trên cụm 6 máy lab

Tóm tắt (chi tiết từng máy ở [docs/TRIEN_KHAI_CUM.md](docs/TRIEN_KHAI_CUM.md)):

```bash
# Từ laptop điều khiển (Git Bash), sau khi cài SSH theo docs/TRIEN_KHAI_CUM.md mục 1.5:
bash scripts/cum/ssh_tat_ca.sh --dong-bo-ma                                        # đẩy mã sang 6 máy
bash scripts/cum/ssh_tat_ca.sh "cd ~/bd03_khuyen_nghi && bash scripts/cum/khoi_dong_may.sh"   # bật theo vai trò

# Từ bất kỳ máy nào: kiểm tra toàn cụm
bash scripts/cum/kiem_tra_cum.sh

# Trên laptop: tải RetailRocket bằng khóa Kaggle của laptop, chép sang master và máy Kafka
bash scripts/cum/chep_du_lieu.sh
# Trên máy master (labsoe-11), lần đầu:
bash scripts/tai_du_lieu.sh                        # ~/du_lieu/retailrocket -> HDFS /lakehouse/bd03_khuyen_nghi/landing
# Trên máy Kafka (labsit):
bash scripts/tao_topic.sh
python3 producer/phat_lai_su_kien.py --tep ~/du_lieu/retailrocket/events.csv &
# Trên máy master:
bash scripts/chay_lan_dau.sh                       # toàn bộ pipeline theo thứ tự của DAG 1
bash scripts/luong.sh khoi_dong tat_ca             # 3 luồng streaming chạy liên tục
```

Cụm đã chạy từ trước ngày 15/11/2026 với gốc cũ `/lake`: chuyển sang gốc chuẩn MỘT LẦN từ laptop bằng
`bash scripts/cum/chuyen_lakehouse.sh --chay` (dừng luồng, đồng bộ mã, `hdfs dfs -mv`, viết lại `_spark_metadata`
của bronze, kiểm tra, bật lại; chi tiết docs/TRIEN_KHAI_CUM.md mục 8). Sau đó thu bằng chứng cho báo cáo trên master:
`bash scripts/thu_bang_chung.sh`.

Sau đó Airflow (http://10.68.71.113:8088) chạy DAG hằng đêm, web ở http://10.68.71.149:8000 (ngoài trường: `ssh -N -L 8001:localhost:8000 labsoe-18`, rồi mở http://localhost:8001).

---

## 4. Ánh xạ nhanh tới đặc tả đề tài

| Mục đề tài | Hiện thực |
|---|---|
| 3.4 Đặc tả bảng | `jobs/common/luoc_do.py`, `jobs/common/cau_hinh.py`; cột bổ sung giải thích trong docs/KIEN_TRUC.md |
| 3.5 Job Spark | 6 job bắt buộc + các job phụ trợ trong `jobs/` |
| 3.6 Mô hình | `jobs/common/als_lib.py`, `jobs/ml/*`; xem MO_HINH.md |
| Cold start (chấm riêng) | `jobs/common/tron.py`, `jobs/ml/tron_goi_y.py`, `web/dich_vu/goi_y.py` |
| 3.7 Delta Lake | MERGE (`ghi_gold`, `tinh_thinh_hanh`, `ghi_hien_thi`), time travel (`kiem_tra/tai_lap_*`), schema evolution (`tien_hoa_luoc_do`), CDF (`tinh_ket_qua_ab`), ZORDER (`bao_tri/*`), partition (`bronze_to_silver`) |
| 3.8 Web | `web/` — 5 màn hình, API đúng tên trong đề |
| 3.9 Airflow | `airflow/dags/` — 5 DAG đúng lịch và đúng nhánh |
| 3.10 Rủi ro | lọc user ≥ 3 tương tác, `numBlocks`, traffic mô phỏng có gắn nhãn, chỉ sinh cho user hoạt động 30 ngày, re-ranking novelty |
| 3.11 Nghiệm thu | Kịch bản từng mục ở [docs/HUONG_DAN_DEMO.md](docs/HUONG_DAN_DEMO.md) |

Ánh xạ tới quy định chung (Phần A của đề cương):

| Mục | Hiện thực |
|---|---|
| A.3 Môi trường | Hadoop 3.5.0, Spark 3.5.3, Delta 3.2.0, Airflow 3.3.1, Kafka 3.7.2 (đều ≥ phiên bản tối thiểu), FastAPI + Jinja2 + Chart.js, Git |
| A.4 Cây HDFS | `/lakehouse/bd03_khuyen_nghi/{bronze,silver,gold,features,models,checkpoints,_quarantine}`; `_quarantine.tuong_tac` giữ mọi dòng bị loại kèm `ly_do` |
| A.6.1 Spark | Job lô (bronze_to_silver...), MLlib ALS, 3 luồng streaming chạy liên tục |
| A.6.2 Delta | MERGE INTO (ghi_gold, tinh_thinh_hanh, ghi_hien_thi, tinh_ket_qua_ab, tien_hoa_luoc_do); DESCRIBE HISTORY và VERSION AS OF (`scripts/demo/delta_sql.sh`, DAG 5); OPTIMIZE/ZORDER có số đo trước/sau (`bao_tri/do_zorder.py`) |
| A.6.3 HDFS | Mọi bảng trên HDFS; `hdfs dfs -du -h /lakehouse/bd03_khuyen_nghi` trong `docs/bang_chung/cum/01_hdfs_du_h.txt` |
| A.6.4 Web | 5 màn hình; ghi ngược Delta trực tiếp (Tái lập → `gold.ket_qua_tai_lap`) và qua Kafka → MERGE (`gold.nhat_ky_hien_thi`) |
| A.6.5 Airflow | 5 DAG khác lịch, retries, **Deadline Alerts** (thay `sla` mà Airflow 3 đã bỏ), BranchPythonOperator + PythonSensor + ShortCircuitOperator |
| A.8 | Không kéo dữ liệu lớn về driver; độ trễ web đo được (`/api/hieu-nang`, `scripts/do_do_tre_web.py`); backfill DAG 1 (khoảng ngày) và DAG 3 (`airflow backfill create`) |
| A.9 | [docs/AI_USAGE.md](docs/AI_USAGE.md) |

Bảng đối chiếu đầy đủ từng yêu cầu: [docs/DOI_CHIEU_YEU_CAU.md](docs/DOI_CHIEU_YEU_CAU.md).

---

## 5. Trạng thái kiểm chứng (cập nhật 15/11/2026)

| Hạng mục | Trạng thái |
|---|---|
| Logic job (làm sạch, quarantine, trọng số, suy giảm, chia train/test, ALS, metric, trộn, cold start, A/B) | Kiểm thử tự động trên Spark local thật (`pytest`) |
| 5 DAG Airflow: nạp, tuần tự hóa, Deadline Alert, tham số backfill | Kiểm thử bằng Airflow 3.3.1 thật (`tests/test_airflow_dags.py`); Deadline Alert và `airflow backfill create` chạy thử trên Airflow thật: docs/bang_chung/kiem_chung_airflow_cuc_bo.md; backfill DAG 3 trên cụm: docs/bang_chung/cum/10_backfill_dag3.txt |
| Chuyển gốc `/lake` → `/lakehouse/bd03_khuyen_nghi` | Thử với Spark thật trên đĩa cục bộ: dời bảng, viết lại `_spark_metadata`, luồng chạy tiếp không xử lý lại, hoàn tác (`tests/test_chuyen_lakehouse.py`) |
| Web: 5 màn hình, API, ghi ngược, đo độ trễ | TestClient ở chế độ giả lập; số đo thật trong docs/bang_chung/cum/ |
| Pipeline đầy đủ với dữ liệu thật RetailRocket | Đã chạy trên cụm 10–11/11/2026: 8 bước, 3 luồng, 5 DAG, 2 lần tái lập trùng khớp. Số liệu ở NHAT_KY.md và báo cáo |

---

## 6. Tài liệu

- [MO_HINH.md](MO_HINH.md) — thẻ mô hình và cơ sở lý thuyết
- [NHAT_KY.md](NHAT_KY.md) — nhật ký thực hiện
- [docs/KIEN_TRUC.md](docs/KIEN_TRUC.md) — kiến trúc, đặc tả bảng, quyết định thiết kế
- [docs/TRIEN_KHAI_CUM.md](docs/TRIEN_KHAI_CUM.md) — triển khai từng máy trong 8 máy
- [docs/HUONG_DAN_DEMO.md](docs/HUONG_DAN_DEMO.md) — kịch bản bảo vệ theo danh mục nghiệm thu 3.11
- [docs/DOI_CHIEU_YEU_CAU.md](docs/DOI_CHIEU_YEU_CAU.md) — đối chiếu từng yêu cầu của đề tài và của đề cương (Phần A, C)
- [docs/AI_USAGE.md](docs/AI_USAGE.md) — khai báo sử dụng AI (mục A.9)
- [docs/bang_chung/](docs/bang_chung/) — output thu trên cụm: du -h, DESCRIBE HISTORY, độ trễ web, EDA
- [notebooks/eda_retailrocket.ipynb](notebooks/eda_retailrocket.ipynb) — khảo sát dữ liệu
- [conf/](conf/) — cấu hình dùng chung
- [models/README.md](models/README.md) — nơi lưu và cách lấy mô hình

## 7. Nhóm thực hiện

Sinh viên Lê Chí Tâm và Lê Nguyễn Quốc Anh. Repo cũ của nhóm (`spark-cluster`, `hadoop-cluster`, `kafka-docker`) vẫn tương thích với repo mới: giữ nguyên tên container, tên volume và cổng, sửa các lỗi cấu hình ghi ở NHAT_KY.md.
