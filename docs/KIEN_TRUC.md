# Kiến trúc hệ thống bd03_khuyen_nghi

## 1. Nguyên tắc thiết kế

1. **Dùng tiếp cụm của nhóm, không dựng lại.** Giữ nguyên phiên bản (Spark 3.5.3, Hadoop 3.5.0, Kafka 3.7.2), tên container (`spark-master`, `hadoop-master`, `hadoop-worker`, `kafka-server`), tên volume và cổng. Repo cũ chỉ được sửa ở những điểm sai cấu hình (xem NHAT_KY.md, mục 2).
2. **Truy cập bảng theo đường dẫn.** Cụm Spark standalone của nhóm không có Hive Metastore, nên tên `silver.tuong_tac` không tồn tại giữa các phiên Spark. Tên bảng logic chỉ là khóa tra cứu trong `jobs/common/cau_hinh.py::_BANG`; SQL dùng dạng `delta.\`hdfs://.../lake/silver/tuong_tac\``.
3. **Mọi job idempotent.** Chạy lại cho cùng kết quả: bronze→silver ghi đè đúng phân vùng ngày bằng `replaceWhere`, các bảng gold dùng `MERGE` hoặc ghi đè cả bảng trung gian. Airflow retry an toàn.
4. **Mỗi commit mang siêu dữ liệu.** `userMetadata` của commit Delta ghi `ma_chay`, `ngay_cat`, các phiên bản nguồn... Đây là cầu nối giữa kết quả và đúng phiên bản dữ liệu đã sinh ra nó.
5. **Một nguồn logic cho mọi nơi.** Logic trộn gợi ý, tổng hợp A/B, metric, kiểm định nằm trong `jobs/common/` và được dùng chung bởi job batch, web, job tái lập và test. Tái lập vì thế chắc chắn dùng cùng công thức.
6. **Kết quả job là dòng JSON cuối stdout** (`in_ket_qua`). Airflow đọc dòng này qua XCom để rẽ nhánh.

## 2. Triển khai vật lý

```
                                 ZeroTier ttunet 633e31d8a29b038c (10.68.71.0/24)
┌───────────────────────────── labsoe-11 · 10.68.71.113 ─────────────────────────────┐
│ hadoop-master (NameNode :8020, UI :9870)    spark-master (:7077, UI :8080,         │
│ Airflow 3.3.1: api-server :8088, scheduler,  driver 4041-4060, job UI 4140-4150)   │
│   dag-processor, postgres (triggerer tắt)    ./jobs gắn vào /opt/spark/jobs        │
└────────────────────────────────────────────────────────────────────────────────────┘
┌ labsit12 .198 ┐ ┌ labsit .204 ──────────────┐ ┌ may25 .42 ┐ ┌ may26 .90 ┐ ┌ labsoe-18 .149 ┐
│ hadoop-worker │ │ hadoop-worker             │ │ hadoop-w. │ │ hadoop-w. │ │ hadoop-worker  │
│ spark-worker  │ │ spark-worker              │ │ spark-w.  │ │ spark-w.  │ │ spark-worker   │
│               │ │ kafka-server :9092        │ │           │ │           │ │ bd03-web :8000 │
│               │ │ kafka-ui :8080            │ │           │ │           │ │                │
│               │ │ producer/phat_lai_su_kien │ │           │ │           │ │                │
└───────────────┘ └───────────────────────────┘ └───────────┘ └───────────┘ └────────────────┘
┌ Laptop whitehathacker · 10.68.71.182 ────┐   ┌ Windows · 10.68.71.59 ──────────────────────┐
│ Git Bash: SSH vào cụm, trình chiếu       │   │ Trình duyệt: web :8000, Spark :8080,        │
│ WSL2 "Ctam" .205: dự phòng               │   │ HDFS :9870, Airflow :8088, Kafka UI :8080   │
└──────────────────────────────────────────┘   └─────────────────────────────────────────────┘
```

- Spark Worker chạy `network_mode: host` và khai báo đúng IP ZeroTier của máy (`--host $NODE_IP`), nên executor trên máy khác nối được.
- DataNode khai báo `dfs.datanode.hostname = ${env.NODE_IP}` và client dùng `dfs.client.use.datanode.hostname=true`: client (driver, executor, web) nối tới DataNode bằng IP ZeroTier, không bằng IP nội bộ của container.
- Số bản sao HDFS lấy từ `HE_SO_SAO_CHEP` trong `cau_hinh/cum.env`, áp dụng cho cả HDFS và mọi job Spark (`spark.hadoop.dfs.replication`): 2 khi các máy nối thẳng với nhau (mất một máy vẫn còn dữ liệu), 1 khi ZeroTier phải đi đường chuyển tiếp (executor ghi bản duy nhất xuống DataNode của chính máy mình).
- Spark Master chạy `network_mode: host` với `--host 10.68.71.113` để quảng bá đúng địa chỉ cho worker. Spark driver chạy bên trong container `spark-master` (client mode, do `submit.sh` gọi `docker exec`), nên dùng chung mạng của máy master. Mỗi hồ sơ job có cổng driver riêng để nhiều job chạy song song.
- NameNode chạy trong mạng bridge: `dfs.namenode.rpc-address = 10.68.71.113:8020` là địa chỉ quảng bá (DataNode đọc dòng này để tìm NameNode), còn `rpc-bind-host = 0.0.0.0` để lắng nghe bên trong container.
- Web (`bd03-web`, trên labsoe-18 cạnh DataNode và Spark worker) chạy Spark cục bộ `local[2]` ngay trong tiến trình FastAPI để đọc Delta trực tiếp từ HDFS; dùng 2 trong 4 lõi máy chưa giao cho worker, không chiếm lõi của cụm.

### Hồ sơ tài nguyên (`ha_tang/spark/submit.sh`)

| Hồ sơ | Dùng cho | `cores.max` | Lõi/executor | RAM executor | RAM driver | Shuffle |
|---|---|---|---|---|---|---|
| `nap` | streaming/nap_su_kien (chạy liên tục) | 1 | 1 | 768m | 512m | 4 |
| `thinh_hanh` | streaming/tinh_thinh_hanh (chạy liên tục, state store RocksDB) | 2 | 2 | 1g | 768m | 8 |
| `hien_thi` | streaming/ghi_hien_thi (chạy khi có traffic web) | 1 | 1 | 768m | 640m | 4 |
| `lo` | job lô thường | `LOI_LO` (8) | 2 | `RAM_LO` (2g) | 1g | 64 |
| `als` | tuning, đánh giá, sinh gợi ý | `LOI_ALS` (10) | 2 | `RAM_ALS` (2g) | 1536m | 64 |
| `bao_tri` | OPTIMIZE / ZORDER / VACUUM | 4 | 2 | 1536m | 1g | 32 |
| `tai_lap` | DAG 5 | như `als` | 2 | như `als` | 1536m | 64 |
| `nhanh` | job kiểm tra nhỏ | 2 | 1 | 768m | 640m | 8 |

Cỡ theo máy thật (12 lõi, 7 GB RAM mỗi máy; tổng RAM executor của 5 worker là 13 GB). Driver chạy trong container `spark-master` trên máy master nên RAM driver được giữ nhỏ. Luồng thịnh hành để trạng thái cửa sổ trong RocksDB thay vì heap JVM. NameNode, DataNode giới hạn heap 1 GB; tiến trình Spark Master/Worker 512 MB.

Ở chế độ standalone, ứng dụng đến trước sẽ chiếm hết lõi nếu không đặt `spark.cores.max`; ba luồng streaming luôn giữ tổng cộng 5 lõi, phần còn lại dành cho job lô.

## 3. Luồng dữ liệu

### 3.1. Đường streaming (giây)

1. `producer/phat_lai_su_kien.py` (máy Kafka) và web đẩy JSON vào topic `retailrocket-events`.
2. `streaming/nap_su_kien.py` (trigger 15 giây) chuẩn hóa sang lược đồ tiếng Việt, bổ sung danh mục từ `silver.san_pham`, ghi **Parquet** phân vùng theo `ngay` vào `bronze/su_kien_v2`, checkpoint trên HDFS.
3. `streaming/tinh_thinh_hanh.py` (trigger 20 giây) đọc **cùng topic**, đếm theo cửa sổ trượt 15 phút / bước 1 phút trên `kafka_ts`, watermark 2 phút, `outputMode("update")`, `foreachBatch` + `MERGE` vào `gold.thinh_hanh_15p`. DAG 4 xóa các cửa sổ cũ hơn 2 ngày mỗi đêm.

Hai luồng đọc độc lập từ Kafka (không đọc nối tiếp qua bronze), nên độ trễ của khối thịnh hành không cộng thêm độ trễ ghi bronze. Mục tiêu nghiệm thu ≤ 90 giây: trigger 20 giây + thời gian xử lý lô + web đọc lại bảng.

### 3.2. Đường batch (hằng đêm, DAG 1)

```
cho_du_lieu_ngay_moi (sensor) → tao_ma_chay → bronze_to_silver ─┬→ thong_ke_san_pham → san_pham_lien_quan
                                                                │            └──────────────┐
                                                                └→ chuan_bi_ma_tran → huan_luyen_als → danh_gia
                                                                                                     │
                                                         [ndcg_co_cai_thien] ─ có ──→ sinh_goi_y_toan_bo → tron_goi_y → ghi_gold
                                                                             └ không → giu_mo_hinh_cu → canh_bao
```

`thong_ke_san_pham` là đầu vào của `tron_goi_y` (bán chạy theo danh mục, item mới), nên có thêm cạnh `thong_ke_san_pham >> tron_goi_y`.

### 3.3. Vòng khép kín của web

```
Trang chủ ─ GET /api/goi-y/{u} ─► đọc gold.goi_y_nguoi_dung (+ thay ô thịnh hành bằng gold.thinh_hanh_15p mới nhất)
    │                              └─► đẩy 10 impression vào topic hien-thi
    ▼
Trang sản phẩm ─ POST /api/su-kien (Xem / Thêm giỏ / Mua) ─► topic retailrocket-events ─► bronze, thịnh hành
    │                                                     └─► topic hien-thi (phản hồi click / mua)
    ▼
streaming/ghi_hien_thi ─► MERGE gold.nhat_ky_hien_thi (CDF bật) ─► transform/tinh_ket_qua_ab (DAG 3, mỗi giờ)
                                                                   └─► gold.ket_qua_ab (+ phien_ban_delta_du_lieu)
```

Web **không ghi thẳng** impression vào Delta: mỗi impression sẽ là một commit và vô số tệp nhỏ. Web đẩy vào Kafka, `ghi_hien_thi` gom theo lô 30 giây rồi `MERGE` (dòng mới được chèn, phản hồi click/mua cập nhật dòng đã có theo `ma_hien_thi`). Đây là job bổ sung so với bảng 3.5.

## 4. Bố cục hồ dữ liệu trên HDFS

Gốc theo quy ước mục A.4 của đề cương: `hdfs:///lakehouse/<ma_de_tai>/` (đường dẫn trong
`jobs/common/cau_hinh.py`: `LAKE_DUONG_DAN = /lakehouse/bd03_khuyen_nghi`).

```
/lakehouse/bd03_khuyen_nghi
├── landing/retailrocket/        CSV gốc (events, item_properties_part1/2, category_tree)
├── bronze/su_kien/              Parquet, ngay=YYYY-MM-DD/   (+ _spark_metadata của file sink), append-only
├── silver/tuong_tac/            Delta, phân vùng theo ngay
├── silver/san_pham/             Delta
├── features/ma_tran_tuong_tac/  Delta
├── gold/<bảng>/                 Delta (12 bảng, mục 5)
├── checkpoints/<luồng>/         checkpoint Structured Streaming (offset Kafka, trạng thái RocksDB)
├── models/
│   ├── user_indexer/, item_indexer/     StringIndexerModel (chỉ nối nhãn, không fit lại)
│   ├── als/<ma_chay>/                    mô hình tốt nhất của lần tuning (học trên train)
│   └── als/<ma_chay>_phuc_vu/            mô hình phục vụ (fit lại trên train + test)
└── _quarantine/tuong_tac/       Delta, phân vùng theo ngay: mọi dòng bronze bị loại khỏi silver + cột ly_do
```

**Chuyển gốc ngày 15/11/2026.** Từ 09/11 đến 15/11 hồ dữ liệu nằm ở `/lake` (bronze ở `/lake/bronze/su_kien_v2`
vì `/lake/bronze/su_kien` là dữ liệu cũ của nhóm, lược đồ khác). `scripts/cum/chuyen_lakehouse.sh` chuyển sang gốc
chuẩn bằng `hdfs dfs -mv`, không chép lại khối dữ liệu nào:

- Bảng Delta ghi đường dẫn **tương đối** của tệp dữ liệu trong `_delta_log`, nên dời cả thư mục bảng vẫn đọc được;
  lịch sử phiên bản và `VERSION AS OF` giữ nguyên.
- Checkpoint streaming chỉ chứa offset Kafka, mã lô và trạng thái RocksDB theo đường dẫn tương đối.
- Ngoại lệ là bronze: file sink Parquet ghi **đường dẫn tuyệt đối** của từng tệp vào `_spark_metadata`. Script
  viết lại tiền tố của mọi tệp trong đó. Nếu không viết lại, đọc bronze sẽ báo FileNotFound.
- Đường dẫn mô hình đã ghi trong `gold.lich_su_huan_luyen` (dạng `.../lake/models/...`) được đổi sang gốc hiện tại
  khi đọc (`cau_hinh.duong_dan_mo_hinh_hien_tai`). Bảng lịch sử giữ nguyên, không ghi lại.
- Chốt an toàn: `submit.sh` không nộp job khi mã đã dùng gốc mới mà dữ liệu vẫn còn ở `/lake`, để không job nào
  tạo bảng rỗng ở gốc mới trước khi chuyển. Web cũng chỉ ghi ngược khi bảng đích đã có ở gốc hiện tại.

Toàn bộ quy trình (dời, viết lại metadata, luồng chạy tiếp không xử lý lại lô cũ, hoàn tác) được kiểm thử với
Spark thật trên đĩa cục bộ: `tests/test_chuyen_lakehouse.py`.

## 5. Đặc tả bảng

Cột in đậm là cột **thêm vào** so với mục 3.4 của đề tài; mọi cột của đặc tả được giữ đúng tên và kiểu.

### bronze.su_kien (Parquet, phân vùng `ngay`)

`ma_su_kien, thoi_diem, ma_nguoi_dung, ma_phien, ma_san_pham, loai_su_kien, ma_danh_muc, thiet_bi, ngay` + **`ma_giao_dich, kafka_ts, _nguon, _ingest_ts, _schema_ver`**

| Cột thêm | Lý do |
|---|---|
| `ma_su_kien` | SHA-256 của `topic|partition|offset|value`: duy nhất và tất định, nên giao lại cùng thông điệp vẫn khử trùng được |
| `ma_phien` | RetailRocket không có phiên: gom theo khung 30 phút của từng user nếu thông điệp không mang sẵn |
| `ma_giao_dich` | Giữ `transactionid` gốc cho phân tích mua hàng |
| `kafka_ts` | Thời điểm broker nhận; luồng thịnh hành gom theo cột này (MO_HINH.md, mục 10.1); dùng để đo độ trễ |
| `_nguon` | `phat_lai` / `nguoi_moi` / `web` / `cu`: loại sự kiện web khỏi train/test, đếm tỷ lệ user mới |
| `_ingest_ts` | Mốc xử lý tăng dần của bronze→silver |
| `_schema_ver` | Phân biệt với dữ liệu bronze cũ của nhóm |

Bronze để Parquet thuần như job cũ của nhóm: tầng thô chỉ ghi nối, không cần MERGE hay time travel.

### _quarantine.tuong_tac (Delta, phân vùng `ngay`) — mục A.4

Mọi cột của bronze + **`ly_do`** + **`_cach_ly_luc`**. `transform/bronze_to_silver.py` chia mỗi dòng bronze của các
ngày đang xử lý vào đúng một trong hai bảng: silver hoặc quarantine. Lý do theo đúng thứ tự các bước làm sạch:
`thieu_truong`, `trung_ma_su_kien`, `trung_noi_dung`, `bot`, `nhap_lap_5_giay`, `loai_su_kien_khong_dung`
(vd. `bo_gio`). Bảng ghi đè đúng các phân vùng ngày vừa tính, như silver, nên chạy lại không nhân đôi.
Đếm theo `ly_do` chính là bảng thống kê làm sạch của báo cáo (`scripts/demo/delta_sql.sh cach_ly`). Chọn bản
giữ lại trong nhóm trùng bằng `row_number` theo `_ingest_ts` (bản nạp sớm nhất), không dùng `dropDuplicates`:
`dropDuplicates` giữ một dòng tùy ý, tính hai lần (cho silver và cho quarantine) có thể chọn hai dòng khác nhau.

### silver.tuong_tac (Delta, phân vùng `ngay`)

`ma_nguoi_dung, ma_san_pham, thoi_diem, loai_su_kien, trong_so, ngay` + **`_nguon`**; sau schema evolution tuần 5 có thêm `gia_san_pham, xep_hang_sao`.

**Vì sao phân vùng theo `ngay` mà không theo `ma_nguoi_dung`:** 1,4 triệu user sẽ tạo 1,4 triệu thư mục, mỗi thư mục vài chục byte dữ liệu (trung bình chưa tới 2 sự kiện/user). NameNode giữ siêu dữ liệu của mọi tệp trong RAM (khoảng 150 byte mỗi đối tượng), nhật ký Delta phình to theo số tệp, và mỗi truy vấn quét toàn bảng phải mở hàng triệu tệp nhỏ. Ngược lại, 4,5 tháng chỉ có khoảng 140 phân vùng ngày, mỗi phân vùng đủ lớn; mọi job batch đều lọc theo khoảng ngày (xử lý tăng dần, chia train/test theo thời gian), nên cắt tỉa phân vùng theo `ngay` có lợi thật. Truy vấn theo user (truy vấn nóng của web) được phục vụ ở bảng gold bằng Z-ORDER, không phải bằng phân vùng.

### silver.san_pham (Delta)

`ma_san_pham, ma_danh_muc, danh_muc_cha, gia_san_pham, con_hang` (+ `xep_hang_sao` sau tuần 5). Bảng phụ trợ, không có trong đặc tả: cần cho danh mục ở bronze, baseline 2, cold start và item mới.

### features.ma_tran_tuong_tac (Delta)

`user_idx INT, item_idx INT, diem DOUBLE` + **`tap`** (`train`/`test`). Lý do ở MO_HINH.md, mục 3.5. `userMetadata` của commit: `ngay_cat, ngay_cuoi, min_tuong_tac, phien_ban_silver, phien_ban_san_pham`.

### gold.goi_y_nguoi_dung (Delta, ZORDER BY `ma_nguoi_dung`)

Đúng 7 cột của đặc tả: `ma_nguoi_dung, xep_hang (1..20), ma_san_pham, diem_du_doan, nguon (als / pho_bien_danh_muc / thinh_hanh), phien_ban_mo_hinh, sinh_luc`. `phien_ban_mo_hinh` là `als_<ma_chay>` hoặc `als_v2_<ma_chay>`. Cập nhật bằng `MERGE` theo `(ma_nguoi_dung, xep_hang)`; thứ hạng thừa của chính user đó (danh sách mới ngắn hơn) bị xóa qua nhánh `WHEN MATCHED AND s._xoa THEN DELETE`. User không có trong lần chạy giữ danh sách cũ; DAG 4 xóa danh sách cũ hơn 7 ngày.

### gold.thinh_hanh_15p (Delta)

Đúng 6 cột của đặc tả. `MERGE` theo `(cua_so_bat_dau, cua_so_ket_thuc, ma_san_pham)`. Bảng nhận một commit mỗi 20 giây nên chỉ giữ lịch sử ngắn (`deletedFileRetentionDuration = 2 ngày`); DAG 4 xóa cửa sổ cũ hơn 2 ngày rồi `OPTIMIZE ZORDER BY (ma_san_pham)`.

### gold.nhat_ky_hien_thi (Delta, Change Data Feed bật từ phiên bản 0)

8 cột của đặc tả + **`nguon_traffic`** (`web` / `mo_phong` / `mo_phong_web`) và **`chien_luoc`** (`als` / `it_tuong_tac` / `nguoi_moi`). `nguon_traffic` bắt buộc theo mục 3.10 ("ghi rõ đây là traffic mô phỏng"); `chien_luoc` để chỉ đưa lượt hiển thị thuộc thí nghiệm (khối ALS) vào so sánh A/B.

### gold.ket_qua_ab (Delta)

9 cột của đặc tả + **`p_value_chuyen_doi`** (đề có hai tỷ lệ, `p_value` dành cho CTR) và **`ty_le_mo_phong`**. Hai nhóm của cùng một ngày có cùng `p_value` và cùng `phien_ban_delta_du_lieu`.

### Bảng phục vụ đánh giá và nghiệm thu (không có trong đặc tả)

| Bảng | Nội dung | Mục nghiệm thu |
|---|---|---|
| `gold.thong_ke_san_pham` | Đếm theo sản phẩm, `diem_pho_bien`, `so_xem_24h`, `trung_binh_24h` | Baseline, cold start, mẫu số điểm thịnh hành |
| `gold.san_pham_lien_quan` | `ma_san_pham, xep_hang, ma_san_pham_lien_quan, do_lien_quan` | Khối "Sản phẩm liên quan" |
| `gold.ket_qua_tuning` | Một dòng mỗi tổ hợp siêu tham số | Bảng tuning ≥ 9 tổ hợp |
| `gold.lich_su_huan_luyen` | Một dòng mỗi lần đánh giá: metric, baseline, quyết định, `chi_tiet_json` | So sánh ALS và 2 baseline, metric theo phiên bản |
| `gold.ket_qua_tai_lap` | So sánh metric cũ/mới khi chạy lại trên `VERSION AS OF` | Demo tái lập |
| `gold.do_hieu_nang_zorder` | Thời gian và số tệp đọc trước/sau ZORDER | Số liệu OPTIMIZE/ZORDER |
| `gold.goi_y_als_tam`, `gold.goi_y_tam` | Bảng trung gian giữa `sinh_goi_y_toan_bo` → `tron_goi_y` → `ghi_gold` | — |

## 6. Ánh xạ mục 3.7 (Delta Lake)

| Yêu cầu | Cách thực hiện | Tệp |
|---|---|---|
| MERGE | `gold.goi_y_nguoi_dung` theo user (không ghi đè toàn bảng); `gold.thinh_hanh_15p` upsert theo cửa sổ; `gold.nhat_ky_hien_thi` upsert theo `ma_hien_thi` | `ml/ghi_gold.py`, `streaming/tinh_thinh_hanh.py`, `streaming/ghi_hien_thi.py` |
| Time travel | Mỗi lần huấn luyện ghi `phien_ban_features`; DAG 5 nhận `phien_ban_delta` qua `dag_run.conf` và chạy lại trên `versionAsOf`. Màn hình "Tái lập" đọc impression log đúng `phien_ban_delta_du_lieu` | `kiem_tra/tai_lap_huan_luyen.py`, `kiem_tra/tai_lap_ab.py`, `web/dich_vu/kho_du_lieu.py` |
| Schema evolution | Tuần 5: `ALTER TABLE ADD COLUMNS (gia_san_pham, xep_hang_sao)` rồi `MERGE` điền giá trị; job silver tự bổ sung 2 cột cho dòng mới; mô hình v2 lọc theo tầm giá | `transform/tien_hoa_luoc_do.py`, `transform/bronze_to_silver.py`, `ml/tron_goi_y.py` |
| Change Data Feed | Bật lúc tạo `gold.nhat_ky_hien_thi`; job A/B đọc `table_changes` từ phiên bản đã xử lý lần trước để biết những ngày nào thay đổi, chỉ tính lại các ngày đó | `kiem_tra/khoi_tao_bang.py`, `transform/tinh_ket_qua_ab.py` |
| OPTIMIZE / Z-ORDER | `ZORDER BY (ma_nguoi_dung)` trên `gold.goi_y_nguoi_dung`; đo thời gian và số tệp đọc của truy vấn `WHERE ma_nguoi_dung = ?` trước/sau | `bao_tri/do_zorder.py`, `bao_tri/toi_uu_delta.py` |
| Partition | `silver.tuong_tac` theo `ngay`; giải thích ở mục 5 | `transform/bronze_to_silver.py` |

**Lưu lịch sử đủ lâu để tái lập.** Mặc định Delta xóa tệp cũ sau 7 ngày khi VACUUM; DAG 4 chạy VACUUM mỗi đêm nên nếu để mặc định, không thể tái lập thí nghiệm của tuần trước. Mọi bảng do job tạo nhận mặc định `deletedFileRetentionDuration = 30 ngày`, `logRetentionDuration = 60 ngày` (qua `spark.databricks.delta.properties.defaults.*` trong `tao_spark`). VACUUM không truyền `RETAIN` để tôn trọng thuộc tính của từng bảng.

## 7. Web (mục 3.8)

| Màn hình | Đường dẫn | API |
|---|---|---|
| Trang chủ giả lập | `/` (chọn user từ danh sách hoặc nhập mã; nút tạo user mới) | `GET /api/goi-y/{user_id}`, `POST /api/hien-thi` |
| Trang sản phẩm | `/san-pham/{ma}` | `POST /api/su-kien` |
| Bảng điều khiển A/B | `/ab` | `GET /api/ab/ket-qua` |
| Chất lượng mô hình | `/mo-hinh` | `GET /api/mo-hinh/chi-so` |
| Tái lập thí nghiệm | `/tai-lap` | `POST /api/tai-lap/{ngay}` |

API phụ: `GET /api/thinh-hanh`, `GET /api/thinh-hanh/{ma}` (dùng cho đo độ trễ), `GET /api/nguoi-dung-mau`, `POST /api/tai-lap-mo-hinh` (kích hoạt DAG 5 qua REST API Airflow 3: lấy JWT ở `/auth/token`, gọi `POST /api/v2/dags/kn_tai_lap/dagRuns`), `GET /api/suc-khoe`.

Hai chế độ dữ liệu: `WEB_CHE_DO=delta` (đọc cụm thật, mặc định) và `WEB_CHE_DO=gia` (dữ liệu giả lập để làm giao diện, không cần cụm). Chế độ được hiển thị ở `/api/suc-khoe`.

**Ghi ngược vào Delta (mục A.6.4).** Hai đường:

1. Trực tiếp: nút "Chạy lại" ở màn hình Tái lập. Sau khi trả kết quả, web ghi nền một dòng
   `loai='ab_web'` vào `gold.ket_qua_tai_lap` (append) bằng phiên Spark + Delta ngay trong tiến trình web
   (`KhoDelta.ghi_ket_qua_tai_lap`). Append không xung đột với commit của DAG 5. Bảng cuối màn hình Tái lập
   hiển thị các dòng đã ghi.
2. Qua Kafka: mỗi lần hiển thị khối "Dành cho bạn", mỗi lần nhấp, mua → topic `hien-thi` → luồng `ghi_hien_thi`
   MERGE vào `gold.nhat_ky_hien_thi` (mỗi 30 giây). Xem/Thêm giỏ/Mua → topic `retailrocket-events` → bronze →
   bảng thịnh hành.

Xác nhận bản ghi mới trong Delta: `bash scripts/demo/delta_sql.sh demo <ma_nguoi_dung>`.

**Hiệu năng (phản hồi < 3 giây, mục A.8).** Middleware đo thời gian xử lý mọi yêu cầu, gắn header
`X-Thoi-Gian-Xu-Ly-Ms`, gom theo mẫu route. `GET /api/hieu-nang` trả trung vị, p95, lớn nhất và tỷ lệ dưới 3 giây.
`scripts/do_do_tre_web.py` đo từ phía máy khách (chỉ yêu cầu đọc, không ghi impression). Bộ đệm TTL: thịnh hành
10 giây, gợi ý 30 giây, bảng nhỏ 60 giây, sản phẩm liên quan 600 giây, bảng lớn 3.600 giây.

**Biểu đồ.** Chart.js 4.6.1 đóng gói sẵn trong `web/static/vendor/` (phòng lab không cần Internet);
`web/static/bieu_do.js` giữ giao diện hàm `veDuong` / `veCot`.

## 8. Airflow (mục 3.9)

| DAG | Lịch | Nội dung |
|---|---|---|
| `kn_pipeline_hang_ngay` | `0 2 * * *` | Mục 3.2 ở trên; sensor chờ bronze có dữ liệu mới (`mode="reschedule"` để không giữ slot) |
| `kn_giam_sat_streaming` | `*/15 * * * *` | Kiểm tra ứng dụng `bd03-tinh-thinh-hanh` còn chạy trên Spark Master, và `gold.thinh_hanh_15p` có cửa sổ trong 15 phút qua; hỏng thì khởi động lại luồng và cảnh báo |
| `kn_tinh_ab` | `0 * * * *` | Đọc CDF → tổng hợp → z-test → ghi `gold.ket_qua_ab` kèm phiên bản |
| `kn_bao_tri` | `0 4 * * *` | Xóa gợi ý > 7 ngày → OPTIMIZE (+ZORDER) → VACUUM |
| `kn_tai_lap` | thủ công | Tham số `phien_ban_delta` (hoặc `ma_chay`) và `che_do`; chạy lại huấn luyện, so với lịch sử |

**SLA bằng Deadline Alerts (mục A.6.5).** Airflow 3.0 đã gỡ `sla`/`sla_miss_callback` của Airflow 2; từ 3.1
tính năng thay thế là Deadline Alerts. Mỗi DAG có `deadline=han_chot(phút)` (`airflow/dags/bd03_chung.py`):
mốc `DAGRUN_QUEUED_AT` (có ở mọi kiểu lượt chạy), hạn DAG 1 = 7 giờ (lượt 02:00 phải xong trước 09:00), DAG 2 =
10 phút, DAG 3 = 30 phút, DAG 4 = 90 phút, DAG 5 = 3 giờ. Quá hạn mà lượt chạy chưa xong thì scheduler gọi
`canh_bao_tre_han` ngay (SLA cũ chỉ kiểm tra khi lượt chạy kết thúc), ghi `logs/canh_bao.log`. Dùng
`SyncCallback` (chạy trong executor) vì triggerer mặc định không bật. Kiểm chứng trên Airflow 3.3.1 thật:
`docs/bang_chung/kiem_chung_airflow_cuc_bo.md`. Giới hạn cứng vẫn là `dagrun_timeout` và `execution_timeout`.

**Xử lý lỗi.** `retries=1`, `retry_delay` 5 phút, `on_failure_callback=canh_bao` cho DAG 1, 3, 4; DAG 2 không
retry (lượt sau 15 phút nữa) mà có nhánh `trigger_rule="one_failed"` tự khởi động lại luồng; nhánh
`BranchPythonOperator` của DAG 1 giữ mô hình cũ khi NDCG không cải thiện; sensor `mode="reschedule"`,
`soft_fail=True`. Mọi bước ghi đều idempotent (replaceWhere theo ngày, MERGE theo khóa, ghi theo `ma_chay`),
nên retry hay chạy lại DAG không nhân đôi dữ liệu.

**Backfill.**

- DAG 1: tham số `tu_ngay`/`den_ngay` (ngày dữ liệu, từ "Trigger DAG w/ config" hoặc
  `airflow backfill create ... --dag-run-conf`). `bronze_to_silver` tính lại đúng các phân vùng đó bằng
  replaceWhere (cả silver lẫn quarantine), không chờ sensor, **không đổi mốc tăng dần**: commit backfill ghi
  `userMetadata = {"backfill": ...}` thay vì `moc_ingest`, nên lượt tăng dần kế tiếp vẫn xử lý các ngày mới chưa tính.
- DAG 3: backfill bằng lệnh chuẩn
  `airflow backfill create --dag-id kn_tinh_ab --from-date ... --to-date ... --reprocess-behavior completed`.
  Cổng `cong_backfill` (ShortCircuitOperator) cho lượt 00:00 UTC của mỗi ngày tính lại cả ngày đó
  (`--tu-ngay`), 23 lượt còn lại bỏ qua. Job vẫn gộp các ngày CDF báo có thay đổi, nên
  `phien_ban_delta_du_lieu` ghi vào là mốc đúng cho lượt tăng dần kế tiếp.

Mọi task gọi `ha_tang/spark/submit.sh` bằng `BashOperator`. Airflow chạy `LocalExecutor` trên máy master: task chỉ là lệnh `spark-submit` chờ kết quả, việc nặng chạy trên cụm Spark. Container Airflow gắn **cùng thư mục dự án trên máy chủ** mà container `spark-master` đang gắn (`BD03_HOST_DIR` → `/opt/bd03`), và gắn `/var/run/docker.sock`; nhờ vậy `submit.sh` chạy trong Airflow ghi `jobs/common.zip` vào đúng chỗ Spark nhìn thấy và gọi được `docker exec spark-master ...`.


## 9. Dữ liệu đưa về driver (mục A.8: "không kéo dữ liệu về driver")

Pipeline chính không `collect()` dữ liệu theo dòng sự kiện hay theo cặp người dùng – sản phẩm. Các chỗ đưa dữ liệu
về driver đều là bảng nhỏ có chủ đích:

| Chỗ | Đưa về driver | Cỡ | Lý do |
|---|---|---|---|
| `bronze_to_silver.chon_ngay_can_xu_ly` | Danh sách ngày cần tính lại | ≤ 140 dòng | Dựng điều kiện replaceWhere |
| `bronze_to_silver` (thống kê) | Số dòng bị loại theo lý do | ≤ 6 dòng | In kết quả JSON |
| `sinh_goi_y_toan_bo.bang_novelty`, `als_lib.xep_lai_theo_novelty` | Độ phổ biến của từng sản phẩm, rồi `broadcast` | 126.230 cặp (int, float), cỡ 2–3 MB | Bảng chiều nhỏ dùng tra cứu trong UDF xếp lại, tương đương broadcast join. Giữ nguyên vì DAG 5 tái lập metric phải trùng khớp tuyệt đối |
| `als_lib.gop_nhan_tren_driver` | Ground truth của tập kiểm tra | khoảng 9.500 người dùng | Cắt lineage dài (NHAT_KY lỗi về tính lại từ HDFS) |
| `chuan_bi_ma_tran` | Nhãn mới để nối vào StringIndexer | số người dùng/sản phẩm mới | StringIndexerModel vốn giữ toàn bộ nhãn trên driver |
| `danh_gia`, `tron_goi_y`, `ab.py` | Kết quả tuning, đếm theo nguồn, tổng hợp A/B | ≤ vài chục dòng | Ghi bảng kết quả |
| `bao_tri/do_zorder.chon_user_mau` | n mã người dùng để đo truy vấn | n = số truy vấn (mặc định 30) | Trước 15/11 lấy toàn bộ ~180 nghìn mã rồi chọn ngẫu nhiên trên driver; nay sắp theo băm trên cụm và chỉ lấy n dòng |
| `kiem_tra/*` (thí nghiệm bổ sung) | Danh sách gợi ý của khoảng 9.500 người dùng | vài trăm nghìn số nguyên | Chỉ chạy tay, cắt lineage cho nhiều lần đo |
