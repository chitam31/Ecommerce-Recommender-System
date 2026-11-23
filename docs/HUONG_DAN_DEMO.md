# Hướng dẫn demo và bảo vệ — theo danh mục nghiệm thu 3.11 và kịch bản C.4 của đề cương

Phần B' (ngay sau phần A) đi qua 6 bước của kịch bản demo mục C.4 đề cương; phần B đi qua đủ 8 mục nghiệm thu 3.11.
Kịch bản chạy thật cho buổi 19/11 (từng lệnh, từng cửa sổ): `bao_cao/kich_ban/KICH_BAN_DEMO_19_11.md`.

Kịch bản dưới đây đi qua đủ 8 mục nghiệm thu, mỗi mục ghi rõ **làm gì**, **ở đâu**, **bằng chứng cần thấy** và **lệnh dự phòng** nếu giao diện gặp sự cố. Toàn bộ trình chiếu từ máy Windows (10.68.71.59); thao tác dòng lệnh trên máy master (10.68.71.113) qua SSH.

---

## A. Chuẩn bị

### Trước buổi bảo vệ 1 ngày

- [ ] `bash scripts/cum/kiem_tra_cum.sh` không có dòng `DONG`.
- [ ] Đã chạy `bash scripts/chay_lan_dau.sh` (đủ 27 tổ hợp) **trên dữ liệu thật**; `gold.lich_su_huan_luyen` có ít nhất 1 dòng `trang_thai = trien_khai`.
- [ ] Đã chạy `transform/tien_hoa_luoc_do.py` rồi chạy lại pipeline một lần nữa, để có cả mô hình v1 lẫn v2 trong lịch sử.
- [ ] `gold.ket_qua_ab` có ít nhất 3 ngày (mô phỏng `--so-ngay 7` đã sinh đủ).
- [ ] Đã chạy DAG `kn_tai_lap` ít nhất một lần thành công (bảng `gold.ket_qua_tai_lap` có dòng `trung_khop = true`).
- [ ] Đã chạy `bao_tri/do_zorder.py` (bảng `gold.do_hieu_nang_zorder` có 3 dòng `truoc_toi_uu` / `sau_optimize` / `sau_zorder`: so dòng 2 với dòng 3 để thấy tác dụng riêng của ZORDER, tách khỏi tác dụng gộp tệp).
- [ ] Chọn sẵn: 1 user quen (có ALS, nhóm A), 1 user quen nhóm B, 1 sản phẩm **ít người xem** (không nằm trong khối thịnh hành) để demo độ trễ. Lấy user mẫu ở `http://10.68.71.149:8000/api/nguoi-dung-mau`; xem nhóm A/B ngay trên trang chủ.
- [ ] Chụp màn hình dự phòng mọi màn hình web.

### Trước giờ bảo vệ 30 phút

```bash
bash scripts/luong.sh trang_thai          # 3 luồng phải đang chạy
bash scripts/luong.sh khoi_dong tat_ca    # nếu có luồng không chạy
```

Trên máy labsit, bật bộ phát ở tốc độ thật để khối thịnh hành có dữ liệu "sống" trong lúc demo:

```bash
python3 producer/phat_lai_su_kien.py --tep ~/du_lieu/retailrocket/events.csv --tu-ngay-ms 1438387200000 --ty-le-nguoi-moi 0 --giay-moi-ngay 90
```

`1438387200000` là 01/08/2015 (UTC): phát 48 ngày cuối; với `--giay-moi-ngay 90` bộ phát chạy khoảng 72 phút (mặc định 30 giây/ngày chỉ khoảng 24 phút, cửa sổ thịnh hành sẽ vơi trước khi tới phần demo). Ngày bảo vệ: xem thêm `docs/CHUAN_BI_BAO_VE.md`. Đây là lần phát thứ hai của cùng dữ liệu nên tắt người dùng mới (`--ty-le-nguoi-moi 0`): hạt giống cố định sẽ sinh lại các mã `moi_<n>` trùng với lần đầu nhưng duyệt sản phẩm khác. Các sự kiện gốc bị phát trùng không làm sai silver vì `bronze_to_silver` bỏ trùng theo nội dung trước khi lọc bot (MO_HINH mục 2).

Mở sẵn các tab: web (`:8000`), Spark Master (`:8080`), Kafka UI (`10.68.71.204:8080`), Airflow (`:8088`), HDFS (`:9870`).

---

## B'. Kịch bản demo mục C.4 đề cương (các yêu cầu chung)

| Phút | C.4 yêu cầu | Làm gì | Bằng chứng |
|---|---|---|---|
| 0–2 | Sơ đồ kiến trúc | Slide kiến trúc; Spark UI 5 worker, HDFS UI 5 DataNode | — |
| 2–4 | Airflow UI: các DAG, **chạy 1 DAG trực tiếp** | Mở `:8088/dags`, chỉ 5 DAG khác lịch và mục "Deadline"; bấm **Trigger** `kn_tinh_ab` | Lượt chạy xanh trong khoảng 1–2 phút; log task in JSON `phien_ban_delta_du_lieu` |
| 4–6 | Terminal: `hdfs dfs -ls -R`, DESCRIBE HISTORY, VERSION AS OF | Trên master: `bash scripts/demo/cay_hdfs.sh` rồi `bash scripts/demo/delta_sql.sh demo <user>` | Cây `/lakehouse/bd03_khuyen_nghi/{bronze,silver,gold,features,models,checkpoints,_quarantine}`; bảng HISTORY có STREAMING UPDATE/MERGE; số dòng ở `VERSION AS OF 1` khác hiện tại |
| 6–8 | Streaming: bật producer, dữ liệu chảy vào bronze | Kafka UI: offset `retailrocket-events` tăng; Spark UI: 3 luồng **đã chạy ≥ 30 phút** (cột Duration) | Số sự kiện bronze tăng (`bash scripts/demo/delta_sql.sh sql "SELECT max(_ingest_ts), count(*) FROM {bronze.su_kien}"`) |
| 8–11 | Web: các màn hình, **ghi ngược**, quay lại Delta xác nhận | Trang chủ chọn user → bấm sản phẩm (click) → Tái lập: **Chạy lại** | `delta_sql.sh demo <user>`: dòng `da_click = true` mới trong `gold.nhat_ky_hien_thi`, dòng `ab_web` mới trong `gold.ket_qua_tai_lap` |
| 11–13 | Kết quả mô hình, baseline, phân tích lỗi | Web → Chất lượng mô hình; slide phân tích lỗi | Bảng ALS vs 2 baseline, biểu đồ phân bố độ phổ biến |
| 13–15 | Câu hỏi phản biện | `docs/CHUAN_BI_BAO_VE.md` mục câu hỏi C.5 | — |

`delta_sql.sh` chạy Spark local[2] trong container `spark-master`: không lấy lõi của cụm, không tranh tài nguyên
với 3 luồng. Mỗi lần gọi mất khoảng 15–20 giây khởi động, nên gom mọi câu bằng `demo`.

---

## B. Trình tự demo (khoảng 15 phút)

### 0. Kiến trúc và cụm (1 phút)

Mở Spark Master UI: 5 worker `ALIVE` với đúng IP ZeroTier, 3 ứng dụng streaming đang chạy. Mở HDFS UI → Datanodes: 5 DataNode. Mở Airflow: 5 DAG `kn_*`.

### 1. ☐ Bảng tuning ≥ 9 tổ hợp ALS với NDCG@10 (1 phút)

- **Ở đâu:** web → **Chất lượng mô hình** → mục "Bảng tuning của lần chạy mới nhất".
- **Bằng chứng:** 27 dòng (hoặc tối thiểu 9), đủ `rank ∈ {10,30,50}`, `regParam ∈ {0.01,0.1,1.0}`, `alpha ∈ {1,10,40}`, cột NDCG@10, xếp giảm dần; dòng đầu trùng siêu tham số của mô hình đang triển khai.
- **Dự phòng:**
  ```bash
  bash scripts/demo/delta_sql.sh sql "SELECT rank, reg_param, alpha, round(ndcg_at_10,5) ndcg FROM {gold.ket_qua_tuning} WHERE ma_chay = '<ma_chay>' ORDER BY ndcg DESC"
  ```

### 2. ☐ So sánh ALS với 2 baseline theo 4 metric + Coverage + Novelty (2 phút)

- **Ở đâu:** cùng trang, bảng đầu tiên.
- **Bằng chứng:** các dòng ALS, Baseline 1 (bán chạy toàn sàn), Baseline 2 (bán chạy theo danh mục), ALS + xếp lại novelty (β = 0,1 và 0,3), Mô hình v2 lọc tầm giá; đủ P@10, R@10, NDCG@10, MAP@10, Coverage, Novelty. Dòng "Ngưỡng đạt" ghi rõ mức tăng NDCG so với baseline 1 và coverage → ĐẠT / CHƯA ĐẠT.
- **Điểm cần nói:** biểu đồ "Phân phối độ phổ biến" cho thấy baseline 1 dồn 100% vào nhóm phổ biến nhất, ALS lệch về phía phổ biến (popularity bias), re-ranking kéo phân phối sang phải và tăng coverage, đổi lại NDCG giảm bao nhiêu. Nếu coverage < 10%, trình bày nguyên nhân và kết quả re-ranking: đề tài coi đây là kết quả hợp lệ khi có phân tích.

### 3. ☐ Ba chiến lược cold start, demo với user mới tạo tại chỗ (3 phút)

1. **User mới hoàn toàn:** trang chủ → **"+ Tạo người dùng mới (cold start)"**. Web sinh mã `moi_xxxxxxxx`, nhãn chiến lược `nguoi_moi`, cả 10 ô "Dành cho bạn" có nguồn `thinh_hanh` và trùng khối "Đang thịnh hành".
2. **User có < 5 tương tác:** với chính user đó, mở 2–3 sản phẩm cùng một danh mục, bấm **Xem** hoặc **Thêm giỏ**. Quay lại trang chủ: nhãn chiến lược chuyển sang `it_tuong_tac`, danh sách xen kẽ `pho_bien_danh_muc` / `thinh_hanh` (5/5), các món danh mục đúng là danh mục vừa xem.
3. **Item mới:** mở trang chủ của một user quen. Vị trí 9–10 có nguồn `pho_bien_danh_muc`: đó là item **không có trong mô hình ALS**, cùng danh mục chính của user. Đối chiếu: các mã này không có trong `item_indexer` của mô hình phục vụ (`tron_goi_y.py` lọc bằng `left_anti` với `itemFactors`).
- **Điểm cần nói:** user mới sinh từ bộ phát (5% sự kiện, mã `moi_<n>`) đi theo đúng các nhánh này ở job batch; web dùng bộ nhớ phiên để demo ngay mà không phải chờ batch đêm.

### 4–5. ☐ Vòng khép kín và ☐ thịnh hành cập nhật ≤ 90 giây (3 phút)

Làm trên giao diện:

1. Mở trang sản phẩm ít người xem đã chọn: khối "Đang thịnh hành" của trang hiển thị số lượt thêm giỏ hiện tại của chính sản phẩm đó (thường là 0).
2. Mở Kafka UI → topic `retailrocket-events` → Messages (chế độ live).
3. Bấm **Thêm giỏ** 3 lần, bấm giờ.
4. Kafka UI hiện ngay 3 thông điệp có `"nguon": "web"`.
5. Tải lại trang sản phẩm mỗi 10–15 giây cho tới khi số thêm giỏ tăng thêm 3. Ghi số giây.

Đo tự động và in thời gian (từ máy Windows hoặc master):

```bash
python3 scripts/demo_vong_khep_kin.py --web http://10.68.71.149:8000 --san-pham <ma_san_pham> --so-lan 3
# ... Bảng thịnh hành đã cập nhật sau NN giây (cửa sổ ..., thêm giỏ ...) -> ĐẠT/KHÔNG ĐẠT
```

Chứng minh sự kiện đã vào bronze (khép kín vòng dữ liệu):

```bash
bash ha_tang/spark/submit.sh nhanh kiem_tra/tim_su_kien_bronze.py --ma-nguoi-dung demo_vong_kin
# in các dòng bronze của user, kèm độ trễ thoi_diem -> kafka_ts -> _ingest_ts
```

- **Điểm cần nói:** luồng thịnh hành đọc thẳng từ Kafka (trigger 20 giây), cửa sổ gom theo `kafka_ts` nên sự kiện web năm 2026 và sự kiện phát lại năm 2015 không làm hỏng watermark của nhau.

### 6. ☐ Kết quả A/B có p-value và kết luận thống kê (2 phút)

- **Ở đâu:** web → **Bảng điều khiển A/B**.
- **Bằng chứng:** CTR và tỷ lệ chuyển đổi theo nhóm A/B, biểu đồ CTR theo ngày, p-value (CTR và chuyển đổi), câu kết luận ở mức 5%, cỡ mẫu cần thiết, **tỷ lệ traffic mô phỏng** của từng ngày.
- **Điểm cần nói:**
  - Phân nhóm bằng `md5(ma_nguoi_dung)`: cùng user luôn cùng nhóm (tải lại trang chủ nhiều lần để thấy).
  - Nhóm B: vị trí 3, 6, 8 là thịnh hành (30% của 10 ô hiển thị); vị trí 9–10 của cả hai nhóm là item mới, nên khác biệt chỉ còn ở phần trộn thịnh hành.
  - Traffic chủ yếu là mô phỏng theo mô hình phụ thuộc vị trí (mục 3.10), đã gắn nhãn; không cộng điểm cho thịnh hành nên không giả định trước B tốt hơn.
  - Job A/B chỉ xử lý phần mới nhờ Change Data Feed: log của task trong Airflow (`kn_tinh_ab`) có dòng JSON cuối với `tu_phien_ban`, `phien_ban_delta_du_lieu` và `so_ngay` được tính lại.

### 7. ☐ Demo tái lập thí nghiệm bằng VERSION AS OF cho ra cùng metric (2 phút) — **bắt buộc**

**Tái lập A/B (trên web):**

1. Web → **Tái lập thí nghiệm**: danh sách các ngày đã có kết quả A/B kèm **phiên bản Delta** của impression log.
2. Chọn một ngày cũ → **Chạy lại**. Web đọc `gold.nhat_ky_hien_thi` bằng `versionAsOf = phien_ban_delta_du_lieu`, chạy lại đúng hàm tổng hợp và z-test.
3. **Bằng chứng:** bảng so sánh cũ/mới từng chỉ số (số hiển thị, click, mua, CTR, tỷ lệ chuyển đổi, p-value) với cột lệch = 0, kết luận "Trùng khớp".
4. Để thấy ý nghĩa của time travel: bấm thêm vài lượt trên web (impression log có phiên bản mới), rồi chạy lại ngày cũ — kết quả vẫn trùng khớp vì đọc đúng phiên bản cũ.

**Tái lập huấn luyện (DAG 5):**

1. Cùng trang → mục "Tái lập huấn luyện mô hình": nhập `phien_ban_features` của một lần huấn luyện cũ (cột "Features v" ở trang Chất lượng mô hình) → **Kích hoạt kn_tai_lap**. Hoặc Airflow → `kn_tai_lap` → Trigger với `{"phien_ban_delta": <v>, "che_do": "mo_hinh_chon"}`.
2. Chờ DAG xong (vài phút, tùy cỡ dữ liệu), tải lại trang.
3. **Bằng chứng:** dòng mới trong bảng tái lập: metric cũ, metric mới, độ lệch lớn nhất < 10⁻⁶, `trung_khop = true`.
- **Điểm cần nói:** cùng phiên bản features + cùng siêu tham số, `maxIter`, hạt giống 42, số khối → cùng metric. StringIndexer chỉ nối nhãn nên chỉ số của phiên bản cũ vẫn đúng. Lịch sử Delta giữ 30 ngày nên VACUUM hằng đêm không xóa mất phiên bản cũ.

### 8. ☐ Số liệu OPTIMIZE/ZORDER trên truy vấn lấy gợi ý theo user (1 phút)

- **Ở đâu:** web → **Chất lượng mô hình** → mục cuối "OPTIMIZE / ZORDER".
- **Bằng chứng:** 2 dòng `truoc` / `sau`: số tệp của bảng, thời gian trung bình / trung vị / p95 của truy vấn `WHERE ma_nguoi_dung = ?`, **số tệp phải đọc trung bình mỗi truy vấn**.
- **Điểm cần nói:** thời gian trên cụm dao động theo tải, còn số tệp đọc thì không; Z-ORDER gom các dòng cùng user vào ít tệp nên data skipping theo min/max loại được gần hết tệp. Đây chính là truy vấn nóng của trang chủ.
- **Chạy lại tại chỗ (nếu được yêu cầu):**
  ```bash
  bash ha_tang/spark/submit.sh bao_tri bao_tri/do_zorder.py --phan-manh 200 --so-truy-van 30
  ```

---

## C. Câu hỏi thường gặp khi bảo vệ

| Câu hỏi | Ý trả lời chính | Tài liệu |
|---|---|---|
| Vì sao ALS implicit mà không explicit? | Không có rating; `r_ui` là độ tin cậy, `c = 1 + α r` | MO_HINH.md §4 |
| Vì sao không phân vùng silver theo user? | 1,4 triệu thư mục nhỏ, áp lực NameNode và nhật ký Delta; truy vấn theo user giải bằng Z-ORDER ở gold | KIEN_TRUC.md §5 |
| Vì sao fit lại mô hình phục vụ? | Mô hình đánh giá không biết tháng cuối; gộp bằng nhân hệ số suy giảm | MO_HINH.md §6 |
| Coverage tính trên catalog nào? | Ngưỡng trên item có trong train; báo cáo thêm toàn sàn | MO_HINH.md §5.2 |
| p-value có tin được không? | z-test theo impression, traffic mô phỏng có nhãn, giới hạn tương quan trong user | MO_HINH.md §8.3–8.4 |
| Tái lập có thật sự giống hệt? | A/B: lệch 0; ALS: < 10⁻⁶ do số thực phân tán | MO_HINH.md §11 |
| Xử lý OOM với 1,4 triệu user? | Lọc ≥ 3 tương tác, `numBlocks`, chỉ sinh cho user hoạt động 30 ngày | MO_HINH.md §3.4, §6 |
| Item mới chen vào thế nào mà không phá A/B? | Vị trí 9–10 ở cả hai nhóm | MO_HINH.md §8.1 |

---

## D. Nếu sự cố trong lúc demo

| Sự cố | Xử lý nhanh |
|---|---|
| Web không phản hồi | `docker restart bd03-web` trên labsoe-18 (khởi động lại mất khoảng 30 giây vì phải tạo phiên Spark) |
| Khối thịnh hành đứng yên | `bash scripts/luong.sh trang_thai`; `bash scripts/luong.sh khoi_dong thinh_hanh`; kiểm tra bộ phát còn chạy |
| Một worker rớt | Spark tự chạy lại task trên worker khác; HDFS còn bản sao (replication 2) — có thể dùng để minh họa khả năng chịu lỗi |
| DAG 5 chạy quá lâu | Trình bày kết quả của lần chạy trước trong bảng tái lập, giải thích thời gian bằng cỡ dữ liệu |
| Mất mạng ZeroTier | Dùng ảnh chụp đã chuẩn bị; **không** dùng chế độ web giả lập làm bằng chứng vì số liệu ở chế độ đó là giả |
