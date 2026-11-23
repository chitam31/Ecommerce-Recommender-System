# Chuẩn bị bảo vệ và demo — BD03 (đề tài 3, hệ thống gợi ý TMĐT)

Tài liệu dùng trong tuần cuối và ngày bảo vệ **19/11/2026**. Đọc kèm:

- `bao_cao/kich_ban/BD03_Kich_ban_thuyet_trinh.docx`: lời nói từng slide, phân công, thời lượng.
- `bao_cao/hoi_dap/BD03_Hoi_dap_bao_ve.docx`: 52 câu hỏi – đáp dự kiến.
- `docs/HUONG_DAN_DEMO.md`: bản demo đầy đủ 15 phút theo 8 tiêu chí, kèm lệnh dự phòng cho từng tiêu chí.
- `docs/TRIEN_KHAI_CUM.md`: cách bật, dừng và sửa cụm.

Quy ước trong tài liệu này:

- **T** là giờ bắt đầu trình bày. T−30 nghĩa là 30 phút trước giờ trình bày.
- Lệnh có ghi `[laptop]` chạy trong Git Bash ở `/d/BIG DATA/bd03_khuyen_nghi`.
- Lệnh có ghi `[labsoe-11]`, `[labsit]` hoặc `[labsoe-18]` chạy trên máy đó, trong thư mục `~/bd03_khuyen_nghi`.
- ☐ là việc cần đánh dấu khi đã làm xong.

---

## 0. Thông tin nhanh

| Mục | Giá trị |
|---|---|
| Ngày bảo vệ | 19/11/2026 tại phòng lab: demo trước, thuyết trình sau (không giới hạn thời gian thuyết trình). Nộp báo cáo và sản phẩm sau 19/11, trước 22/11/2026 |
| Thời lượng | Buổi 19/11: demo trước (khoảng 14 phút, `bao_cao/kich_ban/KICH_BAN_DEMO_19_11.md`), rồi thuyết trình không giới hạn thời gian (26 slide, thêm slide 27 dự phòng cho hỏi – đáp) |
| Thành viên | Lê Chí Tâm (2302220) trình bày toàn bộ phần slide và bấm máy khi demo. Lê Nguyễn Quốc Anh (2302283) nói lời dẫn khi demo |
| Giảng viên hướng dẫn | TS. Cao Tiến Dũng |
| Web | `http://10.68.71.149:8000`. Từ ngoài mạng lab: đường hầm SSH (mục 3.3), rồi mở `http://localhost:8001` |
| Airflow | `http://10.68.71.113:8088`, tài khoản `admin`, mật khẩu đã đổi ngày 11/11. Không ghi mật khẩu ra giấy hay lên màn hình |
| Spark Master | `http://10.68.71.113:8080` |
| HDFS NameNode | `http://10.68.71.113:9870` |
| Kafka UI | `http://10.68.71.204:8080` |
| User nhóm A | `1287993`: 10 ô đều là ALS |
| User nhóm B | `270018`: ô 3, 6, 8 là sản phẩm thịnh hành |
| Sản phẩm cho demo vòng khép kín | `102605` (đã dùng khi đo 51 giây), dự phòng `320844`. Kiểm tra lại ở mục 2.6 |
| Mô hình đang phục vụ | `als_v2_r20261110T230924` (v2, rank 50, regParam 1,0, alpha 40) |
| Repo | https://github.com/chitam31/CS411V_Final-Project_Ecommerce-Recommender-System (riêng tư), tag v1.0 và v1.1 |

### Bản đồ 8 tiêu chí: hội đồng hỏi tiêu chí nào thì mở đâu

| # | Tiêu chí | Màn hình hoặc bằng chứng sống | Slide dự phòng | Số cần nói |
|---|---|---|---|---|
| 1 | Bảng tuning ≥ 9 tổ hợp | Web → **Chất lượng mô hình** → bảng tuning | 11 | 27 tổ hợp, tốt nhất NDCG@10 = 0,0680 |
| 2 | ALS so với 2 baseline | Web → **Chất lượng mô hình** → bảng so sánh, biểu đồ độ phổ biến | 12–14 | +1.237 %, +43 %; coverage 3,44 % (đã phân tích), khi phục vụ 6,47 % |
| 3 | Ba chiến lược cold start | Web → **+ Người dùng mới (cold start)** | 15 | User mới, user < 5 tương tác, item mới ở ô 9–10 |
| 4 | Thịnh hành ≤ 90 giây | Trang sản phẩm → **Thêm vào giỏ** → dòng “ĐÃ CẬP NHẬT sau N giây” | 21 | Đo ngày 11/11: 51 giây |
| 5 | Vòng khép kín | Kafka UI (thông điệp `"nguon": "web"`), lệnh tìm sự kiện trong bronze (mục 5) | 21 | Kafka → bronze 10,3 giây |
| 6 | A/B có p-value | Web → **Bảng điều khiển A/B** | 16 | p = 0,015 (7 ngày mô phỏng); bảng web hiện số gộp, ví dụ 0,0135 |
| 7 | Tái lập bằng VERSION AS OF | Web → **Tái lập thí nghiệm** → **Chạy lại** | 19 | Độ lệch 0,000000 (1,78·10⁻¹⁵) |
| 8 | OPTIMIZE / Z-ORDER | Web → **Chất lượng mô hình**, mục cuối trang | 18 | Tệp đọc 67 → 1,07; 8.280 → 980 ms; nhanh hơn 8,4 lần |

---

## 0b. Việc bắt buộc thêm sau khi đối chiếu đề cương (15–16/11)

| Khi | Việc | Lệnh | Xong |
|---|---|---|---|
| 15/11 tối hoặc 16/11 sáng | `git pull` trên laptop, chuyển gốc hồ dữ liệu sang `/lakehouse/bd03_khuyen_nghi` (mục A.4, A.6.3) | `[laptop] bash scripts/cum/chuyen_lakehouse.sh` rồi `--chay` (TRIEN_KHAI_CUM mục 8b) | ☐ |
| Ngay sau đó | Thu bằng chứng: du -h, DESCRIBE HISTORY, quarantine, độ trễ web, EDA, phân tích lỗi | `[labsoe-11] bash scripts/thu_bang_chung.sh` (trong tmux, 20–25 phút) | ☐ |
| Ngay sau đó | Chép kết quả về laptop, giải nén vào `docs/bang_chung/cum/`, commit + push | `[laptop] scp labsoe-11:~/bd03_bang_chung/<thời điểm>.tar.gz .` | ☐ |
| 16–18/11 | Tập phần demo C.4: chạy DAG `kn_tinh_ab` trực tiếp, `cay_hdfs.sh`, `delta_sql.sh demo <user>`, ghi ngược rồi xác nhận | `docs/HUONG_DAN_DEMO.md` mục B' | ☐ |

## 1. Lịch chuẩn bị đến 19/11

Báo cáo, slide, bằng chứng và khai báo AI đã hoàn tất trong gói nộp bài ngày 16/11 (`BD03_khuyen_nghi_16_11.zip`). Còn lại là phần tập:

| Ngày | Việc | Xong |
|---|---|---|
| 16/11 | Hỏi – đáp thử theo `bao_cao/hoi_dap/BD03_Hoi_dap_bao_ve.docx`, hai người đổi vai hỏi và trả lời. Quốc Anh đọc phần mã mình giải thích (`producer/`, `jobs/streaming/`, `jobs/transform/`, `web/`) cùng `docs/KIEN_TRUC.md` | ☐ |
| 17/11 | Tập trọn kịch bản demo trên cụm (`bao_cao/kich_ban/KICH_BAN_DEMO_19_11.md`), bấm giờ (khoảng 14 phút); ghi lại bước chậm hoặc lỗi. Tâm tập thuyết trình 26 slide theo kịch bản chi tiết | ☐ |
| 18/11 sáng | Làm toàn bộ **mục 2** (kiểm tra cụm, chọn sản phẩm, quay video dự phòng) | ☐ |
| 18/11 tối | Tập lần cuối. Kiểm tra DAG `kn_pipeline_hang_ngay` **vẫn tạm dừng** (mục 2.5). Sạc đầy pin laptop, chuẩn bị đồ ở mục 3 | ☐ |
| 19/11 | Làm theo **mục 4** (T−60 đến T), demo theo **mục 5**, dọn dẹp theo **mục 11** | ☐ |

---

## 2. Kiểm tra cụm và chuẩn bị (18/11)

### 2.1 Laptop SSH được tới các máy

```bash
# [laptop] chạy lần lượt trên 6 máy lab, master trước
bash scripts/cum/ssh_tat_ca.sh hostname
```

- Mong đợi: in đủ 6 tên máy, không có dòng lỗi.
- Nếu `ssh labsoe-11` treo ở bước trao khóa (lỗi MTU khi ở ngoài trường): dùng tên tắt `m11` (ProxyJump qua labsoe-18), xem `docs/TRIEN_KHAI_CUM.md` mục 10c.

### 2.2 Toàn cụm

```bash
# [labsoe-11]
bash scripts/cum/kiem_tra_cum.sh
bash scripts/cum/tinh_trang.sh
```

| Mục | Phải thấy |
|---|---|
| Cổng | Không có dòng `DONG` |
| HDFS | `DataNode sống: 5`, `chết: 0` |
| Spark | `Worker sống: 5`, `lõi: 34` |
| RAM master | Còn trên 1 GB khả dụng khi 3 luồng đang chạy |

Nếu có máy chết hoặc vừa bị khởi động lại: xem mục 6.

### 2.3 Ba luồng streaming

```bash
# [labsoe-11]
bash scripts/luong.sh trang_thai
```

- Phải có 3 ứng dụng đang chạy: `bd03-nap-su-kien`, `bd03-tinh-thinh-hanh`, `bd03-ghi-hien-thi`.
- Thiếu luồng nào thì bật lại: `bash scripts/luong.sh khoi_dong tat_ca`. Lệnh này tự bỏ qua luồng đang chạy và tự dừng driver “ma” nếu còn.
- Xem log một luồng: `bash scripts/luong.sh log thinh_hanh`.

### 2.4 Web

```bash
# [labsoe-18]
curl -s http://localhost:8000/api/suc-khoe
# mong đợi: {"che_do":"delta","kafka":"GuiKafka"}
```

- Nếu ra `"che_do":"gia"`: web đang ở **chế độ giả lập**. **Không** dùng làm bằng chứng. Khởi động lại web (mục 7).
- Mở lần lượt 4 trang `/`, `/mo-hinh`, `/ab`, `/tai-lap` trên laptop trình chiếu. Trang nào trắng hoặc báo lỗi thì xử lý ngay hôm nay.
- Giao diện cũ hoặc ảnh không khớp tên ngành hàng: nhấn **Ctrl+F5**. Đây là bộ nhớ đệm của trình duyệt (lỗi 30 đã sửa).

### 2.5 Airflow

- Đăng nhập `http://10.68.71.113:8088`. Phải thấy 5 DAG: `kn_pipeline_hang_ngay`, `kn_giam_sat_streaming`, `kn_tinh_ab`, `kn_bao_tri`, `kn_tai_lap`.
- Nút **Kích hoạt kn_tai_lap** trên web **đã thử thành công tối 11/11** (features v3, 19:37–19:56, lệch 0, trùng khớp). Ngày 18/11 không cần thử lại nếu không đổi mật khẩu Airflow; ngày 19/11 kích hoạt đúng một lần ở mốc T−15.
- **Khuyến nghị: tạm dừng (pause) `kn_pipeline_hang_ngay` từ tối 18/11 đến sau buổi bảo vệ.**
  - Lý do: buổi tập tối 18/11 sẽ đẩy sự kiện mới vào bronze. Sensor của DAG 1 thấy dữ liệu mới thì chạy lại toàn bộ lúc 02:00 ngày 19/11.
  - Lần chạy đó có thể đổi tên mô hình đang phục vụ, đổi số trên màn hình Chất lượng mô hình, hoặc để lại một lần chạy đỏ trên trang chủ Airflow đúng ngày bảo vệ.
  - Nếu bị hỏi: “We froze the nightly pipeline for the defence so that the screens match the report. It runs on schedule otherwise; on 11 November it deployed version two by itself.”
  - Các DAG khác vẫn bật: giám sát streaming, A/B mỗi giờ, bảo trì 04:00.
- Ô **Triggerer** màu đỏ là **cố ý** (profile `day_du` tắt). Dự án không có deferrable operator, sensor chạy `mode=reschedule`. Câu trả lời đã có trong bộ hỏi – đáp.

### 2.6 Chọn sản phẩm cho demo vòng khép kín

Cần một sản phẩm **ít người xem**, không nằm trong khối “Đang thịnh hành”, để con số chỉ thay đổi do chính lần bấm của mình.

```bash
# [laptop] xem dòng thịnh hành mới nhất của từng ứng viên
for sp in 102605 320844 461686; do echo "== $sp"; curl -s http://10.68.71.149:8000/api/thinh-hanh/$sp; echo; done
```

- Chọn sản phẩm trả về `null`, hoặc có `cua_so_ket_thuc` cũ (không phải 15 phút gần nhất).
- Ghi mã đã chọn ra giấy: ☐ sản phẩm chính `______`, ☐ dự phòng `______`.

### 2.7 Thử trọn demo một lần và quay video dự phòng

- Chạy đúng 5 bước ở mục 5, có bật bộ phát (mục 4, bước T−10).
- Quay màn hình bằng **Win + Alt + R** (Xbox Game Bar) hoặc OBS. Lưu vào laptop và USB.
- Video này là phương án cuối cùng nếu mất mạng trong lúc bảo vệ.
- Ghi lại các số đang hiện để so vào ngày 19/11:
  - ☐ p-value trên **Bảng điều khiển A/B**: `______`
  - ☐ tên mô hình có trạng thái triển khai trên **Chất lượng mô hình**: `______`
  - ☐ số giây đo được ở bước vòng khép kín: `______`

> **Lưu ý về p-value trên bảng web:** mỗi lần mở trang chủ của user có ALS, web ghi 10 impression thật vào nhật ký. DAG `kn_tinh_ab` tính lại mỗi giờ, nên p trên bảng có thể lệch khỏi 0,0135 sau các buổi tập. **Nói đúng số đang hiện trên bảng** (kịch bản Word ghi 0,0135, thay bằng số đang hiện), rồi nhắc rằng phân tích chính trong báo cáo là 7 ngày mô phỏng, p = 0,015. Khi tập, hạn chế mở trang chủ user ALS nếu không cần.

### 2.8 Thử đường mạng từ phòng bảo vệ (nếu vào được phòng trước)

- Nối laptop vào Wi-Fi của phòng bảo vệ. Bật ZeroTier, kiểm tra laptop vẫn có IP 10.68.71.59.
- Mở `http://10.68.71.149:8000`.
  - Trang tải bình thường: dùng địa chỉ trực tiếp.
  - Trang treo hoặc chỉ tải được một nửa (lỗi MTU giống lần làm ở nhà): dùng đường hầm SSH (mục 3.3).

---

## 3. Đồ mang theo và chuẩn bị laptop

### 3.1 Đồ mang theo

- ☐ Laptop trình chiếu (Windows, ZeroTier 10.68.71.59) và sạc.
- ☐ Đầu chuyển HDMI hoặc USB-C sang HDMI/VGA. Hỏi trước phòng dùng cổng nào.
- ☐ Chuột, bút trình chiếu (nếu có).
- ☐ USB chứa: slide (PDF và PowerPoint tải từ trang slide), báo cáo (Word và PDF), bộ hỏi – đáp, kịch bản, thư mục ảnh chụp màn hình `bao_cao/hinh/s0*.png`, video demo dự phòng.
- ☐ Điện thoại có 4G để phát Wi-Fi khi Wi-Fi phòng hỏng. Qua 4G, nên dùng đường hầm SSH ngay từ đầu.
- ☐ Bản in báo cáo, nếu khoa yêu cầu.
- ☐ Giấy ghi: mã sản phẩm demo, user A/B, p-value sáng 19/11.

### 3.2 Chuẩn bị laptop (sáng 19/11)

- ☐ Cắm sạc. Tắt chế độ ngủ: Settings → System → Power → Never.
- ☐ Bật **Focus assist / Do not disturb**. Thoát Zalo, Messenger, Teams, ứng dụng đồng bộ.
- ☐ Trình duyệt dùng một profile sạch: ẩn thanh dấu trang, đóng tab cá nhân, không mở trang có token hay khóa (GitHub settings, Kaggle).
- ☐ Phóng to trình duyệt 110–125 % để hội đồng đọc được trên máy chiếu. Thử ngay trên máy chiếu.
- ☐ Mở sẵn 6 tab theo thứ tự demo: (1) slide, (2) web trang chủ, (3) Chất lượng mô hình, (4) Bảng điều khiển A/B, (5) Tái lập thí nghiệm, (6) Kafka UI. Các tab phụ (Airflow, Spark, HDFS) để ở một cửa sổ khác.
- ☐ Mở sẵn một cửa sổ Git Bash đã SSH vào labsoe-11, đứng ở `~/bd03_khuyen_nghi`, để chạy lệnh dự phòng. Cỡ chữ lớn: chuột phải → Options → Text.

### 3.3 Đường hầm khi ở ngoài mạng lab hoặc khi trang web treo

```bash
# [laptop] để nguyên cửa sổ này trong suốt buổi bảo vệ
ssh -N -L 8001:localhost:8000 labsoe-18
```

- Mở web ở `http://localhost:8001`.
- Muốn xem cả Spark và Airflow qua đường hầm: thêm `-L 8081:10.68.71.113:8080 -L 8089:10.68.71.113:8088`, rồi mở `localhost:8081` và `localhost:8089`.
- **Phải thử trước ngày 19/11.** Trang Airflow có thể chuyển hướng về địa chỉ gốc; nếu vậy chỉ dùng đường hầm cho web.
- Lệnh dự phòng ở bước vòng khép kín khi dùng đường hầm: thay `--web http://10.68.71.149:8000` bằng `--web http://localhost:8001`.

---

## 4. Ngày bảo vệ: các mốc từ T−60 đến T

| Mốc | Việc | Lệnh hoặc nơi làm | Phải thấy | Xong |
|---|---|---|---|---|
| T−60 | Có mặt, cắm máy chiếu, thử độ phân giải, mở slide | Phòng bảo vệ | Slide hiển thị đủ, chữ đọc được | ☐ |
| T−45 | Kiểm tra cụm | `[labsoe-11] bash scripts/cum/kiem_tra_cum.sh` | Không `DONG`, 5 DataNode, 5 worker | ☐ |
| T−30 | Kiểm tra luồng | `[labsoe-11] bash scripts/luong.sh trang_thai` | 3 luồng đang chạy | ☐ |
| T−30 | Kiểm tra web | Mở 4 trang web; `curl .../api/suc-khoe` | `"che_do":"delta"` | ☐ |
| T−20 | Đăng nhập Airflow, mở Kafka UI, Spark, HDFS | Trình duyệt | Đã đăng nhập, không phải gõ mật khẩu lúc demo | ☐ |
| T−15 | Ghi p-value đang hiện trên bảng A/B | Web → Bảng điều khiển A/B | Báo cho Quốc Anh số cần nói | ☐ |
| T−15 | **Kích hoạt DAG 5** (đề ghi “Bắt buộc demo”): web → **Tái lập thí nghiệm** → mục *Tái lập huấn luyện mô hình* → chọn dòng **features v3 · als_v2_r20261110T230924** (mô hình đang phục vụ) → **Kích hoạt kn_tai_lap** | Web, hoặc Airflow → `kn_tai_lap` → Trigger với `{"phien_ban_delta": 3, "che_do": "mo_hinh_chon"}` | Web báo `"trang_thai": "queued"`; Airflow có lần chạy mới. Job khoảng 20 phút, xong trong lúc trình bày | ☐ |
| T−10 | **Bật bộ phát** để khối thịnh hành có dữ liệu sống | `[labsit]` lệnh ngay dưới bảng | Dòng tiến độ in ra đều | ☐ |
| T−5 | Tải lại trang chủ | Web | Khối “Đang thịnh hành” có nhiều sản phẩm (không chỉ 1–2) | ☐ |
| T−2 | Về slide 1, điện thoại im lặng, uống nước | — | — | ☐ |

Lệnh bật bộ phát ở mốc T−10:

```bash
# [labsit] trong tmux để đứt SSH vẫn chạy
ssh labsit
tmux new -A -s phat
python3 ~/bd03_khuyen_nghi/producer/phat_lai_su_kien.py \
  --tep ~/du_lieu/retailrocket/events.csv \
  --tu-ngay-ms 1438387200000 --ty-le-nguoi-moi 0 --giay-moi-ngay 90
# Ctrl+B rồi D để thoát tmux mà bộ phát vẫn chạy
```

Giải thích các tham số:

- `1438387200000` là 01/08/2015 (UTC), tức 48 ngày cuối của dữ liệu.
- `--giay-moi-ngay 90` phát một ngày dữ liệu trong 90 giây, nên chạy khoảng **72 phút**: đủ cho 20 phút trình bày, phần hỏi – đáp và demo.
  - Với mặc định 30 giây/ngày, bộ phát chỉ chạy 24 phút. Nếu demo diễn ra sau đó, cửa sổ thịnh hành 15 phút sẽ vơi dần và khối thịnh hành chỉ còn vài sản phẩm (lỗi 29).
- `--ty-le-nguoi-moi 0`: đây là lần phát lặp, nên tắt user mới để không sinh lại các mã `moi_<n>` trùng.
- Sự kiện phát lặp không làm sai silver, vì silver bỏ trùng theo nội dung.

---

## 5. Kịch bản demo chi tiết (khoảng 5 phút)

- **Tâm** bấm máy, **Quốc Anh** nói lời dẫn tiếng Anh. Lời dẫn đầy đủ nằm trong kịch bản Word, mục 3.
- Mỗi bước có: thao tác, bằng chứng phải thấy trên màn hình, câu chốt, và cách xử lý khi lỗi.
- Nếu hội đồng muốn xem demo đầy đủ 15 phút theo 8 tiêu chí: theo `docs/HUONG_DAN_DEMO.md`, mục B.

### Bước 0 (tùy chọn, 30 giây): hạ tầng đang chạy

Chỉ làm khi hội đồng hỏi “hệ thống có chạy phân tán thật không”.

- Spark Master `:8080`: 5 worker `ALIVE` với IP ZeroTier, 3 ứng dụng streaming đang chạy.
- HDFS `:9870` → Datanodes: 5 DataNode sống.
- Airflow: 5 DAG `kn_*`.
- Câu chốt: “Six lab machines, five workers, three streams running right now.”

### Bước 1 (1 phút): Chất lượng mô hình (tiêu chí 1, 2, 8)

- **Thao tác:** menu **Chất lượng mô hình**, cuộn từ trên xuống, dừng 5–10 giây ở mỗi bảng.
- **Phải thấy:**
  - Bảng so sánh ALS với 2 baseline, có dòng “Ngưỡng đạt”.
  - Bảng tuning 27 dòng, xếp theo NDCG giảm dần; dòng đầu là rank 50 / regParam 1,0 / alpha 40.
  - Lịch sử mô hình: dòng triển khai `als_v2_…`.
  - Mục OPTIMIZE / ZORDER ở cuối trang.
- **Câu chốt:** “Every number on the slides comes from these gold tables.”
- **Nếu trang chậm** (web đang đọc Delta lần đầu): nói tiếp lời dẫn, chờ tối đa 20 giây. Vẫn không tải được thì chuyển sang slide 11–15.

### Bước 2 (1 phút): Cold start với người dùng mới (tiêu chí 3)

- **Thao tác:** **Trang chủ** → **+ Người dùng mới (cold start)**.
- **Phải thấy:**
  - Mã `moi_xxxxxxxx`.
  - Nhãn chiến lược `nguoi_moi`.
  - Chip “ngoài thí nghiệm A/B”.
  - 10 ô có nhãn **Thịnh hành**, trùng với khối “Đang thịnh hành”.
- **Ghi lại mã `moi_…`** để dùng ở bước 3.
- **Câu chốt:** “ALS has no vector for this user, so the page falls back to the newest trending window.”
- **Nếu 10 ô ít hoặc trùng lặp:** khối thịnh hành đang thưa vì bộ phát chưa chạy. Kiểm tra tmux `phat` trên labsit.

### Bước 3 (1,5 phút): Vòng khép kín ≤ 90 giây (tiêu chí 4, 5)

- **Thao tác:**
  1. Mở trang sản phẩm đã chọn ở mục 2.6, kèm mã người dùng vừa tạo. Ví dụ: `http://10.68.71.149:8000/san-pham/102605?u=moi_xxxxxxxx`. Nút bấm chỉ hiện khi địa chỉ có `?u=`.
  2. Đọc ô “Trạng thái trong bảng thịnh hành” (thường là “Chưa có trong 15 phút gần nhất”).
  3. Bấm **Thêm vào giỏ** 3 lần.
  4. Tùy chọn: chuyển sang tab Kafka UI → topic `retailrocket-events` → Messages, chỉ vào 3 thông điệp có `"nguon": "web"`, rồi quay lại tab sản phẩm.
  5. Chờ trang tự hỏi lại (mỗi 5 giây). Quốc Anh nói lời dẫn trong lúc chờ.
- **Phải thấy:** “ĐÃ CẬP NHẬT sau N giây”, với N ≤ 90 (ngày 11/11 đo được 51 giây). Số thêm giỏ của sản phẩm tăng 3.
- **Thêm, nếu còn thời gian:** quay lại trang chủ của user này. Nhãn chiến lược có thể đã chuyển sang `it_tuong_tac`: bán chạy theo danh mục vừa xem, xen kẽ thịnh hành. Đây là chiến lược cold start thứ hai.
- **Câu chốt:** “From a click on the web page to the trending table in under ninety seconds.”
- **Nếu quá 90 giây chưa cập nhật:**
  1. Nói: “The stream may be in a micro-batch; let me check it.”
  2. Chạy ở `[labsoe-11]`: `bash scripts/luong.sh trang_thai`. Thiếu luồng thịnh hành thì `bash scripts/luong.sh khoi_dong thinh_hanh`. Checkpoint giữ offset, nên click vừa bấm vẫn được tính, đúng như sự cố ngày 11/11.
  3. Nếu không kịp: chỉ slide 22 (51 giây đo ngày 11/11) và đi tiếp.
- **Chứng minh sự kiện đã vào bronze** (khi hội đồng hỏi):
  ```bash
  # [labsoe-11]
  bash ha_tang/spark/submit.sh nhanh kiem_tra/tim_su_kien_bronze.py --ma-nguoi-dung moi_xxxxxxxx
  ```
  Lệnh in các dòng bronze của user, kèm độ trễ thời điểm → `kafka_ts` → `_ingest_ts`.
- **Lệnh đo tự động thay cho giao diện:**
  ```bash
  # [laptop]
  python scripts/demo_vong_khep_kin.py --web http://10.68.71.149:8000 --san-pham <mã dự phòng> --so-lan 3
  # ... "Bảng thịnh hành đã cập nhật sau NN giây ... -> ĐẠT (<= 90s)"
  ```

### Bước 4 (1 phút): Thí nghiệm A/B (tiêu chí 6)

- **Thao tác:**
  1. Mở `/?u=270018` (nhóm B): chỉ vào ô **3, 6, 8** có nhãn Thịnh hành.
  2. Tải lại trang một lần để thấy nhóm không đổi.
  3. Tùy chọn: mở `/?u=1287993` (nhóm A) để so, cả 10 ô là ALS.
  4. Menu **Bảng điều khiển A/B**.
- **Phải thấy:** CTR và tỷ lệ chuyển đổi theo nhóm, p-value của CTR và của chuyển đổi, biểu đồ CTR theo ngày, tỷ lệ traffic mô phỏng của từng ngày.
- **Câu chốt:** nói p đang hiện trên bảng (số đã ghi ở mốc T−15), rồi: “The report keeps the seven simulated days, p equals 0.015. Both say B is significantly worse under this simulation.”
- **Nếu bị hỏi vì sao số trên bảng khác số trong báo cáo:** bảng web gộp cả các impression thật ghi được trong lúc chụp màn hình và tập demo. Báo cáo cố định 7 ngày mô phỏng để có thể tái lập.

### Bước 5 (30 giây): Tái lập thí nghiệm (tiêu chí 7)

- **Thao tác:** menu **Tái lập thí nghiệm** → chọn một ngày cũ → **Chạy lại**.
- **Phải thấy:** bảng cũ/mới cho từng chỉ số, cột lệch = 0, kết luận trùng khớp. Bảng tái lập huấn luyện bên dưới có độ lệch 1,78·10⁻¹⁵.
- **DAG 5 đã được kích hoạt ở mốc T−15** (đề bài ghi DAG 5 “Bắt buộc demo”). Tải lại trang: bảng tái lập huấn luyện có **dòng mới với giờ hôm nay**, độ lệch < 10⁻⁶, trùng khớp. Mở thêm Airflow → `kn_tai_lap` → lần chạy hôm nay → Graph để thấy các bước.
- Nếu lần chạy chưa xong: mở Airflow cho thấy nó đang chạy, nói “it retrains the deployed model on feature version 3, about twenty minutes; the run of 11 November is here” và chỉ dòng của 11/11. Không kích hoạt thêm lần nữa.
- **Câu chốt:** “Same Delta version, same seed, same metrics.”

---

## 6. Nếu máy lab bị tắt hoặc khởi động lại trước ngày bảo vệ

Phòng lab dùng chung, nên máy có thể bị tắt hoặc khởi động lại. Bật lại theo thứ tự:

1. **Master** `[labsoe-11]`: `bash scripts/cum/khoi_dong_may.sh`
2. **Các worker** (labsit12, labsit, labsit-may25, labsit-may26, labsoe-18), trên từng máy: `bash scripts/cum/khoi_dong_may.sh`
   - Không đụng tới container của nhóm khác trên labsit-may25 và labsit-may26.
3. **Kafka trên labsit** được bật cùng `khoi_dong_may.sh` của máy đó. Kiểm tra Kafka UI `:8080`.
4. **Web** `[labsoe-18]`: `bash scripts/cum/khoi_dong_may.sh --chi=web`, rồi `curl -s http://localhost:8000/api/suc-khoe`.
5. **Luồng** `[labsoe-11]`: `bash scripts/luong.sh khoi_dong tat_ca`, rồi `bash scripts/luong.sh trang_thai`.
6. Kiểm tra lại theo mục 2.2–2.5.

Ghi chú:

- Máy ở ngoài trường cần hạ MTU: `ZT_MTU=1280 bash scripts/cum/khoi_dong_may.sh` (cần sudo).
- Nếu Cassandra của phòng lab tự bật lại trên labsoe-11 sau khi khởi động và chiếm RAM master: **hỏi cán bộ phòng lab trước** khi dừng nó.

---

## 7. Xử lý sự cố trong lúc bảo vệ

| Dấu hiệu | Nguyên nhân thường gặp | Xử lý trong 1 phút | Nếu vẫn không được |
|---|---|---|---|
| Web không phản hồi | Container web treo | `[labsoe-18] docker restart bd03-web`, chờ khoảng 30 giây để web tạo phiên Spark | Dùng ảnh chụp (slide 16–17, `bao_cao/hinh/s0*.png`), nói rõ là ảnh chụp ngày 11/11 |
| Trang treo, tải được một nửa | Lỗi MTU trên đường ZeroTier | Chuyển sang đường hầm (mục 3.3) | Phát Wi-Fi từ điện thoại rồi dùng đường hầm |
| Khối thịnh hành đứng yên hoặc chỉ 1–2 món | Bộ phát đã dừng, hoặc luồng thịnh hành chết | `tmux attach -t phat` trên labsit; `bash scripts/luong.sh trang_thai` | `bash scripts/luong.sh khoi_dong thinh_hanh` |
| Bảng thịnh hành không cập nhật sau 90 giây | Luồng đang chạy micro-batch dài, hoặc đã chết | Như dòng trên | Chỉ slide 22 (51 giây đã đo) |
| Một Spark worker hoặc DataNode chết | Wi-Fi chập chờn | Không cần làm gì: Spark chạy lại task, HDFS còn bản sao thứ hai. Có thể dùng làm ví dụ chịu lỗi | Nói tiếp, xử lý sau buổi bảo vệ |
| Airflow có ô đỏ | Triggerer (cố ý tắt), hoặc lần chạy cũ bị lỗi (lỗi 24) | Giải thích: triggerer không cần vì không có deferrable operator; lần chạy lỗi đã có trong nhật ký lỗi | — |
| p trên bảng A/B khác 0,0135 | Có thêm impression thật từ các buổi tập | Nói đúng số đang hiện; phân tích chính là p = 0,015 | — |
| Mất toàn bộ mạng | Wi-Fi phòng hoặc ZeroTier | Chuyển sang slide và ảnh chụp | Mở video demo dự phòng |
| Máy chiếu không nhận laptop | Đầu chuyển hoặc độ phân giải | Win + P → Duplicate; đổi độ phân giải 1920×1080 hoặc 1280×720 | Mở PDF slide trên máy của phòng |

**Không bao giờ** dùng chế độ giả lập của web (`"che_do":"gia"`) làm bằng chứng: số liệu ở chế độ đó là giả.

---

## 8. Những việc KHÔNG làm

- Không dừng hay xóa container của nhóm khác trên labsit-may25 và labsit-may26.
- Không chạy `docker compose down -v`: tham số `-v` xóa volume dữ liệu.
- Không kích hoạt DAG 1 (`kn_pipeline_hang_ngay`) trong buổi bảo vệ. DAG 5 (`kn_tai_lap`) chỉ kích hoạt **đúng một lần** ở mốc T−15; không chạy job tay nào khác trong lúc nó chạy (lỗi 31).
- Không chạy `producer/nguoi_dung_ao.py` vào ngày bảo vệ: script này ghi thêm traffic `mo_phong_web` vào nhật ký hiển thị và làm số trên bảng A/B thay đổi.
- Không mở trên màn hình các tệp bí mật: `ha_tang/*/.env`, `ha_tang/web/.env.rieng`, `ha_tang/airflow/mat_khau.json`, `kaggle.json`. Không gõ mật khẩu khi màn hình đang chiếu.
- Không đưa token GitHub hay khóa Kaggle lên máy lab. Mã chỉ lên máy lab qua `--dong-bo-ma`.
- Không sửa mã trên máy lab trong ngày bảo vệ. Có lỗi thì sửa trên laptop, commit, rồi `--dong-bo-ma`.

---

## 9. Số liệu cần thuộc

| Hạng mục | Số liệu |
|---|---|
| Dữ liệu | 2,89 triệu sự kiện, 139 ngày, 417.053 sản phẩm; mật độ ma trận huấn luyện 3,3·10⁻⁵ |
| Train / test | 179.999 user train (≥ 3 tương tác), 126.230 item; 9.467 user test; tháng test 19/8–18/9/2015 |
| Trọng số | Xem 1, thêm giỏ 3, mua 5; suy giảm λ = 0,01/ngày |
| Tuning | 27 tổ hợp; tốt nhất rank 50, regParam 1,0, alpha 40, NDCG@10 = 0,0680 |
| Baseline | Toàn sàn 0,0051 (ALS +1.237 %), theo danh mục 0,0475 (ALS +43 %) |
| Coverage | ALS 3,44 % (ngưỡng 10 %); 99,2 % ô thuộc 10 % item phổ biến nhất; trần ≈ 75 % |
| Xếp lại novelty | β = 1,0, 200 ứng viên: NDCG 0,0865 (+27 %), coverage 6,47 % (+88 %) |
| A/B | 350.160 impression, 7 ngày; CTR A 2,094 %, B 1,978 %; z = −2,43; p = 0,015; chuyển đổi p = 0,75; cần 76.972/nhóm; theo user: p = 0,037 (cụm), 0,029 (bootstrap) |
| Z-ORDER | Tệp đọc 67 → 1,07; trung vị 8.280 → 980 ms; nhanh hơn 8,4 lần so với chỉ OPTIMIZE |
| Tái lập | Độ lệch 0,000000 (1,78·10⁻¹⁵) trên features phiên bản 0 |
| Nguồn ứng viên (offline) | 7 ô ALS + 3 ô đồng xuất hiện: coverage 6,47 % → 14,5 % (ngưỡng 10 %), NDCG@10 0,0865 → 0,0892; chưa triển khai |
| Kỳ validation | Chọn trên 19/8–2/9, báo cáo trên 3–18/9: chọn đúng cấu hình đang phục vụ và β = 1,0; kỳ kiểm tra ALS 0,0579 (+40 % so với danh mục), xếp lại 0,0742 (+80 %); lạc quan 0,0002 |
| Vòng khép kín | Kafka → bronze 10,3 giây; bấm → bảng thịnh hành 51 giây |
| Mô hình v2 | NDCG 0,0680 → 0,0694 (+2,1 %); coverage 3,44 → 3,72 %; 4.346 → 4.699 item; tự triển khai ngày 11/11 |
| Cụm | 6 máy lab, mỗi máy 7 GB RAM; 5 worker, 34 lõi, 13 GB executor; HDFS sao chép 2 |
| Kỹ thuật | 33 lỗi đã ghi và sửa, 143 kiểm thử, khoảng 13.500 dòng mã, báo cáo chính 17 trang thân bài (bản đầy đủ 113 trang) |

---

## 10. Câu hỏi hay gặp ngay trong lúc demo

Trả lời ngắn, rồi quay lại demo. Câu dài hơn xem bộ hỏi – đáp.

- **Tên và ảnh sản phẩm có thật không?**
  - Không. RetailRocket đã ẩn danh hóa, nên ảnh và tên là **minh họa** theo ngành hàng (danh mục cấp 2).
  - Mã sản phẩm thật luôn hiện trên thẻ. Chân trang có ghi chú điều này.
- **Vì sao khối thịnh hành toàn sản phẩm của năm 2015?**
  - Bộ phát phát lại log năm 2015. Cửa sổ thịnh hành tính theo **thời điểm Kafka nhận**, nên vẫn là “15 phút gần nhất” theo giờ thật.
- **Vì sao người dùng mới “ngoài thí nghiệm A/B”?**
  - Thí nghiệm chỉ so danh sách ALS (A) với ALS trộn thịnh hành (B). User chưa có ALS không thuộc nhóm nào.
- **Traffic A/B có phải người thật không?**
  - Chủ yếu là mô phỏng theo mô hình click phụ thuộc vị trí, có gắn nhãn, và không cộng điểm cho sản phẩm thịnh hành.
  - Kết luận chỉ đúng trong giả định đó.
- **Kafka chết thì sao?**
  - Web vẫn gợi ý được, vì danh sách đọc từ gold.
  - Khi Kafka sống lại, luồng đọc tiếp từ offset trong checkpoint, không mất và không trùng sự kiện.
- **Vì sao DAG 1 đang tạm dừng?** Xem câu trả lời ở mục 2.5.
- **Chọn siêu tham số trên tháng test có gian lận không?** / **Impression không độc lập thì p còn đúng không?**
  - Mở slide 27 (dự phòng): chọn trên kỳ validation ra đúng cấu hình đang phục vụ; theo user p = 0,029–0,037.
- **Coverage dưới 10 % có phải không đạt tiêu chí?**
  - Hệ thống đang chạy 6,47 % (đã phân tích theo yêu cầu đề); thí nghiệm offline với 3 ô đồng xuất hiện đạt 14,5 % và NDCG còn tăng; chưa triển khai để giữ hệ thống ổn định trước buổi bảo vệ.
- **Danh sách cá nhân có đổi ngay sau click không?**
  - Phần ALS đổi mỗi đêm (DAG 1).
  - Phần thịnh hành đổi trong khoảng 1 phút. User mới đổi chiến lược ngay trong phiên.
  - Cập nhật trực tuyến là hướng phát triển (slide 25).

---

## 11. Sau buổi bảo vệ

- ☐ Dừng bộ phát: `ssh labsit`, `tmux attach -t phat`, **Ctrl+C**.
- ☐ Bật lại DAG `kn_pipeline_hang_ngay`, hoặc để tạm dừng nếu không chạy dự án nữa.
- ☐ Trả lại Cassandra cho phòng lab: `[labsoe-11] sudo systemctl start cassandra`.
- ☐ Laptop: bỏ khối `Host m11 … ProxyJump labsoe-18` trong `~/.ssh/config` nếu không dùng nữa.
- ☐ Muốn dừng cụm hẳn: `[labsoe-11] bash scripts/luong.sh dung tat_ca`, rồi `bash scripts/cum/khoi_dong_may.sh --dung` trên từng máy. **Không** thêm `-v`.
- ☐ Hỏi cán bộ phòng lab trước khi gỡ ZeroTier hay openssh khỏi máy lab.
- ☐ Nếu hội đồng yêu cầu sửa: sửa, commit, gắn tag mới (`v1.6`), nộp lại.

---

## 12. Phụ lục: lệnh nhanh

```bash
# ---- [laptop] Git Bash, thư mục /d/BIG DATA/bd03_khuyen_nghi
bash scripts/cum/ssh_tat_ca.sh hostname                       # SSH được tới 6 máy
ssh labsoe-11 "bash ~/bd03_khuyen_nghi/scripts/cum/tinh_trang.sh"   # tình trạng master, chỉ đọc
ssh -N -L 8001:localhost:8000 labsoe-18                        # đường hầm web -> http://localhost:8001
curl -s http://10.68.71.149:8000/api/suc-khoe                  # {"che_do":"delta","kafka":"GuiKafka"}
curl -s http://10.68.71.149:8000/api/thinh-hanh/102605         # dòng thịnh hành mới nhất của sản phẩm
python scripts/demo_vong_khep_kin.py --web http://10.68.71.149:8000 --san-pham 102605 --so-lan 3

# ---- [labsoe-11] ~/bd03_khuyen_nghi
bash scripts/cum/kiem_tra_cum.sh                               # cổng, DataNode, worker
bash scripts/luong.sh trang_thai                               # 3 luồng
bash scripts/luong.sh khoi_dong tat_ca                         # bật luồng còn thiếu
bash scripts/luong.sh log thinh_hanh                           # log luồng thịnh hành
bash ha_tang/spark/submit.sh nhanh kiem_tra/tim_su_kien_bronze.py --ma-nguoi-dung <ma_user>

# ---- [labsit]
tmux new -A -s phat                                            # vào phiên bộ phát
python3 ~/bd03_khuyen_nghi/producer/phat_lai_su_kien.py --tep ~/du_lieu/retailrocket/events.csv \
  --tu-ngay-ms 1438387200000 --ty-le-nguoi-moi 0 --giay-moi-ngay 90

# ---- [labsoe-18]
curl -s http://localhost:8000/api/suc-khoe
docker restart bd03-web                                        # web treo (chờ ~30 giây)
bash scripts/cum/khoi_dong_may.sh --chi=web                    # bật lại web sau khi máy khởi động lại
```


## 13. Câu hỏi phản biện mẫu mục C.5 của đề cương (giảng viên hỏi ít nhất 3 câu)

### C.5.1 Vì sao phân vùng theo cột này mà không phải cột khác? Chọn sai thì sao?

- `bronze.su_kien`, `silver.tuong_tac`, `_quarantine.tuong_tac` phân vùng theo `ngay` (139 ngày → 140 tệp ở silver sau
  khi sửa lỗi tệp nhỏ). Mọi job lô đọc theo khoảng ngày: tăng dần theo ngày mới, chia train/test theo ngày cắt,
  backfill một khoảng ngày, ghi đè đúng phân vùng bằng `replaceWhere` (`jobs/transform/bronze_to_silver.py`).
- Không phân vùng theo `ma_nguoi_dung`: 1,4 triệu người dùng → hàng triệu thư mục và tệp nhỏ, NameNode phải giữ metadata
  của từng tệp trong RAM, nhật ký Delta phình to, mỗi truy vấn theo ngày phải mở hàng triệu tệp. Nhóm đã gặp đúng
  hiện tượng này ở dữ liệu mẫu: 7.334 tệp cho 20.624 dòng trước khi gom theo phân vùng (NHAT_KY).
- Truy vấn nóng của web là lấy gợi ý theo một người dùng. Bài toán này giải bằng `ZORDER BY (ma_nguoi_dung)` trên
  `gold.goi_y_nguoi_dung` (data skipping theo min/max của từng tệp), không bằng phân vùng.

### C.5.2 Một bản ghi đến trễ 3 ngày thì sao? Bảng gold có tự cập nhật không?

- `nap_su_kien` ghi bản ghi vào phân vùng của **ngày xảy ra sự kiện** (`ngay = to_date(thoi_diem)`), kèm `_ingest_ts` là lúc nạp.
- Lượt `bronze_to_silver` kế tiếp chọn mọi ngày có `_ingest_ts` mới hơn mốc đã xử lý (`chon_ngay_can_xu_ly`), nên
  ngày cũ đó được tính lại **trọn phân vùng** bằng `replaceWhere`: khử trùng, lọc bot, khoảng 5 giây đều tính lại đúng.
- DAG 1 chạy tiếp các bước sau: `thong_ke_san_pham` ghi đè, ma trận đặc trưng dựng lại (phiên bản Delta mới), mô hình
  huấn luyện lại, `ghi_gold` MERGE theo người dùng. Vậy bảng gold tự cập nhật ở lượt DAG 1 kế tiếp.
- Riêng khối thịnh hành 15 phút tính theo thời điểm thông điệp tới Kafka (`kafka_ts`, watermark 2 phút). Bản ghi trễ
  được tính vào cửa sổ hiện tại, vì "đang thịnh hành" là hoạt động đang diễn ra; cửa sổ cũ không mở lại.
- Impression hay click trễ được MERGE theo `ma_hien_thi`. CDF báo ngày có thay đổi, DAG 3 tính lại A/B của ngày đó.
- Muốn ép tính lại một khoảng ngày, dùng backfill: DAG 1 `{"tu_ngay": ..., "den_ngay": ...}`.

### C.5.3 Chỗ nào trong code có thể gây data leakage, đã ngăn thế nào?

- Chia theo thời gian, không ngẫu nhiên: `jobs/ml/chuan_bi_ma_tran.py` `chia_theo_thoi_gian` / `dung_ma_tran`, train
  `ngay < ngay_cat`, test `ngay >= ngay_cat` (30 ngày cuối, cắt 19/8/2015).
- Suy giảm thời gian tính so với ngày cắt (`tinh_diem(..., ngay_cat)`), không so với ngày cuối dữ liệu.
- Độ phổ biến cho baseline, novelty và độ phủ chỉ đếm trên train: `thong_ke_pho_bien(train)` trong
  `jobs/common/als_lib.py`. Danh mục người dùng của baseline 2 cũng chỉ lấy từ train.
- StringIndexer người dùng fit trên train (`train_tho`). Tập test chỉ giữ người dùng và sản phẩm đã có trong train
  (`doc_features`); phần còn lại thuộc cold start, đánh giá riêng.
- Rủi ro còn lại: siêu tham số được chọn trên chính tháng test. Thí nghiệm bổ sung chọn lại trên một kỳ validation riêng
  (15 ngày đầu tháng test) và báo cáo trên các ngày còn lại (`jobs/kiem_tra/tuning_validation.py`, báo cáo mục 6.10).
- Mô hình phục vụ fit lại trên train + test **sau** khi đã đánh giá, không dùng để báo cáo metric.

### C.5.4 DAG thất bại ở task 3/5, chạy lại có nhân đôi dữ liệu không? Chứng minh.

Không. Mọi bước ghi của DAG 1 đều idempotent:

| Task | Cách ghi | Chạy lại |
|---|---|---|
| `bronze_to_silver` | `replaceWhere ngay IN (...)` cho silver và `_quarantine` | Thay đúng các phân vùng đó |
| `thong_ke_san_pham`, `san_pham_lien_quan`, `chuan_bi_ma_tran` | `overwrite` | Ghi đè, tạo phiên bản Delta mới |
| `huan_luyen_als` | `replaceWhere ma_chay = '<ma_chay>'` vào `gold.ket_qua_tuning` | Thay các dòng của cùng lượt |
| `danh_gia` | `replaceWhere ma_chay = ...` vào `gold.lich_su_huan_luyen` | Một dòng mỗi lượt |
| `sinh_goi_y_toan_bo`, `tron_goi_y` | `overwrite` bảng trung gian | Ghi đè |
| `ghi_gold` | `MERGE INTO gold.goi_y_nguoi_dung ... ON ma_nguoi_dung, xep_hang` | Cập nhật, không chèn trùng |

`ma_chay` được tạo một lần ở task `tao_ma_chay`. Retry hay "Clear" task 3 dùng lại cùng mã. Lượt chạy mới có mã mới
và là một lượt huấn luyện mới (lịch sử), không phải dữ liệu trùng.

Chứng minh tại chỗ bằng hai lệnh:

- `bash scripts/demo/delta_sql.sh sql "SELECT ma_chay, count(*) AS so_dong, count(DISTINCT rank, reg_param, alpha) AS so_to_hop FROM {gold.ket_qua_tuning} GROUP BY ma_chay"`: mỗi lượt `so_dong = so_to_hop`, không tổ hợp nào lặp.
- `DESCRIBE HISTORY` của silver: commit có `operationParameters.predicate` là danh sách ngày.

Luồng streaming cũng không nhân đôi: checkpoint giữ offset Kafka, sink MERGE theo khóa (`ghi_hien_thi` theo `ma_hien_thi`,
thịnh hành theo cửa sổ + sản phẩm), file sink của bronze ghi nhận lô trong `_spark_metadata`.

### C.5.5 OPTIMIZE cải thiện bao nhiêu? Vì sao?

- Truy vấn lấy gợi ý của một người dùng trên `gold.goi_y_nguoi_dung`, đo bằng `bao_tri/do_zorder.py` với 30 truy vấn.
- Ba giai đoạn: 200 tệp (trước) → 67 tệp sau OPTIMIZE chỉ gộp tệp → sau ZORDER mỗi truy vấn chỉ đọc 1,07 tệp.
  Trung vị 10,8 s → 8,3 s → 0,98 s.
- Vì sao: gộp tệp giảm chi phí mở tệp và đọc footer Parquet (giảm khoảng 23 %). ZORDER sắp dữ liệu theo
  `ma_nguoi_dung`, nên khoảng min/max của cột này trong từng tệp hẹp lại và gần như không giao nhau. Delta dùng số
  thống kê min/max trong `_delta_log` để bỏ qua tệp (data skipping): truy vấn `WHERE ma_nguoi_dung = ?` chỉ còn mở
  khoảng 1 tệp, gấp khoảng 11 lần nhanh hơn.

### C.5.6 Mô hình kém baseline ở trường hợp nào? Vì sao?

- Độ phủ và độ mới: baseline 2 (bán chạy trong danh mục) có độ phủ 5,28 % và novelty 11,88, cao hơn ALS thuần (3,44 %,
  10,86). ALS implicit với alpha lớn đẩy điểm về các sản phẩm nhiều tương tác (popularity bias); bản phục vụ có xếp
  lại theo novelty nâng độ phủ lên 6,47 %.
- Theo người dùng: `jobs/kiem_tra/phan_tich_loi.py` tính NDCG@10 từng người dùng cho ALS và hai baseline, gom theo số
  tương tác train, độ phổ biến của sản phẩm test, số danh mục đã xem (`docs/bang_chung/cum/phan_tich_loi.json`,
  báo cáo mục phân tích lỗi). Dự đoán cần đối chiếu với số đo:
  - người dùng ít tương tác (3–4) có nhân tố ẩn kém tin cậy;
  - người dùng chỉ quanh quẩn một danh mục thì baseline 2 đã gần đúng.
- Người dùng mới và sản phẩm mới: ALS không chấm điểm được, nên dùng thịnh hành, bán chạy theo danh mục, chèn sản phẩm
  mới vào 20 % vị trí cuối. Đây là giới hạn cấu trúc của phân rã ma trận, không phải lỗi huấn luyện.

### C.5.7 Dữ liệu tăng 100 lần thì chỗ nào vỡ trước?

Theo thứ tự dự kiến:

1. RAM máy master (7,4 GB): driver của mọi job chạy client mode ngay trên master, cùng NameNode và Airflow.
2. Các bảng giữ trên driver theo số sản phẩm và người dùng: nhãn StringIndexer (180 nghìn → 18 triệu người dùng),
   bảng novelty broadcast (126 nghìn → 12,6 triệu sản phẩm, vài trăm MB).
3. `recommendForAllUsers` và phép tự nối đồng xuất hiện.
4. Kafka 3 phân vùng và một broker; băng thông ZeroTier, khi đi đường chuyển tiếp chỉ khoảng 0,4 MB/s.
5. Web dùng Spark local[2].

HDFS và Delta chịu được nhất: thêm DataNode là mở rộng được.

Hướng xử lý:

- Driver chạy cluster mode trên YARN hoặc Kubernetes.
- Thay bảng novelty bằng broadcast join DataFrame.
- Chỉ sinh gợi ý cho người dùng hoạt động trong 30 ngày (đã có).
- Thêm phân vùng Kafka.
- Đưa bảng gợi ý ra kho key-value (Redis, Cassandra) cho web.

Chi tiết ở bổ sung 6 của kịch bản thuyết trình.

### C.5.8 Chỉ một dòng code bất kỳ và giải thích

Mở đúng tệp và đọc docstring đầu tệp và đầu hàm: mỗi job, DAG, script đều có giải thích tiếng Việt về **vì sao**.

Tài liệu tra cứu:

- `docs/KIEN_TRUC.md`: quyết định thiết kế, đặc tả bảng, mục 9 "dữ liệu đưa về driver".
- `MO_HINH.md`: lý thuyết ALS implicit, metric.
- `tests/`: hành vi mong đợi của từng hàm.
- `docs/AI_USAGE.md`: phần nào do AI sinh.
