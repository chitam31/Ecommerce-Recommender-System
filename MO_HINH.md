# MO_HINH.md — Thẻ mô hình của hệ khuyến nghị bd03 

Tài liệu mô tả **mô hình đã được thiết kế và hiện thực như thế nào**, cơ sở lý thuyết, cách đánh giá và các giới hạn. Phần số liệu ở mục 12 để trống: chỉ được điền sau khi chạy thật trên cụm với dữ liệu RetailRocket.

---

## 1. Bài toán

Sàn có 1,4 triệu người dùng và 235.000 sản phẩm. Hệ thống phải phục vụ ba khối gợi ý:

| Khối | Vị trí | Nguồn |
|---|---|---|
| "Dành cho bạn" (10 món) | Trang chủ | `gold.goi_y_nguoi_dung`, trộn theo nhóm A/B |
| "Sản phẩm liên quan" | Trang chi tiết | `gold.san_pham_lien_quan` (đồng xuất hiện, cosine) |
| "Đang thịnh hành" | Trang chủ, trang chi tiết | `gold.thinh_hanh_15p` (streaming, cửa sổ 15 phút) |

Ba thách thức của đề tài và cách dự án xử lý:

| Thách thức | Biểu hiện | Cách xử lý |
|---|---|---|
| Implicit feedback | Chỉ có xem / thêm giỏ / mua, không có điểm đánh giá | ALS implicit (Hu, Koren, Volinsky 2008), trọng số 1/3/5, suy giảm theo thời gian |
| Cold start | User mới hoàn toàn, user ít tương tác, item mới | 3 chiến lược riêng (mục 7), demo được với user tạo tại chỗ |
| Popularity bias | Mô hình chỉ gợi ý hàng bán chạy | Đo coverage, novelty, phân phối theo thập phân vị độ phổ biến; re-ranking theo novelty |

---

## 2. Dữ liệu và tiền xử lý

### 2.1. Nguồn

RetailRocket Recommender System Dataset (Kaggle `retailrocket/ecommerce-dataset`):

- `events.csv`: `timestamp, visitorid, event, itemid, transactionid`
- `item_properties_part1.csv`, `item_properties_part2.csv`: thuộc tính sản phẩm theo thời gian
- `category_tree.csv`: cây danh mục

Thuộc tính sản phẩm đã bị ẩn danh. Dự án dùng `categoryid`, `available` và thuộc tính `790`. Thuộc tính `790` có giá trị dạng số (`n12345.000`) và thường được cộng đồng dùng làm giá; vì đã bị biến đổi nên **giá chỉ có ý nghĩa tương đối** (so sánh đắt/rẻ giữa các món), không phải giá tiền thật. RetailRocket không có điểm đánh giá sao (xem mục 9).

### 2.2. Bộ phát Kafka (mục 3.2)

[producer/phat_lai_su_kien.py](producer/phat_lai_su_kien.py) phát lại `events.csv` theo đúng thứ tự timestamp gốc, nén **1 ngày = 30 giây** (4,5 tháng phát xong trong khoảng 70 phút). Bộ phát sinh thêm khoảng **5% sự kiện của người dùng mới hoàn toàn** (mã `moi_<n>`): người dùng mới duyệt đúng các sản phẩm đang được xem ở thời điểm đó, nên phân phối độ phổ biến giống dữ liệu thật. Mỗi thông điệp có trường `nguon` (`phat_lai` / `nguoi_moi` / `web`).

### 2.3. Bronze → silver ([jobs/transform/bronze_to_silver.py](jobs/transform/bronze_to_silver.py))

| Bước | Quy tắc | Lý do |
|---|---|---|
| Bỏ trùng theo `ma_su_kien` | `dropDuplicates` | File sink và Kafka có thể giao lại cùng một thông điệp |
| Bỏ trùng theo nội dung | `dropDuplicates` trên user–item–loại–thời điểm (mili giây), **trước** khi lọc bot | `ma_su_kien` băm cả offset Kafka; khi bộ phát chạy lại một đoạn đã phát (chuẩn bị demo), mỗi sự kiện gốc có thêm bản sao mang mã mới. Không bỏ thì số sự kiện/giờ bị nhân đôi và user thật 251–500 sự kiện/giờ bị coi nhầm là bot |
| Lọc bot | User có **ít nhất một giờ > 500 sự kiện** bị loại toàn bộ | Đúng ngưỡng đề tài; bot làm méo cả độ phổ biến lẫn ma trận |
| Khử trùng lặp | Bỏ sự kiện cùng user–item–loại cách lần trước **≤ 5 giây** | Bấm đúp, tải lại trang |
| Trọng số | `xem = 1`, `them_gio = 3`, `mua = 5`; `bo_gio` bị loại | `bo_gio` không phải tín hiệu quan tâm |

Xử lý tăng dần theo ngày: chỉ tính lại các phân vùng `ngay` có dữ liệu mới, ghi đè đúng các phân vùng đó bằng `replaceWhere`. Chạy lại bao nhiêu lần cũng cho cùng kết quả (idempotent), nên Airflow retry an toàn.

---

## 3. Ma trận tương tác ([jobs/ml/chuan_bi_ma_tran.py](jobs/ml/chuan_bi_ma_tran.py))

### 3.1. Chia theo thời gian

- `ngay_cuoi` = ngày lớn nhất trong silver (không tính sự kiện do web tạo).
- `ngay_cat` = `ngay_cuoi − 30 ngày`. Train: `ngay < ngay_cat` (khoảng 4 tháng đầu). Test: phần còn lại (tháng cuối).
- Ground truth của user `u` là tập item `u` tương tác trong tháng cuối, **chỉ tính user và item đã xuất hiện trong train**. Người dùng hoặc item chưa từng thấy thuộc về cold start và được đánh giá bằng chiến lược riêng, không trộn vào metric xếp hạng của ALS.

### 3.2. Điểm có suy giảm theo thời gian

$$
\text{diem}(u,i) = \sum_{e \in E(u,i)} w_e \cdot e^{-\lambda \cdot \Delta d_e}, \qquad \lambda = 0{,}01
$$

với `w_e` là trọng số loại sự kiện và `Δd_e` là số ngày từ sự kiện tới mốc. Mốc của train là `ngay_cat`, mốc của test là `ngay_cuoi`, để không có thông tin tương lai rò vào train. Với λ = 0,01, một tương tác cách 70 ngày còn khoảng một nửa trọng số (`e^{-0,7} ≈ 0,50`).

### 3.3. StringIndexer "nối nhãn"

ALS cần chỉ số nguyên. Nếu fit lại StringIndexer mỗi ngày, cùng một `user_idx` có thể trỏ tới người khác giữa hai phiên bản features, và mọi phép so sánh hay tái lập trên phiên bản cũ đều sai. Dự án kế thừa ý tưởng trong mã cũ của nhóm: **nạp danh sách nhãn đã lưu, chỉ nối nhãn mới vào cuối** (`StringIndexerModel.from_labels`). Chỉ số của nhãn cũ không bao giờ đổi. Indexer lưu ở `/lake/models/user_indexer` và `/lake/models/item_indexer`.

### 3.4. Lọc user (rủi ro OOM, mục 3.10)

Chỉ giữ user có **≥ 3 tương tác trong train** (`--min-tuong-tac 3`). Đề tài ước tính còn khoảng 300 nghìn user. Ngoài ra `--so-khoi` đặt `numUserBlocks/numItemBlocks` của ALS.

### 3.5. Cột `tap`

Bảng features có thêm cột `tap` (`train` / `test`) ngoài 3 cột của đặc tả. Một phiên bản Delta phải chứa đủ dữ liệu để chạy lại **cả huấn luyện lẫn đánh giá**; nếu ground truth nằm ở bảng khác thì `VERSION AS OF` trên features không đủ để tái lập metric.

---

## 4. Mô hình ALS cho implicit feedback

### 4.1. Cơ sở lý thuyết

Với implicit feedback, `r_ui` (ở đây là `diem`) không phải mức độ thích mà là **mức độ tin cậy** rằng user quan tâm. Hu, Koren và Volinsky (2008) tách thành:

- sở thích nhị phân: `p_ui = 1` nếu `r_ui > 0`, ngược lại `0`
- độ tin cậy: `c_ui = 1 + α · r_ui`

và tối thiểu hóa trên **toàn bộ** ma trận (kể cả ô trống, với `c_ui = 1`):

$$
\min_{X,Y} \sum_{u,i} c_{ui}\,(p_{ui} - x_u^\top y_i)^2 + \lambda_{reg}\Big(\sum_u n_u \lVert x_u\rVert^2 + \sum_i n_i \lVert y_i\rVert^2\Big)
$$

Spark MLlib giải bằng bình phương tối thiểu luân phiên (cố định Y giải X rồi ngược lại); mỗi bước có nghiệm đóng và song song hóa theo khối user/item. Hệ số điều chuẩn được nhân với số tương tác `n_u`, `n_i` (kiểu ALS-WR) nên `regParam` ít nhạy với quy mô dữ liệu. Điểm dự đoán `x_uᵀ y_i` dùng để **xếp hạng**, không phải để diễn giải như xác suất.

Ý nghĩa siêu tham số:

| Tham số | Ý nghĩa | Lưới bắt buộc |
|---|---|---|
| `rank` | Số chiều nhân tố ẩn | {10, 30, 50} |
| `regParam` | Điều chuẩn, chống quá khớp | {0,01; 0,1; 1,0} |
| `alpha` | Tương tác mạnh được tin cậy hơn bao nhiêu so với ô trống | {1, 10, 40} |

### 4.2. Cấu hình cố định ([jobs/common/als_lib.py](jobs/common/als_lib.py))

`implicitPrefs=True`, `coldStartStrategy="drop"` khi đánh giá, `maxIter=10` (mặc định, ghi vào lịch sử), `seed=42`. Cùng dữ liệu, cùng siêu tham số, cùng `maxIter`, cùng hạt giống và cùng số khối thì Spark cho cùng nhân tố; kiểm thử `test_als_vuot_baseline_va_tai_lap_duoc` khẳng định hai lần huấn luyện cho metric giống hệt.

### 4.3. Tuning ([jobs/ml/huan_luyen_als.py](jobs/ml/huan_luyen_als.py))

- `--luoi day_du`: đủ **27 tổ hợp** (3 × 3 × 3), dùng cho báo cáo.
- `--luoi toi_thieu`: **9 tổ hợp** (`alpha = 10` cố định), dùng cho DAG chạy hằng đêm. Vẫn đạt yêu cầu "tối thiểu 9 tổ hợp".
- Mỗi tổ hợp ghi một dòng vào `gold.ket_qua_tuning` kèm `ma_chay`, `phien_ban_features`, 4 metric, coverage, novelty và thời gian chạy.
- Chọn mô hình theo **NDCG@10 cao nhất**; hòa thì ưu tiên `rank` nhỏ, rồi `regParam` lớn (mô hình đơn giản hơn).

Tuning chọn siêu tham số trên chính tập test tháng cuối (không có tập validation riêng). Đây là cách làm phổ biến trong bài tập lớn nhưng làm metric báo cáo **lạc quan hơn một chút**; báo cáo nên nêu rõ. Muốn khắt khe hơn, chạy `chuan_bi_ma_tran.py --so-ngay-test 15` để tách hai nửa tháng cuối.

---

## 5. Đánh giá

### 5.1. Metric xếp hạng (K = 10)

Ký hiệu `R_K(u)` là K món gợi ý cho `u`, `T(u)` là ground truth, `rel_j = 1` nếu món ở vị trí `j` thuộc `T(u)`.

| Metric | Công thức cho một user | Ý nghĩa |
|---|---|---|
| Precision@10 | `|R_K ∩ T| / K` | Bao nhiêu phần danh sách là đúng |
| Recall@10 | `|R_K ∩ T| / |T|` | Tìm lại được bao nhiêu phần sở thích thật |
| NDCG@10 | `DCG / IDCG`, `DCG = Σ_j rel_j / log2(j+1)`, `IDCG` tính với min(|T|, K) món đúng ở đầu | Đúng **và** đứng cao mới được nhiều điểm |
| MAP@10 | `(1 / min(|T|, K)) · Σ_j P@j · rel_j` | Độ chính xác trung bình theo từng vị trí đúng |

Giá trị báo cáo là trung bình trên mọi user có ground truth; user không nhận được gợi ý tính là 0. Định nghĩa khớp `pyspark.mllib.evaluation.RankingMetrics` (Spark ≥ 3.0); kiểm thử `test_khop_ranking_metrics_cua_spark` đối chiếu trực tiếp.

### 5.2. Hai metric ngoài độ chính xác

- **Coverage** = số item khác nhau xuất hiện trong ít nhất một danh sách top-10 / số item trong catalog. Dự án tính hai mẫu số:
  - `coverage`: catalog = các item có trong train, tức tập item mà **bất kỳ** phương pháp lọc cộng tác nào cũng có thể gợi ý. **Ngưỡng 10% áp dụng trên chỉ số này.**
  - `coverage_toan_san`: catalog = toàn bộ `silver.san_pham` (khoảng 235.000). Chỉ số này luôn nhỏ hơn và được báo cáo song song; nếu hội đồng yêu cầu dùng mẫu số toàn sàn, xem cột này.
- **Novelty** = trung bình trên mọi suất gợi ý của `−log2(n_i / N)`, với `n_i` là số user đã tương tác với item `i` trong train và `N` là số user trong train. Món càng ít người biết, novelty càng cao. Baseline bán chạy có novelty thấp nhất.
- **Phân phối độ phổ biến**: chia item thành 10 nhóm theo số người tương tác (nhóm 1 là 10% phổ biến nhất) rồi đếm tỷ lệ suất gợi ý rơi vào từng nhóm. Không có popularity bias thì mỗi nhóm khoảng 10%. Màn hình "Chất lượng mô hình" vẽ biểu đồ này.

### 5.3. Baseline bắt buộc

1. **Bán chạy toàn sàn**: 10 item có tổng điểm tương tác lớn nhất trong train, giống nhau cho mọi user.
2. **Bán chạy trong danh mục user hay xem**: danh mục có tổng điểm lớn nhất của user trong train, lấy 10 item bán chạy nhất danh mục đó; thiếu thì bù bằng bán chạy toàn sàn. Danh mục lấy từ đúng phiên bản `silver.san_pham` ghi trong features để baseline cũng tái lập được.

### 5.4. Ngưỡng đạt và quyết định triển khai

Hai khái niệm khác nhau, cùng ghi vào `gold.lich_su_huan_luyen`:

- `dat_nguong` (yêu cầu của đề tài): `(NDCG_ALS − NDCG_baseline1) / NDCG_baseline1 ≥ 30%` **và** `coverage ≥ 10%`.
- `cai_thien` (nhánh của DAG 1): mô hình mới được triển khai nếu chưa có mô hình nào đang phục vụ, hoặc NDCG@10 mới không thấp hơn mô hình đang phục vụ quá 1% (`--dung-sai 0.01`). Dung sai tránh việc gợi ý bị "đóng băng" chỉ vì NDCG dao động rất nhỏ giữa hai đêm. Nhánh "không" giữ mô hình cũ và gửi cảnh báo.

### 5.5. Giảm popularity bias: re-ranking theo novelty

Lấy dư top-50 ứng viên ALS cho mỗi user, chuẩn hóa min–max điểm ALS và novelty trong danh sách của user, xếp lại theo

$$
\text{diem\_moi} = \widetilde{\text{diem\_ALS}} + \beta \cdot \widetilde{\text{novelty}}, \qquad \beta \in \{0{,}1;\ 0{,}3\}
$$

rồi cắt còn 10. Kết quả từng β (4 metric, coverage, novelty, phân phối độ phổ biến) lưu trong `chi_tiet_json` và hiện trên web, để thấy rõ đánh đổi giữa NDCG và coverage. Nếu coverage của ALS dưới 10%, đề tài coi đây là **kết quả hợp lệ khi có phân tích**: báo cáo cần trình bày nguyên nhân (phân phối Zipf của RetailRocket, đa số user chỉ có vài lượt xem) và số liệu của biện pháp re-ranking.

---

## 6. Mô hình phục vụ ([jobs/ml/sinh_goi_y_toan_bo.py](jobs/ml/sinh_goi_y_toan_bo.py))

Mô hình dùng để **đánh giá** chỉ học 4 tháng đầu. Mô hình dùng để **phục vụ** phải biết cả tháng cuối, nên được fit lại với đúng siêu tham số đã chọn trên train + test. Để gộp hai tập mà không đọc lại silver, điểm train (đang suy giảm theo `ngay_cat`) được nhân thêm `e^{-λ(ngay_cuoi − ngay_cat)}`, vì

$$
e^{-\lambda(c-d)} \cdot e^{-\lambda(e-c)} = e^{-\lambda(e-d)}.
$$

Chỉ sinh gợi ý bằng `recommendForUserSubset` cho user **hoạt động trong 30 ngày cuối và có ≥ 5 tương tác** (rủi ro "recommendForAllUsers quá chậm", mục 3.10). User còn lại đi theo chiến lược cold start.

**Xếp lại theo độ mới** (từ 10/11/2026, theo kết quả mục 12.2): lấy dư `SO_UNG_VIEN_XEP_LAI` = 200 ứng viên ALS mỗi user, chấm lại bằng

$$
\text{điểm mới} = \widehat{r}_{ui} + \beta \cdot \widehat{\text{nov}}_i, \qquad \beta = 1{,}0,
$$

trong đó $\widehat{r}_{ui}$ là điểm ALS và $\widehat{\text{nov}}_i = -\log_2(\text{tỷ lệ user đã tương tác với } i)$, cả hai chuẩn hóa min–max trong 200 ứng viên của chính user đó. Giữ 20 món đầu (40 với mô hình v2). Novelty tính trên đúng ma trận dùng để fit mô hình phục vụ. `diem_du_doan` vẫn là điểm ALS gốc; thứ tự mới nằm ở `xep_hang`, và bước trộn sắp ứng viên ALS theo `xep_hang`. `danh_gia.py` đo riêng đúng cấu hình này (`chi_tiet_json.phuc_vu`) bên cạnh ALS thuần; quyết định triển khai vẫn dựa trên ALS thuần để so được với các phiên bản trước. Đặt `BETA_XEP_LAI = 0` trong `cau_hinh.py` để quay về ALS thuần.

---

## 7. Cold start (chấm điểm riêng)

| Trường hợp | Chiến lược | Nơi hiện thực |
|---|---|---|
| User mới hoàn toàn | Top thịnh hành 15 phút gần nhất (`gold.thinh_hanh_15p`) | `web/dich_vu/goi_y.py`, `common/tron.danh_sach_nguoi_moi` |
| User có < 5 tương tác | Bán chạy trong tối đa 3 danh mục đã xem (xen kẽ giữa các danh mục), **trộn 50/50** với thịnh hành theo mẫu `d t d t d t d t d t` | `ml/tron_goi_y.tron_nguoi_it_tuong_tac`; web dùng bộ nhớ phiên cho user vừa tạo |
| Item mới (không có trong ALS) | Chọn theo danh mục chiếm nhiều nhất trong top-20 ALS của user, chèn vào **20% vị trí cuối** mỗi khối 10 (vị trí 9–10) | `ml/tron_goi_y.item_moi_theo_danh_muc`, `common/tron.MAU_KHOI` |

**Demo tại chỗ**: nút "Tạo người dùng mới" trên trang chủ sinh mã `moi_<8 ký tự>` → web hiển thị toàn thịnh hành (chiến lược `nguoi_moi`). Bấm Xem/Thêm giỏ vài sản phẩm → sự kiện vào Kafka, đồng thời web nhớ tương tác trong phiên → tải lại trang chủ thấy chiến lược `it_tuong_tac` với các món cùng danh mục vừa xem. Sự kiện web chỉ vào silver/gold ở lần chạy batch tiếp theo, nên bộ nhớ phiên là cách hợp lý để demo ngay (mất khi khởi động lại web, đúng bản chất của phiên).

---

## 8. Trộn gợi ý và thí nghiệm A/B

### 8.1. Mẫu khối ([jobs/common/tron.py](jobs/common/tron.py))

Danh sách cuối có 20 vị trí, gồm 2 khối 10. Web hiển thị khối đầu, nên tỷ lệ từng nguồn được bảo đảm ngay trên phần người dùng nhìn thấy:

| Nhóm | Mẫu 10 ô (a = ALS, t = thịnh hành, m = item mới) | Tỷ lệ trên 10 ô hiển thị |
|---|---|---|
| A — ALS thuần | `a a a a a a a a m m` | 80% ALS, 20% item mới |
| B — ALS trộn thịnh hành | `a a t a a t a t m m` | 50% ALS, **30% thịnh hành**, 20% item mới |

Item mới nằm ở cùng vị trí trong cả hai nhóm, nên khác biệt giữa A và B chỉ còn là phần trộn thịnh hành. Khi một nguồn cạn, ô đó lấy bù theo thứ tự cố định và không bao giờ lặp sản phẩm. Lúc hiển thị, web thay các ô thịnh hành bằng dữ liệu mới nhất của luồng, giữ nguyên vị trí.

### 8.2. Phân nhóm

`nhom = "A"` nếu `int(md5(ma_nguoi_dung)[:8], 16)` chẵn, ngược lại `"B"`. Hàm băm cố định nên cùng một user luôn vào cùng nhóm, ở cả job batch, web và mô phỏng.

### 8.3. Kiểm định ([jobs/common/thong_ke.py](jobs/common/thong_ke.py), [jobs/common/ab.py](jobs/common/ab.py))

- Đơn vị: một impression của khối "Dành cho bạn" với chiến lược `als` (lượt hiển thị cold start không đưa vào so sánh vì không thuộc thí nghiệm).
- CTR = `so_click / so_hien_thi`; tỷ lệ chuyển đổi = `so_mua / so_hien_thi`.
- Kiểm định z hai tỷ lệ, hai phía, phương sai gộp:

$$
z = \frac{\hat p_B - \hat p_A}{\sqrt{\hat p (1-\hat p)\left(\frac{1}{n_A} + \frac{1}{n_B}\right)}}, \quad \hat p = \frac{x_A + x_B}{n_A + n_B}, \quad p\text{-value} = \operatorname{erfc}\!\left(\frac{|z|}{\sqrt 2}\right)
$$

- Kết luận ở mức ý nghĩa 5%. Bảng điều khiển hiển thị thêm cỡ mẫu cần thiết mỗi nhóm để phát hiện mức tăng tương đối cho trước với α = 5%, power = 80%: `n = (z_{α/2} + z_β)² (p₁(1−p₁) + p₂(1−p₂)) / (p₂ − p₁)²`.
- Giới hạn cần nêu trong báo cáo: các impression của cùng một user không độc lập hoàn toàn, nên z-test theo impression có thể cho p-value nhỏ hơn thực tế. Cách chặt chẽ hơn là tổng hợp theo user hoặc dùng bootstrap theo user; dự án giữ z-test theo đúng yêu cầu đề tài.

### 8.4. Traffic mô phỏng (mục 3.10)

Không đủ người thật để A/B có ý nghĩa thống kê, nên [jobs/transform/mo_phong_hien_thi.py](jobs/transform/mo_phong_hien_thi.py) sinh impression theo **mô hình phụ thuộc vị trí**:

$$
P(\text{click}) = \underbrace{v^{-\eta}}_{\text{nhìn thấy vị trí } v} \times \underbrace{P(\text{liên quan} \mid u, i)}_{0{,}40 \text{ nếu } i \in T(u),\ 0{,}03 \text{ nếu không}}, \qquad \eta = 0{,}7
$$

và mua với xác suất 0,12 sau khi click. Mặc định không cộng điểm cho món thịnh hành (`--tang-thinh-hanh 0`), tức **không giả định trước nhóm B tốt hơn**. Mọi dòng có `nguon_traffic = "mo_phong"`; bảng `gold.ket_qua_ab` có cột `ty_le_mo_phong` và web hiển thị rõ tỷ lệ này.

[producer/nguoi_dung_ao.py](producer/nguoi_dung_ao.py) là cách thứ hai: người dùng ảo đi qua web thật (web → Kafka → `ghi_hien_thi` → CDF → `tinh_ket_qua_ab`), xác suất click chỉ phụ thuộc vị trí. Kết quả kỳ vọng là **không khác biệt**: một phép thử A/A để kiểm tra kiểm định không báo khác biệt giả.

---

## 9. Mô hình v2: ràng buộc lọc theo tầm giá (schema evolution tuần 5)

[jobs/transform/tien_hoa_luoc_do.py](jobs/transform/tien_hoa_luoc_do.py) thêm `gia_san_pham` và `xep_hang_sao` vào `silver.tuong_tac` bằng `ALTER TABLE ADD COLUMNS` (không ghi lại bảng; phiên bản cũ vẫn đọc được với lược đồ 7 cột) rồi điền giá trị bằng `MERGE`.

- `gia_san_pham`: từ thuộc tính `790` (giá tương đối, xem mục 2.1).
- `xep_hang_sao`: RetailRocket **không có** điểm sao. Đây là **đại lượng thay thế**: tỷ lệ mua/xem của sản phẩm, đổi về thang 1–5 theo thứ hạng phần trăm. Báo cáo phải nêu rõ.

Khi phiên bản silver dùng để dựng features đã có `gia_san_pham`, bước `danh_gia` bật **mô hình v2** và đặt `phien_ban_mo_hinh = als_v2_<ma_chay>`. Đây là điểm quyết định duy nhất; các bước sau đọc lại tên này.

1. **Tầm giá của user**: `[0,5 × P10 ; 1,5 × P90]` giá các món user đã tương tác. Dùng phân vị thay cho min/max để một lần lỡ xem món rất đắt không làm tầm giá rộng vô nghĩa. User có dưới 3 tương tác có giá thì không lọc.
2. **Lọc**: `sinh_goi_y_toan_bo` lấy dư gấp đôi ứng viên (top-40). `tron_goi_y` loại ứng viên ALS có giá ngoài tầm, giữ nguyên thứ hạng, giữ món chưa rõ giá. Nếu sau khi lọc còn ít hơn 10 món, các món ngoài tầm được nối xuống cuối để khối hiển thị không thiếu ô (ràng buộc khi đó thành ưu tiên).
3. **Đánh giá**: cùng mô hình ALS, lấy dư top-20 rồi lọc, cắt còn 10, tính đủ 4 metric + coverage + novelty. Kết quả nằm ở khóa `loc_tam_gia_v2` của `chi_tiet_json` và một dòng riêng trên màn hình "Chất lượng mô hình", để so v1 và v2 trên cùng phiên bản dữ liệu. Job tái lập so thêm NDCG@10 của v2.

Logic lọc là một hàm thuần Python (`common.tron.loc_theo_tam_gia`) dùng chung cho đánh giá và phục vụ, có kiểm thử riêng.

---

## 10. Các khối không cá nhân hóa

### 10.1. Đang thịnh hành ([jobs/streaming/tinh_thinh_hanh.py](jobs/streaming/tinh_thinh_hanh.py))

Cửa sổ trượt 15 phút, bước 1 phút, watermark 2 phút, `outputMode("update")` + `MERGE` theo `(cua_so_bat_dau, cua_so_ket_thuc, ma_san_pham)`:

$$
\text{diem\_thinh\_hanh} = \frac{\text{luot\_xem} + 3 \times \text{them\_gio}}{\text{trung\_binh\_24h} + 10}
$$

Mẫu số chuẩn hóa theo mức bình thường của chính sản phẩm (trung bình theo cửa sổ 15 phút trong 24 giờ, lấy từ `gold.thong_ke_san_pham`), nên món ít phổ biến đang tăng đột biến vẫn có cơ hội xuất hiện; hằng số 10 tránh chia cho số rất nhỏ. Cửa sổ gom theo `kafka_ts` (thời điểm broker nhận) vì bộ phát giữ timestamp năm 2015 còn web đẩy sự kiện năm 2026: gom theo thời điểm gốc thì sự kiện web sẽ đẩy watermark lên 2026 và mọi sự kiện phát lại bị coi là trễ.

### 10.2. Sản phẩm liên quan ([jobs/ml/san_pham_lien_quan.py](jobs/ml/san_pham_lien_quan.py))

Lọc cộng tác item–item theo đồng xuất hiện: hai sản phẩm liên quan nếu cùng được một người tương tác trong cùng một ngày; độ liên quan là cosine trên số người, `sim(i, j) = n(i, j) / √(n(i) · n(j))`. Cosine (thay vì đếm thô) để món siêu phổ biến không chen vào mọi danh sách. Item thiếu danh sách thì web lấy bán chạy cùng danh mục.

---

## 11. Tái lập

Mỗi lần huấn luyện ghi vào `gold.lich_su_huan_luyen` đủ thông tin để chạy lại y hệt: `phien_ban_features`, `phien_ban_silver`, `ngay_cat`, siêu tham số, `max_iter`, `hat_giong`; `userMetadata` của commit features ghi thêm `phien_ban_san_pham`, `min_tuong_tac`. Mỗi dòng `gold.ket_qua_ab` ghi `phien_ban_delta_du_lieu` của impression log đã đọc.

- Tái lập huấn luyện (DAG 5, [jobs/kiem_tra/tai_lap_huan_luyen.py](jobs/kiem_tra/tai_lap_huan_luyen.py)): đọc features bằng `VERSION AS OF`, gọi lại đúng các hàm tuning và đánh giá với cùng tham số, so với lịch sử. Coi là trùng khớp khi độ lệch lớn nhất < 10⁻⁶ (ALS là phép tính số thực phân tán; chênh lệch nếu có chỉ ở mức làm tròn).
- Tái lập A/B (màn hình "Tái lập thí nghiệm", [jobs/kiem_tra/tai_lap_ab.py](jobs/kiem_tra/tai_lap_ab.py)): đọc impression log đúng phiên bản đã ghi, chạy lại đúng hàm `tong_hop_ab`, so từng chỉ số; phép tính là số nguyên và công thức tất định nên yêu cầu lệch bằng 0.
- Lịch sử Delta giữ 30 ngày (`deletedFileRetentionDuration`) và nhật ký 60 ngày, nên VACUUM hằng đêm không xóa mất phiên bản cần tái lập.

---

## 12. Kết quả trên cụm (dữ liệu thật RetailRocket, 10/11/2026)

Số liệu xuất bằng `jobs/kiem_tra/xuat_ket_qua.py` từ `gold.lich_su_huan_luyen`, `gold.ket_qua_tuning`, `gold.ket_qua_ab`, `gold.do_hieu_nang_zorder`. Diễn biến lần chạy: NHAT_KY.md, ngày 10/11/2026.

- `ma_chay` = `r20261110T075240`, mô hình `als_r20261110T075240`, `phien_ban_features` = 0, `phien_ban_silver` = 0. Toàn bộ pipeline 8 bước: 2 giờ 8 phút trên 5 worker (34 lõi).
- Dữ liệu: 2.894.238 sự kiện (2.756.101 gốc + 138.137 của 48.376 người dùng mới mô phỏng), silver 2.882.479 dòng / 139 ngày. Ngày cắt 2015-08-19, test 30 ngày đến 2015-09-18.
- Ma trận: 179.999 user (≥ 3 tương tác), 746.350 cặp train (độ dày ≈ 1,8 × 10⁻⁵), 32.804 cặp test. Đánh giá trên 9.467 user test; 126.230 item có trong train (mẫu số của Coverage); toàn sàn 417.053 item.
- Tuning 27 tổ hợp: 78,4 phút (khoảng 75 giây mỗi tổ hợp ở rank 10, 160 giây ở rank 30, 290 giây ở rank 50). Tốt nhất **rank = 50, regParam = 1,0, alpha = 40**, maxIter = 10, seed = 42.

| Phương pháp | P@10 | R@10 | NDCG@10 | MAP@10 | Coverage | Coverage toàn sàn | Novelty | Số item được gợi ý |
|---|---|---|---|---|---|---|---|---|
| Baseline 1: bán chạy toàn sàn | 0,0017 | 0,0066 | 0,0051 | 0,0036 | 0,01% | 0,002% | 8,91 | 10 |
| Baseline 2: bán chạy theo danh mục | 0,0122 | 0,0706 | 0,0475 | 0,0352 | 5,28% | 1,60% | 11,88 | 6.660 |
| ALS (rank = 50, reg = 1,0, alpha = 40) | 0,0161 | 0,0921 | **0,0680** | 0,0531 | 3,44% | 1,04% | 10,86 | 4.346 |
| ALS + re-ranking β = 0,1 | 0,0166 | 0,0952 | 0,0702 | 0,0549 | 3,65% | 1,10% | 10,93 | 4.607 |
| ALS + re-ranking β = 0,3 | 0,0173 | 0,1001 | 0,0746 | 0,0589 | 4,14% | 1,25% | 11,09 | 5.221 |
| **Phục vụ: ALS + xếp lại β = 1,0, 200 ứng viên** (mục 6) | **0,0182** | **0,1089** | **0,0865** | **0,0716** | **6,47%** | 1,96% | 12,17 | 8.162 |
| Mô hình v2: ALS + lọc tầm giá (lần `r20261110T230924`, features v3, xem dưới) | 0,0165 | 0,0943 | 0,0694 | 0,0541 | 3,72% | 1,13% | 10,91 | 4.699 |

- Uplift NDCG@10 so với baseline 1: **+1.237%** (ngưỡng ≥ 30%: đạt); so với baseline 2 (mạnh hơn nhiều): +43%. Coverage 3,44% < 10% → `dat_nguong` = false. Lần huấn luyện đầu nên `cai_thien` = true, mô hình vẫn được triển khai; `dat_nguong` chỉ ghi lại để báo cáo.
- **Mô hình v2 (11/11/2026)**: lượt DAG 1 theo lịch 02:00 được sensor đánh thức lúc 05:22 (click đo vòng khép kín vào bronze), chạy hết pipeline và triển khai v2 lúc 06:07; lượt chạy tay xếp hàng từ 04:05 chạy ngay sau đó 06:09–06:52 (bỏ qua sensor), `ma_chay` = `r20261110T230924`, features v3 từ silver v10. Lưới 10 tổ hợp (9 tổ hợp tối thiểu + cấu hình đang phục vụ, lỗi 23) trong 20,7 phút; tốt nhất vẫn rank 50, reg 1,0, alpha 40 với NDCG@10 0,0680 = mô hình đang phục vụ → `cai_thien` = true, triển khai `als_v2_r20261110T230924`. ALS thuần cho đúng từng số của lần `r20261110T075240` (features v0): sự kiện phát lại bị khử trùng lặp, sự kiện web bị loại khỏi ma trận, seed cố định. Lọc tầm giá: NDCG@10 +2,1%, coverage 3,44% → 3,72%, N1 99,1% (gần như không đổi); ở alpha 10 đêm trước cũng +2,3%. Bảng phục vụ sau khi trộn lại (lỗi 29, gold phiên bản 14): 265.043 user, 5.300.616 dòng, 16.001 sản phẩm = 12,7% item của train (thịnh hành 19, danh mục 9.821, ALS 9.237 trên 122.293 ô).
- Cấu hình phục vụ (ALS + xếp lại, mục 6) đo trên cùng tập test: NDCG@10 0,0865 (+1.601% so với baseline 1, +27% so với ALS thuần), coverage 6,47%, vẫn dưới 10%. Số đo khớp đúng lần quét ở mục 12.2, tức job đánh giá và job quét cho cùng kết quả.

### 12.1 Nhận xét về tuning

- NDCG@10 tăng đơn điệu theo **rank** (tốt nhất mỗi rank: 0,024 → 0,050 → 0,068) và theo **alpha** (với cùng rank và regParam, alpha 40 luôn tốt nhất). Với implicit feedback, alpha lớn làm độ tin cậy của các cặp có tương tác (nhất là mua, trọng số 5) vượt hẳn các ô bằng 0, nên mô hình học được tín hiệu từ dữ liệu thưa tốt hơn.
- regParam ảnh hưởng nhỏ khi alpha lớn (rank 50, alpha 40: 0,0668 / 0,0662 / 0,0680), nhưng làm hỏng mô hình khi alpha = 1 (regParam = 1 cho NDCG thấp nhất ở mọi rank, novelty cao bất thường 12,2–12,6: mô hình bị kéo về gần 0 và gợi ý gần như ngẫu nhiên các item hiếm).
- Tổ hợp tốt nhất nằm ở **biên lưới** về rank và alpha: điểm tối ưu thật có thể nằm ngoài lưới. Hướng tiếp theo: thêm rank 100, alpha 80. Chi phí tăng gần tuyến tính theo rank.

### 12.2 Coverage thấp: popularity bias, không phải giới hạn cấu trúc

- Trần coverage về cấu trúc: 10 gợi ý × 9.467 user test / 126.230 item = **75%**. ALS chỉ dùng 4.346 item, nên coverage thấp không do thiếu ô gợi ý.
- Phân phối độ phổ biến (tỷ lệ suất gợi ý rơi vào nhóm 10% item phổ biến nhất, N1): ALS **99,2%**, re-ranking β = 0,3: 98,3%, baseline danh mục 86,5%, baseline toàn sàn 100%. Đây là popularity bias điển hình của matrix factorization trên dữ liệu thưa: item ít tương tác có vector yếu nên hiếm khi lọt top-10.
- Re-ranking novelty (mục 3.10 của đề) tăng **cả coverage lẫn độ chính xác**. Quét thêm β và cỡ tập ứng viên trên đúng mô hình và features của lần chạy (`jobs/kiem_tra/thu_xep_lai.py`):

| Ứng viên | β | P@10 | R@10 | NDCG@10 | MAP@10 | Coverage | Số item | Suất gợi ý vào N1 |
|---|---|---|---|---|---|---|---|---|
| 50 | 0 (ALS gốc) | 0,0161 | 0,0921 | 0,0680 | 0,0531 | 3,44% | 4.346 | 99,2% |
| 50 | 0,3 | 0,0173 | 0,1001 | 0,0746 | 0,0589 | 4,14% | 5.221 | 98,3% |
| 50 | 0,5 | 0,0179 | 0,1037 | 0,0797 | 0,0642 | 4,65% | 5.875 | 97,4% |
| 50 | 1,0 | 0,0180 | 0,1067 | 0,0854 | 0,0709 | 5,67% | 7.152 | 94,7% |
| 50 | 2,0 | 0,0148 | 0,0892 | 0,0683 | 0,0557 | 6,30% | 7.950 | 92,7% |
| 200 | 0,5 | 0,0178 | 0,1037 | 0,0789 | 0,0633 | 4,74% | 5.977 | 96,0% |
| **200** | **1,0** | **0,0182** | **0,1089** | **0,0865** | **0,0716** | **6,47%** | **8.162** | **80,8%** |
| 200 | 2,0 | 0,0118 | 0,0753 | 0,0518 | 0,0402 | 7,21% | 9.099 | 47,5% |

- **200 ứng viên, β = 1,0 trội hơn ALS gốc trên mọi tiêu chí**: NDCG@10 +27% (0,0680 → 0,0865), coverage +88% (3,44% → 6,47%), suất gợi ý vào 10% item phổ biến nhất giảm từ 99,2% xuống 80,8%. Tập test của RetailRocket chứa nhiều item ít phổ biến, nên đưa item "lạ" hơn lên vừa đa dạng hơn vừa trúng hơn.
- Quá ngưỡng đó (β = 2,0), danh sách đa dạng hẳn (N1 còn 47,5%, coverage 7,21%) nhưng NDCG giảm 24% so với ALS gốc; vẫn cao hơn baseline 1 và 2. Đây là đường đánh đổi chính xác – đa dạng điển hình.
- Tập ứng viên rộng hơn (200 so với 50) chỉ có tác dụng khi β đủ lớn (từ 1,0): với β nhỏ, các item ngoài top-50 không vượt lên được.
- **Không cấu hình nào đạt Coverage 10%** (cao nhất 7,21%). Lý do gốc: trong 126.230 item của train, phần lớn chỉ có 1–2 tương tác nên vector ẩn gần 0 và không lọt nổi top-200 của bất kỳ user nào. Muốn vượt 10% cần nguồn ứng viên khác ngoài ALS: ứng viên theo danh mục (baseline danh mục một mình đã đạt 5,28%), item mới (đã có ở bước trộn: 20% vị trí cuối), hoặc mở rộng lưới (coverage tăng theo rank và alpha: rank 100, alpha 80).
- Lưu ý phương pháp: β và cỡ tập ứng viên được chọn trên chính tập test, nên số liệu của cấu hình tốt nhất là lạc quan. Quy trình đúng là tách thêm một tập validation theo thời gian (ví dụ 30 ngày cuối của giai đoạn train) để chọn β, rồi báo cáo trên test.
- Đã áp dụng cấu hình 200 ứng viên, β = 1,0 cho danh sách ALS phục vụ (mục 6, `BETA_XEP_LAI`, `SO_UNG_VIEN_XEP_LAI` trong `cau_hinh.py`); triển khai lại cho `r20261110T075240` bằng `scripts/trien_khai_lai.sh` (không tuning lại, 15 phút). Phân phối độ phổ biến của cấu hình phục vụ: N1 80,8%, N2 11,2%, N3 4,3%, N4 2,0%, N5–N10 1,9%.

**Độ phủ của danh sách thật sự phục vụ.** Coverage ở trên chỉ tính top-10 ALS của 9.467 user test. Đo trên toàn bộ `gold.goi_y_nguoi_dung` sau khi triển khai lại (mọi user, cả 20 vị trí, mọi nguồn):

| Nguồn | Số ô | Sản phẩm khác nhau |
|---|---|---|
| Thịnh hành (dự phòng: xem nhiều 24 giờ) | 2.599.964 | 19 |
| Phổ biến theo danh mục (user ít tương tác) | 2.577.372 | 9.927 |
| ALS đã xếp lại (8.592 user hoạt động) | 123.524 | 8.612 |
| **Tổng** (265.043 user, 5.300.860 dòng) | | **15.582** |

15.582 sản phẩm bằng **12,3%** số item có trong train (3,7% toàn sàn). Con số này vượt 10% nhưng là một định nghĩa khác: độ phủ của cả hệ thống, có cả các khối không cá nhân hóa, chứ không phải Coverage của mô hình mà ngưỡng của đề nhắm tới. Báo cáo nên nêu cả hai và nói rõ ngưỡng mô hình chưa đạt. Khối thịnh hành chiếm gần một nửa số ô mà chỉ dùng 19 sản phẩm, vì 256.451 user ít tương tác nhận 50% thịnh hành theo đặc tả và luồng thịnh hành 15 phút chưa bật (đang dùng nguồn dự phòng). Khi bật streaming, khối này sẽ đổi theo cửa sổ 15 phút.

### 12.3 A/B (traffic mô phỏng, `tang_thinh_hanh` = 0)

Số liệu A/B dưới đây đo trên danh sách **trước** khi bật xếp lại (phiên bản 1 của `gold.goi_y_nguoi_dung`).

- Lần mô phỏng mặc định (5.000 user lấy theo thứ tự băm) chỉ có khoảng 3% user có gợi ý ALS, mà A/B chỉ tính impression của chiến lược ALS: mỗi nhóm khoảng 3.000 impression trong 7 ngày, gộp lại p = 0,77, thiếu mẫu khoảng 25 lần.
- Mô phỏng bổ sung cho toàn bộ 8.592 user có gợi ý ALS (`mo_phong_hien_thi.py --chi-als --hat-giong 7`): 343.680 impression, 7.008 click. Kết quả gộp 7 ngày (CDF đến phiên bản dữ liệu 2):

| Nhóm | Impression | Click | CTR | Mua | Chuyển đổi |
|---|---|---|---|---|---|
| A: ALS thuần | 175.960 | 3.685 | 2,094% | 412 | 0,234% |
| B: ALS + 30% thịnh hành | 174.200 | 3.446 | 1,978% | 417 | 0,239% |

- CTR: z = −2,43, **p = 0,015**: B thấp hơn A khoảng 5,5% tương đối, khác biệt có ý nghĩa ở mức 5%. Cỡ mẫu cần cho mức tăng 10% là 76.972 impression mỗi nhóm, mẫu hiện tại đủ. Tỷ lệ chuyển đổi: p = 0,75, chưa có khác biệt.
- Kiểm định theo từng ngày đều không có ý nghĩa (p từ 0,07 đến 0,85): mỗi ngày chỉ khoảng 25.000 impression mỗi nhóm, trong khi phát hiện mức chênh 5,5% cần khoảng 250.000. Kiểm định chính là kiểm định gộp cả giai đoạn đã định trước; biểu đồ theo ngày chỉ để theo dõi, không dùng để kết luận sớm (tránh "nhìn trộm" và so sánh bội).
- Diễn giải: trong mô phỏng, xác suất click phụ thuộc vào việc item có thuộc các item user thật sự tương tác ở tháng test hay không, và không cộng điểm cho item thịnh hành. Thay 30% ô ALS cá nhân hóa bằng item thịnh hành chung (ở lần chạy này là nguồn dự phòng "xem nhiều 24 giờ", vì luồng thịnh hành 15 phút chưa bật) làm giảm số ô trúng, nên B thua A. Kết luận chỉ đúng trong giả định của mô phỏng (100% traffic là mô phỏng, có nhãn trên web), không phải bằng chứng về hành vi người dùng thật. Có thể làm thêm phân tích độ nhạy: tăng `--tang-thinh-hanh` để tìm mức hấp dẫn của item thịnh hành mà ở đó B hòa A.

### 12.4 OPTIMIZE / ZORDER (`gold.goi_y_nguoi_dung`, 5,3 triệu dòng, 30 truy vấn theo user)

| Giai đoạn | Số tệp | Tệp đọc mỗi truy vấn | Trung vị (ms) | p95 (ms) |
|---|---|---|---|---|
| Phân mảnh (mô phỏng nhiều ngày MERGE) | 200 | 200 | 10.793 | 13.420 |
| Chỉ OPTIMIZE (gộp tệp, tối đa 1 MB) | 67 | 67 | 8.280 | 9.524 |
| OPTIMIZE ZORDER BY (ma_nguoi_dung) | 55 | 1,07 | **980** | 1.387 |

Chỉ gộp tệp thì mỗi tệp vẫn chứa user rải khắp dải giá trị nên không bỏ qua được tệp nào. ZORDER xếp user liền nhau, min/max của mỗi tệp hẹp lại và Delta chỉ đọc khoảng 1 tệp: **nhanh hơn 8,4 lần so với chỉ gộp tệp**, 11 lần so với lúc phân mảnh.

### 12.5 Tái lập bằng time travel (DAG `kn_tai_lap`, 10/11 23:13)

Kích hoạt với `{"phien_ban_delta": 0, "che_do": "mo_hinh_chon"}`: đọc `features.ma_tran_tuong_tac` `VERSION AS OF 0` (phiên bản dựng cho lần chạy `r20261110T075240`), huấn luyện lại đúng siêu tham số đã chọn, đánh giá lại trên tập test nằm cùng phiên bản, so với `gold.ket_qua_tuning` và `gold.lich_su_huan_luyen`.

| Loại so sánh | Khóa | Phiên bản Delta | Độ lệch lớn nhất | Trùng khớp |
|---|---|---|---|---|
| tuning | r20261110T075240, rank=50, reg=1.0, alpha=40.0 | 0 | 0,000000 | có |
| huấn luyện + đánh giá | r20261110T075240 | 0 | 0,000000 | có |
| tuning (11/11, nút trên web) | r20261110T230924, rank=50, reg=1.0, alpha=40.0 | 3 | 0 | có |
| huấn luyện + đánh giá, gồm NDCG v2 và 2 baseline (11/11) | r20261110T230924 | 3 | 0 | có |

Độ lệch bằng 0 tới 6 chữ số thập phân (ngưỡng < 10⁻⁶). Trong lúc chạy lại, `silver.tuong_tac` đã sang phiên bản 2 (tiến hóa lược đồ) và bronze đã có thêm 857 nghìn sự kiện phát lại; kết quả vẫn trùng vì job chỉ đọc features ở phiên bản 0, nơi chứa đủ cả train lẫn ground truth (mục 3), cùng hạt giống ALS cố định (`seed = 42`) và chỉ mục user/item không đổi giữa các phiên bản (mục 3).

### 12.6 Thí nghiệm bổ sung 11/11/2026 (chỉ đọc, không đổi hệ thống đang phục vụ)

**A/B lấy user làm đơn vị** ([jobs/kiem_tra/bootstrap_ab.py](jobs/kiem_tra/bootstrap_ab.py)), impression log phiên bản 51, chỉ traffic mô phỏng của chiến lược ALS: 8.592 user (A 4.310, B 4.282), khoảng 41 impression/user.

| CTR B so với A (−5,54 %) | p |
|---|---|
| z-test theo impression (phân tích chính) | 0,0151 |
| z-test sai số chuẩn theo cụm user (delta method, hiệu chỉnh m/(m−1)) | 0,0372 |
| Bootstrap lấy mẫu lại user, 2.000 lần, hạt giống 42 | 0,029 |

Khoảng tin cậy 95 % của mức chênh tương đối [−10,46 % ; −0,74 %]; design effect 1,36 (CTR), 1,05 (chuyển đổi, p = 0,75–0,76). Kết luận ở mức 5 % không đổi.

**Kỳ validation riêng** ([jobs/kiem_tra/tuning_validation.py](jobs/kiem_tra/tuning_validation.py)), features v3 (silver v10): chọn trên 19/8–2/9 (6.505 user), báo cáo trên 3–18/9 (4.215 user); 1.253 user có mặt ở cả hai kỳ. Lần 1 đo 14 tổ hợp (đủ rank 10, 5 tổ hợp rank 30) rồi dừng vì chậm 5–6 lần (lỗi 31); lần 2 đo rank 50 × alpha 40. Tổng 17/27 tổ hợp; cột "cả tháng" khớp bảng tuning gốc đến 4 chữ số.

| Trên kỳ kiểm tra (3–18/9) | NDCG@10 | so với toàn sàn | so với danh mục |
|---|---|---|---|
| Bán chạy toàn sàn | 0,0049 | — | — |
| Bán chạy theo danh mục | 0,0413 | — | — |
| ALS chọn trên validation (rank 50, reg 1,0, alpha 40 = cấu hình đang phục vụ) | 0,0579 | +1.086 % | +40 % |
| ALS tốt nhất nếu chọn trên chính kỳ kiểm tra (rank 50, reg 0,01, alpha 40) | 0,0581 | +1.091 % | +41 % |
| Xếp lại β = 1,0 (β chọn trên validation, cũng tốt nhất trên kỳ kiểm tra) | 0,0742 | +1.420 % | +80 % |

β trên validation / kiểm tra: 0 → 0,0789 / 0,0579; 0,25 → 0,0855 / 0,0630; 0,5 → 0,0919 / 0,0674; **1,0 → 0,1012 / 0,0742** (coverage kỳ kiểm tra 5,27 %); 1,5 → 0,0819 / 0,0576; 2,0 → 0,0630 / 0,0439. Thứ hạng cấu hình ở hai kỳ gần như trùng (Spearman 0,995 trên 17 tổ hợp); NDCG kỳ kiểm tra thấp hơn kỳ validation 17–29 % ở mọi tổ hợp. Kết luận: mức lạc quan do chọn trên tháng test chỉ 0,0002.

**Thêm nguồn ứng viên ngoài ALS** ([jobs/kiem_tra/thu_nguon_ung_vien.py](jobs/kiem_tra/thu_nguon_ung_vien.py)): giữ 10 − q_c − q_d ô đầu của danh sách đang phục vụ, thêm q_c món đồng xuất hiện (chỉ từ train) và q_d món bán chạy theo danh mục; chọn cấu hình trên validation theo luật đặt trước (coverage cao nhất trong các cấu hình có NDCG validation ≥ 95 % cấu hình gốc). Chạy 18:57–19:08 ngày 11/11 (11 phút), mô hình `als_v2_r20261110T230924`; 8.673/9.467 user có ứng viên đồng xuất hiện.

| Giữ / đồng xuất hiện / danh mục | NDCG validation | NDCG kiểm tra | NDCG cả tháng | Coverage | Số item | Suất vào N1 |
|---|---|---|---|---|---|---|
| 10 / 0 / 0 (đang phục vụ) | 0,1012 | 0,0742 | 0,0865 | 6,47 % | 8.162 | 80,8 % |
| 9 / 1 / 0 | 0,1022 | 0,0753 | 0,0876 | 10,14 % | 12.805 | 78,2 % |
| 8 / 2 / 0 | 0,1034 | 0,0760 | 0,0886 | 12,68 % | 16.011 | 75,9 % |
| **7 / 3 / 0 (chọn)** | **0,1040** | **0,0764** | **0,0892** | **14,53 %** | **18.341** | **74,1 %** |
| 9 / 0 / 1 | 0,1009 | 0,0740 | 0,0863 | 6,44 % | 8.124 | 82,2 % |
| 8 / 0 / 2 | 0,1010 | 0,0737 | 0,0863 | 6,46 % | 8.151 | 83,3 % |
| 8 / 1 / 1 | 0,1024 | 0,0751 | 0,0877 | 10,09 % | 12.737 | 79,5 % |
| 7 / 2 / 1 | 0,1034 | 0,0762 | 0,0887 | 12,63 % | 15.939 | 77,4 % |
| 6 / 3 / 1 | 0,1041 | 0,0763 | 0,0892 | 14,44 % | 18.231 | 75,6 % |
| 6 / 2 / 2 | 0,1033 | 0,0756 | 0,0884 | 12,59 % | 15.891 | 78,6 % |

Ô đồng xuất hiện tăng cả NDCG lẫn coverage; cấu hình chọn **vượt ngưỡng coverage 10 %** (14,5 %), kỳ kiểm tra +85 % so với bán chạy theo danh mục. Ô danh mục không giúp (coverage giữ 6,4–6,5 %). Tối ưu ở biên khoảng đã thử (3 ô). **Chưa triển khai**: hệ thống đang phục vụ vẫn 6,47 %.

---

## 13. Giới hạn đã biết

- Giá và "sao" là đại lượng gián tiếp (mục 2.1, mục 9).
- Lưới tuning chính chọn trên tập test (mục 4.3); kỳ validation riêng cho cùng lựa chọn, lạc quan 0,0002 (mục 12.6).
- z-test theo impression bỏ qua tương quan trong cùng user (mục 8.3); kiểm định theo user cho p = 0,029–0,037, kết luận không đổi (mục 12.6).
- Traffic A/B chủ yếu là mô phỏng; kết luận A/B chỉ có giá trị minh họa quy trình, không phải bằng chứng về hành vi người dùng thật.
- Trộn thịnh hành ở job batch dùng cửa sổ mới nhất tại thời điểm chạy; web thay bằng thịnh hành mới nhất lúc hiển thị.
- Sự kiện do web tạo bị loại khỏi tập train/test (`_nguon = web`) để thí nghiệm không tự ảnh hưởng lên đánh giá mô hình; chúng vẫn vào bronze, silver và luồng thịnh hành.

## Tài liệu tham khảo

- Y. Hu, Y. Koren, C. Volinsky. *Collaborative Filtering for Implicit Feedback Datasets*. ICDM 2008.
- Y. Zhou, D. Wilkinson, R. Schreiber, R. Pan. *Large-Scale Parallel Collaborative Filtering for the Netflix Prize* (ALS-WR). AAIM 2008.
- J. Leskovec, A. Rajaraman, J. Ullman. *Mining of Massive Datasets*, chương 9 (Recommendation Systems).
- Apache Spark MLlib — Collaborative Filtering; `pyspark.mllib.evaluation.RankingMetrics`.
- Delta Lake 3.2 — MERGE, Time Travel, Change Data Feed, OPTIMIZE/Z-ORDER.
- Giáo trình học phần: Recommendation System, Mining Massive Datasets, Data Mining (thư mục dự án trên claude.ai).
