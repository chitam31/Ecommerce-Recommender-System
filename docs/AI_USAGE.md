# AI_USAGE.md — Khai báo sử dụng AI (mục A.9 đề cương)

Đề tài 3 `bd03_khuyen_nghi` — Hệ khuyến nghị sản phẩm cho sàn TMĐT. Học phần Dữ liệu lớn (CS411V), Trường Đại học Tân Tạo.

Mục A.9 của đề cương cho phép dùng mã do AI sinh nếu ghi rõ hai điều: phần nào do AI sinh và phần nào nhóm sửa.

## 1. Tóm tắt

| Mục | Khai báo |
|---|---|
| Công cụ | Claude (Anthropic): claude.ai Projects (hội thoại, soạn mã và tài liệu) và Claude Code (sửa mã trực tiếp trong repo, commit và push) |
| Thời gian | 01/10/2026 – 16/11/2026 |
| Vai trò của AI | Hướng dẫn kỹ thuật và **sinh kiểm thử, gen ra slide** |
| Vai trò của nhóm | Cung cấp đề cương, mã và cấu hình cụm của nhóm; dựng và vận hành cụm 6 máy lab qua ZeroTier; chạy mọi job, DAG, luồng trên cụm thật; gửi log và ảnh lỗi để AI sửa; ra quyết định triển khai; kiểm chứng kết quả; trình bày và bảo vệ |
| Lịch sử Git | Commit do AI tạo có tác giả `Claude` và/hoặc dòng `Co-Authored-By: Claude ...`. |
| Nguồn số liệu | Mọi số liệu trong báo cáo, slide và README (số sự kiện, metric mô hình, A/B, Z-ORDER, độ trễ...) lấy từ các lần chạy thật trên cụm, in ra bởi job Spark (dòng JSON cuối), `jobs/kiem_tra/xuat_ket_qua.py` hoặc `scripts/thu_bang_chung.sh`. AI chỉ trình bày lại, không tạo số liệu |

## 2. Cách làm việc với AI

1. Nhóm đưa vào dự án Claude: đề cương (`ĐỀ TÀI 3_BIG DATA.pdf`, đề cương chi tiết học phần), giáo trình các môn liên quan, hai tệp `spark-cluster.zip` và `hadoop-cluster.zip`, địa chỉ các repo cũ của nhóm (`spark-cluster`, `hadoop-cluster`, `hadoop-custom`, `kafka-docker`) và ảnh ZeroTier Central.
2. AI rà soát cấu hình nhóm đã làm (NHAT_KY.md mục 2 liệt kê các lỗi tìm thấy), đề xuất kiến trúc rồi hỗ trợ kỹ thuật.
3. Nhóm chạy trên cụm thật. Lỗi (log Spark, Airflow, HDFS) được gửi lại; AI chẩn đoán và hỗ trợ sửa. 34 lỗi đã gặp và cách sửa được ghi trong NHAT_KY.md và mục "Sự cố" của báo cáo.
4. Từ 10/11/2026 AI làm việc trực tiếp trong repo qua Claude Code: commit, push lên GitHub. Nhóm đồng bộ mã sang các máy lab bằng `scripts/cum/ssh_tat_ca.sh --dong-bo-ma`.

## 3. Phần do AI sinh

| Phần | Do AI sinh | Ghi chú |
|---|---|---|
| `jobs/` (PySpark: bảo trì và kiểm tra) | Gần toàn bộ | Phát triển từ job bronze cũ của nhóm (`nap_su_kien_bronze.py`) |
| `web/` (FastAPI, Jinja2, giao diện) | Toàn bộ | Chart.js 4.6.1 trong `web/static/vendor/` là thư viện mã nguồn mở (giấy phép MIT), không do AI viết |
| `ha_tang/` (Docker compose, Dockerfile, submit.sh) | Rà soát dựa trên nền cấu hình của nhóm | Giữ tên container, volume, cổng của repo nhóm |
| `tests/` | Toàn bộ | |
| `docs/`, `README.md`, `MO_HINH.md`, `NHAT_KY.md`, `notebooks/` | Toàn bộ | NHAT_KY ghi lại kết quả các lần chạy do nhóm thực hiện |
| `slide/`  | Toàn bộ | Ảnh chụp màn hình web, Spark UI, Airflow lấy từ hệ thống thật |

## 4. Phần nhóm thực hiện và chỉnh sửa

Những việc dưới đây do nhóm làm. Phần lớn không nằm trong commit vì đó là vận hành trên các máy lab. Vai trò theo mục A.2: Lê Chí Tâm là ML Engineer và DevOps/Orchestration; Lê Nguyễn Quốc Anh là Web Developer và Data Engineer (bảng phân công ở mục 10 báo cáo chính).

- Hạ tầng gốc trước khi dùng AI: repo cụm `spark-cluster`, `hadoop-cluster`, `hadoop-custom`, `kafka-docker` và job bronze ban đầu. AI rà soát và sửa trên nền này (Lê Nguyễn Quốc Anh).
- Mạng ZeroTier cho 6 máy lab và máy cá nhân; xin và phối hợp với phòng lab để dùng máy, chặn card Docker trong ZeroTier, xử lý máy ngủ (Lê Nguyễn Quốc Anh).
- Tải dữ liệu RetailRocket bằng khóa Kaggle trên laptop (khóa không để trên máy lab dùng chung) và chép sang cụm (Lê Nguyễn Quốc Anh).
- Chạy toàn bộ pipeline, 3 luồng streaming, Airflow, web, bộ phát Kafka trên cụm thật; chụp màn hình, gửi log lỗi; kiểm tra web và DAG tái lập (Lê Chí Tâm).
- Quyết định triển khai: chuyển web khỏi máy Ctam sang labsoe-18, tạm dừng Cassandra ngoài dự án trên master để đủ RAM, chọn máy chạy từng vai trò (cả nhóm).
- Commit của nhóm trong repo: đưa dự án lên thư mục gốc và cố định LF (10/11), mẫu bìa báo cáo TTU (11/11), sửa tên thành viên và tiêu đề README (11/11) (Lê Chí Tâm).
- Duyệt và yêu cầu sửa các bản nháp tài liệu, kịch bản demo và thuyết trình (Lê Chí Tâm).

Những phần thành viên tự làm hoặc tự sửa trực tiếp:

| Thành viên | Phần đã tự làm / tự sửa | Nội dung |
|---|---|---|
| Lê Chí Tâm | Cấu trúc repo, `README.md`, mẫu bìa báo cáo, kết quả thí nghiệm (`docs/ket_qua_cum/`) | Đưa dự án lên thư mục gốc và cố định LF, sửa thông tin nhóm, mẫu bìa TTU, đưa kết quả thí nghiệm chạy trên cụm vào repo (6 commit) |
| Lê Nguyễn Quốc Anh | Hạ tầng cụm gốc và dữ liệu | Dựng và chỉnh cấu hình cụm Spark, Hadoop, Kafka ban đầu; tải và chuẩn bị dữ liệu RetailRocket |

## 5. Kiểm chứng phần do AI sinh

- 143 kiểm thử pytest chạy trên Spark local thật và Airflow 3.3.1 thật: `pytest` (Spark, web) và `tests/test_airflow_dags.py` (cần `requirements-airflow.txt`).
- Pipeline chạy thật trên cụm, kết quả ghi ở NHAT_KY.md và các tệp trong `docs/bang_chung/`.
- Tái lập bằng time travel: DAG `kn_tai_lap` cho metric trùng khớp với lịch sử ở hai phiên bản đặc trưng (độ lệch 0).
- Deadline Alert và backfill được thử trên Airflow 3.3.1 thật trước khi đưa lên cụm: `docs/bang_chung/kiem_chung_airflow_cuc_bo.md`; backfill DAG 3 chạy lại trên cụm ngày 16/11: `docs/bang_chung/cum/10_backfill_dag3.txt`.

## 6. Giải thích mã khi bảo vệ

Mục A.9 yêu cầu nhóm giải thích được mọi dòng mã. Tài liệu hỗ trợ:

- `docs/KIEN_TRUC.md`: kiến trúc, đặc tả bảng, lý do từng quyết định thiết kế.
- `MO_HINH.md`: thẻ mô hình và cơ sở lý thuyết.
- Docstring và chú thích tiếng Việt ở đầu mỗi job, DAG và script.
- `docs/CHUAN_BI_BAO_VE.md` và `bao_cao/hoi_dap/`: câu hỏi phản biện kèm đáp án (gồm các câu mẫu mục C.5).
