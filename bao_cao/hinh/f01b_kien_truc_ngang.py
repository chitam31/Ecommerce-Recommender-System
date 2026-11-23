"""Hình: kiến trúc tổng thể, bản khổ ngang (slide và báo cáo): luồng chính trái → phải, vòng phản hồi phía trên."""
from ve import So

s = So(7.4, 3.24)
CO, CB = 7.0, 6.6

# Nguồn dữ liệu
s.nhom(0.1, 0.34, 0.98, 1.94, "Data sources", "xam", co=7.6)
s.hop("csv", 0.18, 0.62, 0.82, 0.58, "RetailRocket\n2.76 M events\n139 days", "xam", co=CB)
s.hop("prod", 0.18, 1.5, 0.82, 0.58, "Replay producer\n1 day = 30 s\n+5 % new users", "xam", co=CB)
s.mui("csv", "prod", "s", "n")

# Kafka
s.nhom(1.16, 0.34, 0.82, 1.94, "Kafka 3.7", "kafka", co=7.6)
s.hop("t1", 1.22, 0.6, 0.7, 0.66, "retailrocket-\nevents\n3 partitions", "kafka", co=CB, kieu="tru")
s.hop("t2", 1.22, 1.46, 0.7, 0.66, "hien-thi\nimpressions,\nclicks", "kafka", co=CB, kieu="tru")
s.mui("prod", "t1", "e", "w", ta=0.35, tb=0.62)

# Streaming
s.nhom(2.06, 0.34, 1.08, 1.94, "Structured Streaming", "luong", co=7.6)
s.hop("s1", 2.13, 0.52, 0.94, 0.42, "nap_su_kien\ningest, every 15 s", "luong", co=CB)
s.hop("s2", 2.13, 1.0, 0.94, 0.42, "tinh_thinh_hanh\nwindow 15 min / 1 min", "luong", co=CB)
s.hop("s3", 2.13, 1.62, 0.94, 0.42, "ghi_hien_thi\nimpression log, 30 s", "luong", co=CB)
s.mui("t1", "s1", "e", "w", ta=0.45, tb=0.5, loai="luong")
s.mui("t1", "s2", "e", "w", ta=0.75, tb=0.4, loai="luong")
s.mui("t2", "s3", "e", "w", ta=0.55, tb=0.5, loai="luong")

# Lakehouse
s.nhom(3.22, 0.34, 3.14, 1.08, "Lakehouse: HDFS 3.5 (replication 2) + Delta Lake 3.2", "gold", co=7.6)
xs = [3.3, 4.093, 4.887, 5.68]
ten = [("br", "Bronze\nsu_kien", "bronze"), ("sv", "Silver\ntuong_tac", "silver"),
       ("ft", "Features\nmatrix", "feat"), ("gd", "Gold\nserved tables", "gold")]
for x, (k, nhan, mau) in zip(xs, ten):
    s.hop(k, x, 0.52, 0.6, 0.58, nhan, mau, co=CB, kieu="tru")
s.mui("s1", "br", "e", "w", ta=0.5, tb=0.5, loai="luong")
s.duong([(3.07, 1.21), (6.02, 1.21), (6.02, 1.1)], loai="luong")
s.duong([(3.07, 1.83), (3.17, 1.83), (3.17, 1.33), (6.18, 1.33), (6.18, 1.1)], loai="luong")

# Batch
s.nhom(3.22, 1.5, 3.14, 0.78, "Batch: Spark 3.5.3 + MLlib", "lo", co=7.6, duoi=True)
jobs = [("j1", "bronze_to_silver\ndedup, bot filter"), ("j2", "chuan_bi_ma_tran\nsplit, decay, index"),
        ("j3", "huan_luyen_als\ntune, evaluate, serve")]
for i, (k, nhan) in enumerate(jobs):
    cx = (xs[i] + xs[i + 1]) / 2 + 0.3
    s.hop(k, cx - 0.375, 1.58, 0.75, 0.44, nhan, "lo", co=6.2)
    trai, phai = cx - 0.375, cx + 0.375
    x_xuong = (trai + xs[i] + 0.6) / 2      # bảng i → job (thẳng đứng)
    x_len = (xs[i + 1] + phai) / 2          # job → bảng i+1 (thẳng đứng)
    s.mui((x_xuong, 1.1), (x_xuong, 1.58))
    s.mui((x_len, 1.58), (x_len, 1.1))

# Phục vụ
s.nhom(6.44, 0.34, 0.86, 1.94, "Serving", "web", co=7.6)
s.hop("web", 6.5, 0.56, 0.74, 0.62, "FastAPI web\nlabsoe-18\nlocal Spark", "web", co=CB)
s.hop("br2", 6.5, 1.46, 0.74, 0.62, "Browser\nFor you, trending,\nA/B, reproduce", "trang", co=6.2)
s.mui("gd", "web", "e", "w", ta=0.45, tb=0.45, loai="doc")
s.mui("web", "br2", "s", "n", hai_chieu=True, loai="doc")
# Vòng phản hồi: sự kiện của người dùng quay lại Kafka
s.duong([(7.17, 0.56), (7.17, 0.19), (1.57, 0.19), (1.57, 0.34)], loai="phan_hoi")
s.chu(4.2, 0.19, "closed loop: views, carts, purchases, impressions and clicks from the web go back to Kafka",
      co=6.4, nghieng=True, mau="#1F5FAD", nen=True)

# Điều phối
s.hop("af", 0.1, 2.46, 3.04, 0.68, "Apache Airflow 3.3.1: five DAGs\ndaily pipeline · stream monitor · hourly A/B test\nnightly maintenance · reproduction (time travel)", "af", co=CB)
s.duong([(3.14, 2.8), (3.4, 2.8), (3.4, 2.28)], loai="dieu_khien")
s.chu(3.47, 2.37, "spark-submit", co=6.0, nghieng=True, mau="#B85450", canh="left")
s.mui((2.6, 2.46), (2.6, 2.28), loai="dieu_khien")
s.chu(2.05, 2.37, "restart if stale", co=6.0, nghieng=True, mau="#B85450")

s.chu_giai(3.66, 2.44, [
    ("tru", "kafka", "Kafka topic"), ("hop", "luong", "Streaming job"), ("hop", "lo", "Batch job"),
    ("tru", "gold", "Delta table"), ("hop", "af", "Orchestration"), ("hop", "web", "Serving"),
    ("mui", "du_lieu", "Batch data"), ("mui", "luong", "Streaming data"), ("mui", "dieu_khien", "Control"),
    ("mui", "phan_hoi", "User feedback"), ("mui", "doc", "Read for serving"), ("hop", "xam", "External source"),
], cot=3, rong_cot=1.18, co=6.2, tieu_de=None, dong_cao=0.155)
s.luu("f01b_kien_truc_ngang.png")
