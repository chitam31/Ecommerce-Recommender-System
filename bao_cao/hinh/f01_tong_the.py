"""Hình: kiến trúc tổng thể theo tầng."""
from ve import MAU, So

s = So(7.4, 8.75)
s.canh_nhom = "left"
W = 1.32  # bề rộng hộp chuẩn

# Tầng 1: nguồn dữ liệu
s.nhom(0.15, 0.12, 7.1, 1.18, "1  Data sources")
s.hop("csv", 0.35, 0.42, 1.95, 0.72, "RetailRocket dataset\nevents.csv: 2.76 M events\nitem properties, category tree", MAU["nguon"])
s.hop("prod", 2.75, 0.42, 1.95, 0.72, "Kafka producer\nreplays original timestamps\n1 day = 30 s, +5 % new users", MAU["nguon"])
s.hop("user", 5.15, 0.42, 1.9, 0.72, "Shoppers on the web app\nview / add-to-cart / buy,\nimpressions of 'For you'", MAU["nguon"])
s.mui("csv", "prod", "e", "w")

# Tầng 2: Kafka
s.nhom(0.15, 1.45, 7.1, 1.05, "2  Ingestion: Kafka 3.7.2 (KRaft), broker on labsit:9092")
s.hop("t1", 1.6, 1.72, 2.2, 0.68, "topic retailrocket-events\n3 partitions\nkey = visitor id", MAU["kafka"], kieu="tru")
s.hop("t2", 4.75, 1.72, 2.0, 0.68, "topic hien-thi\n3 partitions\nimpressions, clicks", MAU["kafka"], kieu="tru")
s.mui("prod", "t1", "s", "n", tb=0.65)
s.mui("user", "t1", "s", "n", ta=0.25, tb=0.9, cong=0.0, loai="phan_hoi")
s.mui("user", "t2", "s", "n", ta=0.6, loai="phan_hoi")

# Tầng 3: streaming
s.nhom(0.15, 2.65, 7.1, 1.12, "3  Stream processing: Spark Structured Streaming (always on)")
s.hop("s1", 0.35, 2.95, 1.95, 0.7, "nap_su_kien (ingest)\ntrigger 15 s, checkpoint on HDFS\nKafka → bronze Parquet", MAU["luong"])
s.hop("s2", 2.75, 2.95, 1.95, 0.7, "tinh_thinh_hanh (trending)\nwindow 15 min / slide 1 min\nwatermark 2 min → MERGE", MAU["luong"])
s.hop("s3", 5.15, 2.95, 1.9, 0.7, "ghi_hien_thi (impressions)\ntrigger 30 s\nMERGE by impression id", MAU["luong"])
s.mui("t1", "s1", "s", "n", ta=0.2, tb=0.6, loai="luong")
s.mui("t1", "s2", "s", "n", ta=0.75, tb=0.4, loai="luong")
s.mui("t2", "s3", "s", "n", ta=0.5, tb=0.55, loai="luong")

# Tầng 4: lakehouse
s.nhom(0.15, 3.92, 7.1, 1.95, "4  Lakehouse: HDFS 3.5 (replication 2) + Delta Lake 3.2", MAU["hdfs"], "#D9A47E")
s.hop("br", 0.35, 4.25, W, 0.78, "BRONZE\nbronze.su_kien\nParquet, by day", MAU["bronze"])
s.hop("sv", 1.83, 4.25, W, 0.78, "SILVER\nsilver.tuong_tac (by day)\nsilver.san_pham", MAU["silver"])
s.hop("ft", 3.31, 4.25, W, 0.78, "FEATURES\nma_tran_tuong_tac\ntrain / test, versioned", MAU["feat"])
s.hop("md", 3.8, 5.15, 0.83, 0.62, "MODELS\nALS per run,\nindexers", MAU["trang"], co=7.1)
s.hop("gd", 4.85, 4.25, 2.2, 1.5, "GOLD (Delta)\ngoi_y_nguoi_dung (ZORDER)\nthinh_hanh_15p\nnhat_ky_hien_thi (CDF)\nket_qua_ab · thong_ke_san_pham\nsan_pham_lien_quan\nket_qua_tuning · lich_su_huan_luyen\nket_qua_tai_lap", MAU["gold"], co=7.1)
s.mui("s1", "br", "s", "n", ta=0.4, tb=0.5, loai="luong")
s.mui("s2", "gd", "s", "n", ta=0.8, tb=0.25, nhan="upsert windows", vi_tri_nhan=0.45, loai="luong")
s.mui("s3", "gd", "s", "n", ta=0.5, tb=0.8, nhan="upsert", vi_tri_nhan=0.45, loai="luong")

# Tầng 5: batch
s.nhom(0.15, 6.02, 7.1, 1.25, "5  Batch processing: Spark 3.5.3 + MLlib (DAG 1 nightly, DAG 3 hourly, DAG 4 nightly)", MAU["xam"])
bw, by = 1.3, 6.33
s.hop("b1", 0.27, by, bw, 0.8, "bronze_to_silver\ndedup, bot filter,\nweights 1/3/5", MAU["lo"])
s.hop("b2", 1.68, by, bw, 0.8, "chuan_bi_ma_tran\ntime split, decay,\nappend-only index", MAU["lo"])
s.hop("b3", 3.09, by, bw, 0.8, "huan_luyen_als\n+ danh_gia: 27 combos,\nmetrics, baselines", MAU["lo"])
s.hop("b4", 4.5, by, bw, 0.8, "serve: re-rank,\nblend, cold start,\nMERGE gold", MAU["lo"])
s.hop("b5", 5.91, by, 1.22, 0.8, "tinh_ket_qua_ab\nCDF + z-test;\ntoi_uu_delta", MAU["lo"])
s.mui("br", "b1", "s", "n", ta=0.3, tb=0.3)
s.mui("b1", "sv", "n", "s", ta=0.75, tb=0.3)
s.mui("sv", "b2", "s", "n", ta=0.6, tb=0.5)
s.duong([(2.75, 6.33), (2.75, 5.45), (3.42, 5.45), (3.42, 5.03)])
s.duong([(3.62, 5.03), (3.62, 6.33)])
s.duong([(4.1, 6.33), (4.1, 5.77)])
s.mui("md", "b4", "e", "n", ta=0.5, tb=0.35, cong=-0.25)
s.mui("b4", "gd", "n", "s", ta=0.75, tb=0.3)
s.mui("b5", "gd", "n", "s", ta=0.5, tb=0.8, hai_chieu=True)

# Tầng 6: điều phối và phục vụ
s.nhom(0.15, 7.42, 7.1, 1.2, "6  Orchestration and serving")
s.hop("af", 0.35, 7.72, 3.15, 0.75, "Apache Airflow 3.3.1 (LocalExecutor, pool = 1)\nDAG 1 daily pipeline · DAG 2 stream monitor\nDAG 3 hourly A/B · DAG 4 maintenance · DAG 5 reproduce", MAU["af"])
s.hop("web", 3.85, 7.72, 3.2, 0.75, "FastAPI web app (host labsoe-18, local Spark)\nhome · product · A/B dashboard\nmodel quality · reproduction", MAU["web"])
s.mui("af", (1.9, 7.27), "n", None, loai="dieu_khien")
s.chu(2.45, 7.35, "spark-submit (BashOperator)", co=6.3, mau="#B85450", nghieng=True)
s.duong([(7.05, 5.0), (7.18, 5.0), (7.18, 8.1), (7.05, 8.1)], loai="doc")
s.chu(6.62, 7.55, "reads gold", co=6.3)
s.duong([(7.05, 7.95), (7.31, 7.95), (7.31, 0.78), (7.05, 0.78)], loai="phan_hoi")
s.luu("f01_tong_the.png")
