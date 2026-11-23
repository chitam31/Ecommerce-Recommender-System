"""Hình: mặt phẳng điều khiển: phân phối mã và sinh cấu hình từ một danh bạ duy nhất."""
from ve import MAU, So

s = So(7.4, 4.35)
s.hop("gh", 0.12, 0.35, 1.15, 0.8, "GitHub\nprivate repository\n(source of truth\nfor code)", MAU["xam"], co=6.4, kieu="tru")
s.hop("lap", 0.12, 1.5, 1.85, 1.55, "Control laptop (Git Bash)\ngit pull (token stays here)\nssh_tat_ca.sh --dong-bo-ma\nchep_du_lieu.sh (Kaggle key\nstays here) · tinh_trang.sh\nSSH config: ProxyJump\nlabsoe-18 when off campus", MAU["trang"], co=6.3)
s.mui("gh", "lap", "s", "n", ta=0.5, tb=0.3, nhan="git pull", co=6.0)

for i, y in enumerate((0.3, 0.55, 0.8)):
    s.hop("m%d" % i, 2.45 + i * 0.08, y, 1.85, 1.1, "", MAU["may"] if i < 2 else MAU["trang"])
s.hop("m", 2.61, 0.8, 1.85, 1.1, "each lab machine\n~/bd03_khuyen_nghi\nworking tree updated by\nreceive.denyCurrentBranch\n= updateInstead", MAU["trang"], co=6.3)
s.mui("lap", "m", "e", "w", ta=0.25, tb=0.6, nhan="git push over SSH\n(retries on drops)", co=6.0, vi_tri_nhan=0.5, lech_nhan=(0.05, -0.28))

s.hop("kd", 4.85, 0.22, 2.43, 2.38, "scripts/cum/khoi_dong_may.sh\n1  read cau_hinh/cum.env\n2  find own row by ZeroTier IP\n3  roles → services (--chi=<role>: one)\n4  ZT_MTU=1280: lower ZeroTier MTU\n5  write ha_tang/*/.env:\n    NODE_IP, cores, memory,\n    replication, HDFS-config hash\n    (+ .env.rieng appended)\n6  stop if a container name belongs\n    to another compose project\n7  docker compose up -d per role\n    (--build if bd03/spark is absent)", MAU["trang"], co=6.3, canh="left")
s.mui("m", "kd", "e", "w", ta=0.5, tb=0.35)

s.hop("svc", 4.85, 2.8, 2.43, 1.43, "per role\nnamenode → hadoop master\ndatanode → hadoop worker\nspark-master / spark-worker\nkafka → broker + UI\nairflow → Airflow stack\nweb → bd03-web", MAU["spark"], co=6.3)
s.mui("kd", "svc", "s", "n")

s.hop("env", 0.12, 3.25, 4.45, 0.98, "cau_hinh/cum.env (excerpt)\nMAY_1=\"labsoe-11|10.68.71.113|namenode,spark-master,airflow|0|0\"\nMAY_3=\"labsit|10.68.71.204|datanode,spark-worker,kafka|6|2g\"\nMAY_6=\"labsoe-18|10.68.71.149|datanode,spark-worker,web|8|3g\"   HE_SO_SAO_CHEP=2   LOI_ALS=10",
      "#FBFBF3", co=6.0, canh="left")
s.mui("env", "kd", "e", "w", ta=0.3, tb=0.85, net="--", nhan="read", co=6.0)
s.chu(2.35, 3.1, "secrets never committed: ha_tang/*/.env, .env.rieng, mat_khau.json, kaggle.json", co=6.0, nghieng=True, mau="#B04A4A")
s.luu("f10_cau_hinh.png")
