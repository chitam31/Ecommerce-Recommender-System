"""Hình: vòng khép kín giữa web, Kafka, các luồng và pipeline A/B (khổ ngang)."""
from ve import So

s = So(7.4, 3.5)
Y0 = 0.36          # đỉnh các khung
B = 0.66           # đỉnh hàng khối đầu tiên
CO = 6.4

s.nhom(0.1, Y0, 1.2, 2.66, "Browser", "xam", co=7.4)
s.hop("home", 0.18, B, 1.04, 0.82, "Home page\n'For you': 10 slots\n+ 'Trending now'", "trang", co=CO)
s.hop("prod", 0.18, 1.62, 1.04, 0.57, "Product page\nview · cart · buy", "trang", co=CO)
s.hop("dash", 0.18, 2.3, 1.04, 0.62, "Dashboards\nA/B · model quality\nreproduction", "trang", co=CO)

s.nhom(1.42, Y0, 1.42, 2.66, "FastAPI web (labsoe-18)", "web", co=7.4)
s.hop("w1", 1.5, B, 1.26, 0.82, "GET /api/goi-y/{u}\ngroup = parity of md5(u)\nnewest trending slots", "web", co=6.2)
s.hop("w2", 1.5, 1.62, 1.26, 0.57, "POST /api/su-kien\nPOST /api/hien-thi", "web", co=6.2)
s.hop("w3", 1.5, 2.3, 1.26, 0.62, "GET /api/ab/ket-qua\nPOST /api/tai-lap/{day}\nlocal Spark reads Delta", "web", co=6.2)
s.mui("home", "w1", "e", "w", hai_chieu=True, loai="doc")
s.mui("prod", "w2", "e", "w", loai="phan_hoi")
s.mui("dash", "w3", "e", "w", hai_chieu=True, loai="doc")

s.nhom(2.96, Y0, 0.86, 2.66, "Kafka", "kafka", co=7.4)
s.hop("ke", 3.03, B, 0.72, 0.82, "retailrocket-\nevents", "kafka", co=CO, kieu="tru", dam_dong_dau=False)
s.hop("kh", 3.03, 1.58, 0.72, 0.64, "hien-thi", "kafka", co=CO, kieu="tru", dam_dong_dau=False)
s.mui("w2", "ke", "e", "w", ta=0.3, tb=0.72, loai="phan_hoi")
s.mui("w2", "kh", "e", "w", ta=0.7, tb=0.6, loai="phan_hoi")

s.nhom(3.94, Y0, 1.52, 2.66, "Streaming and batch jobs", "luong", co=7.4)
s.hop("st", 4.02, B, 1.36, 0.36, "tinh_thinh_hanh · every 20 s", "luong", co=6.2)
s.hop("nap", 4.02, 1.12, 1.36, 0.36, "nap_su_kien → bronze", "luong", co=6.2)
s.hop("gh", 4.02, 1.62, 1.36, 0.36, "ghi_hien_thi · MERGE 30 s", "luong", co=6.2)
s.hop("mp", 4.02, 2.06, 1.36, 0.36, "mo_phong_hien_thi (simulated)", "lo", co=6.2)
s.hop("d3", 4.02, 2.52, 1.36, 0.4, "DAG 3 hourly: CDF → z-test", "af", co=6.2)
s.mui("ke", "st", "e", "w", ta=0.25, tb=0.5, loai="luong")
s.mui("ke", "nap", "e", "w", ta=0.75, tb=0.5, loai="luong")
s.mui("kh", "gh", "e", "w", ta=0.55, tb=0.5, loai="luong")

s.nhom(5.58, Y0, 1.72, 2.66, "Gold tables (Delta)", "gold", co=7.4)
s.hop("gt", 5.66, B, 1.56, 0.36, "thinh_hanh_15p", "gold", co=6.4)
s.hop("gg", 5.66, 1.12, 1.56, 0.36, "goi_y_nguoi_dung (nightly DAG 1)", "gold", co=6.4)
s.hop("nk", 5.66, 1.62, 1.56, 0.8, "nhat_ky_hien_thi\nimpression log\nChange Data Feed, version v", "gold", co=6.4)
s.hop("ab", 5.66, 2.52, 1.56, 0.4, "ket_qua_ab + data version", "gold", co=6.4)
s.mui("st", "gt", "e", "w", loai="luong")
s.mui("nap", "gg", "e", "w")
s.mui("gh", "nk", "e", "w", ta=0.5, tb=0.17, loai="luong")
s.mui("mp", "nk", "e", "w", ta=0.5, tb=0.72)
s.mui("nk", "d3", "w", "n", ta=0.9, tb=0.88, nhan="CDF", co=5.8, vi_tri_nhan=0.5, lech_nhan=(0.12, -0.02))
s.mui("d3", "ab", "e", "w")

# Web đọc lại gold: vòng ≤ 90 s của khối thịnh hành (trên) và bảng điều khiển (dưới)
s.duong([(7.05, B), (7.05, 0.18), (2.6, 0.18), (2.6, 0.36)], loai="doc")
s.chu(4.8, 0.18, "web reads the newest trending window and the user's list (trending loop ≤ 90 s)",
      co=6.2, nghieng=True, mau="#0E8088", nen=True)
s.duong([(6.44, 2.92), (6.44, 3.3), (2.6, 3.3), (2.6, 3.02)], loai="doc")
s.chu(4.5, 3.3, "dashboard reads ket_qua_ab; reproduction re-reads nhat_ky_hien_thi at the recorded version",
      co=6.2, nghieng=True, mau="#0E8088", nen=True)
s.luu("f08_vong_khep_kin.png")
