"""Hình: DAG 1 kn_pipeline_hang_ngay, bản gọn khổ ngang cho slide (cùng phụ thuộc với f05_dag1)."""
from ve import So

s = So(7.4, 2.66)
W, G, X0 = 0.94, 0.08, 0.17
X = [X0 + i * (W + G) for i in range(7)]
R1, R2, R3, H = 0.3, 1.1, 1.92, 0.5
CO = 6.2

hang1 = [("sen", "cho_du_lieu_ngay_moi\nsensor: new bronze data?", "xam"), ("mc", "tao_ma_chay\nrun id → XCom", "trang"),
         ("bs", "bronze_to_silver\n[lo] by day", "lo"), ("cb", "chuan_bi_ma_tran\n[lo] features v", "lo"),
         ("hl", "huan_luyen_als\n[als] 9 or 27 combos", "lo"), ("dg", "danh_gia\n[als] metrics, baselines", "lo")]
for i, (k, t, m) in enumerate(hang1):
    s.hop(k, X[i], R1, W, H, t, m, co=CO)
    if i:
        s.mui(hang1[i - 1][0], k, "e", "w")
s.hop("br", X[6], R1 - 0.06, W, H + 0.12, "NDCG@10\nimproved?", "af", co=CO, kieu="thoi")
s.mui("dg", "br", "e", "w")

s.hop("tk", X[2], R2, W, H, "thong_ke_san_pham\n[lo] statistics", "lo", co=CO)
s.hop("lq", X[1], R2, W, H, "san_pham_lien_quan\n[lo] co-occurrence", "lo", co=CO)
s.hop("gg", X[3], R2, W, H, "ghi_gold\n[lo] MERGE gold", "lo", co=CO)
s.hop("tr", X[4], R2, W, H, "tron_goi_y\n[lo] A/B, cold start", "lo", co=CO)
s.hop("sg", X[5], R2, W, H, "sinh_goi_y_toan_bo\n[als] top-200 → re-rank", "lo", co=CO)
s.hop("gc", X[6], R2, W, H, "giu_mo_hinh_cu\nkeep serving lists", "xam", co=CO)
s.hop("cw", X[6], R3, W, H, "canh_bao\nwarning in the log", "xam", co=CO)
s.mui("bs", "tk", "s", "n")
s.mui("tk", "lq", "w", "e")
xg = X[6] + W * 0.25
s.duong([(xg, R1 + 0.31 + 0.155), (xg, 0.99), (X[5] + W * 0.7, 0.99), (X[5] + W * 0.7, R2)])
s.chu(X[6] + 0.03, 0.99, "yes", co=6.4, nghieng=True, nen=True)
s.mui("sg", "tr", "w", "e")
s.mui("tr", "gg", "w", "e")
s.mui("br", "gc", "s", "n", nhan="no", co=6.4, vi_tri_nhan=0.5, lech_nhan=(0.12, 0))
s.mui("gc", "cw", "s", "n")
s.duong([(X[2] + W / 2, R2 + H), (X[2] + W / 2, 1.8), (X[4] + W * 0.5, 1.8), (X[4] + W * 0.5, R2 + H)])
s.chu(X[3] + W / 2, 1.8, "statistics feed the blend", co=6.0, nghieng=True, nen=True)

s.chu_giai(0.17, 1.98, [("hop", "lo", "Spark task [profile]"), ("hop", "xam", "sensor / keep path"),
                        ("hop", "trang", "Python task"), ("hop", "af", "branch (BranchPythonOperator)")],
           cot=2, rong_cot=1.78, co=6.3, tieu_de=None, dong_cao=0.2)
s.chu(4.02, 2.08, "schedule 02:00, max_active_runs = 1", co=6.3, canh="left", nghieng=True, mau="#444444")
s.chu(4.02, 2.26, "Spark tasks share an Airflow pool of 1 slot", co=6.3, canh="left", nghieng=True, mau="#444444")
s.luu("f05b_dag1_ngang.png")
