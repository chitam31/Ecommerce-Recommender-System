"""Hình: tái lập thí nghiệm bằng time travel của Delta (hai lần: 10/11 trên features v0, 11/11 trên v3)."""
from matplotlib.patches import Circle
from ve import BANG, So

s = So(7.4, 3.1)


def truc(y, ten, moc, mau):
    nen, vien, _ = BANG[mau]
    s.chu(0.15, y, ten, co=7.4, canh="left", dam=True)
    s.ax.plot([1.5, 7.15], [y, y], color="#555555", lw=0.9, zorder=2)
    s.mui((7.1, y), (7.24, y), mau="#555555")
    for x, nhan, mo_ta in moc:
        s.ax.add_patch(Circle((x, y), 0.095, fc=nen, ec=vien, lw=1.0, zorder=4))
        s.ax.text(x, y, nhan, fontsize=6.8, ha="center", va="center", zorder=5, fontweight="bold")
        if mo_ta:
            s.ax.text(x, y + 0.15, mo_ta, fontsize=6.6, ha="center", va="top", zorder=5, color="#333333",
                      linespacing=1.15)


truc(0.36, "features.\nma_tran_tuong_tac", [
    (1.95, "v0", "10/11 ~08:00\nrun r20261110T075240"),
    (5.65, "v1", "10/11 23:35, DAG 1\nfrom silver v3"),
    (6.35, "v2", ""),
    (6.85, "v3", "11/11 06:52, run\nr20261110T230924"),
], "feat")
truc(1.22, "silver.tuong_tac", [
    (1.95, "v0", "10/11 first load\n7 columns"),
    (3.42, "v1", "23:12 ALTER TABLE\nADD COLUMNS"),
    (4.36, "v2", "23:18 MERGE fills\nprice, stars"),
    (5.3, "v3", "23:21+ incremental\nbronze_to_silver"),
], "silver")
s.chu(6.45, 1.74, "later versions do not affect the reproduction", co=6.2, nghieng=True, mau="#555555")

s.hop("ls", 0.12, 1.95, 2.3, 1.03, "gold.lich_su_huan_luyen\nrow of run r20261110T075240:\nphien_ban_features = 0, cut date,\nrank 50, reg 1.0, alpha 40,\nmaxIter 10, seed 42, metrics", "gold", co=7.0)
s.hop("dag", 2.75, 1.95, 2.15, 1.03, "DAG 5 kn_tai_lap\nconf {\"phien_ban_delta\": 0}\nreads features VERSION AS OF 0,\nre-runs tuning + evaluation\n(11/11: same DAG on version 3)", "af", co=7.0)
s.hop("kq", 5.2, 1.95, 2.08, 1.03, "gold.ket_qua_tai_lap\ntuning row: deviation $1.78\\times10^{-15}$\ntraining row: deviation 0\nmatch (tolerance $10^{-6}$)\n11/11, v3: both rows 0", "gold", co=7.0)
s.duong([(2.9, 1.95), (2.9, 0.18), (1.95, 0.18), (1.95, 0.265)], loai="dieu_khien")
s.chu(3.0, 0.75, "time travel", co=6.6, mau="#B85450", dam=True, canh="left")
s.mui("ls", "dag", "e", "w")
s.mui("dag", "kq", "e", "w")
s.luu("f09_tai_lap.png")
