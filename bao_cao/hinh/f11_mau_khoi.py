"""Hình: mẫu 10 ô hiển thị của khối 'For you' theo loại người dùng và nhóm A/B."""
from matplotlib.patches import FancyBboxPatch
from ve import BANG, So

O = {"a": ("kafka", "ALS (re-ranked)"), "t": ("lo", "trending (15-min window)"),
     "m": ("luong", "new item, main category"), "d": ("feat", "best-seller, viewed category")}
hang = [
    ("Group A: ALS only", "active user, ≥ 5 interactions", "aaaaaaaamm"),
    ("Group B: ALS + 30 % trending", "active user, ≥ 5 interactions", "aataatatmm"),
    ("Few interactions (< 5)", "categories and trending, 50/50", "dtdtdtdtdt"),
    ("Brand-new user", "trending only", "tttttttttt"),
]
s = So(7.4, 2.72)
X0, W, G = 2.56, 0.41, 0.06
for k in range(10):
    s.chu(X0 + k * (W + G) + W / 2, 0.2, "slot %d" % (k + 1), co=6.6, mau="#444444")
for r, (nhan, phu, mau) in enumerate(hang):
    y = 0.34 + r * 0.5
    s.chu(0.16, y + 0.12, nhan, co=7.6, canh="left", dam=True)
    s.chu(0.16, y + 0.29, phu, co=6.8, canh="left", nghieng=True, mau="#444444")
    for k, c in enumerate(mau):
        x = X0 + k * (W + G)
        nen, vien, _ = BANG[O[c][0]]
        s.ax.add_patch(FancyBboxPatch((x, y), W, 0.4, boxstyle="round,pad=0,rounding_size=0.04",
                                      fc=nen, ec=vien, lw=1.0, zorder=3))
        s.ax.text(x + W / 2, y + 0.2, c, fontsize=8.6, ha="center", va="center", zorder=4,
                  fontstyle="italic", fontweight="bold", color="#333333")
s.chu_giai(0.16, 2.36, [("hop", O[c][0], "%s: %s" % (c, O[c][1])) for c in "atmd"], cot=4, rong_cot=1.74,
           co=6.8, tieu_de=None, dong_cao=0.2)
s.luu("f11_mau_khoi.png")
