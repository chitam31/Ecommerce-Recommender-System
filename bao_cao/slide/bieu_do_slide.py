"""Biểu đồ cho slide: vẽ lại đúng số liệu của bao_cao/hinh/bieu_do.py, nhưng ở đúng kích thước hiển thị
trên slide (1 đơn vị SVG = 1 px của khung 1920×1080) và xuất SVG vector để chiếu hay phóng to đều nét.

Chữ trong SVG được đổi thành đường nét (svg.fonttype = path) bằng Liberation Serif — phông có cùng số đo
với Times New Roman — vì slide không nạp phông vào bên trong SVG; công thức dùng STIX (kiểu Times).
Mỗi tệp được làm gọn để nhúng thẳng vào slide: bỏ khai báo XML, metadata, <style>; width/height = viewBox.
Chạy:  python3 bieu_do_slide.py   → svg/*.svg
"""
import math
import os
import re

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

RA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "svg")
os.makedirs(RA, exist_ok=True)

PX = 1 / 72  # 1 px của slide = 1 pt của SVG
plt.rcParams.update({
    "svg.fonttype": "path", "svg.hashsalt": "bd03",
    "font.family": "serif", "font.serif": ["Liberation Serif"], "mathtext.fontset": "stix",
    "font.size": 24, "axes.titlesize": 26, "axes.labelsize": 24, "xtick.labelsize": 22, "ytick.labelsize": 22,
    "legend.fontsize": 22, "legend.frameon": False,
    "axes.linewidth": 1.4, "axes.spines.top": False, "axes.spines.right": False, "axes.edgecolor": "#333333",
    "xtick.direction": "in", "ytick.direction": "in", "xtick.major.width": 1.4, "ytick.major.width": 1.4,
    "xtick.major.size": 7, "ytick.major.size": 7, "xtick.major.pad": 8, "ytick.major.pad": 8,
    "axes.grid": True, "axes.grid.axis": "y", "grid.color": "#D0D0D0", "grid.linewidth": 1.0, "grid.linestyle": ":",
    "axes.axisbelow": True, "patch.linewidth": 1.2, "hatch.linewidth": 1.4, "lines.linewidth": 3, "lines.markersize": 13,
    "axes.titlelocation": "left", "axes.titlepad": 14, "text.color": "#1A1A1A", "axes.labelcolor": "#1A1A1A",
    "xtick.color": "#1A1A1A", "ytick.color": "#1A1A1A",
})
XANH, XANH_NHAT, XAM, CAM, DO = "#2F5D8F", "#A9C1DB", "#A6A6A6", "#C0691F", "#A12C2C"
VIEN = dict(edgecolor="#1F1F1F", linewidth=1.2)


def luu(fig, ten, nhan):
    duong = os.path.join(RA, ten + ".svg")
    fig.savefig(duong, format="svg", bbox_inches="tight", pad_inches=0.06, transparent=True)
    plt.close(fig)
    s = open(duong, encoding="utf-8").read()
    s = re.sub(r"<\?xml[^>]*\?>\s*", "", s)
    s = re.sub(r"<!DOCTYPE[^>]*>\s*", "", s)
    s = re.sub(r"<metadata>.*?</metadata>\s*", "", s, flags=re.S)
    s = re.sub(r"<style[^>]*>.*?</style>\s*", "", s, flags=re.S)
    s = re.sub(r"\s*<!--.*?-->", "", s, flags=re.S)
    assert "data:" not in s, ten + ": còn ảnh raster nhúng"
    s = s.replace("xlink:href", "href")
    # Toạ độ giữ 1 chữ số thập phân (0,1 px là đủ cho slide), bỏ id của nhóm và khoảng trắng thừa
    # để mỗi SVG dưới 52 KB (giới hạn nhúng thẳng vào slide).
    s = re.sub(r"(-?\d+\.\d)\d+", r"\1", s)
    s = re.sub(r'(<g)\s+id="[^"]*"', r"\1", s)
    s = re.sub(r"\s*\n\s*", " ", s)
    s = re.sub(r"<g>\s*</g>", "", s)
    # Tên glyph ngắn lại (#LiberationSerif-Bold-30 → #b30) và đường nét viết gọn (M 1 2 L 3 4 → M1 2L3 4).
    for dai, ngan in (("LiberationSerif-BoldItalic-", "bi_"), ("LiberationSerif-Bold-", "b_"),
                      ("LiberationSerif-Italic-", "i_"), ("LiberationSerif-", "r_"), ("STIXGeneral-Italic-", "si_"),
                      ("STIXGeneral-Regular-", "s_"), ("STIXGeneral-Bold-", "sb_"), ("STIXNonUnicode-Regular-", "sn_"),
                      ("STIXSizeOneSym-Regular-", "s1_"), ("STIXSizeTwoSym-Regular-", "s2_"), ("STIXSizeThreeSym-Regular-", "s3_")):
        s = s.replace(dai, ngan)
    s = re.sub(r'd="([^"]*)"', lambda m: 'd="%s"' % re.sub(r"\s*([MLQCZz])\s*", r"\1", m.group(1)).strip(), s)
    s = re.sub(r"\s*/>", "/>", s)
    m = re.search(r'viewBox="0 0 ([\d.]+) ([\d.]+)"', s)
    w, h = float(m.group(1)), float(m.group(2))
    s = re.sub(r'<svg[^>]*>', '<svg aria-label="%s" width="%s" height="%s" viewBox="0 0 %s %s">'
               % (nhan, m.group(1), m.group(2), m.group(1), m.group(2)), s, count=1)
    s = s.strip() + "\n"
    open(duong, "w", encoding="utf-8").write(s)
    print("%-14s %5.0f × %4.0f px  %5.1f KB" % (ten, w, h, len(s.encode()) / 1024))


# ---------------------------------------------------------------- tuning: ba bản đồ nhiệt
ndcg = {10: [[0.0158, 0.0213, 0.0228], [0.0148, 0.0221, 0.0233], [0.0121, 0.0234, 0.0241]],
        30: [[0.0252, 0.0396, 0.0493], [0.0250, 0.0409, 0.0490], [0.0227, 0.0454, 0.0505]],
        50: [[0.0313, 0.0526, 0.0668], [0.0328, 0.0531, 0.0662], [0.0309, 0.0574, 0.0680]]}
fig, axs = plt.subplots(1, 3, figsize=(1500 * PX, 470 * PX), sharey=True, layout="constrained")
for ax, r in zip(axs, (10, 30, 50)):
    m = np.array(ndcg[r])
    im = ax.pcolormesh(np.arange(4) - 0.5, np.arange(4) - 0.5, m, cmap="cividis", vmin=0.01, vmax=0.07,
                       edgecolors="white", linewidth=2)
    ax.set_xlim(-0.5, 2.5); ax.set_ylim(2.5, -0.5)
    for i in range(3):
        for j in range(3):
            tot = (r == 50 and i == 2 and j == 2)
            ax.text(j, i, "%.4f" % m[i, j], ha="center", va="center", fontsize=24,
                    color="white" if m[i, j] < 0.042 else "black", fontweight="bold" if tot else "normal")
    if True:
        ax.add_patch(plt.Rectangle((1.5, 1.5), 1, 1, fill=False, ec="#A12C2C", lw=3.5)) if r == 50 else None
    ax.set_xticks(range(3), ["1", "10", "40"]); ax.set_yticks(range(3), ["0.01", "0.1", "1.0"])
    ax.set_xlabel(r"$\alpha$ (confidence weight)")
    ax.set_title("(%s) rank = %d" % ("abc"[(10, 30, 50).index(r)], r))
    ax.grid(False); ax.tick_params(length=0)
    for sp in ax.spines.values():
        sp.set_visible(False)
axs[0].set_ylabel(r"regParam $\lambda$")
cb = fig.colorbar(im, ax=axs, shrink=0.9, pad=0.02, aspect=14, boundaries=np.linspace(0.01, 0.07, 25))
cb.solids.set_rasterized(False); cb.set_label("NDCG@10"); cb.outline.set_linewidth(1.0); cb.ax.tick_params(direction="in", width=1.0, labelsize=22)
luu(fig, "tuning", "NDCG@10 over the 27-combination grid: three heat maps for rank 10, 30 and 50; best cell rank 50, regParam 1.0, alpha 40 = 0.0680")

# ---------------------------------------------------------------- baselines: NDCG và coverage
ten = ["Global\nbest-sellers", "Category\nbest-sellers", "ALS", "Re-rank\nβ = 0.1", "Re-rank\nβ = 0.3", "Served\nβ = 1.0, 200"]
nd = [0.0051, 0.0475, 0.0680, 0.0702, 0.0746, 0.0865]
cov = [0.01, 5.28, 3.44, 3.65, 4.14, 6.47]
mau = [XAM, XAM, XANH_NHAT, XANH_NHAT, XANH_NHAT, XANH]
fig, (a1, a2) = plt.subplots(1, 2, figsize=(1560 * PX, 500 * PX))
a1.bar(range(6), nd, color=mau, width=0.64, **VIEN); a1.set_xticks(range(6), ten, fontsize=20)
a1.set_title("(a) NDCG@10, test month, 9,467 users")
for i, v in enumerate(nd):
    a1.text(i, v + 0.0018, "%.4f" % v, ha="center", fontsize=21)
a1.set_ylim(0, 0.098)
a2.bar(range(6), cov, color=mau, width=0.64, **VIEN); a2.set_xticks(range(6), ten, fontsize=20)
a2.set_title("(b) Coverage of training items (%)")
a2.axhline(10, color=DO, ls="--", lw=2.2); a2.text(5.45, 10.35, "target 10 %", color=DO, fontsize=22, ha="right", fontstyle="italic")
for i, v in enumerate(cov):
    a2.text(i, v + 0.22, "%.2f" % v, ha="center", fontsize=21)
a2.set_ylim(0, 11.8)
fig.tight_layout(w_pad=3)
luu(fig, "baselines", "NDCG@10 and coverage of the two baselines, ALS, two re-ranked variants and the served configuration")

# ---------------------------------------------------------------- popularity bias: cột chồng 0–100 %
nhom = ["N1 (top 10 %)", "N2", "N3", "N4", "N5–N10"]
dl = {"Global best-sellers": [100, 0, 0, 0, 0], "Category best-sellers": [86.5, 8.1, 2.6, 1.3, 1.4],
      "ALS": [99.2, 0.6, 0.1, 0.0, 0.1], "ALS + re-rank β = 0.3": [98.3, 1.3, 0.2, 0.1, 0.1],
      "Served (β = 1.0, 200 cand.)": [80.8, 11.2, 4.3, 2.0, 1.9]}
mau_n = ["#24405F", "#5B7FA6", "#B3C6DB", "#E6C9A8", "#B07A45"]
fig, ax = plt.subplots(figsize=(1440 * PX, 450 * PX))
y = np.arange(len(dl))[::-1]
trai = np.zeros(len(dl))
for k, n in enumerate(nhom):
    v = np.array([dl[m][k] for m in dl])
    ax.barh(y, v, left=trai, color=mau_n[k], label=n, height=0.62, **VIEN)
    trai += v
for yi, m in zip(y, dl):
    ax.text(1.5, yi, "N1 = %.1f %%" % dl[m][0], ha="left", va="center", fontsize=22, color="white", fontweight="bold")
ax.set_yticks(y, list(dl.keys()), fontsize=22); ax.set_xlim(0, 100); ax.set_xticks(range(0, 101, 10))
ax.set_xlabel("Share of recommendation slots (%), test month, 9,467 users × 10 slots")
ax.grid(True, axis="x"); ax.grid(axis="y", visible=False); ax.tick_params(axis="y", length=0)
ax.legend(ncol=5, loc="lower center", bbox_to_anchor=(0.42, 1.0), handlelength=1.4, columnspacing=1.6,
          title="Popularity decile of the recommended item (training interactions)", title_fontsize=22)
luu(fig, "pho_bien", "Share of recommendation slots by popularity decile for five methods; ALS puts 99.2 percent in the top decile, the served configuration 80.8 percent")

# ---------------------------------------------------------------- re-ranking: đánh đổi độ chính xác – coverage
d50 = [(0, 3.44, 0.0680), (0.3, 4.14, 0.0746), (0.5, 4.65, 0.0797), (1.0, 5.67, 0.0854), (2.0, 6.30, 0.0683)]
d200 = [(0.5, 4.74, 0.0789), (1.0, 6.47, 0.0865), (2.0, 7.21, 0.0518)]
fig, ax = plt.subplots(figsize=(1000 * PX, 590 * PX))
ax.plot([p[1] for p in d50], [p[2] for p in d50], "-o", color=XANH, label="50 candidates", mfc="white", mew=2.6)
ax.plot([p[1] for p in d200], [p[2] for p in d200], "--s", color=CAM, label="200 candidates", mfc="white", mew=2.6)
lech50 = {0: (-10, 14), 0.3: (-14, 14), 0.5: (-30, 14), 1.0: (-16, 14), 2.0: (10, 8)}
for b, c, n in d50:
    ax.annotate(r"$\beta$ = %s" % b, (c, n), textcoords="offset points", xytext=lech50[b], fontsize=21, color=XANH,
                ha="left" if b == 2.0 else "center")
for b, c, n in d200:
    ax.annotate(r"$\beta$ = %s" % b, (c, n), textcoords="offset points", xytext=(12, -24), fontsize=21, color=CAM)
ax.scatter([5.28], [0.0475], marker="^", s=160, color=XAM, edgecolor="#1F1F1F", linewidth=1.2, zorder=3)
ax.annotate("Category best-sellers", (5.28, 0.0475), xytext=(12, -8), textcoords="offset points", fontsize=21, color="#444444")
ax.axvline(10, color=DO, ls="--", lw=2.2)
ax.text(9.85, 0.0425, "target 10 %", color=DO, fontsize=22, ha="right", fontstyle="italic")
ax.annotate("served", (6.47, 0.0865), xytext=(7.55, 0.0905), fontsize=22, fontweight="bold",
            arrowprops=dict(arrowstyle="-|>", color="#333333", lw=1.6, mutation_scale=18))
ax.grid(True, axis="both")
ax.set_xlabel("Coverage of training items (%)"); ax.set_ylabel("NDCG@10")
ax.set_xlim(3, 10.5); ax.set_ylim(0.04, 0.095)
ax.legend(loc="lower left", handlelength=2.4)
luu(fig, "xep_lai", "Accuracy versus coverage of novelty re-ranking for 50 and 200 candidates; beta 1.0 with 200 candidates is the served configuration")

# ---------------------------------------------------------------- Z-ORDER
gd = ["Fragmented\n200 files", "OPTIMIZE only\n67 files", "OPTIMIZE + ZORDER BY\nuser, 55 files"]
med, p95, doc = [10793, 8280, 980], [13420, 9524, 1387], ["200", "67", "1.07"]
fig, ax = plt.subplots(figsize=(1000 * PX, 520 * PX))
x = np.arange(3)
ax.bar(x - 0.19, med, 0.38, color=XANH, label="median", **VIEN)
ax.bar(x + 0.19, p95, 0.38, color=XANH_NHAT, label="p95", hatch="//", **VIEN)
for i in range(3):
    ax.text(x[i] - 0.19, med[i] + 280, "{:,}".format(med[i]), ha="center", fontsize=21)
    ax.text(x[i], max(med[i], p95[i]) + 1500, "files read: %s" % doc[i], ha="center", fontsize=22, color=DO, fontstyle="italic")
ax.set_xticks(x, gd, fontsize=21); ax.set_ylabel("Per-user lookup latency (ms)"); ax.set_ylim(0, 17000)
ax.legend(loc="upper right", title="30 lookups per phase", title_fontsize=21)
luu(fig, "zorder", "Median and p95 latency of the per-user lookup and files read per query in three phases: fragmented, OPTIMIZE only, OPTIMIZE with Z-ORDER")


# ---------------------------------------------------------------- A/B: CTR và chuyển đổi, khoảng tin cậy 95 %
def kt(x_, n):
    p = x_ / n
    return 100 * p, 100 * 1.96 * math.sqrt(p * (1 - p) / n)


ctr = [kt(3685, 175960), kt(3446, 174200)]
cvr = [kt(412, 175960), kt(417, 174200)]
fig, (a1, a2) = plt.subplots(1, 2, figsize=(960 * PX, 560 * PX))
for ax, d, t, lim in ((a1, ctr, "(a) CTR (%)", (1.80, 2.22)), (a2, cvr, "(b) Conversion (%)", (0.18, 0.29))):
    ax.bar([0, 1], [v[0] for v in d], yerr=[v[1] for v in d], color=[XANH, XANH_NHAT], hatch=["", "//"], capsize=10,
           width=0.56, error_kw=dict(elinewidth=1.8, capthick=1.8), **VIEN)
    ax.set_xticks([0, 1], ["A\nALS only", "B\n+ 30 % trending"], fontsize=21)
    ax.set_title(t)
    for i, v in enumerate(d):
        ax.text(i, v[0] + v[1] + (lim[1] - lim[0]) * 0.03, "%.3f" % v[0], ha="center", fontsize=21)
    ax.set_ylim(*lim)
a1.text(0.5, 0.94, "p = 0.015", transform=a1.transAxes, ha="center", fontsize=23, fontweight="bold", color=DO)
a2.text(0.5, 0.94, "p = 0.75", transform=a2.transAxes, ha="center", fontsize=23, fontweight="bold", color="#444444")
fig.tight_layout(w_pad=2.5)
luu(fig, "ab", "CTR and conversion of groups A and B with 95 percent confidence intervals; CTR p = 0.015, conversion p = 0.75")

# ---------------------------------------------------------------- công thức ALS
CT = {
    "eq_pref": r"$p_{ui}=\mathbf{1}\,[\,r_{ui}>0\,],\qquad c_{ui}=1+\alpha\, r_{ui}$",
    "eq_als": r"$\min_{X,\,Y}\ \sum_{u,i} c_{ui}\left(p_{ui}-x_u^{\top}y_i\right)^2+\lambda\left(\sum_u n_u\Vert x_u\Vert^2+\sum_i n_i\Vert y_i\Vert^2\right)$",
}
for k, bt in CT.items():
    fig = plt.figure(figsize=(0.01, 0.01))
    fig.text(0, 0, bt, fontsize=40)
    luu(fig, k, "Preference and confidence of an observation" if k == "eq_pref" else "ALS objective for implicit feedback")
