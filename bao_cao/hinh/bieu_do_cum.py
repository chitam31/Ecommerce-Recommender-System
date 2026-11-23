"""Biểu đồ từ bằng chứng thu trên cụm ngày 16/11/2026 (scripts/thu_bang_chung.sh).

Đọc thẳng docs/bang_chung/cum/eda.json và phan_tich_loi.json, không chép tay số liệu:
  c08_eda.png            sự kiện theo ngày, theo giờ UTC, phân bố số sự kiện mỗi người dùng (c08_eda_en: tiếng Anh)
  c09_phan_tich_loi.png  NDCG@10 của ALS và hai baseline theo ba cách chia nhóm người dùng

Chạy trong bao_cao/hinh:  python3 bieu_do_cum.py
"""
import json
import os
from datetime import date

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.ticker  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

plt.rcParams.update({
    "font.family": "serif", "font.serif": ["Liberation Serif", "DejaVu Serif"], "mathtext.fontset": "stix",
    "font.size": 8, "axes.titlesize": 8.5, "axes.labelsize": 8,
    "axes.linewidth": 0.6, "axes.spines.top": False, "axes.spines.right": False,
    "xtick.direction": "in", "ytick.direction": "in", "xtick.major.width": 0.6, "ytick.major.width": 0.6,
    "xtick.major.size": 3, "ytick.major.size": 3, "xtick.labelsize": 7.5, "ytick.labelsize": 7.5,
    "axes.grid": True, "axes.grid.axis": "y", "grid.color": "#D9D9D9", "grid.linewidth": 0.4, "grid.linestyle": ":",
    "axes.axisbelow": True, "legend.frameon": False, "legend.fontsize": 7,
    "patch.linewidth": 0.5, "lines.linewidth": 1.0,
    "savefig.dpi": 300, "svg.fonttype": "path", "savefig.bbox": "tight", "savefig.pad_inches": 0.03})
XANH, CAM, XAM = "#3E6A9E", "#C47A3A", "#A6A6A6"
VIEN = dict(edgecolor="#1F1F1F", linewidth=0.5)
CUM = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "docs", "bang_chung", "cum")


def doc(ten):
    with open(os.path.join(CUM, ten), encoding="utf-8") as f:
        return json.load(f)


def so_vn(x, _=None):
    return "{:,.0f}".format(x).replace(",", ".")


# ---------------------------------------------------------------- C8 EDA (bản tiếng Việt và tiếng Anh)
eda = doc("eda.json")
NHAN_C8 = {
    "vi": ("Nghìn sự kiện / ngày", "(a) Sự kiện theo ngày", "Giờ (UTC)", "Nghìn sự kiện", "(b) Theo giờ trong ngày",
           "Số sự kiện của một người dùng", "Số người dùng (log)", "(c) Phân bố người dùng", "c08_eda"),
    "en": ("Thousand events / day", "(a) Events per day", "Hour (UTC)", "Thousand events", "(b) By hour of day",
           "Events per user", "Users (log)", "(c) Distribution of users", "c08_eda_en"),
}
for nn, (y1, t1, x2, y2, t2, x3, y3, t3, ten_tep) in NHAN_C8.items():
    fig, (a1, a2, a3) = plt.subplots(1, 3, figsize=(7.2, 2.4), gridspec_kw={"width_ratios": [1.5, 1, 1]})
    ngay = [date.fromisoformat(d["ngay"]) for d in eda["theo_ngay"]]
    a1.plot(ngay, [d["so_su_kien"] / 1000 for d in eda["theo_ngay"]], color=XANH)
    a1.set_ylabel(y1)
    a1.set_xlim(ngay[0], ngay[-1])
    a1.set_ylim(bottom=0)
    a1.xaxis.set_major_locator(mdates.MonthLocator())
    a1.xaxis.set_major_formatter(mdates.DateFormatter("%m/%Y"))
    a1.set_title(t1, loc="left")
    gio = eda["theo_gio_utc"]
    a2.bar([d["gio"] for d in gio], [d["count"] / 1000 for d in gio], width=0.8, color=XANH, **VIEN)
    a2.set_xticks([0, 6, 12, 18, 23])
    a2.set_xlabel(x2)
    a2.set_ylabel(y2)
    a2.set_title(t2, loc="left")
    pb = eda["phan_bo_nguoi_dung"]
    a3.bar(range(len(pb)), [d["so_luong"] for d in pb], color=XANH, **VIEN)
    a3.set_yscale("log")
    a3.set_xticks(range(len(pb)), [d["nhom"] for d in pb], rotation=45, ha="right")
    a3.set_xlabel(x3)
    a3.set_ylabel(y3)
    a3.set_title(t3, loc="left")
    fig.tight_layout()
    fig.savefig(ten_tep + ".png")
    fig.savefig(ten_tep + ".svg")
    plt.close(fig)

# ---------------------------------------------------------------- C9 phân tích lỗi
pt = doc("phan_tich_loi.json")
TEN_DUOI_DAI = {"duoi dai (0% SP test pho bien)": "Đuôi dài\n(0 % phổ biến)", "tron": "Trộn",
                "dau (>=50% SP test pho bien)": "Đầu\n(≥ 50 % phổ biến)"}
KHUNG = [("theo_so_tuong_tac", "nhom_tuong_tac", "(a) Theo số tương tác trong train", lambda x: x),
         ("theo_pho_bien_test", "nhom_pho_bien_test", "(b) Theo độ phổ biến của SP kiểm tra", TEN_DUOI_DAI.get),
         ("theo_so_danh_muc", "nhom_danh_muc", "(c) Theo số danh mục đã xem", lambda x: x)]
fig, axs = plt.subplots(1, 3, figsize=(7.2, 2.6), sharey=True)
for ax, (khoa, cot, tieu_de, nhan) in zip(axs, KHUNG):
    nhom = pt[khoa]
    x = np.arange(len(nhom))
    w = 0.27
    for i, (k, ten, mau) in enumerate((("ndcg_b1", "Baseline 1 (toàn sàn)", XAM), ("ndcg_b2", "Baseline 2 (danh mục)", CAM),
                                       ("ndcg_als", "ALS", XANH))):
        ax.bar(x + (i - 1) * w, [d[k] for d in nhom], width=w, color=mau, label=ten, **VIEN)
    ax.set_xticks(x, ["{}\nn = {}".format(nhan(d[cot]), so_vn(d["so_nguoi"])) for d in nhom], fontsize=6.5)
    ax.set_title(tieu_de, loc="left")
axs[0].set_ylabel("NDCG@10")
axs[0].yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: "{:.2f}".format(v).replace(".", ",")))
axs[0].legend(loc="upper right")
fig.tight_layout()
fig.savefig("c09_phan_tich_loi.png")
fig.savefig("c09_phan_tich_loi.svg")
plt.close(fig)
print("ok: c08_eda, c09_phan_tich_loi")

# ---------------------------------------------------------------- C9 bản tiếng Anh cho slide bảo vệ
TEN_EN = {"duoi dai (0% SP test pho bien)": "Long tail\n(0 % popular)", "tron": "Mixed",
          "dau (>=50% SP test pho bien)": "Head\n(≥ 50 % popular)"}
KHUNG_EN = [("theo_so_tuong_tac", "nhom_tuong_tac", "(a) By training interactions", lambda x: x),
            ("theo_pho_bien_test", "nhom_pho_bien_test", "(b) By popularity of test items", TEN_EN.get),
            ("theo_so_danh_muc", "nhom_danh_muc", "(c) By categories viewed", lambda x: x)]
fig, axs = plt.subplots(1, 3, figsize=(7.2, 2.6), sharey=True)
for ax, (khoa, cot, tieu_de, nhan) in zip(axs, KHUNG_EN):
    nhom = pt[khoa]
    x = np.arange(len(nhom))
    w = 0.27
    for i, (k, ten, mau) in enumerate((("ndcg_b1", "Global best-sellers", XAM), ("ndcg_b2", "Category best-sellers", CAM),
                                       ("ndcg_als", "ALS", XANH))):
        ax.bar(x + (i - 1) * w, [d[k] for d in nhom], width=w, color=mau, label=ten, **VIEN)
    ax.set_xticks(x, ["{}\nn = {:,}".format(nhan(d[cot]), d["so_nguoi"]) for d in nhom], fontsize=6.5)
    ax.set_title(tieu_de, loc="left")
axs[0].set_ylabel("NDCG@10")
axs[0].legend(loc="upper right")
fig.tight_layout()
fig.savefig("c09_phan_tich_loi_en.png")
fig.savefig("c09_phan_tich_loi_en.svg")
plt.close(fig)
print("ok: c09_phan_tich_loi_en")
