"""ve.py - thư viện vẽ sơ đồ khối cho báo cáo và slide (matplotlib, tọa độ inch, trục y hướng xuống).

Phong cách theo hình kiến trúc của các bài báo hội nghị (vẽ bằng draw.io): khối tô màu nhạt có viền
đậm cùng tông, khung phân vùng tô rất nhạt có tiêu đề ở giữa, mũi tên phân loại theo màu và nét,
khung chú giải ký hiệu, viền ngoài bao toàn hình. Chữ có chân (Liberation Serif, cùng số đo với
Times New Roman). Mỗi hình lưu hai bản: PNG 300 dpi cho báo cáo và SVG vector (chữ đổi thành đường
cong) cho slide, nên phóng to hay chiếu lên màn hình lớn vẫn nét.
"""
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Arc, Ellipse, FancyArrowPatch, FancyBboxPatch, Rectangle  # noqa: E402

plt.rcParams["font.family"] = "serif"
plt.rcParams["font.serif"] = ["Liberation Serif", "DejaVu Serif"]
plt.rcParams["mathtext.fontset"] = "stix"
plt.rcParams["svg.fonttype"] = "path"

# Bảng màu chuẩn của draw.io: (nền khối, viền khối, nền khung phân vùng)
BANG = {
    "xam": ("#F5F5F5", "#666666", "#FAFAFA"),
    "trang": ("#FFFFFF", "#666666", "#FFFFFF"),
    "kafka": ("#DAE8FC", "#6C8EBF", "#F3F7FD"),
    "luong": ("#D5E8D4", "#82B366", "#F4F9F3"),
    "lo": ("#FFE6CC", "#D79B00", "#FFF9F1"),
    "gold": ("#FFF2CC", "#D6B656", "#FFFCF2"),
    "af": ("#F8CECC", "#B85450", "#FDF4F3"),
    "feat": ("#E1D5E7", "#9673A6", "#F8F4FA"),
    "web": ("#B0E3E6", "#0E8088", "#EFF9FA"),
    "bronze": ("#F3DCC6", "#A0613A", "#FCF6F0"),
    "silver": ("#E6E6E6", "#7F7F7F", "#F7F7F7"),
    "hdfs": ("#FFF2CC", "#D6B656", "#FFFCF2"),
    "spark": ("#FFE6CC", "#D79B00", "#FFF9F1"),
}
# Tên cũ dùng trong các script → khóa bảng màu
_TEN_CU = {"nguon": "xam", "may": "xam", "trang": "trang"}
MAU = {k: v[0] for k, v in BANG.items()}
MAU.update({k: BANG[v][0] for k, v in _TEN_CU.items()})
_THEO_NEN = {v[0].upper(): k for k, v in BANG.items()}

# Loại mũi tên: (màu, kiểu nét)
MUI = {
    "du_lieu": ("#333333", "-"),       # dữ liệu theo lô
    "luong": ("#2E7D32", "-"),         # dữ liệu luồng liên tục
    "dieu_khien": ("#B85450", "--"),   # điều phối / kích hoạt
    "phan_hoi": ("#1F5FAD", "--"),     # phản hồi của người dùng (vòng khép kín)
    "doc": ("#0E8088", "-"),           # web đọc để phục vụ
}
CHU = "#1A1A1A"
NET = 0.9


def khoa_mau(mau):
    """Trả về khóa bảng màu từ khóa hoặc mã màu nền; None nếu là màu tự do."""
    if mau in BANG:
        return mau
    if mau in _TEN_CU:
        return _TEN_CU[mau]
    if isinstance(mau, str):
        return _THEO_NEN.get(mau.upper())
    return None


class So:
    def __init__(self, rong, cao, dpi=300, y0=0.0, khung=True):
        self.rong, self.cao, self.y0, self.khung = rong, cao, y0, khung
        self.fig = plt.figure(figsize=(rong, cao), dpi=dpi)
        self.ax = self.fig.add_axes([0, 0, 1, 1])
        self.ax.set_xlim(0, rong)
        self.ax.set_ylim(cao + y0, y0)
        self.ax.axis("off")
        self.hops = {}
        self.canh_nhom = "center"

    # ------------------------------------------------------------------ khung phân vùng
    def nhom(self, x, y, w, h, nhan, mau="xam", vien=None, co=8.4, net="-", canh=None, dam=False, duoi=False):
        canh = canh or self.canh_nhom
        k = khoa_mau(mau)
        nen, vien_k = (BANG[k][2], BANG[k][1]) if k else (mau, "#999999")
        vien = vien_k if (vien is None or k) else vien
        self.ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0,rounding_size=0.035",
                                         fc=nen, ec=vien, lw=0.8, ls=net, zorder=1))
        if nhan:
            tx = x + w / 2 if canh == "center" else x + 0.08
            self.ax.text(tx, (y + h - 0.12) if duoi else (y + 0.13), nhan, ha=canh, va="center", fontsize=co, color="#262626",
                         fontweight="bold" if dam else "normal", zorder=6,
                         bbox=dict(boxstyle="square,pad=0.05", fc=nen, ec="none"))
        return self

    # ------------------------------------------------------------------ khối
    def hop(self, ten, x, y, w, h, chu, mau="trang", co=7.4, dam_dong_dau=True, kieu="tron",
            vien=None, lw=NET, chu_mau=CHU, canh="center", nghieng_dong=()):
        k = khoa_mau(mau)
        nen, vien_k = (BANG[k][0], BANG[k][1]) if k else (mau, "#666666")
        vien = vien or vien_k
        if kieu == "tru":  # hình trụ: topic Kafka, bảng dữ liệu
            e = min(0.075, h * 0.12)
            self.ax.add_patch(Ellipse((x + w / 2, y + h - e), w, 2 * e, fc=nen, ec="none", zorder=3))
            self.ax.add_patch(Rectangle((x, y + e), w, h - 2 * e, fc=nen, ec="none", zorder=3))
            self.ax.add_patch(Arc((x + w / 2, y + h - e), w, 2 * e, theta1=0, theta2=180, ec=vien, lw=lw, zorder=4))
            self.ax.plot([x, x], [y + e, y + h - e], color=vien, lw=lw, zorder=4)
            self.ax.plot([x + w, x + w], [y + e, y + h - e], color=vien, lw=lw, zorder=4)
            self.ax.add_patch(Ellipse((x + w / 2, y + e), w, 2 * e, fc=nen, ec=vien, lw=lw, zorder=5))
        elif kieu == "thoi":  # hình thoi: nhánh rẽ
            self.ax.add_patch(plt.Polygon([(x + w / 2, y), (x + w, y + h / 2), (x + w / 2, y + h), (x, y + h / 2)],
                                          closed=True, fc=nen, ec=vien, lw=lw, zorder=3))
        else:
            r = {"tron": 0.05, "vuong": 0.0}.get(kieu, 0.05)
            self.ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0,rounding_size=%s" % r,
                                             fc=nen, ec=vien, lw=lw, zorder=3))
        dong = chu.split("\n") if chu else []
        tx = x + w / 2 if canh == "center" else x + 0.07
        cy = y + h / 2 + (0.045 if kieu == "tru" else 0)
        buoc = co / 72 * 1.3
        y0 = cy - buoc * (len(dong) - 1) / 2
        for i, d in enumerate(dong):
            self.ax.text(tx, y0 + i * buoc, d, ha=canh, va="center", fontsize=co, color=chu_mau,
                         fontweight="bold" if (i == 0 and dam_dong_dau) else "normal",
                         fontstyle="italic" if i in nghieng_dong else "normal", zorder=6)
        self.hops[ten] = (x, y, w, h)
        return self

    def diem(self, ten, phia="s", t=0.5):
        x, y, w, h = self.hops[ten]
        return {"n": (x + w * t, y), "s": (x + w * t, y + h), "w": (x, y + h * t), "e": (x + w, y + h * t)}[phia]

    # ------------------------------------------------------------------ mũi tên
    def _kieu(self, loai, mau, net):
        m, n = MUI.get(loai, MUI["du_lieu"])
        return (mau or m), (net or n)

    def mui(self, a, b, tu="s", den="n", ta=0.5, tb=0.5, nhan=None, net=None, mau=None, cong=0.0,
            hai_chieu=False, co=6.6, vi_tri_nhan=0.5, lech_nhan=(0, 0), lw=NET, nen_nhan=True, loai="du_lieu"):
        mau, net = self._kieu(loai, mau, net)
        p = self.diem(a, tu, ta) if isinstance(a, str) else a
        q = self.diem(b, den, tb) if isinstance(b, str) else b
        kieu = "<|-|>" if hai_chieu else "-|>"
        self.ax.add_patch(FancyArrowPatch(p, q, arrowstyle=kieu, mutation_scale=8.5, color=mau, lw=lw, ls=net,
                                          connectionstyle="arc3,rad=%s" % cong, zorder=2.5,
                                          shrinkA=0, shrinkB=0, joinstyle="miter", capstyle="butt"))
        if nhan:
            mx = p[0] + (q[0] - p[0]) * vi_tri_nhan + lech_nhan[0]
            my = p[1] + (q[1] - p[1]) * vi_tri_nhan + lech_nhan[1]
            self.ax.text(mx, my, nhan, fontsize=co, ha="center", va="center", color="#262626", zorder=7,
                         fontstyle="italic",
                         bbox=dict(boxstyle="square,pad=0.08", fc="white", ec="none", alpha=0.95) if nen_nhan else None)
        return self

    def duong(self, diem_list, net=None, mau=None, mui_cuoi=True, lw=NET, loai="du_lieu", mui_dau=False):
        """Đường gấp khúc qua các điểm, mũi tên ở đoạn cuối (và đoạn đầu nếu mui_dau)."""
        mau, net = self._kieu(loai, mau, net)
        xs, ys = zip(*diem_list)
        self.ax.plot(xs[1:-1] if mui_dau else xs[:-1], ys[1:-1] if mui_dau else ys[:-1],
                     color=mau, lw=lw, ls=net, zorder=2.5, solid_joinstyle="miter")
        if len(diem_list) > 2 or not mui_dau:
            self.ax.add_patch(FancyArrowPatch(diem_list[-2], diem_list[-1], arrowstyle="-|>" if mui_cuoi else "-",
                                              mutation_scale=8.5, color=mau, lw=lw, ls=net, zorder=2.5,
                                              shrinkA=0, shrinkB=0))
        if mui_dau:
            self.ax.add_patch(FancyArrowPatch(diem_list[1], diem_list[0], arrowstyle="-|>",
                                              mutation_scale=8.5, color=mau, lw=lw, ls=net, zorder=2.5,
                                              shrinkA=0, shrinkB=0))
        return self

    def chu(self, x, y, noi_dung, co=7, canh="center", dam=False, mau=CHU, nghieng=False, xoay=0, nen=False):
        self.ax.text(x, y, noi_dung, fontsize=co, ha=canh, va="center", color=mau, rotation=xoay,
                     fontweight="bold" if dam else "normal", fontstyle="italic" if nghieng else "normal", zorder=7,
                     bbox=dict(boxstyle="square,pad=0.06", fc="white", ec="none") if nen else None)
        return self

    def so_tron(self, x, y, n, co=6.6, mau="#333333"):
        """Số thứ tự trong vòng tròn đặc (đánh dấu các bước)."""
        self.ax.text(x, y, n, fontsize=co, ha="center", va="center", color="white", fontweight="bold", zorder=8,
                     bbox=dict(boxstyle="circle,pad=0.16", fc=mau, ec="none"))
        return self

    # ------------------------------------------------------------------ chú giải
    def chu_giai(self, x, y, muc, cot=1, rong_cot=1.6, co=6.6, tieu_de="Legend", dong_cao=0.2):
        """Khung chú giải. muc: ("hop"|"tru", khóa màu, nhãn) hoặc ("mui", loại mũi tên, nhãn)."""
        so_dong = (len(muc) + cot - 1) // cot
        top = 0.22 if tieu_de else 0.06
        w, h = cot * rong_cot + 0.12, top + so_dong * dong_cao + 0.06
        self.ax.add_patch(Rectangle((x, y), w, h, fc="white", ec="#666666", lw=0.7, zorder=6))
        if tieu_de:
            self.ax.text(x + 0.08, y + 0.12, tieu_de, fontsize=co + 0.4, fontweight="bold", va="center", zorder=7)
        for i, (loai, k, nhan) in enumerate(muc):
            c, r = i // so_dong, i % so_dong
            bx, by = x + 0.08 + c * rong_cot, y + top + r * dong_cao + dong_cao / 2
            if loai == "mui":
                m, n = MUI[k]
                self.ax.add_patch(FancyArrowPatch((bx, by), (bx + 0.34, by), arrowstyle="-|>", mutation_scale=8,
                                                  color=m, lw=NET, ls=n, zorder=7, shrinkA=0, shrinkB=0))
            else:
                nen, vien, _ = BANG[k]
                if loai == "tru":
                    self.ax.add_patch(Rectangle((bx + 0.04, by - 0.04), 0.26, 0.08, fc=nen, ec="none", zorder=7))
                    self.ax.add_patch(Ellipse((bx + 0.17, by + 0.04), 0.26, 0.05, fc=nen, ec=vien, lw=0.7, zorder=8))
                    self.ax.plot([bx + 0.04, bx + 0.04], [by - 0.04, by + 0.04], color=vien, lw=0.7, zorder=8)
                    self.ax.plot([bx + 0.30, bx + 0.30], [by - 0.04, by + 0.04], color=vien, lw=0.7, zorder=8)
                    self.ax.add_patch(Ellipse((bx + 0.17, by - 0.04), 0.26, 0.05, fc=nen, ec=vien, lw=0.7, zorder=8))
                else:
                    self.ax.add_patch(FancyBboxPatch((bx + 0.02, by - 0.055), 0.3, 0.11,
                                                     boxstyle="round,pad=0,rounding_size=0.02",
                                                     fc=nen, ec=vien, lw=0.8, zorder=7))
            self.ax.text(bx + 0.42, by, nhan, fontsize=co, va="center", ha="left", color=CHU, zorder=7)
        return (w, h)

    def luu(self, ten_tep):
        if self.khung:
            self.ax.add_patch(Rectangle((0.015, self.y0 + 0.015), self.rong - 0.03, self.cao - 0.03,
                                        fc="none", ec="#222222", lw=1.1, zorder=20))
        goc = os.path.splitext(ten_tep)[0]
        self.fig.savefig(goc + ".png", dpi=300, facecolor="white")
        self.fig.savefig(goc + ".svg", facecolor="white")
        plt.close(self.fig)
