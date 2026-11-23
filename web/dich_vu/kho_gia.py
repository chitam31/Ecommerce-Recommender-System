"""Kho dữ liệu GIẢ LẬP cho web: dùng cho test tự động và xem giao diện khi chưa có cụm.

Bật bằng biến môi trường WEB_CHE_DO=gia. Mọi trang đều hiện dòng cảnh báo "dữ liệu giả
lập" để không ai nhầm số liệu ở đây với kết quả thật của đề tài.
"""
import random
import time
from datetime import date, datetime, timedelta

from common.thong_ke import ztest_hai_ty_le
from common.tron import (
    NGUON_ALS,
    NGUON_DANH_MUC,
    NGUON_THINH_HANH,
    chon_nhom,
    danh_sach_it_tuong_tac,
    danh_sach_nguoi_quen,
)

SO_SAN_PHAM = 300
SO_DANH_MUC = 10


class KhoGia:
    la_gia_lap = True

    def __init__(self, hat_giong=42):
        self._tai_lap_da_ghi = []
        rnd = random.Random(hat_giong)
        self._san_pham = {
            "sp{}".format(i): {
                "ma_san_pham": "sp{}".format(i),
                "ma_danh_muc": "dm{}".format(i % SO_DANH_MUC),
                "gia_san_pham": float(rnd.randint(50, 900) * 1000),
            }
            for i in range(1, SO_SAN_PHAM + 1)
        }
        self._diem = {k: rnd.random() for k in self._san_pham}
        self._them_gio_moi = {}
        self._luc_them_gio = {}  # epoch ms của từng lượt thêm giỏ, để giả lập cửa sổ theo phút
        self._goi_y = {}
        ma = list(self._san_pham)
        th = self.thinh_hanh(30)
        for i in range(1, 9):
            u = "u_als_{}".format(i)
            als = [
                {"ma_san_pham": m, "diem_du_doan": round(1.0 - j * 0.03, 3), "nguon": NGUON_ALS}
                for j, m in enumerate(rnd.sample(ma, 20))
            ]
            moi = [{"ma_san_pham": m, "diem_du_doan": 0.0, "nguon": NGUON_DANH_MUC} for m in rnd.sample(ma, 4)]
            self._goi_y[u] = self._dong_goi_y(u, danh_sach_nguoi_quen(chon_nhom(u), als, th, moi))
        for i in range(1, 5):
            u = "u_it_{}".format(i)
            dm = self.ban_chay_danh_muc("dm{}".format(i))
            self._goi_y[u] = self._dong_goi_y(u, danh_sach_it_tuong_tac(dm, th))

    def _dong_goi_y(self, u, ds):
        return [dict(g, xep_hang=i + 1, ma_nguoi_dung=u, phien_ban_mo_hinh="als_gia_lap") for i, g in enumerate(ds)]

    # ----- gợi ý
    def goi_y_cua(self, ma_nguoi_dung):
        return list(self._goi_y.get(ma_nguoi_dung, []))

    def thinh_hanh(self, n=10):
        diem = {k: v + 3.0 * self._them_gio_moi.get(k, 0) for k, v in self._diem.items()}
        top = sorted(diem, key=lambda k: (-diem[k], k))[:50]
        return [
            {
                "ma_san_pham": k,
                "diem_du_doan": round(diem[k], 4),
                "nguon": NGUON_THINH_HANH,
                "so_luot_xem": int(diem[k] * 100),
                "so_luot_them_gio": self._them_gio_moi.get(k, 0),
            }
            for k in top
        ][:n]

    def thinh_hanh_san_pham(self, ma_san_pham, bat_dau_ms=None):
        """Cùng hợp đồng với KhoDelta: không có bat_dau_ms là cửa sổ đang chạy; có thì là cửa sổ
        15 phút bắt đầu ở phút đó (None nếu cửa sổ chưa có lượt thêm giỏ nào)."""
        phut = 60000
        if bat_dau_ms is not None:
            bd = int(bat_dau_ms) // phut * phut
            n = sum(bd <= t < bd + 15 * phut for t in self._luc_them_gio.get(ma_san_pham, []))
            if not n:
                return None
            kt = bd + 15 * phut
            tg = n
        else:
            kt = (int(time.time() * 1000) // phut + 1) * phut
            bd = kt - 15 * phut
            tg = self._them_gio_moi.get(ma_san_pham, 0)
        return {
            "ma_san_pham": ma_san_pham,
            "so_luot_xem": 0,
            "so_luot_them_gio": tg,
            "cua_so_bat_dau": datetime.utcfromtimestamp(bd / 1000).isoformat(sep=" ", timespec="seconds"),
            "cua_so_ket_thuc": datetime.utcfromtimestamp(kt / 1000).isoformat(sep=" ", timespec="seconds"),
            "cua_so_bat_dau_ms": bd,
            "cua_so_ket_thuc_ms": kt,
        }

    def ghi_nhan_them_gio(self, ma_san_pham):
        """Chỉ kho giả lập có: mô phỏng việc luồng thịnh hành nhận được sự kiện."""
        self._them_gio_moi[ma_san_pham] = self._them_gio_moi.get(ma_san_pham, 0) + 1
        self._luc_them_gio.setdefault(ma_san_pham, []).append(int(time.time() * 1000))

    # ----- sản phẩm
    def san_pham(self, ma_san_pham):
        return self._san_pham.get(ma_san_pham, {"ma_san_pham": ma_san_pham})

    def ban_chay_danh_muc(self, ma_danh_muc, n=20):
        ds = [k for k, v in self._san_pham.items() if v["ma_danh_muc"] == ma_danh_muc]
        ds.sort(key=lambda k: (-self._diem[k], k))
        return [{"ma_san_pham": k, "diem_du_doan": self._diem[k], "nguon": NGUON_DANH_MUC} for k in ds[:n]]

    def lien_quan(self, ma_san_pham, n=10):
        dm = self.san_pham(ma_san_pham).get("ma_danh_muc")
        return [g for g in self.ban_chay_danh_muc(dm) if g["ma_san_pham"] != ma_san_pham][:n]

    def nguoi_dung_mau(self, n=12):
        return [{"ma_nguoi_dung": u, "chien_luoc": "als" if "als" in u else "it_tuong_tac"} for u in sorted(self._goi_y)][:n]

    # ----- báo cáo
    def ket_qua_ab(self):
        rnd = random.Random(7)
        ket_qua = []
        for i in range(7):
            ngay = date(2026, 11, 3) + timedelta(days=i)
            so = {}
            for nhom, ctr in (("A", 0.060), ("B", 0.066)):
                hien = 4000 + rnd.randint(0, 500)
                click = int(hien * (ctr + rnd.uniform(-0.004, 0.004)))
                so[nhom] = (hien, click, int(click * 0.12))
            _, p = ztest_hai_ty_le(so["A"][1], so["A"][0], so["B"][1], so["B"][0])
            _, p_cd = ztest_hai_ty_le(so["A"][2], so["A"][0], so["B"][2], so["B"][0])
            for nhom, (hien, click, mua) in so.items():
                ket_qua.append(
                    {
                        "ngay": ngay.isoformat(),
                        "nhom_ab": nhom,
                        "so_hien_thi": hien,
                        "so_click": click,
                        "so_mua": mua,
                        "ctr": click / hien,
                        "ty_le_chuyen_doi": mua / hien,
                        "p_value": p,
                        "p_value_chuyen_doi": p_cd,
                        "ty_le_mo_phong": 1.0,
                        "phien_ban_delta_du_lieu": 10 + i,
                    }
                )
        return ket_qua

    def lich_su_mo_hinh(self):
        chi_tiet = {
            "baseline_toan_san": {"precision_at_10": 0.010, "recall_at_10": 0.030, "ndcg_at_10": 0.020, "map_at_10": 0.010, "coverage": 0.0003, "novelty": 6.1},
            "baseline_danh_muc": {"precision_at_10": 0.014, "recall_at_10": 0.045, "ndcg_at_10": 0.028, "map_at_10": 0.014, "coverage": 0.05, "novelty": 8.0},
            "xep_lai_novelty": {"0.1": {"ndcg_at_10": 0.030, "coverage": 0.16, "novelty": 11.2}},
            "phan_phoi_pho_bien": {
                "als": [0.42, 0.18, 0.11, 0.08, 0.06, 0.05, 0.04, 0.03, 0.02, 0.01],
                "baseline_toan_san": [1.0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                "xep_lai_beta_0.1": [0.30, 0.17, 0.12, 0.10, 0.08, 0.07, 0.06, 0.05, 0.03, 0.02],
            },
        }
        return [
            {
                "ma_chay": "gia_lap_{}".format(i),
                "phien_ban_mo_hinh": "als_gia_lap_{}".format(i),
                "thoi_diem": "2026-11-0{} 02:30:00".format(i + 1),
                "phien_ban_features": i,
                "rank": 30,
                "reg_param": 0.1,
                "alpha": 10.0,
                "precision_at_10": 0.018 + i * 0.001,
                "recall_at_10": 0.06,
                "ndcg_at_10": 0.031 + i * 0.001,
                "map_at_10": 0.017,
                "coverage": 0.12,
                "novelty": 9.5,
                "ndcg_baseline_toan_san": 0.020,
                "ndcg_baseline_danh_muc": 0.028,
                "uplift_ndcg": (0.031 + i * 0.001) / 0.020 - 1,
                "dat_nguong": True,
                "trang_thai": "trien_khai",
                "chi_tiet": chi_tiet,
            }
            for i in range(3)
        ]

    def tuning(self, ma_chay):
        return [
            {"ma_chay": ma_chay, "rank": r, "reg_param": g, "alpha": 10.0, "ndcg_at_10": 0.02 + r / 3000.0 - g / 100.0,
             "precision_at_10": 0.015, "recall_at_10": 0.05, "map_at_10": 0.012, "coverage": 0.1, "novelty": 9.0, "so_giay": 12.0}
            for r in (10, 30, 50) for g in (0.01, 0.1, 1.0)
        ]

    def ket_qua_tai_lap(self):
        return list(self._tai_lap_da_ghi)

    def ghi_ket_qua_tai_lap(self, dong):
        """Chế độ giả lập: giữ trong bộ nhớ thay cho Delta, để giao diện hiển thị như thật."""
        self._tai_lap_da_ghi.append(dict(dong, thoi_diem=str(dong["thoi_diem"])[:19]))
        return len(self._tai_lap_da_ghi) - 1

    def do_zorder(self):
        return [
            {"giai_doan": "truoc_toi_uu", "so_tep": 200, "trung_vi_ms": 900.0, "p95_ms": 1300.0, "tep_doc_trung_binh": 200},
            {"giai_doan": "sau_optimize", "so_tep": 8, "trung_vi_ms": 420.0, "p95_ms": 560.0, "tep_doc_trung_binh": 8},
            {"giai_doan": "sau_zorder", "so_tep": 8, "trung_vi_ms": 300.0, "p95_ms": 420.0, "tep_doc_trung_binh": 1},
        ]

    def tai_lap_ab(self, ngay):
        cu = [d for d in self.ket_qua_ab() if d["ngay"] == ngay]
        if not cu:
            return None
        return {"ngay": ngay, "phien_ban_delta_du_lieu": cu[0]["phien_ban_delta_du_lieu"], "cu": cu, "moi": cu,
                "phien_ban_hien_tai": cu, "so_giay": 0.0}

    def xoa_bo_dem(self):
        pass
