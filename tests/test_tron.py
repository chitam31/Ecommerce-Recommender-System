"""Phân nhóm A/B và mẫu trộn gợi ý (common/tron.py)."""
from collections import Counter

from common.tron import (
    MAU_KHOI,
    bo_sung_thinh_hanh,
    chon_nhom,
    danh_sach_it_tuong_tac,
    danh_sach_nguoi_moi,
    danh_sach_nguoi_quen,
    la_mo_hinh_v2,
    lap_theo_mau,
    loc_theo_tam_gia,
    ten_phien_ban_mo_hinh,
    thay_o_thinh_hanh,
    xep_lai_novelty,
    xep_ung_vien_phuc_vu,
)


def ds(tien_to, n, nguon):
    return [{"ma_san_pham": "{}{}".format(tien_to, i), "diem_du_doan": 1.0 - i / 100.0, "nguon": nguon} for i in range(n)]


def test_phan_nhom_on_dinh_va_can_bang():
    nguoi = ["u{}".format(i) for i in range(10000)]
    lan_1 = [chon_nhom(u) for u in nguoi]
    assert lan_1 == [chon_nhom(u) for u in nguoi]  # cùng user luôn cùng nhóm
    dem = Counter(lan_1)
    assert set(dem) == {"A", "B"}
    assert abs(dem["A"] - 5000) < 250


def test_mau_dung_10_o():
    for ten, mau in MAU_KHOI.items():
        assert len(mau) == 10, ten
    assert MAU_KHOI["B"].count("t") == 3  # 30% thịnh hành
    assert MAU_KHOI["A"].count("t") == 0  # A là ALS thuần
    assert MAU_KHOI["A"][-2:] == MAU_KHOI["B"][-2:] == "mm"  # item mới ở 20% cuối


def test_nhom_a_va_b():
    als, th, moi = ds("a", 30, "als"), ds("t", 30, "thinh_hanh"), ds("m", 6, "pho_bien_danh_muc")
    a = danh_sach_nguoi_quen("A", als, th, moi)
    b = danh_sach_nguoi_quen("B", als, th, moi)
    assert len(a) == len(b) == 20
    assert [g["nguon"][0] for g in a[:10]] == list("aaaaaaaapp")
    assert [g["nguon"][0] for g in b[:10]] == list("aataatatpp")
    assert sum(g["nguon"] == "thinh_hanh" for g in b[:10]) == 3
    assert sum(g["nguon"] == "pho_bien_danh_muc" for g in b[:10]) == 2
    assert [g["ma_san_pham"] for g in a[:8]] == ["a{}".format(i) for i in range(8)]  # giữ thứ hạng ALS


def test_khong_trung_va_lay_bu():
    als = ds("x", 12, "als")
    th = [{"ma_san_pham": "x0", "diem_du_doan": 9, "nguon": "thinh_hanh"}] + ds("t", 3, "thinh_hanh")
    kq = danh_sach_nguoi_quen("B", als, th, [])
    ma = [g["ma_san_pham"] for g in kq]
    assert len(ma) == len(set(ma))
    # không có item mới -> ô "m" lấy bù từ ALS; thịnh hành cạn -> lấy bù ALS
    assert len(kq) == 12 + 3


def test_it_tuong_tac_va_nguoi_moi():
    kq = danh_sach_it_tuong_tac(ds("d", 20, "pho_bien_danh_muc"), ds("t", 20, "thinh_hanh"))
    assert Counter(g["nguon"] for g in kq[:10]) == {"pho_bien_danh_muc": 5, "thinh_hanh": 5}
    moi = danh_sach_nguoi_moi(ds("t", 25, "thinh_hanh"))
    assert len(moi) == 20 and all(g["nguon"] == "thinh_hanh" for g in moi)
    assert lap_theo_mau("a", {"a": [], "t": [], "m": [], "d": []}) == []


def test_thay_o_thinh_hanh_giu_vi_tri():
    cu = danh_sach_nguoi_quen("B", ds("a", 20, "als"), ds("t", 20, "thinh_hanh"), [])
    moi = ds("n", 10, "thinh_hanh")
    kq = thay_o_thinh_hanh(cu, moi)
    assert [g["nguon"] for g in kq] == [g["nguon"] for g in cu]
    assert [g["ma_san_pham"] for g in kq if g["nguon"] == "thinh_hanh"][:3] == ["n0", "n1", "n2"]


def test_xep_lai_novelty():
    ung_vien = [("pho_bien", 1.0), ("vua", 0.9), ("hiem", 0.8)]
    nov = {"pho_bien": 1.0, "vua": 5.0, "hiem": 12.0}
    assert xep_lai_novelty(ung_vien, nov, 0.0, 3) == ["pho_bien", "vua", "hiem"]
    assert xep_lai_novelty(ung_vien, nov, 5.0, 3)[0] == "hiem"
    assert xep_lai_novelty([], nov, 1.0, 3) == []


def test_loc_theo_tam_gia_mo_hinh_v2():
    gia = [100, 5000, 120, None, 90, 80, 110, 9000, 130, 95, 105, 100]
    als = [{"ma_san_pham": "a{}".format(i), "diem_du_doan": 1.0, "nguon": "als", "gia": g} for i, g in enumerate(gia)]
    # Đủ món trong tầm giá: loại hẳn món ngoài tầm (a1, a7), giữ món chưa biết giá (a3), giữ thứ hạng.
    kq = [g["ma_san_pham"] for g in loc_theo_tam_gia(als, 50, 200, toi_thieu=10)]
    assert kq == ["a0", "a2", "a3", "a4", "a5", "a6", "a8", "a9", "a10", "a11"]
    # Thiếu món trong tầm giá: món ngoài tầm được nối xuống cuối thay vì để khối bị hụt.
    kq = [g["ma_san_pham"] for g in loc_theo_tam_gia(als, 50, 200, toi_thieu=11)]
    assert kq[-2:] == ["a1", "a7"] and len(kq) == len(als)
    # User chưa xác định được tầm giá: không lọc.
    assert loc_theo_tam_gia(als, None, None) == als


def test_ten_phien_ban_mo_hinh():
    assert ten_phien_ban_mo_hinh("r1", False) == "als_r1"
    assert ten_phien_ban_mo_hinh("r1", True) == "als_v2_r1"
    assert la_mo_hinh_v2("als_v2_r1") and not la_mo_hinh_v2("als_r1") and not la_mo_hinh_v2(None)


def test_xep_ung_vien_phuc_vu():
    """beta = 0 giữ thứ tự ALS và cắt đúng số món; beta > 0 xếp lại nhưng giữ điểm ALS gốc."""
    ung_vien = [("pho_bien", 0.9), ("vua", 0.8), ("hiem", 0.7), ("rat_hiem", 0.1)]
    nov = {"pho_bien": 1.0, "vua": 5.0, "hiem": 9.0, "rat_hiem": 12.0}
    assert xep_ung_vien_phuc_vu(ung_vien, nov, 0.0, 2) == [("pho_bien", 0.9), ("vua", 0.8)]
    ket_qua = xep_ung_vien_phuc_vu(ung_vien, nov, 1.0, 3)
    assert len(ket_qua) == 3 and ket_qua[0] == ("hiem", 0.7)  # 0,75 + 0,73 cao nhất
    assert dict(ket_qua) == {i: d for i, d in ung_vien if i in dict(ket_qua)}
    assert xep_ung_vien_phuc_vu([], nov, 1.0, 3) == []


def test_bo_sung_thinh_hanh_khi_cua_so_vang():
    """Lỗi 29: cửa sổ mới nhất chỉ có 1 món (luồng vắng) thì cả bảng gold chỉ có 1 món thịnh hành."""
    g = lambda m: {"ma_san_pham": m, "diem_du_doan": 1.0, "nguon": "thinh_hanh"}
    du_phong = [g("x"), g("p1"), g("y"), g("z")]
    kq = [d["ma_san_pham"] for d in bo_sung_thinh_hanh([g("p1")], du_phong, 3)]
    assert kq == ["p1", "x", "y"]                     # giữ món của luồng trước, bù không trùng
    assert len(bo_sung_thinh_hanh([g("a"), g("b"), g("c")], du_phong, 3)) == 3
    assert [d["ma_san_pham"] for d in bo_sung_thinh_hanh([], du_phong, 2)] == ["x", "p1"]
    assert len(bo_sung_thinh_hanh([], du_phong[:1], 5)) == 1   # dự phòng cũng thiếu thì trả những gì có
