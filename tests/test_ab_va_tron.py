"""Tổng hợp A/B, mô phỏng traffic và job trộn gợi ý (phần DataFrame, không cần Delta)."""
from datetime import date, datetime

import pytest

from common.ab import tong_hop_ab
from common.thong_ke import ztest_hai_ty_le
from ml.tron_goi_y import ban_chay_theo_danh_muc, tron_nguoi_it_tuong_tac, tron_nguoi_quen
from transform.mo_phong_hien_thi import mo_phong_mot_nguoi

NHAT_KY = """ma_hien_thi string, ma_nguoi_dung string, nhom_ab string, vi_tri int, ma_san_pham string,
    thoi_diem_hien_thi timestamp, da_click boolean, da_mua boolean, nguon_traffic string, chien_luoc string"""


def test_tong_hop_ab(spark):
    t = datetime(2026, 11, 9, 9)
    dong = []
    for i in range(1000):
        dong.append(("a{}".format(i), "ua", "A", 1, "x", t, i < 50, i < 5, "mo_phong", "als"))
        dong.append(("b{}".format(i), "ub", "B", 1, "x", t, i < 80, i < 9, "web", "als"))
    dong.append(("c1", "uc", "A", 1, "x", t, True, True, "web", "nguoi_moi"))  # cold start: không tính
    kq = tong_hop_ab(spark.createDataFrame(dong, " ".join(NHAT_KY.split())))
    a, b = kq
    assert (a["nhom_ab"], a["so_hien_thi"], a["so_click"], a["so_mua"]) == ("A", 1000, 50, 5)
    assert b["ctr"] == pytest.approx(0.08) and a["ty_le_mo_phong"] == 1.0 and b["ty_le_mo_phong"] == 0.0
    assert a["p_value"] == pytest.approx(ztest_hai_ty_le(50, 1000, 80, 1000)[1])
    assert a["ngay"] == date(2026, 11, 9)
    # chạy lại trên cùng dữ liệu cho kết quả giống hệt
    assert kq == tong_hop_ab(spark.createDataFrame(dong, " ".join(NHAT_KY.split())))


def test_mo_phong_tat_dinh_va_phu_thuoc_vi_tri():
    goi_y = [{"ma_san_pham": "sp{}".format(i), "diem_du_doan": 1.0, "nguon": "als"} for i in range(20)]
    tham_so = {"so_phien": 400, "so_ngay": 7, "p_lien_quan": 0.4, "p_nen": 0.03, "p_mua": 0.1, "eta": 0.7,
               "tang_thinh_hanh": 0.0, "hat_giong": 42}
    d1 = mo_phong_mot_nguoi("u1", goi_y, ["sp0", "sp9"], date(2026, 11, 10), tham_so)
    assert d1 == mo_phong_mot_nguoi("u1", goi_y, ["sp0", "sp9"], date(2026, 11, 10), tham_so)
    assert len(d1) == 400 * 10 and all(d[8] == "mo_phong" and d[9] == "als" for d in d1)
    click = lambda vt: sum(1 for d in d1 if d[3] == vt and d[6])
    assert click(1) > click(2)  # cùng nền, vị trí 1 vừa liên quan vừa cao hơn
    assert all(not d[7] or d[6] for d in d1)  # mua thì phải click


def test_tron_nguoi_quen_va_it_tuong_tac(spark):
    als = spark.createDataFrame([("u{}".format(u), r, "a{}".format(r), 1.0 - r / 100) for u in range(4) for r in range(1, 21)],
                                "ma_nguoi_dung string, xep_hang int, ma_san_pham string, diem_du_doan double")
    dm_item = spark.createDataFrame([("a{}".format(r), "dm1") for r in range(1, 21)] + [("moi1", "dm1"), ("moi2", "dm1")],
                                    "ma_san_pham string, ma_danh_muc string")
    moi = spark.createDataFrame([("dm1", [(1, "moi1", 0.0), (2, "moi2", 0.0)])],
                                "ma_danh_muc string, ds_moi array<struct<hang:int,ma_san_pham:string,diem:double>>")
    th = [{"ma_san_pham": "t{}".format(i), "diem_du_doan": 5.0, "nguon": "thinh_hanh"} for i in range(30)]
    bc = spark.sparkContext.broadcast(th)
    kq = tron_nguoi_quen(spark, als, dm_item, moi, bc).collect()
    from common.tron import chon_nhom
    for u in ("u0", "u1", "u2", "u3"):
        nguon = [r["nguon"] for r in sorted((r for r in kq if r["ma_nguoi_dung"] == u), key=lambda r: r["xep_hang"])]
        assert len(nguon) == 20
        assert nguon[:10].count("thinh_hanh") == (3 if chon_nhom(u) == "B" else 0)
        assert nguon[8:10] == ["pho_bien_danh_muc", "pho_bien_danh_muc"]

    thong_ke = spark.createDataFrame([("x{}".format(i), "dm1", float(100 - i)) for i in range(30)],
                                     "ma_san_pham string, ma_danh_muc string, diem_pho_bien double")
    silver = spark.createDataFrame([("it", "a1", date(2015, 9, 1), 1.0)], "ma_nguoi_dung string, ma_san_pham string, ngay date, trong_so double")
    it = tron_nguoi_it_tuong_tac(spark, silver, dm_item, ban_chay_theo_danh_muc(thong_ke, 20),
                                 spark.createDataFrame([("u0",)], "ma_nguoi_dung string"), "2015-08-15", bc).collect()
    assert len(it) == 20 and sum(r["nguon"] == "thinh_hanh" for r in it if r["xep_hang"] <= 10) == 5


def test_tron_nguoi_quen_v2_loai_mon_ngoai_tam_gia(spark):
    als = spark.createDataFrame([("u0", r, "a{}".format(r), 1.0 - r / 100) for r in range(1, 41)],
                                "ma_nguoi_dung string, xep_hang int, ma_san_pham string, diem_du_doan double")
    dm_item = spark.createDataFrame([("a{}".format(r), "dm1") for r in range(1, 41)], "ma_san_pham string, ma_danh_muc string")
    moi = spark.createDataFrame([], "ma_danh_muc string, ds_moi array<struct<hang:int,ma_san_pham:string,diem:double>>")
    gia = spark.createDataFrame([("a{}".format(r), 100.0 if r % 4 else 99999.0) for r in range(1, 41)], "ma_san_pham string, gia double")
    tam_gia = spark.createDataFrame([("u0", 50.0, 200.0)], "ma_nguoi_dung string, gia_thap double, gia_cao double")
    bc = spark.sparkContext.broadcast([])
    kq = tron_nguoi_quen(spark, als, dm_item, moi, bc, gia, tam_gia).collect()
    chon = [r["ma_san_pham"] for r in sorted(kq, key=lambda r: r["xep_hang"])]
    assert len(chon) == 20 and all(int(x[1:]) % 4 for x in chon)  # a4, a8, ... (ngoài tầm giá) bị loại
    # Không truyền giá: mô hình v1 giữ nguyên thứ tự ALS.
    v1 = [r["ma_san_pham"] for r in sorted(tron_nguoi_quen(spark, als, dm_item, moi, bc).collect(), key=lambda r: r["xep_hang"])]
    assert v1[:3] == ["a1", "a2", "a3"] and "a4" in v1


def test_chon_nguoi_chi_als(spark):
    """--chi-als chỉ giữ user có ít nhất một ô nguồn als; thứ tự băm ổn định giữa các lần chạy."""
    from transform.mo_phong_hien_thi import chon_nguoi

    dong = [("u{}".format(i), [(1, "sp", 0.5, "als" if i % 3 == 0 else "pho_bien_danh_muc")]) for i in range(30)]
    df = spark.createDataFrame(
        dong, "ma_nguoi_dung string, goi_y array<struct<xep_hang:int,ma_san_pham:string,diem_du_doan:double,nguon:string>>"
    )
    tat_ca = [r["ma_nguoi_dung"] for r in chon_nguoi(df, 100).collect()]
    chi_als = [r["ma_nguoi_dung"] for r in chon_nguoi(df, 100, chi_als=True).collect()]
    assert len(tat_ca) == 30 and sorted(chi_als) == sorted("u{}".format(i) for i in range(0, 30, 3))
    assert [r["ma_nguoi_dung"] for r in chon_nguoi(df, 4, chi_als=True).collect()] == chi_als[:4]
