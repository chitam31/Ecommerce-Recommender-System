"""Kiểm định z hai tỷ lệ: so với công thức của statsmodels/scipy và các trường hợp biên."""
import math

import pytest

from common.thong_ke import co_mau_can_thiet, ket_luan, ztest_hai_ty_le


def test_gia_tri_biet_truoc():
    # 200/4000 = 5% so với 260/4000 = 6.5%
    z, p = ztest_hai_ty_le(200, 4000, 260, 4000)
    p_gop = 460 / 8000
    z_tay = (0.065 - 0.05) / math.sqrt(p_gop * (1 - p_gop) * (2 / 4000))
    assert z == pytest.approx(z_tay)
    assert 0.0 < p < 0.01


def test_khop_scipy():
    stats = pytest.importorskip("scipy.stats")
    z, p = ztest_hai_ty_le(120, 2000, 150, 2100)
    assert p == pytest.approx(2 * stats.norm.sf(abs(z)))


def test_bang_nhau_va_thieu_du_lieu():
    z, p = ztest_hai_ty_le(50, 1000, 50, 1000)
    assert z == 0.0 and p == pytest.approx(1.0)
    assert ztest_hai_ty_le(0, 0, 5, 10) == (None, None)
    assert ztest_hai_ty_le(0, 10, 0, 10) == (None, None)


def test_ket_luan_va_co_mau():
    assert "có ý nghĩa" in ket_luan(0.01)
    assert "Chưa đủ bằng chứng" in ket_luan(0.2)
    assert "Chưa đủ dữ liệu" in ket_luan(None)
    # CTR nền 5%, muốn phát hiện tăng 10% tương đối: cỡ vài chục nghìn mỗi nhóm
    assert 25000 < co_mau_can_thiet(0.05, 0.10) < 40000


# ---- kiểm định theo user (bổ sung 11/11/2026)
from common.thong_ke import bootstrap_theo_user, he_so_thiet_ke, ztest_theo_cum  # noqa: E402


def test_cum_mot_impression_moi_user_trung_z_khong_gop():
    # 1000 user mỗi nhóm, mỗi user 1 impression: 50 click ở A, 80 click ở B
    a = [(1, 1)] * 50 + [(1, 0)] * 950
    b = [(1, 1)] * 80 + [(1, 0)] * 920
    kq = ztest_theo_cum(a, b)
    pa, pb = 0.05, 0.08
    se = math.sqrt(pa * (1 - pa) / 1000 * 1000 / 999 + pb * (1 - pb) / 1000 * 1000 / 999)
    assert kq["hieu"] == pytest.approx(0.03)
    assert kq["sai_so_chuan"] == pytest.approx(se)
    assert kq["p_value"] == pytest.approx(math.erfc(abs(0.03 / se) / math.sqrt(2)))
    assert he_so_thiet_ke(a, b) == pytest.approx(1000 / 999, rel=1e-6)


def test_cum_tuong_quan_trong_user_lam_p_lon_hon():
    # Cùng tổng số như trên nhưng click dồn vào vài user: impression không còn độc lập.
    a = [(10, 5)] * 10 + [(10, 0)] * 90   # 1000 impression, 50 click
    b = [(10, 8)] * 10 + [(10, 0)] * 90   # 1000 impression, 80 click
    _, p_impression = ztest_hai_ty_le(50, 1000, 80, 1000)
    kq = ztest_theo_cum(a, b)
    assert kq["hieu"] == pytest.approx(0.03)
    assert kq["p_value"] > p_impression
    assert he_so_thiet_ke(a, b) > 3.0


def test_bootstrap_tai_lap_va_khoang_tin_cay():
    a = [(10, 0)] * 300 + [(10, 1)] * 150 + [(10, 2)] * 50
    b = [(10, 0)] * 250 + [(10, 1)] * 150 + [(10, 2)] * 100
    kq1 = bootstrap_theo_user(a, b, so_lan=400, hat_giong=7)
    kq2 = bootstrap_theo_user(a, b, so_lan=400, hat_giong=7)
    assert kq1 == kq2
    assert kq1["ci_hieu"][0] < kq1["hieu"] < kq1["ci_hieu"][1]
    assert kq1["ci_hieu"][0] > 0          # B rõ ràng cao hơn A
    assert kq1["p_value"] == pytest.approx(1 / 400)
    giong = bootstrap_theo_user(a, a, so_lan=400, hat_giong=7)
    assert giong["ci_hieu"][0] < 0 < giong["ci_hieu"][1]
    assert giong["p_value"] > 0.3
    assert bootstrap_theo_user([(1, 0)], b) is None
