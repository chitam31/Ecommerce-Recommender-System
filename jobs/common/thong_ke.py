"""Kiểm định z hai tỷ lệ cho thí nghiệm A/B, viết thuần Python để không phụ thuộc scipy."""
import math


def ztest_hai_ty_le(thanh_cong_a, tong_a, thanh_cong_b, tong_b):
    """Kiểm định hai phía H0: p_A = p_B với độ lệch chuẩn gộp.

    Trả về (z, p_value). Trả về (None, None) khi không đủ dữ liệu để tính.
    """
    if tong_a <= 0 or tong_b <= 0:
        return None, None
    p_a = float(thanh_cong_a) / tong_a
    p_b = float(thanh_cong_b) / tong_b
    p_gop = float(thanh_cong_a + thanh_cong_b) / (tong_a + tong_b)
    phuong_sai = p_gop * (1.0 - p_gop) * (1.0 / tong_a + 1.0 / tong_b)
    if phuong_sai <= 0.0:
        return None, None
    z = (p_b - p_a) / math.sqrt(phuong_sai)
    # P(|Z| > |z|) = erfc(|z| / sqrt(2))
    return z, math.erfc(abs(z) / math.sqrt(2.0))


def ket_luan(p_value, muc_y_nghia=0.05):
    """Diễn giải p-value thành câu kết luận hiển thị trên bảng điều khiển."""
    if p_value is None:
        return "Chưa đủ dữ liệu để kiểm định"
    if p_value < muc_y_nghia:
        return "Khác biệt có ý nghĩa thống kê (p < {})".format(muc_y_nghia)
    return "Chưa đủ bằng chứng về khác biệt (p >= {})".format(muc_y_nghia)


def co_mau_can_thiet(p_nen, muc_tang_tuong_doi, z_alpha=1.96, z_beta=0.8416):
    """Ước lượng số impression mỗi nhóm cần có (alpha 5%, power 80%)."""
    p_moi = p_nen * (1.0 + muc_tang_tuong_doi)
    hieu = p_moi - p_nen
    phuong_sai = p_nen * (1.0 - p_nen) + p_moi * (1.0 - p_moi)
    return int(math.ceil(((z_alpha + z_beta) ** 2) * phuong_sai / (hieu ** 2)))


# ---------------------------------------------------------------------------
# Kiểm định theo user (bổ sung 11/11/2026)
# ---------------------------------------------------------------------------
# z-test ở trên coi mọi impression là độc lập. Thực tế các impression của cùng một user
# tương quan với nhau (user có nhiều món "đúng" trong danh sách click nhiều ở mọi phiên), nên
# phương sai thật lớn hơn và p-value theo impression có thể quá lạc quan. Hai hàm dưới đây lấy
# USER làm đơn vị: mỗi phần tử của nhom_a / nhom_b là (so_hien_thi, so_thanh_cong) của một user.


def _ty_le_va_phuong_sai_cum(nhom):
    """Tỷ lệ gộp R = Σc / Σn và phương sai theo cụm (delta method, hiệu chỉnh m/(m-1))."""
    m = len(nhom)
    tong_n = float(sum(n for n, _ in nhom))
    if m < 2 or tong_n <= 0:
        return None, None
    r = sum(c for _, c in nhom) / tong_n
    tong_bp = sum((c - r * n) ** 2 for n, c in nhom)
    return r, tong_bp / (tong_n ** 2) * m / (m - 1.0)


def ztest_theo_cum(nhom_a, nhom_b):
    """Kiểm định hai phía H0: tỷ lệ A = tỷ lệ B, lấy user làm đơn vị (sai số chuẩn theo cụm).

    Trả về dict {ty_le_a, ty_le_b, hieu, sai_so_chuan, z, p_value}; None nếu thiếu dữ liệu.
    Khi mỗi user chỉ có một impression, kết quả trùng z-test hai tỷ lệ không gộp phương sai.
    """
    r_a, v_a = _ty_le_va_phuong_sai_cum(nhom_a)
    r_b, v_b = _ty_le_va_phuong_sai_cum(nhom_b)
    if r_a is None or r_b is None or v_a + v_b <= 0.0:
        return None
    se = math.sqrt(v_a + v_b)
    z = (r_b - r_a) / se
    return {"ty_le_a": r_a, "ty_le_b": r_b, "hieu": r_b - r_a, "sai_so_chuan": se,
            "z": z, "p_value": math.erfc(abs(z) / math.sqrt(2.0))}


def bootstrap_theo_user(nhom_a, nhom_b, so_lan=2000, hat_giong=42, muc=0.95):
    """Bootstrap lấy mẫu lại USER (có hoàn lại) trong từng nhóm.

    Trả về dict: hieu (B − A) và hieu_tuong_doi ((B − A)/A) trên mẫu gốc, khoảng tin cậy
    phân vị cho cả hai, và p_value hai phía = 2 x min(P(hiệu ≤ 0), P(hiệu ≥ 0)), không nhỏ
    hơn 1/so_lan. Cùng hạt giống thì cùng kết quả.
    """
    import numpy as np

    rng = np.random.default_rng(hat_giong)
    n_a = np.array([n for n, _ in nhom_a], dtype=float)
    c_a = np.array([c for _, c in nhom_a], dtype=float)
    n_b = np.array([n for n, _ in nhom_b], dtype=float)
    c_b = np.array([c for _, c in nhom_b], dtype=float)
    if len(n_a) < 2 or len(n_b) < 2 or n_a.sum() <= 0 or n_b.sum() <= 0:
        return None
    r_a, r_b = c_a.sum() / n_a.sum(), c_b.sum() / n_b.sum()

    hieu, tuong_doi = [], []
    lo = 100  # số lần lấy mẫu mỗi lô: giữ bộ nhớ ở mức vài chục MB dù có hàng chục nghìn user
    con_lai = so_lan
    while con_lai > 0:
        b = min(lo, con_lai)
        ia = rng.integers(0, len(n_a), size=(b, len(n_a)))
        ib = rng.integers(0, len(n_b), size=(b, len(n_b)))
        ra = c_a[ia].sum(axis=1) / n_a[ia].sum(axis=1)
        rb = c_b[ib].sum(axis=1) / n_b[ib].sum(axis=1)
        hieu.append(rb - ra)
        tuong_doi.append(np.where(ra > 0, (rb - ra) / np.where(ra > 0, ra, 1.0), np.nan))
        con_lai -= b
    hieu = np.concatenate(hieu)
    tuong_doi = np.concatenate(tuong_doi)
    duoi, tren = (1.0 - muc) / 2.0 * 100.0, (1.0 + muc) / 2.0 * 100.0
    p = 2.0 * min(float(np.mean(hieu <= 0.0)), float(np.mean(hieu >= 0.0)))
    return {
        "ty_le_a": float(r_a),
        "ty_le_b": float(r_b),
        "hieu": float(r_b - r_a),
        "ci_hieu": [float(np.percentile(hieu, duoi)), float(np.percentile(hieu, tren))],
        "hieu_tuong_doi": float((r_b - r_a) / r_a) if r_a > 0 else None,
        "ci_hieu_tuong_doi": [float(np.nanpercentile(tuong_doi, duoi)), float(np.nanpercentile(tuong_doi, tren))],
        "p_value": max(min(p, 1.0), 1.0 / so_lan),
        "so_lan": int(so_lan),
        "muc_tin_cay": muc,
    }


def he_so_thiet_ke(nhom_a, nhom_b):
    """Design effect = phương sai theo cụm / phương sai nhị thức (coi impression độc lập).

    Bằng khoảng 1 khi impression của cùng user không tương quan; lớn hơn 1 nghĩa là z-test
    theo impression đánh giá thấp sai số chuẩn khoảng sqrt(deff) lần.
    """
    tong = 0.0
    nhi_thuc = 0.0
    for nhom in (nhom_a, nhom_b):
        r, v = _ty_le_va_phuong_sai_cum(nhom)
        if r is None:
            return None
        tong += v
        nhi_thuc += r * (1.0 - r) / float(sum(n for n, _ in nhom))
    return tong / nhi_thuc if nhi_thuc > 0 else None
