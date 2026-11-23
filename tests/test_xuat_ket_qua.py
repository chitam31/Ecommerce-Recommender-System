"""kiem_tra/xuat_ket_qua.py: các hàm dựng bảng Markdown (không cần Spark)."""
import json

from kiem_tra.xuat_ket_qua import bang_md, bang_pho_bien, bang_phuong_phap, bang_tuning, so, thong_tin_mo_hinh


def _lich_su():
    m = {"precision_at_10": 0.01, "recall_at_10": 0.05, "ndcg_at_10": 0.067, "map_at_10": 0.03,
         "coverage": 0.032, "novelty": 9.1, "so_item_duoc_goi_y": 7500, "so_nguoi_danh_gia": 9000}
    ct = {
        "als": dict(m, coverage_toan_san=0.018),
        "baseline_toan_san": dict(m, ndcg_at_10=0.02, coverage=0.00004, so_item_duoc_goi_y=10),
        "baseline_danh_muc": dict(m, ndcg_at_10=0.03),
        "loc_tam_gia_v2": None,
        "xep_lai_novelty": {"0.1": dict(m, coverage=0.05), "0.3": dict(m, coverage=0.09)},
        "phuc_vu": dict(m, ndcg_at_10=0.0865, coverage=0.0647, beta=1.0, so_ung_vien=200),
        "phan_phoi_pho_bien": {"als": [0.6, 0.2, 0.1, 0.05, 0.05, 0, 0, 0, 0, 0]},
        "so_item_catalog_als": 235061, "so_item_toan_san": 417053, "so_nguoi_train": 179999, "so_nguoi_test": 9000,
    }
    return {"ma_chay": "r1", "phien_ban_mo_hinh": "als_r1", "phien_ban_features": 0, "phien_ban_silver": 0,
            "rank": 50, "reg_param": 0.01, "alpha": 40.0, "max_iter": 10, "hat_giong": 42,
            "uplift_ndcg": 2.35, "coverage": 0.032, "coverage_toan_san": 0.018, "dat_nguong": False,
            "cai_thien": True, "trang_thai": "cho_trien_khai", "chi_tiet_json": json.dumps(ct)}


def test_dinh_dang_so():
    assert so(None) == "—" and so(True) == "có" and so(179999) == "179.999" and so(0.066841) == "0.0668"


def test_bang_phuong_phap_va_thong_tin():
    bang = bang_phuong_phap(_lich_su())
    dong = bang.splitlines()
    assert len(dong) == 2 + 6  # tiêu đề, gạch, 2 baseline, ALS, 2 re-ranking, phục vụ; v2 None thì không có
    assert "Phục vụ: ALS + xếp lại β = 1.0, 200 ứng viên" in bang and "0.0865" in bang
    assert "ALS (rank=50, reg=0.01, alpha=40.0)" in bang and "re-ranking novelty β = 0.3" in bang
    tt = thong_tin_mo_hinh(_lich_su())
    assert "235.061" in tt and "`dat_nguong` = không" in tt and "235.0%" in tt
    # trần coverage = 10 x 9.000 / 235.061 = 38,3%
    assert "= 38.3%" in tt


def test_bang_pho_bien_va_tuning():
    assert "60.0%" in bang_pho_bien(_lich_su())
    dong = [{"rank": r, "reg_param": 0.1, "alpha": 40.0, "precision_at_10": 0.01, "recall_at_10": 0.02,
             "ndcg_at_10": n, "map_at_10": 0.01, "coverage": 0.01, "novelty": 9.0, "so_giay": 60.0}
            for r, n in ((50, 0.067), (10, 0.024))]
    bang = bang_tuning(dong)
    assert bang.index("| 10 |") < bang.index("| **50** |")  # sắp theo rank, in đậm tổ hợp tốt nhất
    assert "2 tổ hợp: 2.0 phút" in bang
    assert bang_md(["a"], [["x"]]) == "| a |\n|---|\n| x |"


def test_ab_gop_khop_so_lieu_that():
    """Số liệu 7 ngày của lần chạy thật 10/11: gộp lại p ≈ 0,77, cần ~79 nghìn impression mỗi nhóm."""
    from kiem_tra.xuat_ket_qua import mo_ta_ab_gop, tong_hop_ab_gop

    a = [(460, 11), (490, 11), (480, 6), (470, 8), (530, 7), (590, 12), (540, 11)]
    b = [(360, 10), (420, 8), (490, 9), (450, 5), (380, 10), (410, 7), (410, 8)]
    dong = [{"nhom_ab": "A", "so_hien_thi": n, "so_click": c} for n, c in a]
    dong += [{"nhom_ab": "B", "so_hien_thi": n, "so_click": c} for n, c in b]
    g = tong_hop_ab_gop(dong)
    assert (g["n_a"], g["c_a"], g["n_b"], g["c_b"]) == (3560, 66, 2920, 57)
    assert abs(g["p"] - 0.773) < 0.01 and 75000 < g["can_moi_nhom"] < 90000
    mo_ta = mo_ta_ab_gop(g, 7)
    assert "CHƯA ĐỦ" in mo_ta and "chưa có bằng chứng" in mo_ta
    assert tong_hop_ab_gop([{"nhom_ab": "A", "so_hien_thi": 1, "so_click": 0}]) is None


def test_dong_ket_qua_xep_lai():
    from kiem_tra.thu_xep_lai import dong_ket_qua

    m = {"precision_at_10": 0.017, "recall_at_10": 0.1, "ndcg_at_10": 0.0746, "map_at_10": 0.059,
         "coverage": 0.12, "novelty": 11.1, "so_item_duoc_goi_y": 15000}
    d = dong_ket_qua(200, 1.0, m, 0.0051, 417053, 0.8)
    assert d[-1] == "đạt" and d[-2] == "+1363%" and d[6] == "12.00%" and d[10] == "80.0%"
    assert dong_ket_qua(50, 0.3, dict(m, coverage=0.04), 0.0051, 417053, 0.98)[-1] == "chưa"
