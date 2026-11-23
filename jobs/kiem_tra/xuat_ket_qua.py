"""kiem_tra/xuat_ket_qua.py - in toàn bộ số liệu cho báo cáo dưới dạng bảng Markdown.

Đọc các bảng gold trên HDFS và in ra stdout (log của Spark đi stderr), để điền MO_HINH.md
mục 12 và báo cáo mà không phải chép tay từng con số:
  - gold.lich_su_huan_luyen: metric của mô hình, 2 baseline, re-ranking novelty, mô hình v2,
    phân phối độ phổ biến, uplift và ngưỡng triển khai
  - gold.ket_qua_tuning: bảng đủ các tổ hợp của lần tuning, kèm thời gian từng tổ hợp
  - gold.ket_qua_ab, gold.do_hieu_nang_zorder, gold.ket_qua_tai_lap: các dòng mới nhất

Chạy từ laptop, kết quả ghi thẳng thành tệp trên laptop:
    ssh labsoe-11 "cd ~/bd03_khuyen_nghi && bash ha_tang/spark/submit.sh nhanh kiem_tra/xuat_ket_qua.py 2>/dev/null" > ket_qua.md
    (thêm --ma-chay <mã> sau tên job để xuất một lần chạy cụ thể; mặc định lần mới nhất)
"""
import argparse
import json

from pyspark.sql import functions as F

from common.spark_utils import bang_ton_tai, doc_delta, tao_spark
from common.thong_ke import co_mau_can_thiet, ztest_hai_ty_le

COT_CHI_SO = [
    ("precision_at_10", "P@10"),
    ("recall_at_10", "R@10"),
    ("ndcg_at_10", "NDCG@10"),
    ("map_at_10", "MAP@10"),
    ("coverage", "Coverage"),
    ("novelty", "Novelty"),
    ("so_item_duoc_goi_y", "Số item được gợi ý"),
]


def so(x, chu_so=4):
    """Định dạng số cho bảng: None -> "—", số nguyên giữ nguyên, số thực làm tròn."""
    if x is None:
        return "—"
    if isinstance(x, bool):
        return "có" if x else "không"
    if isinstance(x, int):
        return "{:,}".format(x).replace(",", ".")
    return "{:.{}f}".format(float(x), chu_so)


def phan_tram(x):
    return "—" if x is None else "{:.1f}%".format(float(x) * 100)


def bang_md(tieu_de, cac_dong):
    """Bảng Markdown từ danh sách tiêu đề cột và danh sách dòng (mỗi dòng là list chuỗi)."""
    ra = ["| " + " | ".join(tieu_de) + " |", "|" + "---|" * len(tieu_de)]
    ra += ["| " + " | ".join(str(o) for o in d) + " |" for d in cac_dong]
    return "\n".join(ra)


def bang_phuong_phap(lich_su):
    """Bảng so sánh các phương pháp, đúng khung MO_HINH.md mục 12."""
    ct = json.loads(lich_su["chi_tiet_json"])
    dong = [
        ("Baseline 1: bán chạy toàn sàn", ct.get("baseline_toan_san")),
        ("Baseline 2: bán chạy theo danh mục", ct.get("baseline_danh_muc")),
        ("ALS (rank={}, reg={}, alpha={})".format(lich_su["rank"], lich_su["reg_param"], lich_su["alpha"]),
         ct.get("als")),
    ]
    for beta, m in sorted((ct.get("xep_lai_novelty") or {}).items()):
        dong.append(("ALS + re-ranking novelty β = {}".format(beta), m))
    if ct.get("phuc_vu"):
        pv = ct["phuc_vu"]
        dong.append(("**Phục vụ: ALS + xếp lại β = {}, {} ứng viên**".format(pv.get("beta"), pv.get("so_ung_vien")), pv))
    if ct.get("loc_tam_gia_v2"):
        dong.append(("Mô hình v2: ALS + lọc tầm giá", ct["loc_tam_gia_v2"]))
    return bang_md(
        ["Phương pháp"] + [ten for _, ten in COT_CHI_SO],
        [[ten] + [so(m.get(k)) if m else "—" for k, _ in COT_CHI_SO] for ten, m in dong],
    )


def thong_tin_mo_hinh(lich_su):
    """Các dòng tóm tắt: dữ liệu, uplift, ngưỡng, trần coverage có thể đạt."""
    ct = json.loads(lich_su["chi_tiet_json"])
    so_item = ct.get("so_item_catalog_als") or 0
    so_test = ct.get("so_nguoi_test") or 0
    # Mỗi user test nhận 10 gợi ý, nên số item khác nhau không thể vượt 10 x số user test.
    tran = min(1.0, 10.0 * so_test / so_item) if so_item else None
    als = ct.get("als") or {}
    return "\n".join([
        "- `ma_chay`: {} · mô hình: `{}` · `phien_ban_features`: {} · `phien_ban_silver`: {}".format(
            lich_su["ma_chay"], lich_su["phien_ban_mo_hinh"], lich_su["phien_ban_features"],
            lich_su["phien_ban_silver"]),
        "- Siêu tham số chọn: rank={}, regParam={}, alpha={}, maxIter={}, seed={}".format(
            lich_su["rank"], lich_su["reg_param"], lich_su["alpha"], lich_su["max_iter"], lich_su["hat_giong"]),
        "- User train: {} · user test được đánh giá: {} · item trong train: {} · item toàn sàn: {}".format(
            so(ct.get("so_nguoi_train")), so(so_test), so(so_item), so(ct.get("so_item_toan_san"))),
        "- Uplift NDCG@10 so với baseline 1: {} (ngưỡng ≥ 30%) · Coverage: {} (ngưỡng ≥ 10%) "
        "· coverage toàn sàn: {} → `dat_nguong` = {}".format(
            phan_tram(lich_su["uplift_ndcg"]), phan_tram(lich_su["coverage"]),
            phan_tram(lich_su["coverage_toan_san"]), so(lich_su["dat_nguong"])),
        "- Trần coverage về mặt cấu trúc: 10 gợi ý × {} user test / {} item = {} "
        "(ALS đang dùng {} item khác nhau)".format(
            so(so_test), so(so_item), phan_tram(tran), so(als.get("so_item_duoc_goi_y"))),
        "- Quyết định: `cai_thien` = {} → `trang_thai` = {}".format(
            so(lich_su["cai_thien"]), lich_su["trang_thai"]),
    ])


def bang_pho_bien(lich_su):
    """Tỷ lệ suất gợi ý rơi vào từng nhóm 10% item (nhóm 1 = phổ biến nhất): popularity bias."""
    pp = json.loads(lich_su["chi_tiet_json"]).get("phan_phoi_pho_bien") or {}
    return bang_md(
        ["Danh sách"] + ["N{}".format(i) for i in range(1, 11)],
        [[ten] + [phan_tram(x) for x in ty_le] for ten, ty_le in pp.items()],
    )


def bang_tuning(cac_dong):
    cac_dong = sorted(cac_dong, key=lambda d: (d["rank"], d["reg_param"], d["alpha"]))
    tot = max(cac_dong, key=lambda d: d["ndcg_at_10"] or 0)
    bang = bang_md(
        ["rank", "regParam", "alpha", "P@10", "R@10", "NDCG@10", "MAP@10", "Coverage", "Novelty", "Giây"],
        [[("**{}**" if d is tot else "{}").format(d["rank"]), d["reg_param"], d["alpha"]]
         + [so(d[k]) for k in ("precision_at_10", "recall_at_10", "ndcg_at_10", "map_at_10", "coverage", "novelty")]
         + [so(d["so_giay"], 1)] for d in cac_dong],
    )
    tong = sum(d["so_giay"] or 0 for d in cac_dong)
    return bang + "\n\nTổng thời gian huấn luyện + đánh giá {} tổ hợp: {:.1f} phút.".format(len(cac_dong), tong / 60)


def tong_hop_ab_gop(cac_dong):
    """Gộp các ngày: tổng impression/click mỗi nhóm, z-test trên tổng, cỡ mẫu cần cho mức tăng 10%."""
    tong = {}
    for d in cac_dong:
        t = tong.setdefault(d["nhom_ab"], [0, 0])
        t[0] += d["so_hien_thi"] or 0
        t[1] += d["so_click"] or 0
    if "A" not in tong or "B" not in tong:
        return None
    (n_a, c_a), (n_b, c_b) = tong["A"], tong["B"]
    z, p = ztest_hai_ty_le(c_a, n_a, c_b, n_b)
    ctr_a = c_a / float(n_a) if n_a else 0.0
    can = co_mau_can_thiet(ctr_a, 0.10) if ctr_a > 0 else None
    return {"n_a": n_a, "c_a": c_a, "n_b": n_b, "c_b": c_b, "ctr_a": ctr_a,
            "ctr_b": c_b / float(n_b) if n_b else 0.0, "z": z, "p": p, "can_moi_nhom": can}


def mo_ta_ab_gop(g, so_ngay):
    if g is None:
        return "- Chưa đủ hai nhóm để gộp."
    du = g["can_moi_nhom"] is not None and min(g["n_a"], g["n_b"]) >= g["can_moi_nhom"]
    return "\n".join([
        "- Gộp {} ngày: A {} impression, {} click, CTR {}; B {} impression, {} click, CTR {}.".format(
            so_ngay, so(g["n_a"]), so(g["c_a"]), phan_tram(g["ctr_a"]), so(g["n_b"]), so(g["c_b"]),
            phan_tram(g["ctr_b"])),
        "- z-test hai tỷ lệ trên tổng: z = {}, p-value = {} → {}.".format(
            so(g["z"], 3), so(g["p"]), "khác biệt có ý nghĩa (p < 0,05)" if g["p"] is not None and g["p"] < 0.05
            else "chưa có bằng chứng khác biệt"),
        "- Cỡ mẫu cần để phát hiện mức tăng tương đối 10% (α = 5%, power 80%): {} impression mỗi nhóm → "
        "mẫu hiện tại {}.".format(so(g["can_moi_nhom"]), "đủ" if du else "CHƯA ĐỦ"),
    ])


def phu_danh_sach_phuc_vu(spark, lich_su):
    """Độ phủ danh mục của danh sách thật sự phục vụ, trên mọi user (không chỉ user test).

    Khác Coverage ở mục Mô hình (chỉ top-10 ALS của user test): đây là toàn bộ 20 vị trí của
    mọi user trong gold, gồm cả ô thịnh hành và ô phổ biến theo danh mục.
    """
    gold = doc_delta(spark, "gold.goi_y_nguoi_dung")
    tong = gold.agg(F.count("*").alias("dong"), F.countDistinct("ma_nguoi_dung").alias("nguoi"),
                    F.countDistinct("ma_san_pham").alias("item")).first()
    theo_nguon = gold.groupBy("nguon").agg(F.count("*").alias("dong"), F.countDistinct("ma_san_pham").alias("item")).collect()
    ct = json.loads((lich_su or {}).get("chi_tiet_json") or "{}")
    so_item_train = ct.get("so_item_catalog_als")
    so_item_san = ct.get("so_item_toan_san")
    dong = ["- {} user, {} dòng, {} sản phẩm khác nhau".format(so(tong["nguoi"]), so(tong["dong"]), so(tong["item"]))
            + (" = {} số item trong train ({}), {} toàn sàn ({})".format(
                phan_tram(tong["item"] / float(so_item_train)), so(so_item_train),
                phan_tram(tong["item"] / float(so_item_san)), so(so_item_san)) if so_item_train and so_item_san else "")]
    return "\n".join(dong) + "\n\n" + bang_md(
        ["Nguồn", "Số ô", "Sản phẩm khác nhau"],
        [[d["nguon"], so(d["dong"]), so(d["item"])] for d in sorted(theo_nguon, key=lambda d: -d["dong"])])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ma-chay", default=None, help="Mặc định: lần huấn luyện mới nhất")
    args = ap.parse_args()
    spark = tao_spark("bd03-xuat-ket-qua")
    ra = ["# Kết quả chạy trên cụm"]

    lich_su = None
    if bang_ton_tai(spark, "gold.lich_su_huan_luyen"):
        ls = doc_delta(spark, "gold.lich_su_huan_luyen")
        if args.ma_chay:
            ls = ls.filter(F.col("ma_chay") == args.ma_chay)
        dong = ls.orderBy(F.desc("thoi_diem")).first()
        lich_su = dong.asDict() if dong else None
    if lich_su:
        ra += ["", "## Mô hình", "", thong_tin_mo_hinh(lich_su), "", bang_phuong_phap(lich_su),
               "", "### Phân phối độ phổ biến của danh sách gợi ý (N1 = 10% item phổ biến nhất)", "",
               bang_pho_bien(lich_su)]
    if bang_ton_tai(spark, "gold.goi_y_nguoi_dung"):
        ra += ["", "## Danh sách đang phục vụ (`gold.goi_y_nguoi_dung`)", "", phu_danh_sach_phuc_vu(spark, lich_su)]

    ma_chay = args.ma_chay or (lich_su or {}).get("ma_chay")
    if ma_chay and bang_ton_tai(spark, "gold.ket_qua_tuning"):
        dong = [r.asDict() for r in doc_delta(spark, "gold.ket_qua_tuning").filter(F.col("ma_chay") == ma_chay).collect()]
        if dong:
            ra += ["", "## Tuning (`ma_chay` = {})".format(ma_chay), "", bang_tuning(dong)]

    if bang_ton_tai(spark, "gold.ket_qua_ab"):
        ab = [r.asDict() for r in doc_delta(spark, "gold.ket_qua_ab").orderBy(F.desc("ngay"), "nhom_ab").limit(14).collect()]
        if ab:
            ra += ["", "## A/B (7 ngày mới nhất)", "", mo_ta_ab_gop(tong_hop_ab_gop(ab), len({d["ngay"] for d in ab})),
                   "", bang_md(
                ["Ngày", "Nhóm", "Hiển thị", "Click", "Mua", "CTR", "Chuyển đổi", "p-value CTR",
                 "p-value chuyển đổi", "Tỷ lệ mô phỏng", "Phiên bản Delta"],
                [[d["ngay"], d["nhom_ab"], so(d["so_hien_thi"]), so(d["so_click"]), so(d["so_mua"]),
                  phan_tram(d["ctr"]), phan_tram(d["ty_le_chuyen_doi"]), so(d["p_value"]),
                  so(d["p_value_chuyen_doi"]), phan_tram(d["ty_le_mo_phong"]), d["phien_ban_delta_du_lieu"]]
                 for d in ab])]

    if bang_ton_tai(spark, "gold.do_hieu_nang_zorder"):
        zo = [r.asDict() for r in doc_delta(spark, "gold.do_hieu_nang_zorder").orderBy(F.desc("thoi_diem")).limit(3).collect()]
        if zo:
            ra += ["", "## OPTIMIZE / ZORDER (lần đo mới nhất)", "", bang_md(
                ["Giai đoạn", "Số tệp của bảng", "Tệp đọc mỗi truy vấn", "Trung bình (ms)", "Trung vị (ms)", "p95 (ms)"],
                [[d["giai_doan"], so(d["so_tep"]), so(d["tep_doc_trung_binh"], 1), so(d["trung_binh_ms"], 0),
                  so(d["trung_vi_ms"], 0), so(d["p95_ms"], 0)] for d in reversed(zo)])]

    if bang_ton_tai(spark, "gold.ket_qua_tai_lap"):
        tl = [r.asDict() for r in doc_delta(spark, "gold.ket_qua_tai_lap").orderBy(F.desc("thoi_diem")).limit(5).collect()]
        if tl:
            ra += ["", "## Tái lập (mới nhất)", "", bang_md(
                ["Thời điểm", "Loại", "Khóa", "Phiên bản Delta", "Độ lệch lớn nhất", "Trùng khớp"],
                [[d["thoi_diem"], d["loai"], d["khoa"], d["phien_ban_delta"], so(d["lech_lon_nhat"], 6),
                  so(d["trung_khop"])] for d in tl])]

    print("\n".join(ra), flush=True)


if __name__ == "__main__":
    main()
