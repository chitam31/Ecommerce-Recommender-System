"""Tổng hợp kết quả A/B. Job batch và màn hình Tái lập dùng chung đúng hàm này,
nên chạy lại trên cùng một phiên bản Delta chắc chắn cùng công thức."""
from pyspark.sql import functions as F

from .thong_ke import ztest_hai_ty_le


def tong_hop_ab(nhat_ky, cac_ngay=None):
    """Từ impression log tính số liệu theo ngày và nhóm, kèm z-test giữa A và B.

    Chỉ tính lượt hiển thị của chiến lược ALS: user cold start nhận cùng một kiểu danh
    sách dù ở nhóm nào, đưa họ vào chỉ làm loãng khác biệt cần đo. Dòng cũ chưa có cột
    chien_luoc (null) vẫn được tính.

    Trả về list dict, sắp theo (ngay, nhom_ab) để so sánh chính xác giữa hai lần chạy.
    """
    df = nhat_ky.withColumn("ngay", F.to_date("thoi_diem_hien_thi"))
    if "chien_luoc" in nhat_ky.columns:
        df = df.filter(F.col("chien_luoc").isNull() | (F.col("chien_luoc") == "als"))
    if cac_ngay is not None:
        df = df.filter(F.col("ngay").isin(list(cac_ngay)))
    dong = (
        df.groupBy("ngay", "nhom_ab")
        .agg(
            F.count("*").alias("so_hien_thi"),
            F.sum(F.col("da_click").cast("long")).alias("so_click"),
            F.sum(F.col("da_mua").cast("long")).alias("so_mua"),
            F.avg((F.col("nguon_traffic") == "mo_phong").cast("double")).alias("ty_le_mo_phong"),
        )
        .collect()
    )

    theo_ngay = {}
    for d in dong:
        theo_ngay.setdefault(d["ngay"], {})[d["nhom_ab"]] = d

    ket_qua = []
    for ngay in sorted(theo_ngay):
        nhom = theo_ngay[ngay]
        a, b = nhom.get("A"), nhom.get("B")
        p_ctr = p_cd = None
        if a is not None and b is not None:
            _, p_ctr = ztest_hai_ty_le(
                a["so_click"], a["so_hien_thi"], b["so_click"], b["so_hien_thi"]
            )
            _, p_cd = ztest_hai_ty_le(
                a["so_mua"], a["so_hien_thi"], b["so_mua"], b["so_hien_thi"]
            )
        for ten in sorted(nhom):
            d = nhom[ten]
            ket_qua.append({
                "ngay": ngay,
                "nhom_ab": ten,
                "so_hien_thi": int(d["so_hien_thi"]),
                "so_click": int(d["so_click"]),
                "so_mua": int(d["so_mua"]),
                "ctr": float(d["so_click"]) / d["so_hien_thi"],
                "ty_le_chuyen_doi": float(d["so_mua"]) / d["so_hien_thi"],
                "p_value": p_ctr,
                "p_value_chuyen_doi": p_cd,
                "ty_le_mo_phong": float(d["ty_le_mo_phong"] or 0.0),
            })
    return ket_qua
