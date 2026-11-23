"""transform/thong_ke_san_pham.py - silver -> gold.thong_ke_san_pham.

Bảng phục vụ baseline, cold start và mẫu số trung_binh_24h của điểm thịnh hành.
"""
import argparse

from pyspark.sql import functions as F

from common.spark_utils import bang_ton_tai, doc_delta, ghi_delta, in_ket_qua, tao_spark


def tinh_thong_ke(silver, san_pham=None):
    """Đếm tương tác theo sản phẩm; 24 giờ tính từ mốc cuối của dữ liệu lịch sử."""
    lich_su = silver.filter(F.col("_nguon") != "web")
    moc = lich_su.agg(F.max("thoi_diem")).first()[0]
    moc_24h = F.lit(moc).cast("timestamp") - F.expr("INTERVAL 24 HOURS")

    thong_ke = silver.groupBy("ma_san_pham").agg(
        F.sum((F.col("loai_su_kien") == "xem").cast("long")).alias("so_luot_xem"),
        F.sum((F.col("loai_su_kien") == "them_gio").cast("long")).alias("so_luot_them_gio"),
        F.sum((F.col("loai_su_kien") == "mua").cast("long")).alias("so_luot_mua"),
        F.count("*").alias("tong_tuong_tac"),
        F.sum("trong_so").alias("diem_pho_bien"),
        F.sum(
            ((F.col("loai_su_kien") == "xem") & (F.col("thoi_diem") >= moc_24h)).cast("long")
        ).alias("so_xem_24h"),
    )
    # Một ngày có 96 khung 15 phút.
    thong_ke = thong_ke.withColumn("trung_binh_24h", F.col("so_xem_24h") / 96.0)
    if san_pham is not None:
        thong_ke = thong_ke.join(
            san_pham.select("ma_san_pham", "ma_danh_muc"), "ma_san_pham", "left"
        )
    else:
        thong_ke = thong_ke.withColumn("ma_danh_muc", F.lit(None).cast("string"))
    return thong_ke.withColumn("cap_nhat_luc", F.current_timestamp())


def main():
    argparse.ArgumentParser().parse_args()
    spark = tao_spark("bd03-thong-ke-san-pham")
    san_pham = None
    if bang_ton_tai(spark, "silver.san_pham"):
        san_pham = doc_delta(spark, "silver.san_pham")
    ket_qua = tinh_thong_ke(doc_delta(spark, "silver.tuong_tac"), san_pham).cache()
    ghi_delta(ket_qua, "gold.thong_ke_san_pham", tuy_chon={"overwriteSchema": "true"})
    in_ket_qua({"so_san_pham": ket_qua.count()})


if __name__ == "__main__":
    main()
