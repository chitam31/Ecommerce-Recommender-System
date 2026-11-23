"""ml/san_pham_lien_quan.py - khối "Sản phẩm liên quan" của trang chi tiết.

Lọc cộng tác item-item theo đồng xuất hiện: hai sản phẩm liên quan nếu cùng được một
người tương tác trong cùng một ngày. Độ liên quan dùng cosine trên số người:

    sim(i, j) = so_nguoi(i, j) / sqrt(so_nguoi(i) x so_nguoi(j))

Cosine (thay vì đếm thô) để sản phẩm siêu phổ biến không xuất hiện ở mọi danh sách,
cùng tinh thần giảm popularity bias của đề tài. Item thiếu danh sách thì web lấy bán
chạy cùng danh mục.
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.spark_utils import doc_delta, ghi_delta, in_ket_qua, tao_spark

TOI_DA_ITEM_MOI_PHIEN = 50


def tinh_lien_quan(silver, so_lien_quan=10, so_nguoi_toi_thieu=2):
    """Trả về ma_san_pham, xep_hang, ma_san_pham_lien_quan, do_lien_quan."""
    phien = silver.select("ma_nguoi_dung", "ngay", "ma_san_pham").distinct()
    # Chặn phiên quá dài (thường là bot sót lại) để số cặp không bùng nổ bình phương.
    phien = phien.withColumn(
        "n", F.count("*").over(Window.partitionBy("ma_nguoi_dung", "ngay"))
    ).filter(F.col("n") <= TOI_DA_ITEM_MOI_PHIEN).drop("n")

    so_nguoi_item = phien.groupBy("ma_san_pham").agg(F.countDistinct("ma_nguoi_dung").alias("ni"))
    a, b = phien.alias("a"), phien.alias("b")
    cap = (
        a.join(
            b,
            (F.col("a.ma_nguoi_dung") == F.col("b.ma_nguoi_dung"))
            & (F.col("a.ngay") == F.col("b.ngay"))
            & (F.col("a.ma_san_pham") != F.col("b.ma_san_pham")),
        )
        .groupBy(F.col("a.ma_san_pham").alias("i"), F.col("b.ma_san_pham").alias("j"))
        .agg(F.countDistinct("a.ma_nguoi_dung").alias("nij"))
        .filter(F.col("nij") >= so_nguoi_toi_thieu)
    )
    cap = (
        cap.join(so_nguoi_item.withColumnRenamed("ma_san_pham", "i").withColumnRenamed("ni", "n_i"), "i")
        .join(so_nguoi_item.withColumnRenamed("ma_san_pham", "j").withColumnRenamed("ni", "n_j"), "j")
        .withColumn("do_lien_quan", F.col("nij") / F.sqrt(F.col("n_i") * F.col("n_j")))
    )
    return (
        cap.withColumn(
            "xep_hang",
            F.row_number().over(Window.partitionBy("i").orderBy(F.desc("do_lien_quan"), "j")),
        )
        .filter(F.col("xep_hang") <= so_lien_quan)
        .select(
            F.col("i").alias("ma_san_pham"),
            "xep_hang",
            F.col("j").alias("ma_san_pham_lien_quan"),
            "do_lien_quan",
        )
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--so-lien-quan", type=int, default=10)
    args = ap.parse_args()
    spark = tao_spark("bd03-san-pham-lien-quan")
    kq = tinh_lien_quan(doc_delta(spark, "silver.tuong_tac"), args.so_lien_quan).cache()
    ghi_delta(kq, "gold.san_pham_lien_quan", tuy_chon={"overwriteSchema": "true"})
    in_ket_qua({"so_san_pham_co_lien_quan": kq.select("ma_san_pham").distinct().count()})


if __name__ == "__main__":
    main()
