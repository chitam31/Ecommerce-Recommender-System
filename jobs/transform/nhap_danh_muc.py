"""transform/nhap_danh_muc.py - nạp thuộc tính sản phẩm RetailRocket vào silver.san_pham.

Job chạy một lần (hoặc khi cập nhật catalog), cần có trước bước nap_su_kien để bronze
có ma_danh_muc. Đầu vào là các CSV gốc đã hdfs dfs -put vào /lakehouse/bd03_khuyen_nghi/landing/retailrocket/.
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.spark_utils import ghi_delta, in_ket_qua, tao_spark


def dung_bang_san_pham(thuoc_tinh, cay_danh_muc):
    """Lấy giá trị mới nhất của từng thuộc tính rồi xoay thành một dòng mỗi sản phẩm.

    Thuộc tính "790" là cột có giá trị dạng n277200.000, được cộng đồng dùng làm giá
    tham khảo vì các thuộc tính khác của RetailRocket đều đã băm.
    """
    can_lay = thuoc_tinh.filter(F.col("property").isin("categoryid", "790", "available"))
    moi_nhat = (
        can_lay.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("itemid", "property").orderBy(F.col("timestamp").desc())
            ),
        )
        .filter(F.col("rn") == 1)
        .groupBy(F.col("itemid").cast("string").alias("ma_san_pham"))
        .agg(
            F.max(F.when(F.col("property") == "categoryid", F.col("value"))).alias("ma_danh_muc"),
            F.max(F.when(F.col("property") == "790", F.col("value"))).alias("gia_tho"),
            F.max(F.when(F.col("property") == "available", F.col("value"))).alias("con_hang_tho"),
        )
    )
    cay = cay_danh_muc.select(
        F.col("categoryid").cast("string").alias("ma_danh_muc"),
        F.col("parentid").cast("string").alias("danh_muc_cha"),
    )
    return (
        moi_nhat.join(cay, "ma_danh_muc", "left")
        .withColumn(
            "gia_san_pham",
            F.regexp_extract("gia_tho", r"^n([0-9.]+)", 1).cast("double"),
        )
        .withColumn("con_hang", F.col("con_hang_tho") == "1")
        .select("ma_san_pham", "ma_danh_muc", "danh_muc_cha", "gia_san_pham", "con_hang")
    )


def main():
    argparse.ArgumentParser().parse_args()
    spark = tao_spark("bd03-nhap-danh-muc")
    thuoc_tinh = spark.read.option("header", True).csv(
        [
            ch.duong_dan_landing("item_properties_part1.csv"),
            ch.duong_dan_landing("item_properties_part2.csv"),
        ]
    )
    cay = spark.read.option("header", True).csv(ch.duong_dan_landing("category_tree.csv"))
    san_pham = dung_bang_san_pham(thuoc_tinh, cay).cache()
    ghi_delta(san_pham, "silver.san_pham")
    in_ket_qua({"so_san_pham": san_pham.count()})


if __name__ == "__main__":
    main()
