"""transform/tien_hoa_luoc_do.py - Schema evolution tuần 5 của đề tài.

Thêm gia_san_pham và xep_hang_sao vào silver.tuong_tac mà không ghi lại toàn bộ bảng.
Các phiên bản Delta cũ vẫn đọc được với lược đồ 7 cột.

RetailRocket không có điểm đánh giá sao. xep_hang_sao ở đây là ĐẠI LƯỢNG THAY THẾ,
tính từ tỷ lệ mua trên lượt xem rồi đổi về thang 1-5; báo cáo phải nêu rõ điều này.
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.spark_utils import doc_delta, ghi_delta, in_ket_qua, phien_ban_moi_nhat, sql_bang, tao_spark


def tinh_sao_thay_the(thong_ke):
    """Đổi tỷ lệ mua/xem thành thang 1-5 theo thứ hạng phần trăm."""
    tinh = thong_ke.withColumn(
        "ty_le_mua", F.col("so_luot_mua") / F.greatest(F.col("so_luot_xem"), F.lit(1))
    )
    return tinh.withColumn(
        "xep_hang_sao",
        1.0 + 4.0 * F.percent_rank().over(Window.orderBy("ty_le_mua")),
    ).select("ma_san_pham", "xep_hang_sao")


def main():
    argparse.ArgumentParser().parse_args()
    spark = tao_spark("bd03-tien-hoa-luoc-do")
    phien_ban_truoc = phien_ban_moi_nhat(spark, "silver.tuong_tac")

    # Bước 1: bổ sung cột sao vào danh mục sản phẩm.
    san_pham = doc_delta(spark, "silver.san_pham")
    if "xep_hang_sao" not in san_pham.columns:
        sao = tinh_sao_thay_the(doc_delta(spark, "gold.thong_ke_san_pham"))
        moi = san_pham.join(sao, "ma_san_pham", "left").cache()
        moi.count()
        ghi_delta(moi, "silver.san_pham", tuy_chon={"mergeSchema": "true"})

    # Bước 2: thêm cột vào silver.tuong_tac, các tệp cũ không bị viết lại ở bước này.
    if "gia_san_pham" not in doc_delta(spark, "silver.tuong_tac").columns:
        spark.sql(
            "ALTER TABLE {} ADD COLUMNS (gia_san_pham DOUBLE, xep_hang_sao DOUBLE)".format(
                sql_bang("silver.tuong_tac")
            )
        )

    # Bước 3: điền giá trị cho các dòng đã có.
    doc_delta(spark, "silver.san_pham").select(
        "ma_san_pham", "gia_san_pham", "xep_hang_sao"
    ).createOrReplaceTempView("sp")
    spark.sql(
        """
        MERGE INTO {dich} AS t
        USING sp AS s
        ON t.ma_san_pham = s.ma_san_pham
        WHEN MATCHED THEN UPDATE SET
            t.gia_san_pham = s.gia_san_pham,
            t.xep_hang_sao = s.xep_hang_sao
        """.format(dich=sql_bang("silver.tuong_tac"))
    )
    in_ket_qua(
        {
            "phien_ban_truoc": phien_ban_truoc,
            "phien_ban_sau": phien_ban_moi_nhat(spark, "silver.tuong_tac"),
        }
    )


if __name__ == "__main__":
    main()
