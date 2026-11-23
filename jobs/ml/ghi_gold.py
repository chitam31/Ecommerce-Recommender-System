"""ml/ghi_gold.py - gold.goi_y_tam -> gold.goi_y_nguoi_dung bằng MERGE theo user.

Đặc tả 3.7: cập nhật theo user, KHÔNG ghi đè toàn bảng. User không có trong lần chạy
này giữ nguyên danh sách cũ (DAG bảo trì xóa danh sách cũ hơn 7 ngày).

Danh sách mới có thể ngắn hơn danh sách cũ, nên những thứ hạng thừa của chính user đó
phải bị xóa. Điều kiện MERGE không cho phép truy vấn con, vì vậy các dòng thừa được đưa
vào nguồn với cờ _xoa = true và xử lý bằng nhánh WHEN MATCHED ... THEN DELETE.
"""
import argparse

from pyspark.sql import functions as F

from common.spark_utils import doc_delta, in_ket_qua, phien_ban_moi_nhat, sql_bang, tao_spark

COT = ["ma_nguoi_dung", "xep_hang", "ma_san_pham", "diem_du_doan", "nguon", "phien_ban_mo_hinh", "sinh_luc"]


def dung_nguon_merge(moi, cu):
    """Ghép dòng mới (_xoa = false) với dòng cũ thừa của cùng user (_xoa = true)."""
    hang_lon_nhat = moi.groupBy("ma_nguoi_dung").agg(F.max("xep_hang").alias("hang_moi"))
    thua = (
        cu.select("ma_nguoi_dung", "xep_hang")
        .join(hang_lon_nhat, "ma_nguoi_dung")
        .filter(F.col("xep_hang") > F.col("hang_moi"))
        .select(
            "ma_nguoi_dung",
            "xep_hang",
            F.lit(None).cast("string").alias("ma_san_pham"),
            F.lit(None).cast("double").alias("diem_du_doan"),
            F.lit(None).cast("string").alias("nguon"),
            F.lit(None).cast("string").alias("phien_ban_mo_hinh"),
            F.lit(None).cast("timestamp").alias("sinh_luc"),
        )
        .withColumn("_xoa", F.lit(True))
    )
    return moi.select(*COT).withColumn("_xoa", F.lit(False)).unionByName(thua)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ma-chay", required=True)
    args = ap.parse_args()

    spark = tao_spark("bd03-ghi-gold")
    # Tên phiên bản (als_ hay als_v2_) lấy đúng như bước danh_gia đã ghi vào lịch sử.
    ls = (
        doc_delta(spark, "gold.lich_su_huan_luyen")
        .filter(F.col("ma_chay") == args.ma_chay)
        .orderBy(F.desc("thoi_diem"))
        .first()
    )
    ten_phien_ban = ls["phien_ban_mo_hinh"] if ls else "als_" + args.ma_chay
    moi = (
        doc_delta(spark, "gold.goi_y_tam")
        .withColumn("phien_ban_mo_hinh", F.lit(ten_phien_ban))
        .withColumn("sinh_luc", F.current_timestamp())
    )
    nguon = dung_nguon_merge(moi, doc_delta(spark, "gold.goi_y_nguoi_dung"))
    nguon.createOrReplaceTempView("goi_y_moi")

    cap_nhat = ", ".join("t.{0} = s.{0}".format(c) for c in COT)
    them = "({}) VALUES ({})".format(", ".join(COT), ", ".join("s." + c for c in COT))
    spark.sql(
        """
        MERGE INTO {dich} AS t
        USING goi_y_moi AS s
        ON t.ma_nguoi_dung = s.ma_nguoi_dung AND t.xep_hang = s.xep_hang
        WHEN MATCHED AND s._xoa THEN DELETE
        WHEN MATCHED THEN UPDATE SET {cap_nhat}
        WHEN NOT MATCHED AND NOT s._xoa THEN INSERT {them}
        """.format(dich=sql_bang("gold.goi_y_nguoi_dung"), cap_nhat=cap_nhat, them=them)
    )
    so_lieu = spark.sql("DESCRIBE HISTORY {} LIMIT 1".format(sql_bang("gold.goi_y_nguoi_dung"))).first()[
        "operationMetrics"
    ]

    # Đánh dấu mô hình đã thực sự được triển khai.
    spark.sql(
        "UPDATE {} SET trang_thai = 'trien_khai' WHERE ma_chay = '{}' AND trang_thai = 'cho_trien_khai'".format(
            sql_bang("gold.lich_su_huan_luyen"), args.ma_chay.replace("'", "")
        )
    )
    in_ket_qua(
        {
            "ma_chay": args.ma_chay,
            "phien_ban_mo_hinh": ten_phien_ban,
            "phien_ban_gold": phien_ban_moi_nhat(spark, "gold.goi_y_nguoi_dung"),
            "so_dong_chen": so_lieu.get("numTargetRowsInserted"),
            "so_dong_cap_nhat": so_lieu.get("numTargetRowsUpdated"),
            "so_dong_xoa": so_lieu.get("numTargetRowsDeleted"),
        }
    )


if __name__ == "__main__":
    main()
