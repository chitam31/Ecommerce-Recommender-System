"""streaming/ghi_hien_thi.py - topic hien-thi -> gold.nhat_ky_hien_thi.

Job bổ sung so với bảng 3.5: nếu web ghi thẳng từng impression vào Delta thì mỗi
impression là một commit và rất nhiều tệp nhỏ. Web đẩy vào Kafka, job này gom theo lô
rồi MERGE: dòng mới được thêm, phản hồi click/mua cập nhật dòng đã có.

Chạy: ha_tang/spark/submit.sh hien_thi streaming/ghi_hien_thi.py
"""
import argparse

from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.luoc_do import LUOC_DO_HIEN_THI
from common.nguon_luong import dat_nhip, doc_luong
from common.spark_utils import sql_bang, tao_spark, thu_lai_khi_xung_dot


def gop_theo_hien_thi(lo):
    """Gộp mọi thông điệp của cùng một ma_hien_thi trong lô thành một dòng."""
    d = lo.withColumn("d", F.from_json("value", LUOC_DO_HIEN_THI)).select("d.*")
    d = d.filter(F.col("ma_hien_thi").isNotNull())
    return d.groupBy("ma_hien_thi").agg(
        F.first("ma_nguoi_dung", ignorenulls=True).alias("ma_nguoi_dung"),
        F.first("nhom_ab", ignorenulls=True).alias("nhom_ab"),
        F.first("vi_tri", ignorenulls=True).cast("int").alias("vi_tri"),
        F.first("ma_san_pham", ignorenulls=True).alias("ma_san_pham"),
        F.timestamp_millis(
            F.first(F.when(F.col("loai") == "hien_thi", F.col("thoi_diem_ms")), ignorenulls=True)
        ).alias("thoi_diem_hien_thi"),
        F.max((F.col("hanh_dong") == "click").cast("int")).alias("co_click"),
        F.max((F.col("hanh_dong") == "mua").cast("int")).alias("co_mua"),
        F.first("nguon_traffic", ignorenulls=True).alias("nguon_traffic"),
        F.first("chien_luoc", ignorenulls=True).alias("chien_luoc"),
    ).select(
        "ma_hien_thi",
        "ma_nguoi_dung",
        "nhom_ab",
        "vi_tri",
        "ma_san_pham",
        "thoi_diem_hien_thi",
        # Mua hàng cũng được tính là đã click vào sản phẩm đó.
        (F.coalesce(F.col("co_click"), F.lit(0)) + F.coalesce(F.col("co_mua"), F.lit(0)) > 0).alias(
            "da_click"
        ),
        (F.coalesce(F.col("co_mua"), F.lit(0)) > 0).alias("da_mua"),
        F.coalesce(F.col("nguon_traffic"), F.lit("web")).alias("nguon_traffic"),
        "chien_luoc",
    )


def ghi_lo(lo, so_lo):
    """Chèn impression mới và cập nhật cờ click/mua; chạy lại lô cũng không nhân đôi."""
    spark = lo.sparkSession
    gop_theo_hien_thi(lo).createOrReplaceTempView("lo_hien_thi")
    # DAG bảo trì có thể OPTIMIZE bảng này cùng lúc: làm lại thay vì để cả luồng dừng.
    thu_lai_khi_xung_dot(lambda: spark.sql(
        """
        MERGE INTO {dich} AS t
        USING lo_hien_thi AS s
        ON t.ma_hien_thi = s.ma_hien_thi
        WHEN MATCHED AND ((s.da_click AND NOT t.da_click) OR (s.da_mua AND NOT t.da_mua))
          THEN UPDATE SET t.da_click = t.da_click OR s.da_click,
                          t.da_mua = t.da_mua OR s.da_mua
        WHEN NOT MATCHED AND s.thoi_diem_hien_thi IS NOT NULL
          THEN INSERT *
        """.format(dich=sql_bang("gold.nhat_ky_hien_thi"))
    ))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nguon", choices=["kafka", "tep"], default="kafka")
    ap.add_argument("--thu-muc-tep", default=None)
    ap.add_argument("--mot-lan", action="store_true")
    ap.add_argument("--nhip", default="30 seconds")
    args = ap.parse_args()

    spark = tao_spark("bd03-ghi-hien-thi")
    tho = doc_luong(spark, ch.TOPIC_HIEN_THI, args.nguon, args.thu_muc_tep)
    writer = (
        tho.writeStream.foreachBatch(ghi_lo).option(
            "checkpointLocation", ch.duong_dan_checkpoint("ghi_hien_thi_v1")
        )
    )
    dat_nhip(writer, args.mot_lan, args.nhip).start().awaitTermination()


if __name__ == "__main__":
    main()
