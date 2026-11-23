"""streaming/tinh_thinh_hanh.py - Kafka -> gold.thinh_hanh_15p.

Cửa sổ trượt 15 phút, bước 1 phút, watermark 2 phút, outputMode("update") + MERGE.

Vì sao gom theo kafka_ts thay vì thời điểm gốc của sự kiện: bộ phát lại giữ timestamp
năm 2015 và nén 1 ngày thành 30 giây, còn web đẩy sự kiện năm 2026. Nếu gom theo thời
điểm gốc, sự kiện của web sẽ đẩy watermark lên 2026 và mọi sự kiện phát lại bị coi là
trễ. kafka_ts là thời điểm broker nhận, chung một đồng hồ cho cả hai nguồn.

Chạy: ha_tang/spark/submit.sh thinh_hanh streaming/tinh_thinh_hanh.py
"""
import argparse

from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.luoc_do import LUOC_DO_SU_KIEN
from common.nguon_luong import dat_nhip, doc_luong
from common.spark_utils import bang_ton_tai, doc_delta, sql_bang, tao_spark, thu_lai_khi_xung_dot


def lay_su_kien(df_tho):
    """Giữ lại lượt xem và thêm giỏ, là hai loại tham gia công thức điểm."""
    return (
        df_tho.withColumn("d", F.from_json("value", LUOC_DO_SU_KIEN))
        .filter(F.col("d.itemid").isNotNull() & F.col("d.event").isin("view", "addtocart"))
        .select(
            F.col("kafka_ts"),
            F.col("d.itemid").alias("ma_san_pham"),
            F.col("d.event").alias("su_kien"),
        )
    )


def gom_cua_so(su_kien):
    """Đếm theo cửa sổ trượt; watermark để Spark dọn được trạng thái cũ."""
    return (
        su_kien.withWatermark("kafka_ts", "2 minutes")
        .groupBy(F.window("kafka_ts", "15 minutes", "1 minute"), "ma_san_pham")
        .agg(
            F.sum(F.when(F.col("su_kien") == "view", 1).otherwise(0)).cast("int").alias("so_luot_xem"),
            F.sum(F.when(F.col("su_kien") == "addtocart", 1).otherwise(0))
            .cast("int")
            .alias("so_luot_them_gio"),
        )
        .select(
            F.col("window.start").alias("cua_so_bat_dau"),
            F.col("window.end").alias("cua_so_ket_thuc"),
            "ma_san_pham",
            "so_luot_xem",
            "so_luot_them_gio",
        )
    )


def tinh_diem(lo, trung_binh):
    """diem_thinh_hanh = (lượt xem + 3 x thêm giỏ) / (trung bình 24h + 10)."""
    return (
        lo.join(trung_binh, "ma_san_pham", "left")
        .fillna({"trung_binh_24h": 0.0})
        .withColumn(
            "diem_thinh_hanh",
            (F.col("so_luot_xem") + 3.0 * F.col("so_luot_them_gio"))
            / (F.col("trung_binh_24h") + 10.0),
        )
        .drop("trung_binh_24h")
    )


def ghi_lo(lo, so_lo):
    """foreachBatch: upsert theo (cửa sổ, sản phẩm) vào bảng gold."""
    spark = lo.sparkSession
    if bang_ton_tai(spark, "gold.thong_ke_san_pham"):
        # Đọc lại mỗi lô để luôn dùng thống kê mới nhất của job batch ban đêm.
        trung_binh = doc_delta(spark, "gold.thong_ke_san_pham").select(
            "ma_san_pham", "trung_binh_24h"
        )
    else:
        trung_binh = spark.createDataFrame([], "ma_san_pham string, trung_binh_24h double")
    tinh_diem(lo, trung_binh).createOrReplaceTempView("lo_thinh_hanh")
    # DAG bảo trì có thể OPTIMIZE/DELETE bảng này cùng lúc: làm lại thay vì để cả luồng dừng.
    thu_lai_khi_xung_dot(lambda: spark.sql(
        """
        MERGE INTO {dich} AS t
        USING lo_thinh_hanh AS s
        ON t.cua_so_bat_dau = s.cua_so_bat_dau
           AND t.cua_so_ket_thuc = s.cua_so_ket_thuc
           AND t.ma_san_pham = s.ma_san_pham
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """.format(dich=sql_bang("gold.thinh_hanh_15p"))
    ))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nguon", choices=["kafka", "tep"], default="kafka")
    ap.add_argument("--thu-muc-tep", default=None)
    ap.add_argument("--mot-lan", action="store_true")
    ap.add_argument("--nhip", default="20 seconds")
    # latest: bảng thịnh hành chỉ quan tâm hiện tại, không cần tính lại lịch sử.
    ap.add_argument("--bat-dau", default="latest")
    args = ap.parse_args()

    spark = tao_spark("bd03-tinh-thinh-hanh")
    tho = doc_luong(
        spark, ch.TOPIC_SU_KIEN, args.nguon, args.thu_muc_tep, bat_dau=args.bat_dau, gioi_han_lo=200000
    )
    writer = (
        gom_cua_so(lay_su_kien(tho))
        .writeStream.outputMode("update")
        .foreachBatch(ghi_lo)
        .option("checkpointLocation", ch.duong_dan_checkpoint("tinh_thinh_hanh_v1"))
    )
    dat_nhip(writer, args.mot_lan, args.nhip).start().awaitTermination()


if __name__ == "__main__":
    main()
