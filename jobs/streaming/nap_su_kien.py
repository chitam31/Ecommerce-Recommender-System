"""streaming/nap_su_kien.py - Kafka -> bronze.su_kien (Parquet), checkpoint trên HDFS.

Phát triển từ jobs/streaming/nap_su_kien_bronze.py của nhóm: vẫn đọc topic
retailrocket-events, vẫn ghi Parquet phân vùng theo ngày; bổ sung ánh xạ sang đúng
lược đồ tiếng Việt của đề tài và nhận cả sự kiện do web đẩy vào.

Chạy: ha_tang/spark/submit.sh nap streaming/nap_su_kien.py
"""
import argparse

from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.luoc_do import LUOC_DO_SU_KIEN
from common.nguon_luong import dat_nhip, doc_luong
from common.spark_utils import bang_ton_tai, doc_delta, in_ket_qua, tao_spark



def _anh_xa_loai():
    """view/addtocart/transaction -> xem/them_gio/mua. Tạo khi gọi vì F.lit cần SparkContext."""
    return F.create_map(*[F.lit(x) for cap in ch.LOAI_SU_KIEN.items() for x in cap])


def chuan_hoa_su_kien(df_tho, danh_muc=None):
    """Đổi dòng Kafka thô sang lược đồ bronze.

    df_tho có các cột value, kafka_topic, kafka_partition, kafka_offset, kafka_ts.
    danh_muc (tùy chọn) có cột ma_san_pham, ma_danh_muc_sp để bổ sung danh mục.
    """
    df = df_tho.withColumn("d", F.from_json("value", LUOC_DO_SU_KIEN)).filter(
        F.col("d.timestamp").isNotNull()
        & F.col("d.visitorid").isNotNull()
        & F.col("d.itemid").isNotNull()
        & F.col("d.event").isNotNull()
    )
    df = df.select(
        F.sha2(
            F.concat_ws("|", "kafka_topic", "kafka_partition", "kafka_offset", "value"), 256
        ).alias("ma_su_kien"),
        F.timestamp_millis(F.col("d.timestamp")).alias("thoi_diem"),
        F.col("d.visitorid").alias("ma_nguoi_dung"),
        # RetailRocket không có phiên: gom theo khung 30 phút của từng người dùng.
        F.coalesce(
            F.col("d.ma_phien"),
            F.md5(
                F.concat_ws(
                    "|", F.col("d.visitorid"), F.floor(F.col("d.timestamp") / 1800000).cast("string")
                )
            ),
        ).alias("ma_phien"),
        F.col("d.itemid").alias("ma_san_pham"),
        F.coalesce(F.element_at(_anh_xa_loai(), F.col("d.event")), F.col("d.event")).alias(
            "loai_su_kien"
        ),
        F.col("d.ma_danh_muc").alias("ma_danh_muc_goc"),
        F.coalesce(F.col("d.thiet_bi"), F.lit("khong_ro")).alias("thiet_bi"),
        F.col("d.transactionid").alias("ma_giao_dich"),
        F.col("kafka_ts"),
        F.coalesce(F.col("d.nguon"), F.lit("cu")).alias("_nguon"),
    )
    if danh_muc is not None:
        df = df.join(danh_muc, "ma_san_pham", "left").withColumn(
            "ma_danh_muc", F.coalesce("ma_danh_muc_goc", "ma_danh_muc_sp")
        ).drop("ma_danh_muc_goc", "ma_danh_muc_sp")
    else:
        df = df.withColumnRenamed("ma_danh_muc_goc", "ma_danh_muc")
    return df.withColumn("ngay", F.to_date("thoi_diem")).withColumn(
        "_ingest_ts", F.current_timestamp()
    ).withColumn("_schema_ver", F.lit(2))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nguon", choices=["kafka", "tep"], default="kafka")
    ap.add_argument("--thu-muc-tep", default=None)
    ap.add_argument("--mot-lan", action="store_true", help="Xử lý hết dữ liệu hiện có rồi dừng")
    ap.add_argument("--nhip", default="15 seconds")
    ap.add_argument("--gioi-han-lo", type=int, default=200000)
    args = ap.parse_args()

    spark = tao_spark("bd03-nap-su-kien")
    tho = doc_luong(
        spark, ch.TOPIC_SU_KIEN, args.nguon, args.thu_muc_tep, gioi_han_lo=args.gioi_han_lo
    )

    danh_muc = None
    if bang_ton_tai(spark, "silver.san_pham"):
        danh_muc = doc_delta(spark, "silver.san_pham").select(
            "ma_san_pham", F.col("ma_danh_muc").alias("ma_danh_muc_sp")
        )

    writer = (
        chuan_hoa_su_kien(tho, danh_muc)
        .writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", ch.duong_dan_checkpoint("nap_su_kien_v2"))
        .partitionBy("ngay")
    )
    if args.mot_lan:
        # Giữ tiến độ của mọi lô (mặc định chỉ 100 lô gần nhất) để đếm đủ số sự kiện đã nạp.
        spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "100000")
    truy_van = dat_nhip(writer, args.mot_lan, args.nhip).start(ch.duong_dan("bronze.su_kien"))
    truy_van.awaitTermination()
    if args.mot_lan:
        in_ket_qua(tom_tat_tien_do(truy_van.recentProgress))


def tom_tat_tien_do(cac_lo):
    """Số lô và số sự kiện mới đã ghi vào bronze trong một lần chạy --mot-lan."""
    return {
        "so_lo": len(cac_lo),
        "so_su_kien_moi": sum(int(lo.get("numInputRows") or 0) for lo in cac_lo),
    }


if __name__ == "__main__":
    main()
