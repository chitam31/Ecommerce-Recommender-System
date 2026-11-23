"""Đọc luồng đầu vào cho các job streaming.

Chế độ kafka là chế độ chạy thật trên cụm. Chế độ tep đọc các tệp JSON Lines trong
một thư mục, dùng để kiểm thử cục bộ khi không có Kafka; thời điểm nhận được lấy
theo đồng hồ của Spark nên logic cửa sổ phía sau giữ nguyên.
"""
from pyspark.sql import functions as F

from . import cau_hinh as ch


def doc_luong(spark, topic, nguon="kafka", thu_muc_tep=None, bat_dau="earliest", gioi_han_lo=None):
    """Trả về DataFrame luồng có các cột: value, kafka_topic, kafka_partition, kafka_offset, kafka_ts."""
    if nguon == "tep":
        return (
            spark.readStream.text(thu_muc_tep)
            .withColumn("kafka_topic", F.input_file_name())
            .withColumn("kafka_partition", F.lit(0))
            .withColumn("kafka_offset", F.lit(0).cast("long"))
            .withColumn("kafka_ts", F.current_timestamp())
        )
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", ch.KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", bat_dau)
        .option("failOnDataLoss", "false")
    )
    if gioi_han_lo:
        reader = reader.option("maxOffsetsPerTrigger", gioi_han_lo)
    return reader.load().select(
        F.col("value").cast("string").alias("value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
    )


def dat_nhip(writer, mot_lan, nhip):
    """mot_lan=True xử lý hết dữ liệu đang có rồi dừng, dùng cho nạp bù và kiểm thử."""
    if mot_lan:
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=nhip)
