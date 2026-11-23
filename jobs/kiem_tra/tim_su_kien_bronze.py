"""kiem_tra/tim_su_kien_bronze.py - minh chứng vòng khép kín (mục 3.11).

Bấm "Thêm giỏ" trên web -> sự kiện vào Kafka -> job này tìm thấy nó trong bronze và in
độ trễ từ lúc bấm (thoi_diem) tới lúc ghi bronze (_ingest_ts) và lúc broker nhận (kafka_ts).
"""
import argparse

from pyspark.sql import functions as F

from common.spark_utils import doc_bronze, in_ket_qua, tao_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ma-nguoi-dung", required=True)
    ap.add_argument("--ma-san-pham", default=None)
    ap.add_argument("--so-dong", type=int, default=10)
    args = ap.parse_args()

    spark = tao_spark("bd03-tim-su-kien-bronze")
    df = doc_bronze(spark).filter(F.col("ma_nguoi_dung") == args.ma_nguoi_dung)
    if args.ma_san_pham:
        df = df.filter(F.col("ma_san_pham") == args.ma_san_pham)
    dong = (
        df.orderBy(F.desc("thoi_diem"))
        .limit(args.so_dong)
        .select(
            "thoi_diem",
            "ma_nguoi_dung",
            "ma_san_pham",
            "loai_su_kien",
            "_nguon",
            "kafka_ts",
            "_ingest_ts",
            (F.col("_ingest_ts").cast("double") - F.col("thoi_diem").cast("double")).alias("tre_den_bronze_giay"),
        )
        .collect()
    )
    for d in dong:
        print(d.asDict(), flush=True)
    in_ket_qua({"so_su_kien": len(dong), "tre_den_bronze_giay": dong[0]["tre_den_bronze_giay"] if dong else None})


if __name__ == "__main__":
    main()
