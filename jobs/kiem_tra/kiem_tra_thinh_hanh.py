"""kiem_tra/kiem_tra_thinh_hanh.py - DAG 2: gold.thinh_hanh_15p có dữ liệu trong 15 phút qua?

Thoát với mã 1 nếu dữ liệu cũ hơn ngưỡng, để task Airflow chuyển sang thất bại và
nhánh cảnh báo/khởi động lại chạy.
"""
import argparse
import sys
from datetime import datetime

from pyspark.sql import functions as F

from common.spark_utils import bang_ton_tai, doc_delta, in_ket_qua, tao_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nguong-phut", type=int, default=15)
    args = ap.parse_args()

    spark = tao_spark("bd03-kiem-tra-thinh-hanh")
    if not bang_ton_tai(spark, "gold.thinh_hanh_15p"):
        in_ket_qua({"song": False, "ly_do": "chua co bang"})
        sys.exit(1)

    th = doc_delta(spark, "gold.thinh_hanh_15p")
    bay_gio = datetime.utcnow()
    # Cửa sổ trượt kết thúc ở tương lai (tối đa 1 phút); "mới" = cửa sổ chứa sự kiện gần đây.
    gan_day = th.filter(F.col("cua_so_ket_thuc") >= F.lit(bay_gio) - F.expr("INTERVAL {} MINUTES".format(args.nguong_phut)))
    moi_nhat = th.agg(F.max("cua_so_ket_thuc")).first()[0]
    so_dong = gan_day.count()
    song = so_dong > 0
    in_ket_qua(
        {
            "song": song,
            "cua_so_moi_nhat": moi_nhat,
            "so_dong_trong_nguong": so_dong,
            "bay_gio_utc": bay_gio,
            "nguong_phut": args.nguong_phut,
        }
    )
    if not song:
        sys.exit(1)


if __name__ == "__main__":
    main()
