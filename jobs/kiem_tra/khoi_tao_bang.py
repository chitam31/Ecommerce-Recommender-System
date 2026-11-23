"""kiem_tra/khoi_tao_bang.py - tạo các bảng Delta cần có trước khi luồng và web chạy.

Chạy một lần sau khi dựng cụm (chạy lại vô hại vì dùng IF NOT EXISTS). Change Data Feed
được bật ngay lúc tạo gold.nhat_ky_hien_thi để CDF có từ phiên bản 0.
"""
import argparse

from common.luoc_do import BANG_DELTA
from common.spark_utils import bang_ton_tai, in_ket_qua, sql_bang, tao_spark


def main():
    argparse.ArgumentParser().parse_args()
    spark = tao_spark("bd03-khoi-tao-bang")
    ket_qua = {}
    for ten, dac_ta in BANG_DELTA.items():
        da_co = bang_ton_tai(spark, ten)
        thuoc_tinh = ""
        if dac_ta["thuoc_tinh"]:
            thuoc_tinh = " TBLPROPERTIES ({})".format(
                ", ".join("'{}' = '{}'".format(k, v) for k, v in dac_ta["thuoc_tinh"].items())
            )
        spark.sql(
            "CREATE TABLE IF NOT EXISTS {} ({}) USING DELTA{}".format(
                sql_bang(ten), " ".join(dac_ta["cot"].split()), thuoc_tinh
            )
        )
        if da_co and dac_ta["thuoc_tinh"]:
            # Bảng có từ trước: bảo đảm thuộc tính (ví dụ CDF) vẫn được bật.
            spark.sql("ALTER TABLE {} SET{}".format(sql_bang(ten), thuoc_tinh))
        ket_qua[ten] = "da_co" if da_co else "tao_moi"
    in_ket_qua(ket_qua)


if __name__ == "__main__":
    main()
