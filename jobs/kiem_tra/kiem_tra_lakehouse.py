"""kiem_tra/kiem_tra_lakehouse.py - kiểm tra hồ dữ liệu đọc được ở gốc hiện tại (sau khi chuyển gốc).

Đọc bronze qua _spark_metadata (bước dễ hỏng nhất khi dời thư mục), đếm silver, và với mỗi bảng
Delta in phiên bản mới nhất. Bảng chưa có thì ghi "chua_co" chứ không coi là lỗi. In một dòng
JSON ở cuối như mọi job khác.

Chạy: ha_tang/spark/submit.sh nhanh kiem_tra/kiem_tra_lakehouse.py
"""
import argparse

from common import cau_hinh as ch
from common.spark_utils import bang_ton_tai, doc_bronze, doc_delta, in_ket_qua, phien_ban_moi_nhat, tao_spark


def main():
    argparse.ArgumentParser().parse_args()
    spark = tao_spark("bd03-kiem-tra-lakehouse")
    ket_qua = {"goc": ch.LAKE_GOC, "bang": {}}
    ket_qua["so_dong_bronze"] = doc_bronze(spark).count()
    for ten in sorted(ch._BANG):
        if ten == "bronze.su_kien":
            continue
        if not bang_ton_tai(spark, ten):
            ket_qua["bang"][ten] = "chua_co"
            continue
        ket_qua["bang"][ten] = {"phien_ban": phien_ban_moi_nhat(spark, ten)}
    if bang_ton_tai(spark, "silver.tuong_tac"):
        ket_qua["so_dong_silver"] = doc_delta(spark, "silver.tuong_tac").count()
    in_ket_qua(ket_qua)


if __name__ == "__main__":
    main()
