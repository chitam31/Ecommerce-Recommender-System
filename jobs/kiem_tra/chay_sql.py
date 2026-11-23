"""kiem_tra/chay_sql.py - chạy lần lượt các câu Spark SQL và in kết quả dạng bảng (dùng cho demo, thu bằng chứng).

Gọi qua scripts/demo/delta_sql.sh, chạy local[2] trong container spark-master nên không lấy lõi
của cụm. Mỗi đối số là MỘT câu SQL; đối số bắt đầu bằng "#" chỉ in ra như tiêu đề;
"THAO_TAC <bảng>" đếm số commit theo loại thao tác (WRITE, MERGE, OPTIMIZE, ...) từ DESCRIBE HISTORY.
Với DESCRIBE HISTORY chỉ in các cột đọc được trên màn hình chiếu (version, thời điểm, thao tác,
tham số, số liệu); đặt BD03_SQL_DAY_DU=1 để in đủ mọi cột.
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

COT_LICH_SU = ["version", "timestamp", "operation", "operationParameters", "operationMetrics", "userMetadata"]


def rut_gon_lich_su(df, rong=70):
    """Chọn các cột chính của DESCRIBE HISTORY, đổi map sang chuỗi và cắt ngắn để vừa màn hình."""
    cot = []
    for c in COT_LICH_SU:
        if c not in df.columns:
            continue
        if c in ("operationParameters", "operationMetrics"):
            chuoi = F.to_json(F.col(c))
            cot.append(F.when(F.length(chuoi) > rong, F.concat(F.substring(chuoi, 1, rong), F.lit("..."))).otherwise(chuoi).alias(c))
        elif c == "userMetadata":
            cot.append(F.substring(F.col(c), 1, 50).alias(c))
        else:
            cot.append(F.col(c))
    return df.select(*cot)


def main():
    cac_cau = [c for c in sys.argv[1:] if c.strip()]
    spark = (
        SparkSession.builder.appName("bd03-chay-sql")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    day_du = os.environ.get("BD03_SQL_DAY_DU") == "1"
    loi = 0
    for cau in cac_cau:
        if cau.lstrip().startswith("#"):
            print("\n" + cau.strip(), flush=True)
            continue
        print(">> " + " ".join(cau.split()), flush=True)
        try:
            if cau.strip().upper().startswith("THAO_TAC "):
                bang = cau.strip()[len("THAO_TAC "):].strip()
                (spark.sql("DESCRIBE HISTORY " + bang).groupBy("operation")
                 .agg(F.count("*").alias("so_commit"), F.min("version").alias("tu_phien_ban"),
                      F.max("version").alias("den_phien_ban"))
                 .orderBy(F.desc("so_commit")).show(n=50, truncate=False))
                continue
            df = spark.sql(cau)
            if cau.strip().upper().startswith("DESCRIBE HISTORY") and not day_du:
                df = rut_gon_lich_su(df)
            df.show(n=50, truncate=False)
        except Exception as e:  # in lỗi rồi chạy tiếp câu sau, để một bảng chưa có không chặn cả demo
            loi += 1
            print("!! Lỗi: {}".format(str(e).splitlines()[0][:300]), flush=True)
    spark.stop()
    sys.exit(1 if loi else 0)


if __name__ == "__main__":
    main()
