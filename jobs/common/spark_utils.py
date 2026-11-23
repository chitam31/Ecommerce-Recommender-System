"""Các hàm tiện ích cho Spark và Delta Lake.

Toàn bộ thao tác Delta đi qua SQL (MERGE, DESCRIBE HISTORY, VACUUM...) nên job
không cần gói Python delta-spark trên executor.
"""
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import cau_hinh as ch


# Tên các ngoại lệ xung đột ghi đồng thời của Delta (optimistic concurrency). Xảy ra khi hai
# giao dịch cùng sửa một bảng, ví dụ DAG bảo trì OPTIMIZE/DELETE gold.thinh_hanh_15p đúng lúc
# luồng thịnh hành MERGE vào đó: giao dịch commit sau bị từ chối và phải làm lại.
XUNG_DOT_DELTA = (
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException",
    "ConcurrentWriteException",
    "MetadataChangedException",
    "ProtocolChangedException",
)


def la_xung_dot_delta(loi):
    """Ngoại lệ có phải xung đột ghi đồng thời của Delta không (qua PySpark, tên lớp Java
    nằm trong thông điệp lỗi)."""
    van_ban = "{} {}".format(type(loi).__name__, loi)
    return any(ten in van_ban for ten in XUNG_DOT_DELTA)


def thu_lai_khi_xung_dot(ham, so_lan=5, cho_giay=3.0, ngu=time.sleep):
    """Gọi ham(); nếu Delta từ chối vì xung đột ghi đồng thời thì chờ rồi làm lại, tối đa so_lan.

    An toàn vì giao dịch bị từ chối chưa ghi gì; các lệnh dùng hàm này đều là MERGE theo khóa
    hoặc OPTIMIZE/DELETE, chạy lại cho cùng kết quả. Lỗi khác ném ra ngay.
    """
    for lan in range(1, so_lan + 1):
        try:
            return ham()
        except Exception as loi:  # noqa: BLE001 - lọc bằng la_xung_dot_delta ngay dưới
            if lan == so_lan or not la_xung_dot_delta(loi):
                raise
            print("[xung_dot_delta] lần {}: {}; thử lại sau {:.0f} giây".format(
                lan, type(loi).__name__, cho_giay * lan), flush=True)
            ngu(cho_giay * lan)


def tao_spark(ten_app, cau_hinh_them=None):
    """Tạo SparkSession có bật Delta, múi giờ UTC như job bronze cũ của nhóm."""
    builder = (
        SparkSession.builder.appName(ten_app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        # Mọi bảng Delta do job tạo ra giữ tệp cũ 30 ngày, nhật ký 60 ngày. Nếu để
        # mặc định 7 ngày, DAG bảo trì sẽ VACUUM mất tệp của phiên bản cũ và
        # VERSION AS OF không còn đọc được: tái lập thí nghiệm thất bại.
        .config(
            "spark.databricks.delta.properties.defaults.deletedFileRetentionDuration",
            ch.GIU_FILE_DA_XOA,
        )
        .config(
            "spark.databricks.delta.properties.defaults.logRetentionDuration",
            ch.GIU_NHAT_KY_DELTA,
        )
    )
    for khoa, gia_tri in (cau_hinh_them or {}).items():
        builder = builder.config(khoa, gia_tri)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def sql_bang(ten_bang):
    """Định danh bảng theo đường dẫn để dùng trong câu SQL của Delta."""
    return "delta.`{}`".format(ch.duong_dan(ten_bang))


def ton_tai_duong_dan(spark, duong_dan):
    """Kiểm tra đường dẫn trên HDFS hoặc đĩa cục bộ qua Hadoop FileSystem."""
    jvm = spark._jvm
    cau_hinh_hadoop = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(duong_dan)
    return path.getFileSystem(cau_hinh_hadoop).exists(path)


def bang_ton_tai(spark, ten_bang):
    """Một thư mục là bảng Delta khi có _delta_log bên trong."""
    return ton_tai_duong_dan(spark, ch.duong_dan(ten_bang) + "/_delta_log")


def doc_delta(spark, ten_bang, phien_ban=None):
    """Đọc bảng Delta, có thể đọc đúng một phiên bản cũ (time travel)."""
    reader = spark.read.format("delta")
    if phien_ban is not None:
        reader = reader.option("versionAsOf", int(phien_ban))
    return reader.load(ch.duong_dan(ten_bang))


def doc_bronze(spark):
    """Bronze là Parquet ghi bởi file sink, Spark tự đọc qua _spark_metadata."""
    return spark.read.parquet(ch.duong_dan("bronze.su_kien"))


def phien_ban_moi_nhat(spark, ten_bang):
    """Số phiên bản Delta mới nhất của bảng."""
    dong = spark.sql("DESCRIBE HISTORY {} LIMIT 1".format(sql_bang(ten_bang))).first()
    return int(dong["version"])


def gom_theo_phan_vung(df, phan_vung):
    """Đưa mọi dòng cùng giá trị phân vùng về một task, để mỗi phân vùng chỉ ghi ra MỘT tệp.

    Không gom thì mỗi task (spark.sql.shuffle.partitions = 64 ở hồ sơ lo) ghi một tệp cho mỗi
    ngày nó tình cờ chứa: 135 ngày x 64 task = hàng nghìn tệp vài KB (bài toán tệp nhỏ). Đo ngày
    10/11/2026 trên dữ liệu mẫu 20 nghìn dòng: mọi job đọc silver chậm hẳn vì mỗi tệp tốn vài lượt
    hỏi NameNode và DataNode qua Wi-Fi.
    """
    return df.repartition(*[F.col(c) for c in phan_vung])


def ghi_delta(df, ten_bang, che_do="overwrite", phan_vung=None, ghi_chu=None, tuy_chon=None):
    """Ghi DataFrame vào bảng Delta. ghi_chu được lưu ở userMetadata của commit."""
    if phan_vung:
        df = gom_theo_phan_vung(df, phan_vung)
    writer = df.write.format("delta").mode(che_do)
    if phan_vung:
        writer = writer.partitionBy(*phan_vung)
    if ghi_chu is not None:
        writer = writer.option("userMetadata", json.dumps(ghi_chu, ensure_ascii=False))
    for khoa, gia_tri in (tuy_chon or {}).items():
        writer = writer.option(khoa, gia_tri)
    writer.save(ch.duong_dan(ten_bang))


def doc_ghi_chu_commit(spark, ten_bang, phien_ban=None):
    """Đọc lại userMetadata (JSON) của một commit; None nếu commit không có."""
    lich_su = spark.sql("DESCRIBE HISTORY {}".format(sql_bang(ten_bang)))
    if phien_ban is None:
        dong = lich_su.orderBy(lich_su.version.desc()).first()
    else:
        dong = lich_su.filter(lich_su.version == int(phien_ban)).first()
    if dong is None or not dong["userMetadata"]:
        return None
    return json.loads(dong["userMetadata"])


def doc_ghi_chu_gan_nhat(spark, ten_bang, khoa):
    """Tìm commit gần nhất có userMetadata chứa khoa.

    Cần thiết vì các commit ALTER TABLE hoặc MERGE chạy tay không mang userMetadata.
    """
    lich_su = spark.sql("DESCRIBE HISTORY {}".format(sql_bang(ten_bang)))
    for dong in lich_su.orderBy(lich_su.version.desc()).select("userMetadata").collect():
        if not dong["userMetadata"]:
            continue
        try:
            ghi_chu = json.loads(dong["userMetadata"])
        except ValueError:
            continue
        if isinstance(ghi_chu, dict) and khoa in ghi_chu:
            return ghi_chu
    return None


def tim_ghi_chu_theo(spark, ten_bang, khoa, gia_tri):
    """Tìm userMetadata của commit có ghi_chu[khoa] == gia_tri (ví dụ ma_chay)."""
    lich_su = spark.sql("DESCRIBE HISTORY {}".format(sql_bang(ten_bang)))
    for dong in lich_su.orderBy(lich_su.version.desc()).select("userMetadata").collect():
        try:
            ghi_chu = json.loads(dong["userMetadata"] or "null")
        except ValueError:
            continue
        if isinstance(ghi_chu, dict) and ghi_chu.get(khoa) == gia_tri:
            return ghi_chu
    return None


def so_tep(spark, ten_bang):
    """Số tệp dữ liệu đang hoạt động của bảng (DESCRIBE DETAIL)."""
    return int(spark.sql("DESCRIBE DETAIL {}".format(sql_bang(ten_bang))).first()["numFiles"])


def in_ket_qua(ket_qua):
    """In JSON ở dòng stdout cuối cùng: Airflow BashOperator đẩy dòng này vào XCom."""
    print(json.dumps(ket_qua, ensure_ascii=False, default=str), flush=True)
