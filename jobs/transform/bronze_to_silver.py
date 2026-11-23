"""transform/bronze_to_silver.py - khử trùng lặp, lọc bot, gán trọng số; bản ghi bị loại vào _quarantine.

Xử lý tăng dần theo ngày: chỉ tính lại những phân vùng ngày có dữ liệu mới, và ghi đè
đúng các phân vùng đó bằng replaceWhere. Chạy lại bao nhiêu lần cũng ra cùng kết quả,
nên Airflow có thể retry an toàn. Mốc đã xử lý lưu ở userMetadata của commit Delta.

Mỗi dòng bronze của các ngày được xử lý rơi vào đúng MỘT trong hai nơi: silver.tuong_tac, hoặc
_quarantine.tuong_tac kèm cột ly_do (mục A.4 đề cương: "bản ghi lỗi bị loại khỏi silver").
Đếm quarantine theo ly_do chính là bảng thống kê làm sạch của báo cáo.

Chạy:
  submit.sh lo transform/bronze_to_silver.py                         # tăng dần theo mốc (DAG 1)
  submit.sh lo transform/bronze_to_silver.py --tu-ngay 2015-09-01 --den-ngay 2015-09-03
                                                                     # backfill: tính lại đúng khoảng ngày đó
  submit.sh lo transform/bronze_to_silver.py --toan-bo               # tính lại mọi ngày
  submit.sh lo transform/bronze_to_silver.py --toan-bo --chi-cach-ly # chỉ dựng lại _quarantine, không ghi silver
"""
import argparse
from datetime import date
from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.spark_utils import (
    bang_ton_tai,
    doc_bronze,
    doc_delta,
    doc_ghi_chu_gan_nhat,
    ghi_delta,
    in_ket_qua,
    phien_ban_moi_nhat,
    tao_spark,
)

COT_NOI_DUNG = ["ma_nguoi_dung", "ma_san_pham", "loai_su_kien", "thoi_diem"]
COT_SILVER = ["ma_nguoi_dung", "ma_san_pham", "thoi_diem", "loai_su_kien", "trong_so", "ngay", "_nguon"]
# Thứ tự các bước làm sạch, cũng là các giá trị của cột ly_do trong _quarantine.tuong_tac.
LY_DO_LOAI = ("thieu_truong", "trung_ma_su_kien", "trung_noi_dung", "bot", "nhap_lap_5_giay",
              "loai_su_kien_khong_dung")


def _thu_tu(df):
    """Thứ tự cố định để chọn bản giữ lại trong một nhóm trùng: bản nạp vào bronze sớm nhất."""
    return [F.col("_ingest_ts").asc_nulls_last()] if "_ingest_ts" in df.columns else [F.col("thoi_diem")]


def _tach_ban_dau(df, khoa, thu_tu):
    """Trong mỗi nhóm cùng khóa giữ dòng đầu theo thu_tu; trả về (giữ, các bản sao).

    Dùng row_number thay cho dropDuplicates: dropDuplicates giữ một dòng tùy ý, tính lại hai
    lần (cho silver và cho quarantine) có thể chọn hai dòng khác nhau.
    """
    df = df.withColumn("_rn", F.row_number().over(Window.partitionBy(*khoa).orderBy(*thu_tu)))
    return df.filter(F.col("_rn") == 1).drop("_rn"), df.filter(F.col("_rn") > 1).drop("_rn")


def _nguoi_dung_bot(df, nguong=ch.NGUONG_BOT_MOI_GIO):
    """User có ít nhất một giờ vượt ngưỡng sự kiện."""
    return (
        df.groupBy("ma_nguoi_dung", F.date_trunc("hour", "thoi_diem").alias("gio"))
        .count()
        .filter(F.col("count") > nguong)
        .select("ma_nguoi_dung")
        .distinct()
    )


def _khoang_giay():
    """Số giây so với lần trước đó của cùng user - item - loại (null nếu là lần đầu)."""
    cua_so = Window.partitionBy("ma_nguoi_dung", "ma_san_pham", "loai_su_kien").orderBy("thoi_diem")
    return F.col("thoi_diem").cast("double") - F.lag("thoi_diem").over(cua_so).cast("double")


def _bieu_thuc_trong_so():
    bieu_thuc = F.lit(None).cast("double")
    for loai, trong_so in ch.TRONG_SO.items():
        bieu_thuc = F.when(F.col("loai_su_kien") == loai, F.lit(trong_so)).otherwise(bieu_thuc)
    return bieu_thuc


def loc_bot(df, nguong=ch.NGUONG_BOT_MOI_GIO):
    """User có ít nhất một giờ vượt ngưỡng bị coi là bot và loại toàn bộ."""
    return df.join(_nguoi_dung_bot(df, nguong), "ma_nguoi_dung", "left_anti")


def bo_ban_sao_noi_dung(df):
    """Bỏ các dòng trùng cả user, item, loại lẫn thời điểm (tới mili giây).

    ma_su_kien băm cả offset Kafka, nên khi bộ phát chạy lại một đoạn dữ liệu đã phát, cùng
    một sự kiện gốc mang ma_su_kien mới và lọt qua bước bỏ trùng theo ma_su_kien. Phải bỏ
    chúng TRƯỚC khi lọc bot: nếu không, số sự kiện mỗi giờ của user bị nhân đôi và user
    thật có 251-500 sự kiện/giờ bị coi nhầm là bot. Hai sự kiện thật trùng tới mili giây
    vốn cũng bị khu_trung_lap bỏ (cách nhau 0 giây), nên bước này không bỏ thêm dữ liệu thật.
    """
    return _tach_ban_dau(df, COT_NOI_DUNG, _thu_tu(df) + [F.col("ma_su_kien")])[0]


def khu_trung_lap(df, so_giay=ch.GIAY_KHU_TRUNG):
    """Bỏ sự kiện cùng user - item - loại xảy ra trong vòng so_giay so với lần trước đó."""
    return (
        df.withColumn("_khoang", _khoang_giay())
        .filter(F.col("_khoang").isNull() | (F.col("_khoang") > so_giay))
        .drop("_khoang")
    )


def gan_trong_so(df):
    """xem=1, them_gio=3, mua=5; bo_gio không phải tín hiệu quan tâm nên bị loại."""
    return df.withColumn("trong_so", _bieu_thuc_trong_so()).filter(F.col("trong_so").isNotNull())


def phan_loai(bronze, nguong_bot=ch.NGUONG_BOT_MOI_GIO, so_giay=ch.GIAY_KHU_TRUNG):
    """Chuỗi làm sạch đầy đủ; trả về (dòng giữ lại có trong_so, dòng bị loại có ly_do).

    Thứ tự giữ nguyên như bản trước: bỏ trùng ma_su_kien -> bỏ trùng nội dung -> lọc bot ->
    bỏ nhấp lặp trong 5 giây -> gán trọng số (bỏ loại không có trọng số, vd. bo_gio).
    """
    thu_tu = _thu_tu(bronze)
    bi_loai = []

    def loai(phan, ly_do):
        bi_loai.append(phan.withColumn("ly_do", F.lit(ly_do)))

    thieu = (F.col("ma_nguoi_dung").isNull() | F.col("ma_san_pham").isNull()
             | F.col("thoi_diem").isNull() | F.col("loai_su_kien").isNull())
    loai(bronze.filter(thieu), "thieu_truong")
    df = bronze.filter(~thieu)

    df, ban_sao = _tach_ban_dau(df, ["ma_su_kien"], thu_tu)
    loai(ban_sao, "trung_ma_su_kien")
    df, ban_sao = _tach_ban_dau(df, COT_NOI_DUNG, thu_tu + [F.col("ma_su_kien")])
    loai(ban_sao, "trung_noi_dung")

    bot = _nguoi_dung_bot(df, nguong_bot)
    loai(df.join(bot, "ma_nguoi_dung", "left_semi"), "bot")
    df = df.join(bot, "ma_nguoi_dung", "left_anti")

    df = df.withColumn("_khoang", _khoang_giay())
    hop_le = F.col("_khoang").isNull() | (F.col("_khoang") > so_giay)
    loai(df.filter(~hop_le).drop("_khoang"), "nhap_lap_5_giay")
    df = df.filter(hop_le).drop("_khoang")

    df = df.withColumn("trong_so", _bieu_thuc_trong_so())
    loai(df.filter(F.col("trong_so").isNull()).drop("trong_so"), "loai_su_kien_khong_dung")
    giu = df.filter(F.col("trong_so").isNotNull())
    return giu, reduce(lambda a, b: a.unionByName(b), bi_loai)


def lam_sach(bronze):
    """Chuỗi làm sạch đầy đủ từ bronze ra lược đồ silver."""
    return phan_loai(bronze)[0].select(*COT_SILVER)


def chon_ngay_can_xu_ly(spark, bronze, toan_bo, tu_ngay=None, den_ngay=None):
    """Trả về (danh sách ngày cần tính lại, mốc _ingest_ts mới, có phải backfill không).

    Backfill (tu_ngay/den_ngay): tính lại đúng các ngày bronze có dữ liệu trong khoảng đó, bất kể
    mốc; mốc KHÔNG đổi để lượt tăng dần kế tiếp vẫn xử lý những ngày mới chưa tính.
    """
    moc_moi = bronze.agg(F.max("_ingest_ts")).first()[0]
    if tu_ngay or den_ngay:
        tu = date.fromisoformat(tu_ngay or den_ngay)
        den = date.fromisoformat(den_ngay or tu_ngay)
        if den < tu:
            raise ValueError("--den-ngay {} trước --tu-ngay {}".format(den, tu))
        moi = bronze.filter((F.col("ngay") >= F.lit(tu)) & (F.col("ngay") <= F.lit(den)))
        return sorted(r["ngay"] for r in moi.select("ngay").distinct().collect() if r["ngay"]), moc_moi, True
    ghi_chu = None
    if bang_ton_tai(spark, "silver.tuong_tac") and not toan_bo:
        ghi_chu = doc_ghi_chu_gan_nhat(spark, "silver.tuong_tac", "moc_ingest")
    if ghi_chu and ghi_chu.get("moc_ingest"):
        moi = bronze.filter(F.col("_ingest_ts") > F.lit(ghi_chu["moc_ingest"]).cast("timestamp"))
    else:
        moi = bronze
    # Số ngày phân biệt chỉ vài trăm: collect danh sách ngày về driver là an toàn.
    return sorted(r["ngay"] for r in moi.select("ngay").distinct().collect() if r["ngay"]), moc_moi, False


def them_cot_tien_hoa(spark, silver_moi):
    """Sau khi schema evolution chạy (tuần 5), dòng mới cũng phải có giá và sao."""
    if not bang_ton_tai(spark, "silver.tuong_tac"):
        return silver_moi
    cot_hien_co = doc_delta(spark, "silver.tuong_tac").columns
    if "gia_san_pham" not in cot_hien_co or not bang_ton_tai(spark, "silver.san_pham"):
        return silver_moi
    san_pham = doc_delta(spark, "silver.san_pham")
    cot = [c for c in ("gia_san_pham", "xep_hang_sao") if c in san_pham.columns]
    return silver_moi.join(san_pham.select("ma_san_pham", *cot), "ma_san_pham", "left")


def ghi_theo_ngay(spark, df, ten_bang, cac_ngay, ghi_de_toan_bo, ghi_chu=None):
    """Ghi đè đúng các phân vùng ngày đã tính (replaceWhere), hoặc cả bảng nếu chưa có / --toan-bo."""
    if bang_ton_tai(spark, ten_bang) and not ghi_de_toan_bo:
        danh_sach = ",".join("'{}'".format(n) for n in cac_ngay)
        ghi_delta(df, ten_bang, "overwrite", ghi_chu=ghi_chu,
                  tuy_chon={"replaceWhere": "ngay IN ({})".format(danh_sach)})
    else:
        ghi_delta(df, ten_bang, "overwrite", phan_vung=["ngay"], ghi_chu=ghi_chu)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--toan-bo", action="store_true", help="Bỏ qua mốc, tính lại mọi ngày")
    ap.add_argument("--tu-ngay", help="Backfill: ngày đầu (YYYY-MM-DD) cần tính lại")
    ap.add_argument("--den-ngay", help="Backfill: ngày cuối (mặc định bằng --tu-ngay)")
    ap.add_argument("--chi-cach-ly", action="store_true",
                    help="Chỉ ghi _quarantine.tuong_tac (dựng lại cho dữ liệu đã có), không ghi silver")
    args = ap.parse_args()

    spark = tao_spark("bd03-bronze-to-silver")
    bronze = doc_bronze(spark).cache()
    cac_ngay, moc_moi, la_backfill = chon_ngay_can_xu_ly(spark, bronze, args.toan_bo, args.tu_ngay, args.den_ngay)
    if not cac_ngay:
        in_ket_qua({"so_ngay": 0, "so_dong": 0, "ghi_chu": "khong co du lieu moi"})
        return

    giu, bi_loai = phan_loai(bronze.filter(F.col("ngay").isin(cac_ngay)))
    bi_loai = bi_loai.withColumn("_cach_ly_luc", F.current_timestamp()).cache()
    # Tối đa 6 dòng (một dòng mỗi lý do): an toàn khi đưa về driver.
    thong_ke = {r["ly_do"]: r["count"] for r in bi_loai.groupBy("ly_do").count().collect()}
    ket_qua = {"so_ngay": len(cac_ngay), "tu": str(cac_ngay[0]), "den": str(cac_ngay[-1]),
               "backfill": la_backfill, "bi_loai": {k: thong_ke.get(k, 0) for k in LY_DO_LOAI}}

    if not args.chi_cach_ly:
        silver_moi = them_cot_tien_hoa(spark, giu.select(*COT_SILVER)).cache()
        ket_qua["so_dong"] = silver_moi.count()
        if la_backfill:
            ghi_chu = {"backfill": {"tu": str(cac_ngay[0]), "den": str(cac_ngay[-1]), "so_ngay": len(cac_ngay)}}
        else:
            ghi_chu = {"moc_ingest": str(moc_moi)}
        ghi_theo_ngay(spark, silver_moi, "silver.tuong_tac", cac_ngay, args.toan_bo, ghi_chu)
        ket_qua["phien_ban_silver"] = phien_ban_moi_nhat(spark, "silver.tuong_tac")

    ghi_theo_ngay(spark, bi_loai, "_quarantine.tuong_tac", cac_ngay, args.toan_bo)
    ket_qua["phien_ban_quarantine"] = phien_ban_moi_nhat(spark, "_quarantine.tuong_tac")
    in_ket_qua(ket_qua)


if __name__ == "__main__":
    main()
