"""ml/chuan_bi_ma_tran.py - silver.tuong_tac -> features.ma_tran_tuong_tac.

Kế thừa ý tưởng quan trọng nhất trong bản chuan_bi_ma_tran.py cũ của nhóm: KHÔNG fit lại
StringIndexer từ đầu mỗi lần chạy. Nạp nhãn cũ, chỉ nối thêm nhãn mới ở cuối, nhờ vậy
user_idx/item_idx của mọi phiên bản features cũ vẫn trỏ đúng người và đúng sản phẩm.

Bảng features có thêm cột "tap" (train/test) ngoài 3 cột của đặc tả. Lý do: một phiên
bản Delta phải chứa đủ mọi thứ để chạy lại huấn luyện VÀ đánh giá. Nếu ground truth nằm
ở bảng khác, VERSION AS OF trên features không đủ để tái lập metric.

Chia theo thời gian: tháng cuối (30 ngày) là test, phần trước là train. Suy giảm thời
gian tính tương đối với mốc của từng tập (train: ngày cắt; test: ngày cuối) để không rò
rỉ thông tin tương lai vào train.
"""
import argparse

from pyspark.ml.feature import StringIndexerModel
from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.spark_utils import (
    bang_ton_tai,
    doc_delta,
    ghi_delta,
    in_ket_qua,
    phien_ban_moi_nhat,
    tao_spark,
)


def nap_hoac_mo_rong_indexer(spark, df, cot_goc, cot_chi_so, duong_dan):
    """Nạp StringIndexerModel cũ (nếu có) và chỉ nối thêm nhãn chưa từng xuất hiện."""
    gia_tri = df.select(F.col(cot_goc).cast("string").alias(cot_goc)).distinct()
    try:
        nhan_cu = list(StringIndexerModel.load(duong_dan).labels)
    except Exception:  # lần chạy đầu tiên chưa có mô hình đã lưu
        nhan_cu = []
    if nhan_cu:
        df_cu = spark.createDataFrame([(v,) for v in nhan_cu], [cot_goc])
        moi = gia_tri.join(df_cu, cot_goc, "left_anti")
    else:
        moi = gia_tri
    # Sắp xếp để thứ tự nhãn mới không phụ thuộc vào cách Spark chia phân vùng.
    nhan_moi = [r[cot_goc] for r in moi.orderBy(cot_goc).collect()]
    mo_hinh = StringIndexerModel.from_labels(
        nhan_cu + nhan_moi, inputCol=cot_goc, outputCol=cot_chi_so, handleInvalid="skip"
    )
    mo_hinh.write().overwrite().save(duong_dan)
    return mo_hinh, len(nhan_cu), len(nhan_moi)


def chia_theo_thoi_gian(lich_su, so_ngay_test=ch.SO_NGAY_TEST):
    """Trả về (ngày cắt, ngày cuối): train là ngay < ngày cắt, test là phần còn lại."""
    ngay_cuoi = lich_su.agg(F.max("ngay")).first()[0]
    ngay_cat = lich_su.select(F.date_sub(F.lit(ngay_cuoi), so_ngay_test).alias("c")).first()["c"]
    return ngay_cat, ngay_cuoi


def tinh_diem(df, moc):
    """diem = tổng trong_so x exp(-lambda x số ngày trước mốc), gộp theo cặp user-item."""
    so_ngay_truoc = F.datediff(F.lit(moc), F.col("ngay"))
    return df.groupBy("user_idx", "item_idx").agg(
        F.sum(F.col("trong_so") * F.exp(-F.lit(ch.LAMBDA_SUY_GIAM) * so_ngay_truoc)).alias("diem")
    )


def dung_ma_tran(spark, silver, min_tuong_tac, so_ngay_test=ch.SO_NGAY_TEST):
    """Từ silver dựng ma trận train/test; trả về (features, thông tin)."""
    # Sự kiện từ web (năm 2026) dùng cho A/B và thịnh hành, không đưa vào chia train/test.
    lich_su = silver.filter(F.col("_nguon") != "web")
    ngay_cat, ngay_cuoi = chia_theo_thoi_gian(lich_su, so_ngay_test)
    train_tho = lich_su.filter(F.col("ngay") < F.lit(ngay_cat))
    test_tho = lich_su.filter(F.col("ngay") >= F.lit(ngay_cat))

    # Phương án chống OOM ở mục 3.10: chỉ giữ user có ít nhất min_tuong_tac tương tác trong train.
    nguoi_giu = (
        train_tho.groupBy("ma_nguoi_dung").count().filter(F.col("count") >= min_tuong_tac).select("ma_nguoi_dung")
    )
    train_tho = train_tho.join(nguoi_giu, "ma_nguoi_dung")
    test_tho = test_tho.join(nguoi_giu, "ma_nguoi_dung")

    idx_user, user_cu, user_moi = nap_hoac_mo_rong_indexer(
        spark, train_tho, "ma_nguoi_dung", "user_idx", ch.duong_dan_mo_hinh("user_indexer")
    )
    idx_item, item_cu, item_moi = nap_hoac_mo_rong_indexer(
        spark, lich_su, "ma_san_pham", "item_idx", ch.duong_dan_mo_hinh("item_indexer")
    )

    def danh_chi_so(df):
        return idx_item.transform(idx_user.transform(df)).withColumn(
            "user_idx", F.col("user_idx").cast("int")
        ).withColumn("item_idx", F.col("item_idx").cast("int"))

    train = tinh_diem(danh_chi_so(train_tho), ngay_cat).withColumn("tap", F.lit("train"))
    test = tinh_diem(danh_chi_so(test_tho), ngay_cuoi).withColumn("tap", F.lit("test"))
    features = train.unionByName(test).select("user_idx", "item_idx", "diem", "tap")
    thong_tin = {
        "ngay_cat": str(ngay_cat),
        "ngay_cuoi": str(ngay_cuoi),
        "lambda": ch.LAMBDA_SUY_GIAM,
        "min_tuong_tac": min_tuong_tac,
        "so_ngay_test": so_ngay_test,
        "so_user_nhan_cu": user_cu,
        "so_user_nhan_moi": user_moi,
        "so_item_nhan_cu": item_cu,
        "so_item_nhan_moi": item_moi,
    }
    return features, thong_tin


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-tuong-tac", type=int, default=3)
    ap.add_argument("--so-ngay-test", type=int, default=ch.SO_NGAY_TEST)
    args = ap.parse_args()

    spark = tao_spark("bd03-chuan-bi-ma-tran")
    phien_ban_silver = phien_ban_moi_nhat(spark, "silver.tuong_tac")
    # Đọc đúng phiên bản vừa lấy số, không để dữ liệu ghi thêm trong lúc chạy lọt vào.
    silver = doc_delta(spark, "silver.tuong_tac", phien_ban_silver)
    features, thong_tin = dung_ma_tran(spark, silver, args.min_tuong_tac, args.so_ngay_test)
    features = features.cache()

    thong_tin["phien_ban_silver"] = phien_ban_silver
    if bang_ton_tai(spark, "silver.san_pham"):
        thong_tin["phien_ban_san_pham"] = phien_ban_moi_nhat(spark, "silver.san_pham")

    ghi_delta(features, "features.ma_tran_tuong_tac", ghi_chu=thong_tin, tuy_chon={"overwriteSchema": "true"})
    dem = {r["tap"]: r["count"] for r in features.groupBy("tap").count().collect()}
    thong_tin.update(
        {
            "phien_ban_features": phien_ban_moi_nhat(spark, "features.ma_tran_tuong_tac"),
            "so_dong_train": dem.get("train", 0),
            "so_dong_test": dem.get("test", 0),
        }
    )
    in_ket_qua(thong_tin)


if __name__ == "__main__":
    main()
