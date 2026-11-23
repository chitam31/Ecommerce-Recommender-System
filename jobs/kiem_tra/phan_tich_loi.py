"""kiem_tra/phan_tich_loi.py - phân tích lỗi: ALS thua baseline ở nhóm người dùng nào (C.3 mục 6, C.5 câu 6).

Tính NDCG@10 TỪNG người dùng của tập kiểm tra cho ALS (mô hình đánh giá của lượt huấn luyện), baseline 1
(bán chạy toàn sàn) và baseline 2 (bán chạy trong danh mục người dùng xem nhiều nhất), rồi gom theo:
  - số tương tác của người dùng trong tập huấn luyện (ít tương tác thì nhân tố ẩn kém tin cậy),
  - mức phổ biến của các sản phẩm người dùng thật sự tương tác ở tháng kiểm tra (đầu hay đuôi dài),
  - số danh mục khác nhau người dùng đã xem (sở thích tập trung hay phân tán).
Mọi phép tính theo người dùng chạy trên cụm; chỉ bảng gom theo nhóm (vài chục dòng) về driver.

Chạy: ha_tang/spark/submit.sh lo kiem_tra/phan_tich_loi.py [--ma-chay r...]   (dòng cuối là JSON)
"""
import argparse

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from common import cau_hinh as ch
from common.als_lib import (
    _danh_sach_tu_khuyen_nghi,
    bang_danh_muc_item,
    baseline_danh_muc,
    baseline_toan_san,
    doc_features,
    thong_ke_pho_bien,
)
from common.chi_so import ndcg_at_k
from common.spark_utils import doc_delta, in_ket_qua, tao_spark

PHUONG_PHAP = ("als", "b1", "b2")


def _ndcg_udf(k):
    return F.udf(lambda ds, tt: float(ndcg_at_k(list(ds or []), list(tt or []), k)), DoubleType())


def ndcg_tung_nguoi(thuc_te, cac_ds, k=ch.K_DANH_GIA):
    """thuc_te (user_idx, thuc_te) + {tên: (user_idx, ds)} -> user_idx, thuc_te, ndcg_<tên>..."""
    ndcg = _ndcg_udf(k)
    df = thuc_te
    for ten, ds in cac_ds.items():
        df = df.join(ds.select("user_idx", F.col("ds").alias("ds_" + ten)), "user_idx", "left") \
            .withColumn("ndcg_" + ten, ndcg(F.col("ds_" + ten), F.col("thuc_te"))).drop("ds_" + ten)
    return df


def dac_trung_nguoi(train, thuc_te, pho_bien, danh_muc_item=None, ty_le_dau=0.10):
    """Mỗi người dùng test: số tương tác train, tỷ lệ sản phẩm test thuộc nhóm phổ biến nhất, số danh mục đã xem.

    "Nhóm phổ biến nhất" = sản phẩm có số người tương tác từ phân vị (1 - ty_le_dau) trở lên, tính bằng
    approxQuantile trên cụm (không xếp hạng toàn bộ sản phẩm trên một phân vùng).
    """
    nguong = pho_bien.approxQuantile("so_nguoi", [1.0 - ty_le_dau], 0.001)[0]
    dau = pho_bien.filter(F.col("so_nguoi") >= nguong).select("item_idx").withColumn("la_dau", F.lit(1))
    nguoi = thuc_te.select("user_idx")
    so_tt = train.join(nguoi, "user_idx").groupBy("user_idx").agg(F.count("*").alias("so_tuong_tac_train"))
    test_dau = (thuc_te.select("user_idx", F.explode("thuc_te").alias("item_idx"))
                .join(dau, "item_idx", "left").groupBy("user_idx")
                .agg(F.avg(F.coalesce("la_dau", F.lit(0))).alias("ty_le_test_dau")))
    df = nguoi.join(so_tt, "user_idx", "left").join(test_dau, "user_idx", "left")
    if danh_muc_item is not None:
        so_dm = (train.join(nguoi, "user_idx").join(danh_muc_item, "item_idx")
                 .groupBy("user_idx").agg(F.countDistinct("ma_danh_muc").alias("so_danh_muc")))
        return df.join(so_dm, "user_idx", "left")
    return df.withColumn("so_danh_muc", F.lit(None).cast("long"))


def nhom_so(cot, moc):
    """Nhãn nhóm theo các mốc [a, b) liên tiếp; mốc cuối thành ">=x"."""
    bieu_thuc = F.lit(">={}".format(moc[-1]))
    for i in range(len(moc) - 2, -1, -1):
        nhan = "{}-{}".format(moc[i], moc[i + 1] - 1) if moc[i + 1] - moc[i] > 1 else str(moc[i])
        bieu_thuc = F.when(F.col(cot) < moc[i + 1], F.lit(nhan)).otherwise(bieu_thuc)
    return bieu_thuc


def gom_theo(df, cot_nhom, cot_thu_tu=None):
    """Bảng nhỏ: mỗi nhóm số người, NDCG trung bình mỗi phương pháp, tỷ lệ ALS thua/thắng baseline 2.

    cot_thu_tu: cột số để xếp các nhóm theo giá trị nhỏ nhất (vd. nhóm "3-4" trước "10-19").
    """
    agg = [F.count("*").alias("so_nguoi")]
    agg += [F.avg("ndcg_" + p).alias("ndcg_" + p) for p in PHUONG_PHAP]
    agg += [F.avg((F.col("ndcg_als") < F.col("ndcg_b2")).cast("double")).alias("ty_le_als_thua_b2"),
            F.avg((F.col("ndcg_als") > F.col("ndcg_b2")).cast("double")).alias("ty_le_als_thang_b2"),
            F.avg(((F.col("ndcg_als") == 0) & (F.col("ndcg_b2") == 0)).cast("double")).alias("ty_le_ca_hai_bang_0")]
    if cot_thu_tu:
        agg.append(F.min(cot_thu_tu).alias("_thu_tu"))
    g = df.groupBy(cot_nhom).agg(*agg).orderBy("_thu_tu" if cot_thu_tu else cot_nhom)
    if cot_thu_tu:
        g = g.drop("_thu_tu")
    return [{k: (round(v, 5) if isinstance(v, float) else v) for k, v in r.asDict().items()} for r in g.collect()]


def phan_tich(df):
    """df: user_idx, ndcg_als/b1/b2, so_tuong_tac_train, ty_le_test_dau, so_danh_muc."""
    df = (df.withColumn("nhom_tuong_tac", nhom_so("so_tuong_tac_train", [3, 5, 10, 20, 50]))
          .withColumn("nhom_pho_bien_test",
                      F.when(F.col("ty_le_test_dau") >= 0.5, F.lit("dau (>=50% SP test pho bien)"))
                      .when(F.col("ty_le_test_dau") > 0, F.lit("tron"))
                      .otherwise(F.lit("duoi dai (0% SP test pho bien)")))
          .withColumn("nhom_danh_muc", nhom_so("so_danh_muc", [1, 2, 4, 8])))
    return {
        "tong": gom_theo(df.withColumn("tat_ca", F.lit("tat_ca")), "tat_ca")[0],
        "theo_so_tuong_tac": gom_theo(df, "nhom_tuong_tac", "so_tuong_tac_train"),
        "theo_pho_bien_test": gom_theo(df, "nhom_pho_bien_test", "ty_le_test_dau"),
        "theo_so_danh_muc": gom_theo(df.filter(F.col("so_danh_muc").isNotNull()), "nhom_danh_muc", "so_danh_muc"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ma-chay", default=None, help="Mặc định: lượt đang phục vụ (trang_thai = trien_khai)")
    args = ap.parse_args()
    spark = tao_spark("bd03-phan-tich-loi")

    ls = doc_delta(spark, "gold.lich_su_huan_luyen")
    ls = ls.filter(F.col("ma_chay") == args.ma_chay) if args.ma_chay else ls.filter(F.col("trang_thai") == "trien_khai")
    lich_su = ls.orderBy(F.desc("thoi_diem")).first()
    if lich_su is None:
        raise SystemExit("Không tìm thấy lượt huấn luyện phù hợp trong gold.lich_su_huan_luyen")

    train, thuc_te, ghi_chu, phien_ban = doc_features(spark, int(lich_su["phien_ban_features"]))
    train, thuc_te = train.cache(), thuc_te.cache()
    pho_bien = thong_ke_pho_bien(train).cache()
    mo_hinh = ALSModel.load(ch.duong_dan_mo_hinh_hien_tai(lich_su["duong_dan_mo_hinh"]))
    k = ch.K_DANH_GIA
    dm_item = bang_danh_muc_item(spark, ghi_chu)
    cac_ds = {
        "als": _danh_sach_tu_khuyen_nghi(mo_hinh.recommendForUserSubset(thuc_te.select("user_idx"), k)),
        "b1": baseline_toan_san(spark, thuc_te, pho_bien, k),
        "b2": baseline_danh_muc(spark, train, thuc_te, pho_bien, dm_item, k),
    }
    df = ndcg_tung_nguoi(thuc_te, cac_ds, k).join(
        dac_trung_nguoi(train, thuc_te, pho_bien, dm_item.cache() if dm_item is not None else None), "user_idx"
    ).cache()
    kq = phan_tich(df)
    kq.update({"ma_chay": lich_su["ma_chay"], "phien_ban_features": phien_ban, "k": k})
    in_ket_qua(kq)


if __name__ == "__main__":
    main()
