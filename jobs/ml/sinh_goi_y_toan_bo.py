"""ml/sinh_goi_y_toan_bo.py - huấn luyện lại trên toàn bộ dữ liệu và sinh top-20.

Mô hình dùng để ĐÁNH GIÁ chỉ học 4 tháng đầu. Mô hình dùng để PHỤC VỤ phải biết cả
tháng cuối, nên job này fit lại với đúng siêu tham số đã chọn trên train + test.

Gộp hai tập mà không cần đọc lại silver: điểm train đã suy giảm theo ngày cắt, nhân
thêm exp(-lambda x (ngày cuối - ngày cắt)) là thành suy giảm theo ngày cuối, vì
exp(-l(c - d)) x exp(-l(e - c)) = exp(-l(e - d)).

Chỉ sinh cho user hoạt động trong 30 ngày cuối và có ít nhất 5 tương tác (mục 3.10,
"recommendForAllUsers quá chậm"); user ít tương tác hơn đi theo chiến lược cold start.

Xếp lại theo độ mới (MO_HINH.md mục 6): lấy dư SO_UNG_VIEN_XEP_LAI ứng viên ALS, xếp lại bằng
điểm ALS chuẩn hóa + BETA_XEP_LAI x novelty chuẩn hóa, giữ TOP_N_GOLD món (gấp đôi với v2).
Novelty tính trên đúng ma trận dùng để fit mô hình phục vụ (train + test).
"""
import argparse
import math
from datetime import datetime

from pyspark.ml.feature import IndexToString, StringIndexerModel
from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.als_lib import tao_als
from common.spark_utils import doc_delta, doc_ghi_chu_commit, ghi_delta, in_ket_qua, tao_spark
from common.tron import la_mo_hinh_v2, xep_ung_vien_phuc_vu


def gop_toan_bo(features, ngay_cat, ngay_cuoi):
    """Ma trận đầy đủ để fit mô hình phục vụ."""
    he_so = math.exp(-ch.LAMBDA_SUY_GIAM * (ngay_cuoi - ngay_cat).days)
    return (
        features.withColumn(
            "diem", F.when(F.col("tap") == "train", F.col("diem") * he_so).otherwise(F.col("diem"))
        )
        .groupBy("user_idx", "item_idx")
        .agg(F.sum("diem").alias("diem"))
    )


def nguoi_dung_hoat_dong(spark, features, ghi_chu, nguong):
    """user_idx có tương tác trong tháng cuối và tổng số tương tác >= nguong."""
    silver = doc_delta(spark, "silver.tuong_tac", ghi_chu.get("phien_ban_silver"))
    du_tuong_tac = (
        silver.filter(F.col("_nguon") != "web")
        .groupBy("ma_nguoi_dung")
        .count()
        .filter(F.col("count") >= nguong)
        .select("ma_nguoi_dung")
    )
    idx_user = StringIndexerModel.load(ch.duong_dan_mo_hinh("user_indexer"))
    du_tuong_tac = idx_user.transform(du_tuong_tac).select(F.col("user_idx").cast("int"))
    hoat_dong = features.filter(F.col("tap") == "test").select("user_idx").distinct()
    return hoat_dong.join(du_tuong_tac, "user_idx")


def bang_novelty(toan_bo):
    """item_idx -> -log2(tỷ lệ user đã tương tác) trên ma trận phục vụ."""
    tong = toan_bo.select("user_idx").distinct().count()
    pho_bien = toan_bo.groupBy("item_idx").agg(F.countDistinct("user_idx").alias("so_nguoi"))
    return {r["item_idx"]: -math.log(max(r["so_nguoi"], 1) / float(tong), 2) for r in pho_bien.collect()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ma-chay", required=True)
    ap.add_argument("--beta", type=float, default=ch.BETA_XEP_LAI, help="0 = ALS thuần, không xếp lại")
    ap.add_argument("--so-ung-vien", type=int, default=ch.SO_UNG_VIEN_XEP_LAI)
    args = ap.parse_args()

    spark = tao_spark("bd03-sinh-goi-y-toan-bo")
    ls = (
        doc_delta(spark, "gold.lich_su_huan_luyen")
        .filter(F.col("ma_chay") == args.ma_chay)
        .orderBy(F.desc("thoi_diem"))
        .first()
    )
    if ls is None:
        raise SystemExit("Chưa có lịch sử đánh giá cho ma_chay={}".format(args.ma_chay))

    phien_ban = int(ls["phien_ban_features"])
    features = doc_delta(spark, "features.ma_tran_tuong_tac", phien_ban).cache()
    ghi_chu = doc_ghi_chu_commit(spark, "features.ma_tran_tuong_tac", phien_ban) or {}
    ngay_cat = datetime.strptime(ghi_chu["ngay_cat"], "%Y-%m-%d").date()
    ngay_cuoi = datetime.strptime(ghi_chu["ngay_cuoi"], "%Y-%m-%d").date()

    tham_so = {"rank": ls["rank"], "regParam": ls["reg_param"], "alpha": ls["alpha"]}
    toan_bo = gop_toan_bo(features, ngay_cat, ngay_cuoi).cache()
    mo_hinh = tao_als(tham_so, ls["max_iter"], ls["hat_giong"]).fit(toan_bo)
    mo_hinh.write().overwrite().save(ch.duong_dan_mo_hinh("als/{}_phuc_vu".format(args.ma_chay)))

    nguoi = nguoi_dung_hoat_dong(spark, features, ghi_chu, ch.NGUONG_USER_IT_TUONG_TAC)
    # Mô hình v2 sẽ loại bớt món ngoài tầm giá ở bước trộn, nên giữ dư từ đây.
    so_lay = ch.TOP_N_GOLD * (ch.HE_SO_UNG_VIEN_V2 if la_mo_hinh_v2(ls["phien_ban_mo_hinh"]) else 1)
    beta = max(args.beta, 0.0)
    so_ung_vien = max(so_lay, args.so_ung_vien) if beta > 0 else so_lay
    nov_bc = spark.sparkContext.broadcast(bang_novelty(toan_bo) if beta > 0 else {})

    def xep(r):
        ds = [(x["item_idx"], float(x["rating"])) for x in r["recommendations"]]
        return [(r["user_idx"], vt + 1, i, d)
                for vt, (i, d) in enumerate(xep_ung_vien_phuc_vu(ds, nov_bc.value, beta, so_lay))]

    ung_vien = mo_hinh.recommendForUserSubset(nguoi, so_ung_vien)
    goi_y = spark.createDataFrame(
        ung_vien.rdd.flatMap(xep), "user_idx int, xep_hang int, item_idx int, diem_du_doan double"
    )
    nhan_user = StringIndexerModel.load(ch.duong_dan_mo_hinh("user_indexer")).labels
    nhan_item = StringIndexerModel.load(ch.duong_dan_mo_hinh("item_indexer")).labels
    goi_y = goi_y.select(
        F.col("user_idx").cast("double").alias("u"),
        "xep_hang",
        F.col("item_idx").cast("double").alias("i"),
        "diem_du_doan",
    )
    goi_y = IndexToString(inputCol="u", outputCol="ma_nguoi_dung", labels=nhan_user).transform(goi_y)
    goi_y = IndexToString(inputCol="i", outputCol="ma_san_pham", labels=nhan_item).transform(goi_y)
    goi_y = goi_y.select("ma_nguoi_dung", "xep_hang", "ma_san_pham", "diem_du_doan").cache()

    ghi_delta(goi_y, "gold.goi_y_als_tam", tuy_chon={"overwriteSchema": "true"},
              ghi_chu={"ma_chay": args.ma_chay, "beta_xep_lai": beta, "so_ung_vien": so_ung_vien})
    in_ket_qua(
        {
            "ma_chay": args.ma_chay,
            "phien_ban_mo_hinh": ls["phien_ban_mo_hinh"],
            "so_ung_vien_lay_du": so_ung_vien,
            "so_giu_moi_nguoi": so_lay,
            "beta_xep_lai": beta,
            "so_nguoi": goi_y.select("ma_nguoi_dung").distinct().count(),
            "so_dong": goi_y.count(),
        }
    )


if __name__ == "__main__":
    main()
