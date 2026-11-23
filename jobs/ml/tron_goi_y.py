"""ml/tron_goi_y.py - trộn ALS + thịnh hành + phổ biến theo danh mục, xử lý cold start.

Đầu ra gold.goi_y_tam (bảng trung gian), bước ghi_gold mới MERGE vào gold.goi_y_nguoi_dung.

  User có trong ALS     : mẫu A (ALS thuần) hoặc B (ALS + 30% thịnh hành) theo hash,
                          item mới cùng danh mục chèn vào 20% vị trí cuối mỗi khối 10.
  User < 5 tương tác    : bán chạy trong các danh mục đã xem, xen kẽ 50/50 thịnh hành.
  User mới hoàn toàn    : không có dòng nào ở đây; web trả top thịnh hành 15 phút.

Mô hình v2 (bước danh_gia đặt phien_ban_mo_hinh = als_v2_<ma_chay>): trước khi xếp vào
mẫu khối, ứng viên ALS ngoài tầm giá của user bị loại (common/tron.loc_theo_tam_gia).

Logic sắp xếp nằm ở common/tron.py để web và test dùng chung.
"""
import argparse

from pyspark.ml.feature import StringIndexerModel
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.als_lib import tam_gia_nguoi_dung
from common.spark_utils import (
    bang_ton_tai,
    doc_delta,
    doc_ghi_chu_commit,
    ghi_delta,
    in_ket_qua,
    phien_ban_moi_nhat,
    tao_spark,
)
from common.tron import (
    NGUON_ALS,
    NGUON_DANH_MUC,
    NGUON_THINH_HANH,
    bo_sung_thinh_hanh,
    chon_nhom,
    danh_sach_it_tuong_tac,
    danh_sach_nguoi_quen,
    la_mo_hinh_v2,
    loc_theo_tam_gia,
)

SO_THINH_HANH = 30
SO_ITEM_MOI = 4
SO_DANH_MUC_NGUOI_IT = 3

LUOC_DO_DAU_RA = "ma_nguoi_dung string, xep_hang int, ma_san_pham string, diem_du_doan double, nguon string"


def lay_thinh_hanh(spark, thong_ke):
    """Top thịnh hành của cửa sổ mới nhất, bù bằng xem nhiều 24h khi cửa sổ có ít món.

    Trả về (danh sách gợi ý, nhãn nguồn dữ liệu) để báo cáo rõ đã dùng phương án nào.
    """
    tu_luong = []
    if bang_ton_tai(spark, "gold.thinh_hanh_15p"):
        th = doc_delta(spark, "gold.thinh_hanh_15p")
        moi_nhat = th.agg(F.max("cua_so_ket_thuc")).first()[0]
        if moi_nhat is not None:
            tu_luong = [
                {"ma_san_pham": r["ma_san_pham"], "diem_du_doan": float(r["diem_thinh_hanh"]), "nguon": NGUON_THINH_HANH}
                for r in th.filter(F.col("cua_so_ket_thuc") == F.lit(moi_nhat))
                .orderBy(F.desc("diem_thinh_hanh"), "ma_san_pham")
                .limit(SO_THINH_HANH)
                .collect()
            ]
    if len(tu_luong) >= SO_THINH_HANH:
        return tu_luong, "luong_15_phut"
    du_phong = [
        {"ma_san_pham": r["ma_san_pham"], "diem_du_doan": float(r["so_xem_24h"]), "nguon": NGUON_THINH_HANH}
        for r in thong_ke.orderBy(F.desc("so_xem_24h"), F.desc("diem_pho_bien"), "ma_san_pham")
        .limit(SO_THINH_HANH)
        .collect()
    ]
    nhan = "luong_15_phut_{}_mon+xem_24h".format(len(tu_luong)) if tu_luong else "du_phong_xem_24h"
    return bo_sung_thinh_hanh(tu_luong, du_phong, SO_THINH_HANH), nhan


def ban_chay_theo_danh_muc(thong_ke, so_luong):
    """ma_danh_muc -> mảng struct(hang, ma_san_pham, diem) xếp theo điểm phổ biến."""
    return (
        thong_ke.filter(F.col("ma_danh_muc").isNotNull())
        .withColumn(
            "hang",
            F.row_number().over(
                Window.partitionBy("ma_danh_muc").orderBy(F.desc("diem_pho_bien"), "ma_san_pham")
            ),
        )
        .filter(F.col("hang") <= so_luong)
        .groupBy("ma_danh_muc")
        .agg(F.sort_array(F.collect_list(F.struct("hang", "ma_san_pham", F.col("diem_pho_bien").alias("diem")))).alias("ds"))
    )


def item_moi_theo_danh_muc(spark, thong_ke, item_als):
    """Item chưa có trong mô hình ALS, gom theo danh mục, ưu tiên item đã có tương tác."""
    ung_vien = thong_ke.select("ma_san_pham", "ma_danh_muc", "tong_tuong_tac")
    if bang_ton_tai(spark, "silver.san_pham"):
        # Chỉ gợi ý item còn hàng; item chưa rõ trạng thái coi như còn.
        catalog = (
            doc_delta(spark, "silver.san_pham")
            .filter(F.coalesce(F.col("con_hang"), F.lit(True)))
            .select("ma_san_pham", "ma_danh_muc")
        )
        ung_vien = catalog.join(ung_vien.drop("ma_danh_muc"), "ma_san_pham", "left").fillna({"tong_tuong_tac": 0})
    ung_vien = ung_vien.join(item_als, "ma_san_pham", "left_anti").filter(F.col("ma_danh_muc").isNotNull())
    return (
        ung_vien.withColumn(
            "hang",
            F.row_number().over(
                Window.partitionBy("ma_danh_muc").orderBy(F.desc("tong_tuong_tac"), "ma_san_pham")
            ),
        )
        .filter(F.col("hang") <= SO_ITEM_MOI)
        .groupBy("ma_danh_muc")
        .agg(F.sort_array(F.collect_list(F.struct("hang", "ma_san_pham", F.lit(0.0).alias("diem")))).alias("ds_moi"))
    )


def _sang_dict(ds, nguon):
    return [{"ma_san_pham": x["ma_san_pham"], "diem_du_doan": float(x["diem"] or 0.0), "nguon": nguon} for x in ds or []]


def tron_nguoi_quen(spark, als_tam, danh_muc_item, moi_theo_dm, thinh_hanh_bc, gia_item=None, tam_gia=None):
    """Danh sách cuối cho user có gợi ý ALS.

    gia_item (ma_san_pham, gia) và tam_gia (ma_nguoi_dung, gia_thap, gia_cao) chỉ truyền vào
    khi chạy mô hình v2; để None là mô hình v1 như cũ.
    """
    if gia_item is not None:
        als_co_gia = als_tam.join(gia_item, "ma_san_pham", "left")
    else:
        als_co_gia = als_tam.withColumn("gia", F.lit(None).cast("double"))
    als_nguoi = als_co_gia.groupBy("ma_nguoi_dung").agg(
        F.sort_array(
            F.collect_list(F.struct("xep_hang", "ma_san_pham", F.col("diem_du_doan").alias("diem"), "gia"))
        ).alias("als")
    )
    # Danh mục "của" user = danh mục xuất hiện nhiều nhất trong top-20 ALS của họ.
    dm_nguoi = (
        als_tam.join(danh_muc_item, "ma_san_pham")
        .groupBy("ma_nguoi_dung", "ma_danh_muc")
        .count()
        .withColumn(
            "h",
            F.row_number().over(Window.partitionBy("ma_nguoi_dung").orderBy(F.desc("count"), "ma_danh_muc")),
        )
        .filter(F.col("h") == 1)
        .join(moi_theo_dm, "ma_danh_muc", "left")
        .select("ma_nguoi_dung", "ds_moi")
    )
    du_lieu = als_nguoi.join(dm_nguoi, "ma_nguoi_dung", "left")
    if tam_gia is not None:
        du_lieu = du_lieu.join(tam_gia, "ma_nguoi_dung", "left")
    else:
        du_lieu = du_lieu.withColumn("gia_thap", F.lit(None).cast("double")).withColumn(
            "gia_cao", F.lit(None).cast("double")
        )

    def xu_ly(r):
        als = [
            {"ma_san_pham": x["ma_san_pham"], "diem_du_doan": float(x["diem"] or 0.0), "nguon": NGUON_ALS, "gia": x["gia"]}
            for x in r["als"] or []
        ]
        als = loc_theo_tam_gia(als, r["gia_thap"], r["gia_cao"], ch.K_DANH_GIA)
        ds = danh_sach_nguoi_quen(
            chon_nhom(r["ma_nguoi_dung"]),
            als,
            thinh_hanh_bc.value,
            _sang_dict(r["ds_moi"], NGUON_DANH_MUC),
        )
        return [(r["ma_nguoi_dung"], i + 1, g["ma_san_pham"], g["diem_du_doan"], g["nguon"]) for i, g in enumerate(ds)]

    return spark.createDataFrame(du_lieu.rdd.flatMap(xu_ly), LUOC_DO_DAU_RA)


def tron_nguoi_it_tuong_tac(spark, silver, danh_muc_item, ban_chay_dm, nguoi_als, tu_ngay, thinh_hanh_bc):
    """User hoạt động gần đây nhưng có ít hơn 5 tương tác."""
    so_tuong_tac = silver.groupBy("ma_nguoi_dung").count()
    gan_day = silver.filter(F.col("ngay") >= F.lit(tu_ngay)).select("ma_nguoi_dung").distinct()
    nguoi = (
        gan_day.join(so_tuong_tac, "ma_nguoi_dung")
        .filter(F.col("count") < ch.NGUONG_USER_IT_TUONG_TAC)
        .join(nguoi_als, "ma_nguoi_dung", "left_anti")
        .select("ma_nguoi_dung")
    )
    dm_da_xem = (
        silver.join(nguoi, "ma_nguoi_dung")
        .join(danh_muc_item, "ma_san_pham")
        .groupBy("ma_nguoi_dung", "ma_danh_muc")
        .agg(F.sum("trong_so").alias("w"))
        .withColumn(
            "h",
            F.row_number().over(Window.partitionBy("ma_nguoi_dung").orderBy(F.desc("w"), "ma_danh_muc")),
        )
        .filter(F.col("h") <= SO_DANH_MUC_NGUOI_IT)
        .join(ban_chay_dm, "ma_danh_muc")
        .groupBy("ma_nguoi_dung")
        .agg(F.sort_array(F.collect_list(F.struct("h", "ds"))).alias("cac_dm"))
    )

    def xu_ly(r):
        # Xen kẽ các danh mục: món số 1 của danh mục 1, số 1 của danh mục 2, ... rồi tới món số 2.
        cac_ds = [_sang_dict(x["ds"], NGUON_DANH_MUC) for x in r["cac_dm"]]
        dm = [ds[i] for i in range(ch.TOP_N_GOLD) for ds in cac_ds if i < len(ds)]
        ds = danh_sach_it_tuong_tac(dm, thinh_hanh_bc.value)
        return [(r["ma_nguoi_dung"], i + 1, g["ma_san_pham"], g["diem_du_doan"], g["nguon"]) for i, g in enumerate(ds)]

    return spark.createDataFrame(dm_da_xem.rdd.flatMap(xu_ly), LUOC_DO_DAU_RA)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ma-chay", required=True)
    args = ap.parse_args()

    spark = tao_spark("bd03-tron-goi-y")
    thong_ke = doc_delta(spark, "gold.thong_ke_san_pham").cache()
    thinh_hanh, nguon_thinh_hanh = lay_thinh_hanh(spark, thong_ke)
    thinh_hanh_bc = spark.sparkContext.broadcast(thinh_hanh)

    if bang_ton_tai(spark, "silver.san_pham"):
        danh_muc_item = doc_delta(spark, "silver.san_pham").select("ma_san_pham", "ma_danh_muc")
    else:
        danh_muc_item = thong_ke.select("ma_san_pham", "ma_danh_muc")
    danh_muc_item = danh_muc_item.filter(F.col("ma_danh_muc").isNotNull()).cache()

    # Tập item ALS biết, lấy từ chính mô hình phục vụ.
    nhan_item = StringIndexerModel.load(ch.duong_dan_mo_hinh("item_indexer")).labels
    nhan_bc = spark.sparkContext.broadcast(nhan_item)
    item_als = (
        ALSModel.load(ch.duong_dan_mo_hinh("als/{}_phuc_vu".format(args.ma_chay)))
        .itemFactors.select("id")
        .rdd.map(lambda r: (nhan_bc.value[r["id"]],))
        .toDF(["ma_san_pham"])
    )

    phien_ban_features = phien_ban_moi_nhat(spark, "features.ma_tran_tuong_tac")
    ghi_chu = doc_ghi_chu_commit(spark, "features.ma_tran_tuong_tac", phien_ban_features) or {}
    silver = doc_delta(spark, "silver.tuong_tac").filter(F.col("_nguon") != "web")

    # v1 hay v2 do bước danh_gia quyết định và ghi vào lịch sử huấn luyện.
    ls = (
        doc_delta(spark, "gold.lich_su_huan_luyen")
        .filter(F.col("ma_chay") == args.ma_chay)
        .orderBy(F.desc("thoi_diem"))
        .first()
    )
    la_v2 = bool(ls and la_mo_hinh_v2(ls["phien_ban_mo_hinh"]))
    gia_item = tam_gia = None
    if la_v2:
        gia_item = (
            doc_delta(spark, "silver.san_pham")
            .filter(F.col("gia_san_pham") > 0)
            .select("ma_san_pham", F.col("gia_san_pham").alias("gia"))
            .cache()
        )
        tam_gia = tam_gia_nguoi_dung(silver.select("ma_nguoi_dung", "ma_san_pham").join(gia_item, "ma_san_pham"), "ma_nguoi_dung").cache()

    als_tam = doc_delta(spark, "gold.goi_y_als_tam").cache()
    phan_als = tron_nguoi_quen(
        spark, als_tam, danh_muc_item, item_moi_theo_danh_muc(spark, thong_ke, item_als), thinh_hanh_bc,
        gia_item, tam_gia,
    )
    phan_it = tron_nguoi_it_tuong_tac(
        spark,
        silver,
        danh_muc_item,
        ban_chay_theo_danh_muc(thong_ke, ch.TOP_N_GOLD),
        als_tam.select("ma_nguoi_dung").distinct(),
        ghi_chu.get("ngay_cat"),
        thinh_hanh_bc,
    )

    tat_ca = phan_als.unionByName(phan_it).cache()
    ghi_delta(
        tat_ca,
        "gold.goi_y_tam",
        ghi_chu={"ma_chay": args.ma_chay, "mo_hinh_v2": la_v2},
        tuy_chon={"overwriteSchema": "true"},
    )
    dem = {r["nguon"]: r["count"] for r in tat_ca.groupBy("nguon").count().collect()}
    in_ket_qua(
        {
            "ma_chay": args.ma_chay,
            "mo_hinh_v2": la_v2,
            "so_nguoi_co_tam_gia": tam_gia.count() if la_v2 else 0,
            "so_nguoi_als": phan_als.select("ma_nguoi_dung").distinct().count(),
            "so_nguoi_it_tuong_tac": phan_it.select("ma_nguoi_dung").distinct().count(),
            "so_o_theo_nguon": dem,
            "nguon_thinh_hanh": nguon_thinh_hanh,
        }
    )


if __name__ == "__main__":
    main()
