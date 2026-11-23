"""kiem_tra/eda.py - khảo sát dữ liệu gốc RetailRocket (EDA) trên cụm, xuất số liệu tổng hợp nhỏ dạng JSON.

Đọc 4 tệp CSV gốc ở <gốc lakehouse>/landing/retailrocket (chưa qua bronze, chưa thêm người dùng
tổng hợp), tính mọi phép đếm bằng Spark trên cụm, chỉ đưa về driver các bảng tổng hợp vài chục đến
vài trăm dòng (theo ngày, theo giờ, theo nhóm hoạt động). notebooks/eda_retailrocket.ipynb đọc
JSON này để vẽ biểu đồ và viết nhận xét.

Chạy: ha_tang/spark/submit.sh lo kiem_tra/eda.py > eda.json   (dòng cuối là JSON)
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.spark_utils import in_ket_qua, tao_spark

# Nhóm số sự kiện mỗi người dùng / mỗi sản phẩm (cận dưới của từng nhóm).
MOC_HOAT_DONG = [1, 2, 3, 5, 10, 20, 50, 100, 500]


def _nhom(cot, moc=MOC_HOAT_DONG):
    """Nhãn nhóm "a-b" hoặc ">=a" cho số đếm."""
    bieu_thuc = F.lit(">={}".format(moc[-1]))
    for i in range(len(moc) - 2, -1, -1):
        nhan = str(moc[i]) if moc[i + 1] - moc[i] == 1 else "{}-{}".format(moc[i], moc[i + 1] - 1)
        bieu_thuc = F.when(F.col(cot) < moc[i + 1], F.lit(nhan)).otherwise(bieu_thuc)
    return bieu_thuc


def _dang_bang(df, *cot):
    return [{c: (r[c].isoformat() if hasattr(r[c], "isoformat") else r[c]) for c in cot} for r in df.collect()]


def chuan_hoa_su_kien(events):
    return (
        events.select(
            F.col("timestamp").cast("long").alias("ts"),
            F.col("visitorid").cast("string").alias("visitorid"),
            F.col("event"),
            F.col("itemid").cast("string").alias("itemid"),
            F.col("transactionid").cast("string").alias("transactionid"),
        )
        .withColumn("thoi_diem", F.timestamp_millis("ts"))
        .withColumn("ngay", F.to_date("thoi_diem"))
    )


def tinh_eda(ev, thuoc_tinh=None, cay=None, nguong_bot=ch.NGUONG_BOT_MOI_GIO, so_giay=ch.GIAY_KHU_TRUNG):
    """ev: sự kiện đã chuẩn hóa (chuan_hoa_su_kien). Trả về dict các bảng tổng hợp nhỏ."""
    kq = {}
    tq = ev.agg(
        F.count("*").alias("so_su_kien"),
        F.countDistinct("visitorid").alias("so_nguoi_dung"),
        F.countDistinct("itemid").alias("so_san_pham"),
        F.countDistinct("transactionid").alias("so_giao_dich"),
        F.min("ngay").alias("tu_ngay"),
        F.max("ngay").alias("den_ngay"),
        F.countDistinct("ngay").alias("so_ngay"),
        F.countDistinct("visitorid", "itemid").alias("so_cap_nguoi_san_pham"),
    ).first().asDict()
    tq["tu_ngay"], tq["den_ngay"] = str(tq["tu_ngay"]), str(tq["den_ngay"])
    tq["mat_do_ma_tran"] = tq["so_cap_nguoi_san_pham"] / float(max(tq["so_nguoi_dung"] * tq["so_san_pham"], 1))
    kq["tong_quan"] = tq

    kq["theo_loai"] = _dang_bang(ev.groupBy("event").agg(F.count("*").alias("so_su_kien"),
                                                         F.countDistinct("visitorid").alias("so_nguoi_dung"))
                                 .orderBy(F.desc("so_su_kien")), "event", "so_su_kien", "so_nguoi_dung")
    kq["theo_ngay"] = _dang_bang(
        ev.groupBy("ngay").agg(F.count("*").alias("so_su_kien"), F.countDistinct("visitorid").alias("so_nguoi_dung"),
                               F.sum((F.col("event") == "transaction").cast("int")).alias("so_mua"))
        .orderBy("ngay"), "ngay", "so_su_kien", "so_nguoi_dung", "so_mua")
    kq["theo_gio_utc"] = _dang_bang(ev.groupBy(F.hour("thoi_diem").alias("gio")).count().orderBy("gio"), "gio", "count")
    kq["theo_thu"] = _dang_bang(ev.groupBy(F.dayofweek("thoi_diem").alias("thu")).count().orderBy("thu"), "thu", "count")

    # Phân bố hoạt động: người dùng và sản phẩm (đuôi dài -> popularity bias, cold start).
    theo_nguoi = ev.groupBy("visitorid").agg(F.count("*").alias("n"))
    theo_sp = ev.groupBy("itemid").agg(F.count("*").alias("n"))
    for ten, df in (("nguoi_dung", theo_nguoi), ("san_pham", theo_sp)):
        kq["phan_bo_" + ten] = _dang_bang(
            df.groupBy(_nhom("n").alias("nhom")).agg(F.min("n").alias("tu"), F.count("*").alias("so_luong")).orderBy("tu"),
            "nhom", "so_luong")
        kq["phan_vi_" + ten] = dict(zip(["p50", "p90", "p99"], df.approxQuantile("n", [0.5, 0.9, 0.99], 0.001)),
                                    lon_nhat=df.agg(F.max("n")).first()[0])
    so_sp = max(tq["so_san_pham"], 1)
    # orderBy + limit chạy thành takeOrdered trên từng phân vùng: không dồn mọi sản phẩm vào một chỗ.
    phan_top = lambda k: theo_sp.orderBy(F.desc("n")).limit(k).agg(F.sum("n")).first()[0] / float(tq["so_su_kien"])
    kq["tap_trung_san_pham"] = {"top_1pct": phan_top(max(1, so_sp // 100)), "top_10pct": phan_top(max(1, so_sp // 10))}
    kq["nguoi_dung_it_hoat_dong"] = {
        "chi_1_su_kien": theo_nguoi.filter(F.col("n") == 1).count(),
        "duoi_3_su_kien": theo_nguoi.filter(F.col("n") < 3).count(),
        "tu_3_su_kien": theo_nguoi.filter(F.col("n") >= 3).count(),
    }

    # Phễu chuyển đổi theo người dùng.
    co = ev.groupBy("visitorid").agg(*[F.max((F.col("event") == e).cast("int")).alias(e)
                                       for e in ("view", "addtocart", "transaction")])
    kq["pheu_nguoi_dung"] = co.agg(F.sum("view").alias("co_xem"), F.sum("addtocart").alias("co_them_gio"),
                                   F.sum("transaction").alias("co_mua")).first().asDict()

    # Chất lượng dữ liệu: trùng lặp, nhấp lặp trong 5 giây, bot, giá trị thiếu.
    cot5 = ["ts", "visitorid", "event", "itemid", "transactionid"]
    w = Window.partitionBy("visitorid", "itemid", "event").orderBy("ts")
    khoang = (F.col("ts") - F.lag("ts").over(w)) / 1000.0
    gio = ev.groupBy("visitorid", F.date_trunc("hour", "thoi_diem").alias("gio")).count()
    bot = gio.filter(F.col("count") > nguong_bot).select("visitorid").distinct()
    kq["chat_luong"] = {
        "dong_trung_hoan_toan": tq["so_su_kien"] - ev.dropDuplicates(cot5).count(),
        "nhap_lap_trong_{}_giay".format(so_giay): ev.withColumn("k", khoang).filter(F.col("k") <= so_giay).count(),
        "so_nguoi_dung_bot": bot.count(),
        "so_su_kien_cua_bot": ev.join(bot, "visitorid", "left_semi").count(),
        "gio_nhieu_su_kien_nhat_cua_1_nguoi": gio.agg(F.max("count")).first()[0],
        "mua_thieu_ma_giao_dich": ev.filter((F.col("event") == "transaction") & F.col("transactionid").isNull()).count(),
        "khong_mua_co_ma_giao_dich": ev.filter((F.col("event") != "transaction") & F.col("transactionid").isNotNull()).count(),
        "thieu_truong_bat_buoc": ev.filter(F.col("ts").isNull() | F.col("visitorid").isNull() | F.col("itemid").isNull()
                                           | F.col("event").isNull()).count(),
    }

    if thuoc_tinh is not None:
        sp_co = thuoc_tinh.groupBy("property").agg(F.countDistinct("itemid").alias("so_san_pham"))
        kq["thuoc_tinh"] = {
            "so_dong": thuoc_tinh.count(),
            "so_san_pham": thuoc_tinh.select("itemid").distinct().count(),
            "so_thuoc_tinh_khac_nhau": sp_co.count(),
            "san_pham_co_danh_muc": sp_co.filter(F.col("property") == "categoryid").select("so_san_pham").first()[0]
            if sp_co.filter(F.col("property") == "categoryid").count() else 0,
            "san_pham_co_gia_790": sp_co.filter(F.col("property") == "790").select("so_san_pham").first()[0]
            if sp_co.filter(F.col("property") == "790").count() else 0,
        }
        sp_su_kien = ev.select("itemid").distinct()
        kq["thuoc_tinh"]["san_pham_co_su_kien_khong_co_thuoc_tinh"] = sp_su_kien.join(
            thuoc_tinh.select(F.col("itemid").cast("string").alias("itemid")).distinct(), "itemid", "left_anti").count()
    if cay is not None:
        kq["cay_danh_muc"] = {
            "so_danh_muc": cay.count(),
            "so_goc": cay.filter(F.col("parentid").isNull() | (F.col("parentid") == "")).count(),
        }
    return kq


def main():
    argparse.ArgumentParser().parse_args()
    spark = tao_spark("bd03-eda")
    doc = lambda t: spark.read.option("header", True).csv(ch.duong_dan_landing(t))
    ev = chuan_hoa_su_kien(doc("events.csv")).cache()
    thuoc_tinh = doc("item_properties_part1.csv").unionByName(doc("item_properties_part2.csv"))
    kq = tinh_eda(ev, thuoc_tinh, doc("category_tree.csv"))
    kq["nguon"] = ch.duong_dan_landing()
    in_ket_qua(kq)


if __name__ == "__main__":
    main()
