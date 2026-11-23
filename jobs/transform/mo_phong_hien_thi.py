"""transform/mo_phong_hien_thi.py - sinh TRAFFIC MÔ PHỎNG cho thí nghiệm A/B.

Phương án dự phòng ở mục 3.10: không đủ người thật để A/B có ý nghĩa thống kê. Mọi dòng
sinh ra đều có nguon_traffic = "mo_phong" và bảng điều khiển hiển thị rõ tỷ lệ đó.

Mô hình xác suất (position-based model):
    P(click) = P(nhìn thấy vị trí v) x P(liên quan | user, item)
    P(nhìn thấy) = 1 / v^eta
    P(liên quan) cao nếu item nằm trong các item user thật sự tương tác ở tháng test.

Mặc định KHÔNG cộng thêm điểm cho item thịnh hành, tức không giả định sẵn nhóm B tốt hơn.

--chi-als: chỉ mô phỏng cho user có gợi ý ALS. A/B chỉ tính impression của chiến lược ALS
(common/ab.py), mà trên dữ liệu thật chỉ khoảng 3% user có gợi ý ALS: lấy 5.000 user bất kỳ
thì mỗi nhóm chỉ còn khoảng 3.000 impression, quá ít để z-test có ý nghĩa (cần khoảng 79.000
mỗi nhóm để phát hiện mức tăng 10% với CTR nền 2%). Đổi --hat-giong khi chạy thêm lần nữa để
mã impression không trùng lần trước.
"""
import argparse
import hashlib
import random
from datetime import datetime, timedelta

from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.spark_utils import doc_delta, ghi_delta, in_ket_qua, tao_spark
from common.tron import chon_nhom

LUOC_DO = """ma_hien_thi string, ma_nguoi_dung string, nhom_ab string, vi_tri int,
    ma_san_pham string, thoi_diem_hien_thi timestamp, da_click boolean, da_mua boolean,
    nguon_traffic string, chien_luoc string"""


def mo_phong_mot_nguoi(ma_nguoi_dung, goi_y, thuc_te, ngay_goc, tham_so):
    """Sinh impression của một người; hạt giống theo user nên chạy lại cho cùng kết quả."""
    hat = int(hashlib.md5(
        "{}|{}".format(ma_nguoi_dung, tham_so["hat_giong"]).encode("utf-8")
    ).hexdigest()[:8], 16)
    rnd = random.Random(hat)
    nhom = chon_nhom(ma_nguoi_dung)
    chien_luoc = "als" if any(g["nguon"] == "als" for g in goi_y) else "it_tuong_tac"
    tap_thuc_te = set(thuc_te or [])
    dong = []
    for phien in range(tham_so["so_phien"]):
        ngay = ngay_goc - timedelta(days=rnd.randrange(tham_so["so_ngay"]))
        luc = datetime(ngay.year, ngay.month, ngay.day) + timedelta(seconds=rnd.randrange(86400))
        # Web hiển thị đúng 10 vị trí đầu, mô phỏng cũng vậy.
        for vi_tri, g in enumerate(goi_y[:10], start=1):
            p_lien_quan = (
                tham_so["p_lien_quan"] if g["ma_san_pham"] in tap_thuc_te else tham_so["p_nen"]
            )
            if g["nguon"] == "thinh_hanh":
                p_lien_quan += tham_so["tang_thinh_hanh"]
            p_nhin = 1.0 / (vi_tri ** tham_so["eta"])
            co_click = rnd.random() < p_nhin * p_lien_quan
            co_mua = co_click and rnd.random() < tham_so["p_mua"]
            dong.append((
                "mp-{:x}-{}-{}".format(hat, phien, vi_tri),
                ma_nguoi_dung,
                nhom,
                vi_tri,
                g["ma_san_pham"],
                luc,
                co_click,
                co_mua,
                "mo_phong",
                chien_luoc,
            ))
    return dong


def chon_nguoi(goi_y, so_nguoi, chi_als=False):
    """Lấy so_nguoi user theo thứ tự băm (ổn định giữa các lần chạy); chi_als: chỉ user có ô ALS."""
    if chi_als:
        goi_y = goi_y.filter(F.exists("goi_y", lambda g: g["nguon"] == "als"))
    return goi_y.orderBy(F.md5("ma_nguoi_dung")).limit(so_nguoi)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--so-nguoi", type=int, default=5000)
    ap.add_argument("--chi-als", action="store_true", help="Chỉ user có gợi ý ALS (đủ mẫu cho A/B)")
    ap.add_argument("--so-phien", type=int, default=4)
    ap.add_argument("--so-ngay", type=int, default=7)
    ap.add_argument("--p-lien-quan", type=float, default=0.40)
    ap.add_argument("--p-nen", type=float, default=0.03)
    ap.add_argument("--p-mua", type=float, default=0.12)
    ap.add_argument("--eta", type=float, default=0.7)
    ap.add_argument("--tang-thinh-hanh", type=float, default=0.0)
    ap.add_argument("--hat-giong", type=int, default=ch.HAT_GIONG)
    args = ap.parse_args()

    spark = tao_spark("bd03-mo-phong-hien-thi")
    goi_y = (
        doc_delta(spark, "gold.goi_y_nguoi_dung")
        .groupBy("ma_nguoi_dung")
        .agg(
            F.sort_array(
                F.collect_list(F.struct("xep_hang", "ma_san_pham", "diem_du_doan", "nguon"))
            ).alias("goi_y")
        )
    )
    goi_y = chon_nguoi(goi_y, args.so_nguoi, args.chi_als)
    silver = doc_delta(spark, "silver.tuong_tac")
    ngay_max = silver.filter(F.col("_nguon") != "web").agg(F.max("ngay")).first()[0]
    tu_ngay = ngay_max - timedelta(days=ch.SO_NGAY_TEST)
    thuc_te = (
        silver.filter(F.col("ngay") >= F.lit(tu_ngay))
        .groupBy("ma_nguoi_dung")
        .agg(F.collect_set("ma_san_pham").alias("thuc_te"))
    )
    tham_so = {
        "so_phien": args.so_phien,
        "so_ngay": args.so_ngay,
        "p_lien_quan": args.p_lien_quan,
        "p_nen": args.p_nen,
        "p_mua": args.p_mua,
        "eta": args.eta,
        "tang_thinh_hanh": args.tang_thinh_hanh,
        "hat_giong": args.hat_giong,
    }
    hom_nay = datetime.utcnow().date()

    def xu_ly(dong):
        goi_y_dict = [
            {"ma_san_pham": g["ma_san_pham"], "diem_du_doan": g["diem_du_doan"], "nguon": g["nguon"]}
            for g in dong["goi_y"]
        ]
        return mo_phong_mot_nguoi(
            dong["ma_nguoi_dung"], goi_y_dict, dong["thuc_te"], hom_nay, tham_so
        )

    hien_thi = spark.createDataFrame(
        goi_y.join(thuc_te, "ma_nguoi_dung", "left").rdd.flatMap(xu_ly), LUOC_DO
    ).cache()
    ghi_delta(hien_thi, "gold.nhat_ky_hien_thi", "append")
    tong = hien_thi.agg(
        F.count("*").alias("hien_thi"), F.sum(F.col("da_click").cast("long")).alias("click")
    ).first()
    in_ket_qua({"so_hien_thi": tong["hien_thi"], "so_click": tong["click"], "nguon": "mo_phong",
                "chi_als": args.chi_als, "hat_giong": args.hat_giong})


if __name__ == "__main__":
    main()
