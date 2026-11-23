"""ml/danh_gia.py - đánh giá mô hình tốt nhất của một lần tuning và quyết định triển khai.

So sánh ALS với 2 baseline bắt buộc theo Precision/Recall/NDCG/MAP@10 + Coverage + Novelty,
thử re-ranking theo novelty để giảm popularity bias (mục 3.10), đo thêm mô hình v2 lọc
theo tầm giá nếu silver đã qua schema evolution tuần 5, rồi ghi một dòng vào
gold.lich_su_huan_luyen. Dòng JSON cuối stdout có khóa "cai_thien" để Airflow rẽ nhánh.

Quy ước "NDCG có cải thiện?": đạt khi chưa có mô hình nào được triển khai, hoặc NDCG@10
mới không thấp hơn mô hình đang triển khai quá --dung-sai (mặc định 1%). Dung sai này
tránh việc gợi ý bị "đóng băng" chỉ vì NDCG dao động rất nhỏ giữa hai ngày.
"""
import argparse
import json
from datetime import datetime

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.als_lib import chon_tot_nhat, danh_gia_day_du, doc_features, mo_hinh_dang_trien_khai, ty_le_tang
from common.luoc_do import tao_df
from common.tron import ten_phien_ban_mo_hinh
from common.spark_utils import (
    doc_delta,
    ghi_delta,
    in_ket_qua,
    tao_spark,
    tim_ghi_chu_theo,
)


def doc_lan_tuning(spark, ma_chay):
    """Lấy các dòng tuning của ma_chay và thông số chạy (max_iter, so_khoi)."""
    dong = [
        r.asDict()
        for r in doc_delta(spark, "gold.ket_qua_tuning").filter(F.col("ma_chay") == ma_chay).collect()
    ]
    if not dong:
        raise SystemExit("Không tìm thấy kết quả tuning của ma_chay={}".format(ma_chay))
    ghi_chu = tim_ghi_chu_theo(spark, "gold.ket_qua_tuning", "ma_chay", ma_chay) or {}
    return dong, ghi_chu


def quyet_dinh(ndcg_moi, dong_cu, dung_sai):
    """True nếu nên làm mới gợi ý bằng mô hình mới."""
    if dong_cu is None or dong_cu.get("ndcg_at_10") is None:
        return True
    return ndcg_moi >= dong_cu["ndcg_at_10"] * (1.0 - dung_sai)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ma-chay", required=True)
    ap.add_argument("--dung-sai", type=float, default=0.01)
    args = ap.parse_args()

    spark = tao_spark("bd03-danh-gia")
    cac_dong, ghi_chu_tuning = doc_lan_tuning(spark, args.ma_chay)
    tot = chon_tot_nhat(cac_dong)
    phien_ban = int(tot["phien_ban_features"])

    train, thuc_te, ghi_chu_features, _ = doc_features(spark, phien_ban)
    train, thuc_te = train.cache(), thuc_te.cache()
    duong_dan = ch.duong_dan_mo_hinh("als/" + args.ma_chay)
    mo_hinh = ALSModel.load(duong_dan)
    chi_tiet = danh_gia_day_du(spark, mo_hinh, train, thuc_te, ghi_chu_features)

    m = chi_tiet["als"]
    b1 = chi_tiet["baseline_toan_san"]["ndcg_at_10"]
    b2 = chi_tiet["baseline_danh_muc"]["ndcg_at_10"]
    tang = ty_le_tang(m["ndcg_at_10"], b1)
    dat_nguong = bool(
        tang is not None and tang >= ch.NGUONG_TANG_NDCG and m["coverage"] >= ch.NGUONG_COVERAGE
    )
    dong_cu = mo_hinh_dang_trien_khai(spark)
    cai_thien = quyet_dinh(m["ndcg_at_10"], dong_cu, args.dung_sai)
    # Đây là điểm quyết định DUY NHẤT về v1/v2; các bước sau đọc lại phien_ban_mo_hinh.
    la_v2 = chi_tiet.get("loc_tam_gia_v2") is not None

    dong = {
        "ma_chay": args.ma_chay,
        "phien_ban_mo_hinh": ten_phien_ban_mo_hinh(args.ma_chay, la_v2),
        "thoi_diem": datetime.utcnow(),
        "phien_ban_features": phien_ban,
        "phien_ban_silver": ghi_chu_features.get("phien_ban_silver"),
        "ngay_cat": datetime.strptime(ghi_chu_features["ngay_cat"], "%Y-%m-%d").date()
        if ghi_chu_features.get("ngay_cat")
        else None,
        "rank": tot["rank"],
        "reg_param": tot["reg_param"],
        "alpha": tot["alpha"],
        "max_iter": int(ghi_chu_tuning.get("max_iter", 10)),
        "hat_giong": ch.HAT_GIONG,
        "precision_at_10": m["precision_at_10"],
        "recall_at_10": m["recall_at_10"],
        "ndcg_at_10": m["ndcg_at_10"],
        "map_at_10": m["map_at_10"],
        "coverage": m["coverage"],
        "coverage_toan_san": m["coverage_toan_san"],
        "novelty": m["novelty"],
        "ndcg_baseline_toan_san": b1,
        "ndcg_baseline_danh_muc": b2,
        "uplift_ndcg": tang,
        "dat_nguong": dat_nguong,
        "cai_thien": cai_thien,
        "trang_thai": "cho_trien_khai" if cai_thien else "giu_mo_hinh_cu",
        "duong_dan_mo_hinh": duong_dan,
        "chi_tiet_json": json.dumps(chi_tiet, ensure_ascii=False),
    }
    # replaceWhere theo ma_chay: chạy lại bước đánh giá của cùng lượt không sinh hai dòng lịch sử.
    ghi_delta(tao_df(spark, "gold.lich_su_huan_luyen", [dong]), "gold.lich_su_huan_luyen", "overwrite",
              tuy_chon={"replaceWhere": "ma_chay = '{}'".format(args.ma_chay)})

    in_ket_qua(
        {
            "ma_chay": args.ma_chay,
            "phien_ban_mo_hinh": ten_phien_ban_mo_hinh(args.ma_chay, la_v2),
            "cai_thien": cai_thien,
            "dat_nguong": dat_nguong,
            "ndcg_at_10": m["ndcg_at_10"],
            "ndcg_baseline_toan_san": b1,
            "ndcg_baseline_danh_muc": b2,
            "uplift_ndcg": tang,
            "coverage": m["coverage"],
            "ndcg_mo_hinh_cu": dong_cu.get("ndcg_at_10") if dong_cu else None,
            "ndcg_v2_loc_tam_gia": chi_tiet["loc_tam_gia_v2"]["ndcg_at_10"] if la_v2 else None,
            "ndcg_phuc_vu": (chi_tiet.get("phuc_vu") or {}).get("ndcg_at_10"),
            "coverage_phuc_vu": (chi_tiet.get("phuc_vu") or {}).get("coverage"),
        }
    )


if __name__ == "__main__":
    main()
