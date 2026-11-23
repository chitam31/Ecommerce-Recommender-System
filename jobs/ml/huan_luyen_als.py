"""ml/huan_luyen_als.py - tuning ALS implicitPrefs=True trên features, chọn theo NDCG@10.

Lưới bắt buộc: rank {10,30,50} x regParam {0.01,0.1,1.0} x alpha {1,10,40}.
  --luoi day_du    : đủ 27 tổ hợp (dùng cho báo cáo)
  --luoi toi_thieu : 9 tổ hợp, alpha cố định 10 (dùng cho DAG chạy hằng ngày)
Cả hai lưới luôn có thêm tổ hợp của mô hình đang phục vụ nếu lưới chưa có (đặt cuối lưới), để
lần chạy mới được so với mô hình cũ ở cùng siêu tham số; --khong-them-dang-phuc-vu để tắt.

Mỗi tổ hợp ghi một dòng vào gold.ket_qua_tuning kèm ma_chay và phien_ban_features.
Mô hình tốt nhất được lưu ở /lakehouse/bd03_khuyen_nghi/models/als/<ma_chay> để bước danh_gia dùng lại.
"""
import argparse
import itertools
from datetime import datetime

from common import cau_hinh as ch
from common.als_lib import chay_tuning, chon_tot_nhat, mo_hinh_dang_trien_khai, them_to_hop_dang_phuc_vu
from common.luoc_do import tao_df
from common.spark_utils import ghi_delta, in_ket_qua, tao_spark


def to_hop_tham_so(ten_luoi):
    """Sinh danh sách tổ hợp theo thứ tự cố định (quan trọng cho tái lập)."""
    luoi = ch.LUOI_DAY_DU if ten_luoi == "day_du" else ch.LUOI_TOI_THIEU
    return [
        {"rank": r, "regParam": g, "alpha": a}
        for r, g, a in itertools.product(luoi["rank"], luoi["regParam"], luoi["alpha"])
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ma-chay", default=None)
    ap.add_argument("--phien-ban", type=int, default=None, help="Phiên bản features; mặc định mới nhất")
    ap.add_argument("--luoi", choices=["day_du", "toi_thieu"], default="day_du")
    ap.add_argument("--max-iter", type=int, default=10)
    ap.add_argument("--so-khoi", type=int, default=None, help="numBlocks của ALS (chống OOM)")
    ap.add_argument("--khong-them-dang-phuc-vu", action="store_true",
                    help="Không thêm tổ hợp của mô hình đang phục vụ vào lưới")
    args = ap.parse_args()

    ma_chay = args.ma_chay or datetime.utcnow().strftime("r%Y%m%dT%H%M%S")
    spark = tao_spark("bd03-huan-luyen-als")
    cac_to_hop, da_them = to_hop_tham_so(args.luoi), None
    if not args.khong_them_dang_phuc_vu:
        cac_to_hop, da_them = them_to_hop_dang_phuc_vu(cac_to_hop, mo_hinh_dang_trien_khai(spark))
    dong, phien_ban = chay_tuning(
        spark,
        args.phien_ban,
        cac_to_hop,
        args.max_iter,
        ch.HAT_GIONG,
        args.so_khoi,
        luu_mo_hinh_vao=ch.duong_dan_mo_hinh("als/" + ma_chay),
    )
    for d in dong:
        d["ma_chay"] = ma_chay
        d["phien_ban_features"] = phien_ban
    # Ghi đè đúng các dòng của ma_chay này (replaceWhere, một commit): Airflow retry hoặc "Clear" task trong
    # cùng lượt chạy (cùng ma_chay) không nhân đôi bảng tuning. Lượt chạy mới có ma_chay mới nên vẫn giữ lịch sử.
    ghi_delta(
        tao_df(spark, "gold.ket_qua_tuning", dong),
        "gold.ket_qua_tuning",
        "overwrite",
        tuy_chon={"replaceWhere": "ma_chay = '{}'".format(ma_chay)},
        ghi_chu={"ma_chay": ma_chay, "max_iter": args.max_iter, "luoi": args.luoi, "so_khoi": args.so_khoi,
                 "them_to_hop_dang_phuc_vu": da_them},
    )

    tot = chon_tot_nhat(dong)
    in_ket_qua(
        {
            "ma_chay": ma_chay,
            "phien_ban_features": phien_ban,
            "so_to_hop": len(dong),
            "them_to_hop_dang_phuc_vu": da_them,
            "max_iter": args.max_iter,
            "tot_nhat": {k: tot[k] for k in ("rank", "reg_param", "alpha", "ndcg_at_10", "coverage")},
        }
    )


if __name__ == "__main__":
    main()
