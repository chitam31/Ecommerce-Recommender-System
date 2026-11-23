"""kiem_tra/tai_lap_huan_luyen.py - DAG 5: chạy lại huấn luyện trên VERSION AS OF và so metric.

Nhận phiên bản features (hoặc ma_chay), đọc lại đúng phiên bản đó bằng time travel, chạy
lại chính các hàm tuning + đánh giá của pipeline với cùng siêu tham số, maxIter và hạt
giống, rồi so với bản ghi lịch sử. Job không ghi vào bảng gợi ý; chỉ ghi kết quả so
sánh vào gold.ket_qua_tai_lap.

Về "trùng khớp": ALS là thuật toán số thực chạy phân tán. Cùng dữ liệu, cùng hạt giống
và cùng số khối thì kết quả gần như luôn giống hệt; chênh lệch nếu có chỉ ở mức sai số
làm tròn. Vì vậy job báo cáo độ lệch lớn nhất và coi là trùng khớp khi lệch < 1e-6.
"""
import argparse
import json
from datetime import datetime

from pyspark.sql import functions as F

from common.als_lib import chay_tuning, danh_gia_day_du, doc_features, tao_als
from common.luoc_do import tao_df
from common.spark_utils import doc_delta, ghi_delta, in_ket_qua, tao_spark, tim_ghi_chu_theo

CHI_SO = ["precision_at_10", "recall_at_10", "ndcg_at_10", "map_at_10", "coverage", "novelty"]
DUNG_SAI = 1e-6


def tim_lan_chay(spark, phien_ban, ma_chay):
    """Dòng lịch sử cần tái lập: theo ma_chay, hoặc lần gần nhất dùng phiên bản đó."""
    ls = doc_delta(spark, "gold.lich_su_huan_luyen")
    if ma_chay:
        ls = ls.filter(F.col("ma_chay") == ma_chay)
    elif phien_ban is not None:
        ls = ls.filter(F.col("phien_ban_features") == int(phien_ban))
    dong = ls.orderBy(F.desc("thoi_diem")).first()
    if dong is None:
        raise SystemExit("Không có lần huấn luyện nào khớp phien_ban={} ma_chay={}".format(phien_ban, ma_chay))
    return dong.asDict()


def so_sanh(cu, moi, cac_khoa):
    """Trả về (độ lệch lớn nhất, dict độ lệch từng metric)."""
    lech = {}
    for k in cac_khoa:
        if cu.get(k) is None or moi.get(k) is None:
            continue
        lech[k] = abs(float(cu[k]) - float(moi[k]))
    return (max(lech.values()) if lech else 0.0), lech


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phien-ban", type=int, default=None, help="phien_ban_delta của features")
    ap.add_argument("--ma-chay", default=None)
    ap.add_argument("--che-do", choices=["mo_hinh_chon", "toan_bo_luoi"], default="mo_hinh_chon")
    args = ap.parse_args()

    spark = tao_spark("bd03-tai-lap-huan-luyen")
    goc = tim_lan_chay(spark, args.phien_ban, args.ma_chay)
    phien_ban = int(goc["phien_ban_features"])
    ghi_chu_tuning = tim_ghi_chu_theo(spark, "gold.ket_qua_tuning", "ma_chay", goc["ma_chay"]) or {}

    tuning_cu = [
        r.asDict()
        for r in doc_delta(spark, "gold.ket_qua_tuning").filter(F.col("ma_chay") == goc["ma_chay"]).collect()
    ]
    if args.che_do == "mo_hinh_chon" or not tuning_cu:
        cac_to_hop = [{"rank": goc["rank"], "regParam": goc["reg_param"], "alpha": goc["alpha"]}]
    else:
        cac_to_hop = [{"rank": d["rank"], "regParam": d["reg_param"], "alpha": d["alpha"]} for d in tuning_cu]

    # (1) Chạy lại tuning trên VERSION AS OF phien_ban, không lưu mô hình, không ghi bảng.
    tuning_moi, _ = chay_tuning(
        spark, phien_ban, cac_to_hop, goc["max_iter"], goc["hat_giong"], ghi_chu_tuning.get("so_khoi")
    )
    dong_ket_qua = []
    lech_tong = 0.0
    for moi in tuning_moi:
        khop = [
            d for d in tuning_cu
            if d["rank"] == moi["rank"] and d["reg_param"] == moi["reg_param"] and d["alpha"] == moi["alpha"]
        ]
        if not khop:
            continue
        lech, _ = so_sanh(khop[0], moi, CHI_SO)
        lech_tong = max(lech_tong, lech)
        dong_ket_qua.append(
            {
                "thoi_diem": datetime.utcnow(),
                "loai": "tuning",
                "khoa": "{}|rank={}|reg={}|alpha={}".format(goc["ma_chay"], moi["rank"], moi["reg_param"], moi["alpha"]),
                "phien_ban_delta": phien_ban,
                "chi_so_cu_json": json.dumps({k: khop[0][k] for k in CHI_SO}),
                "chi_so_moi_json": json.dumps({k: moi[k] for k in CHI_SO}),
                "lech_lon_nhat": lech,
                "trung_khop": lech < DUNG_SAI,
            }
        )

    # (2) Đánh giá đầy đủ mô hình đã chọn (cả 2 baseline) và so với lịch sử.
    train, thuc_te, ghi_chu_features, _ = doc_features(spark, phien_ban)
    train, thuc_te = train.cache(), thuc_te.cache()
    tham_so = {"rank": goc["rank"], "regParam": goc["reg_param"], "alpha": goc["alpha"]}
    mo_hinh = tao_als(tham_so, goc["max_iter"], goc["hat_giong"], ghi_chu_tuning.get("so_khoi")).fit(train)
    chi_tiet = danh_gia_day_du(spark, mo_hinh, train, thuc_te, ghi_chu_features)
    moi = dict(chi_tiet["als"])
    moi["ndcg_baseline_toan_san"] = chi_tiet["baseline_toan_san"]["ndcg_at_10"]
    moi["ndcg_baseline_danh_muc"] = chi_tiet["baseline_danh_muc"]["ndcg_at_10"]
    # Mô hình v2 (lọc tầm giá): so thêm NDCG@10 của v2 nếu lần chạy gốc có đo.
    v2_cu = (json.loads(goc.get("chi_tiet_json") or "{}").get("loc_tam_gia_v2") or {}).get("ndcg_at_10")
    goc["ndcg_v2_loc_tam_gia"] = v2_cu
    moi["ndcg_v2_loc_tam_gia"] = (chi_tiet.get("loc_tam_gia_v2") or {}).get("ndcg_at_10")
    cac_khoa = CHI_SO + ["ndcg_baseline_toan_san", "ndcg_baseline_danh_muc", "ndcg_v2_loc_tam_gia"]
    lech, chi_tiet_lech = so_sanh(goc, moi, cac_khoa)
    lech_tong = max(lech_tong, lech)
    dong_ket_qua.append(
        {
            "thoi_diem": datetime.utcnow(),
            "loai": "huan_luyen",
            "khoa": goc["ma_chay"],
            "phien_ban_delta": phien_ban,
            "chi_so_cu_json": json.dumps({k: goc.get(k) for k in cac_khoa}),
            "chi_so_moi_json": json.dumps({k: moi.get(k) for k in cac_khoa}),
            "lech_lon_nhat": lech,
            "trung_khop": lech < DUNG_SAI,
        }
    )
    ghi_delta(tao_df(spark, "gold.ket_qua_tai_lap", dong_ket_qua), "gold.ket_qua_tai_lap", "append")

    in_ket_qua(
        {
            "ma_chay": goc["ma_chay"],
            "phien_ban_features": phien_ban,
            "so_to_hop_so_sanh": len(dong_ket_qua) - 1,
            "lech_lon_nhat": lech_tong,
            "trung_khop": lech_tong < DUNG_SAI,
            "lech_tung_metric": chi_tiet_lech,
            "dung_sai": DUNG_SAI,
            "hat_giong": goc["hat_giong"],
        }
    )


if __name__ == "__main__":
    main()
