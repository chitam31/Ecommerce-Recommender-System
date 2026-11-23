"""kiem_tra/tai_lap_ab.py - tái lập kết quả A/B của một ngày bằng VERSION AS OF.

Đọc phien_ban_delta_du_lieu đã ghi kèm kết quả, đọc lại impression log đúng phiên bản đó,
chạy lại đúng hàm common.ab.tong_hop_ab và so với kết quả đã công bố. Web gọi cùng hàm
này ngay trong tiến trình; job này dành cho DAG 5 và dòng lệnh.
"""
import argparse
import json
from datetime import datetime

from pyspark.sql import functions as F

from common.ab import tong_hop_ab
from common.luoc_do import tao_df
from common.spark_utils import doc_delta, ghi_delta, in_ket_qua, tao_spark

CHI_SO_AB = ["so_hien_thi", "so_click", "so_mua", "ctr", "ty_le_chuyen_doi", "p_value"]


def tai_lap_mot_ngay(spark, ngay):
    """Trả về (phiên bản, dòng cũ, dòng mới, độ lệch lớn nhất)."""
    cu = [
        r.asDict()
        for r in doc_delta(spark, "gold.ket_qua_ab").filter(F.col("ngay") == F.lit(ngay)).orderBy("nhom_ab").collect()
    ]
    if not cu:
        raise ValueError("Chưa có kết quả A/B cho ngày {}".format(ngay))
    phien_ban = int(cu[0]["phien_ban_delta_du_lieu"])
    moi = tong_hop_ab(doc_delta(spark, "gold.nhat_ky_hien_thi", phien_ban), [cu[0]["ngay"]])
    lech = 0.0
    for d_cu in cu:
        d_moi = next((d for d in moi if d["nhom_ab"] == d_cu["nhom_ab"]), None)
        if d_moi is None:
            lech = float("inf")
            continue
        for k in CHI_SO_AB:
            if d_cu.get(k) is None and d_moi.get(k) is None:
                continue
            if d_cu.get(k) is None or d_moi.get(k) is None:
                lech = float("inf")
                continue
            lech = max(lech, abs(float(d_cu[k]) - float(d_moi[k])))
    return phien_ban, cu, moi, lech


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ngay", default="moi_nhat", help="YYYY-MM-DD hoặc moi_nhat")
    args = ap.parse_args()
    spark = tao_spark("bd03-tai-lap-ab")
    if args.ngay == "moi_nhat":
        args.ngay = str(doc_delta(spark, "gold.ket_qua_ab").agg(F.max("ngay")).first()[0])
    phien_ban, cu, moi, lech = tai_lap_mot_ngay(spark, args.ngay)
    dong = {
        "thoi_diem": datetime.utcnow(),
        "loai": "ab",
        "khoa": args.ngay,
        "phien_ban_delta": phien_ban,
        "chi_so_cu_json": json.dumps([{k: d.get(k) for k in ["nhom_ab"] + CHI_SO_AB} for d in cu], default=str),
        "chi_so_moi_json": json.dumps([{k: d.get(k) for k in ["nhom_ab"] + CHI_SO_AB} for d in moi], default=str),
        "lech_lon_nhat": lech,
        "trung_khop": lech == 0.0,
    }
    ghi_delta(tao_df(spark, "gold.ket_qua_tai_lap", [dong]), "gold.ket_qua_tai_lap", "append")
    in_ket_qua({"ngay": args.ngay, "phien_ban_delta": phien_ban, "lech_lon_nhat": lech, "trung_khop": lech == 0.0})


if __name__ == "__main__":
    main()
