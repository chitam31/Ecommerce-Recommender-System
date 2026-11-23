"""kiem_tra/thu_xep_lai.py - quét re-ranking novelty: đường đánh đổi NDCG@10 - Coverage.

Lần chạy thật 10/11/2026: ALS đạt NDCG@10 gấp 13 lần baseline nhưng Coverage chỉ 3,4% (ngưỡng
10%); 99% suất gợi ý rơi vào 10% item phổ biến nhất. danh_gia.py chỉ thử β = 0,1 và 0,3 trên 50
ứng viên. Job này dùng lại đúng mô hình và phiên bản features của một lần huấn luyện, thử nhiều
β hơn và tập ứng viên rộng hơn, rồi in bảng Markdown để chọn điểm cân bằng cho báo cáo.

Không huấn luyện lại, không ghi bảng nào. Lưu ý: chọn β trên chính tập test là tuning trên tập
test (cùng giới hạn đã nêu ở MO_HINH.md mục 13).

    ssh labsoe-11 "cd ~/bd03_khuyen_nghi && bash ha_tang/spark/submit.sh als kiem_tra/thu_xep_lai.py 2>/dev/null" > xep_lai.md
"""
import argparse
import math
import sys

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.als_lib import doc_features, phan_phoi_pho_bien, thong_ke_pho_bien, tinh_chi_so
from common.spark_utils import bang_ton_tai, doc_delta, tao_spark
from common.tron import xep_lai_novelty


def dong_ket_qua(so_ung_vien, beta, m, ndcg_b1, so_item_toan_san, ty_le_n1):
    """Một dòng của bảng; đạt ngưỡng của đề khi uplift NDCG ≥ 30% và Coverage ≥ 10%."""
    uplift = (m["ndcg_at_10"] / ndcg_b1 - 1.0) if ndcg_b1 else None
    dat = uplift is not None and uplift >= ch.NGUONG_TANG_NDCG and m["coverage"] >= ch.NGUONG_COVERAGE
    return [
        so_ung_vien, beta,
        "{:.4f}".format(m["precision_at_10"]), "{:.4f}".format(m["recall_at_10"]),
        "{:.4f}".format(m["ndcg_at_10"]), "{:.4f}".format(m["map_at_10"]),
        "{:.2f}%".format(m["coverage"] * 100),
        "{:.2f}%".format(m["so_item_duoc_goi_y"] / float(so_item_toan_san) * 100),
        "{:.2f}".format(m["novelty"]), m["so_item_duoc_goi_y"],
        "{:.1f}%".format(ty_le_n1 * 100),
        "—" if uplift is None else "{:+.0f}%".format(uplift * 100),
        "đạt" if dat else "chưa",
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ma-chay", default=None, help="Mặc định: lần huấn luyện mới nhất")
    ap.add_argument("--beta", default="0,0.1,0.3,0.5,1,2")
    ap.add_argument("--ung-vien", default="50,200", help="Số ứng viên ALS lấy dư trước khi xếp lại")
    args = ap.parse_args()
    cac_beta = [float(b) for b in args.beta.split(",")]
    cac_so_ung_vien = [int(u) for u in args.ung_vien.split(",")]

    spark = tao_spark("bd03-thu-xep-lai")
    ls = doc_delta(spark, "gold.lich_su_huan_luyen")
    if args.ma_chay:
        ls = ls.filter(F.col("ma_chay") == args.ma_chay)
    lich_su = ls.orderBy(F.desc("thoi_diem")).first()
    if lich_su is None:
        raise SystemExit("Chưa có lần huấn luyện nào trong gold.lich_su_huan_luyen")

    train, thuc_te, _, _ = doc_features(spark, int(lich_su["phien_ban_features"]))
    train, thuc_te = train.cache(), thuc_te.cache()
    pho_bien = thong_ke_pho_bien(train).cache()
    so_item = pho_bien.count()
    so_nguoi_train = train.select("user_idx").distinct().count()
    so_item_toan_san = doc_delta(spark, "silver.san_pham").count() if bang_ton_tai(spark, "silver.san_pham") else so_item
    nov = {
        r["item_idx"]: -math.log(max(r["so_nguoi"], 1) / float(so_nguoi_train), 2)
        for r in pho_bien.select("item_idx", "so_nguoi").collect()
    }
    nov_bc = spark.sparkContext.broadcast(nov)
    mo_hinh = ALSModel.load(ch.duong_dan_mo_hinh_hien_tai(lich_su["duong_dan_mo_hinh"]))
    k = ch.K_DANH_GIA

    dong = []
    for so_ung_vien in cac_so_ung_vien:
        # Lấy ứng viên MỘT lần cho mỗi cỡ tập, rồi thử mọi β trên cùng tập đó.
        ung_vien = mo_hinh.recommendForUserSubset(thuc_te.select("user_idx"), so_ung_vien).cache()
        for beta in cac_beta:
            def xep(r, beta=beta):
                ds = [(x["item_idx"], float(x["rating"])) for x in r["recommendations"]]
                return (r["user_idx"], [int(i) for i in xep_lai_novelty(ds, nov_bc.value, beta, k)])

            ds = spark.createDataFrame(ung_vien.rdd.map(xep), "user_idx int, ds array<int>").cache()
            m = tinh_chi_so(ds, thuc_te, pho_bien, so_item, so_nguoi_train, k)
            n1 = phan_phoi_pho_bien(ds, pho_bien, k)[0]
            dong.append(dong_ket_qua(so_ung_vien, beta, m, lich_su["ndcg_baseline_toan_san"], so_item_toan_san, n1))
            print("[xep_lai] ung_vien={} beta={} ndcg@10={:.5f} coverage={:.4f}".format(
                so_ung_vien, beta, m["ndcg_at_10"], m["coverage"]), file=sys.stderr, flush=True)
            ds.unpersist()
        ung_vien.unpersist()

    tieu_de = ["Ứng viên", "β", "P@10", "R@10", "NDCG@10", "MAP@10", "Coverage", "Coverage toàn sàn",
               "Novelty", "Số item", "Suất gợi ý vào N1", "Uplift so với baseline 1", "Ngưỡng đề"]
    print("# Re-ranking novelty: đường đánh đổi NDCG@10 - Coverage")
    print()
    print("Mô hình `{}` (rank={}, regParam={}, alpha={}), phiên bản features {}; baseline 1 NDCG@10 = {:.4f}; "
          "{} user test, {} item trong train.".format(
              lich_su["phien_ban_mo_hinh"], lich_su["rank"], lich_su["reg_param"], lich_su["alpha"],
              lich_su["phien_ban_features"], lich_su["ndcg_baseline_toan_san"], thuc_te.count(), so_item))
    print()
    print("| " + " | ".join(tieu_de) + " |")
    print("|" + "---|" * len(tieu_de))
    for d in dong:
        print("| " + " | ".join(str(o) for o in d) + " |")
    print()
    print("β = 0 là ALS gốc (xếp theo điểm ALS). Điểm xếp lại = điểm ALS chuẩn hóa + β × novelty chuẩn hóa "
          "trong tập ứng viên (common/tron.xep_lai_novelty).", flush=True)


if __name__ == "__main__":
    main()
