"""kiem_tra/tuning_validation.py - chọn siêu tham số trên một kỳ validation riêng (bổ sung 11/11/2026).

Hạn chế đã nêu trong báo cáo: lưới 27 tổ hợp và β của xếp lại novelty đều được chọn trên chính
tháng test, nên số tuyệt đối hơi lạc quan. Job này làm lại đúng việc đó theo quy trình sạch:

  * train: không đổi, 4 tháng đầu của đúng phiên bản features (cùng seed, maxIter, lưới 27 tổ hợp).
  * tháng test được tách theo ngày: --so-ngay-val ngày đầu là VALIDATION (chỉ dùng để chọn),
    phần còn lại là KIỂM TRA (chỉ dùng để báo cáo).
  * chọn (rank, regParam, alpha) theo NDCG@10 trên validation; rồi chọn β (200 ứng viên) cũng
    trên validation; báo cáo cả hai trên kỳ kiểm tra, kèm 2 baseline đo trên cùng kỳ kiểm tra.
  * cột "cả tháng" tính lại NDCG@10 trên toàn tháng test như bảng tuning gốc, để đối chiếu.

Chỉ đọc: không ghi bảng, không lưu mô hình, không nối nhãn vào indexer.

    ssh labsoe-11 "cd ~/bd03_khuyen_nghi && tmux new -d -s val 'bash ha_tang/spark/submit.sh als kiem_tra/tuning_validation.py 2>/dev/null > ~/tuning_validation.md'"

Lần chạy 11/11 thu hẹp còn 12 tổ hợp vì thời gian (rank 10 và alpha 1 đã kém xa ở cả validation):
    ... kiem_tra/tuning_validation.py --cac-rank 30,50 --cac-alpha 10,40 ...
"""
import argparse
import itertools
import json
import math
import time

from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.als_lib import (
    _danh_sach_tu_khuyen_nghi,
    bang_danh_muc_item,
    baseline_danh_muc,
    baseline_toan_san,
    chi_so_nhieu_ky,
    chi_so_xep_hang,
    chia_ky_validation,
    danh_chi_so_bang_indexer_da_luu,
    doc_features,
    gop_nhan_tren_driver,
    tao_als,
    thong_ke_pho_bien,
    thuc_te_cua,
    thuc_te_tu_su_kien,
    tinh_chi_so,
)
from common.spark_utils import doc_delta, tao_spark
from common.tron import xep_lai_novelty

K = ch.K_DANH_GIA
NDCG = "ndcg_at_{}".format(K)


def luoi_day_du(cac_rank=None, cac_reg=None, cac_alpha=None):
    """Mặc định đúng 27 tổ hợp và đúng thứ tự của huan_luyen_als.py --luoi day_du; có thể thu hẹp
    từng trục (giữ nguyên thứ tự)."""
    luoi = ch.LUOI_DAY_DU
    return [
        {"rank": r, "regParam": g, "alpha": a}
        for r, g, a in itertools.product(cac_rank or luoi["rank"], cac_reg or luoi["regParam"],
                                         cac_alpha or luoi["alpha"])
    ]


def doc_danh_sach(chuoi, kieu):
    """'30,50' -> [30, 50]; chuỗi rỗng -> None (dùng giá trị mặc định của lưới)."""
    return [kieu(x) for x in chuoi.split(",") if x.strip()] if chuoi else None


def chon_theo(cac_dong, khoa):
    """Dòng có khoa lớn nhất; hòa thì lấy dòng đứng trước trong lưới (thứ tự cố định)."""
    tot = None
    for d in cac_dong:
        if tot is None or d[khoa] > tot[khoa]:
            tot = d
    return tot


def xep_lai_tu_ung_vien(ung_vien, nov, beta, spark):
    """Xếp lại từ bảng ứng viên đã tính sẵn (user_idx, recommendations) với một β."""
    nov_bc = spark.sparkContext.broadcast(nov)

    def xep(r):
        ds = [(x["item_idx"], float(x["rating"])) for x in r["recommendations"]]
        return (r["user_idx"], [int(i) for i in xep_lai_novelty(ds, nov_bc.value, beta, K)])

    return spark.createDataFrame(ung_vien.rdd.map(xep), "user_idx int, ds array<int>")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phien-ban", type=int, default=None, help="Phiên bản features; mặc định mới nhất")
    ap.add_argument("--so-ngay-val", type=int, default=15)
    ap.add_argument("--max-iter", type=int, default=10)
    ap.add_argument("--cac-beta", default="0,0.25,0.5,1.0,1.5,2.0")
    ap.add_argument("--so-ung-vien", type=int, default=ch.SO_UNG_VIEN_XEP_LAI)
    ap.add_argument("--cac-rank", default="", help="Thu hẹp lưới, ví dụ 30,50")
    ap.add_argument("--cac-reg", default="")
    ap.add_argument("--cac-alpha", default="", help="Ví dụ 10,40")
    args = ap.parse_args()
    bat_dau_job = time.time()

    spark = tao_spark("bd03-tuning-validation")
    train, thuc_te_thang, ghi_chu, phien_ban = doc_features(spark, args.phien_ban)
    if not ghi_chu.get("ngay_cat") or ghi_chu.get("phien_ban_silver") is None:
        raise SystemExit("Commit features phiên bản {} thiếu ngay_cat/phien_ban_silver".format(phien_ban))
    train = train.cache()

    # Sự kiện của tháng test lấy lại từ đúng phiên bản silver đã dựng features, bỏ sự kiện web.
    silver = doc_delta(spark, "silver.tuong_tac", ghi_chu["phien_ban_silver"]).filter(F.col("_nguon") != "web")
    test_tho = silver.filter(F.col("ngay") >= F.lit(ghi_chu["ngay_cat"]).cast("date"))
    if ghi_chu.get("ngay_cuoi"):
        test_tho = test_tho.filter(F.col("ngay") <= F.lit(ghi_chu["ngay_cuoi"]).cast("date"))
    val_tho, kt_tho = chia_ky_validation(test_tho, ghi_chu["ngay_cat"], args.so_ngay_val)
    # Ba bộ ground truth gom về driver thành một bảng nhỏ không còn lineage (gop_nhan_tren_driver).
    nhan = gop_nhan_tren_driver(spark, {
        "thang": thuc_te_thang,
        "val": thuc_te_tu_su_kien(danh_chi_so_bang_indexer_da_luu(val_tho), train),
        "kt": thuc_te_tu_su_kien(danh_chi_so_bang_indexer_da_luu(kt_tho), train),
    }).cache()
    thuc_te_val, thuc_te_kt = thuc_te_cua(nhan, "val").cache(), thuc_te_cua(nhan, "kt").cache()
    nguoi = nhan.select("user_idx").cache()

    pho_bien = thong_ke_pho_bien(train).cache()
    so_item = pho_bien.count()
    so_nguoi_train = train.select("user_idx").distinct().count()
    dem = nhan.select(
        F.count(F.col("thang")).alias("thang"), F.count(F.col("val")).alias("val"), F.count(F.col("kt")).alias("kt"),
        F.sum((F.col("val").isNotNull() & F.col("kt").isNotNull()).cast("int")).alias("ca_hai"),
    ).first()
    quy_mo = {
        "phien_ban_features": phien_ban,
        "phien_ban_silver": ghi_chu["phien_ban_silver"],
        "ngay_cat": ghi_chu["ngay_cat"],
        "ngay_cuoi": ghi_chu.get("ngay_cuoi"),
        "so_ngay_val": args.so_ngay_val,
        "so_user_ca_thang": int(dem["thang"]),
        "so_user_val": int(dem["val"]),
        "so_user_kiem_tra": int(dem["kt"]),
        "so_user_ca_hai_ky": int(dem["ca_hai"]),
        "so_item_train": so_item,
    }
    print("[val] quy mô: " + json.dumps(quy_mo, ensure_ascii=False), flush=True)

    # 1. Lưới (mặc định 27 tổ hợp): một mô hình, một lần sinh top-10, ba bộ ground truth trong một lượt.
    dong = []
    mo_hinh_chon, dong_chon = None, None
    luoi = luoi_day_du(doc_danh_sach(args.cac_rank, int), doc_danh_sach(args.cac_reg, float),
                       doc_danh_sach(args.cac_alpha, float))
    quy_mo["so_to_hop"] = len(luoi)
    for tham_so in luoi:
        t0 = time.time()
        mo_hinh = tao_als(tham_so, args.max_iter, ch.HAT_GIONG).fit(train)
        ds = _danh_sach_tu_khuyen_nghi(mo_hinh.recommendForUserSubset(nguoi, K)).cache()
        m = chi_so_nhieu_ky(ds, nhan, ["val", "kt", "thang"], K)
        m_val, m_kt, m_thang = m["val"], m["kt"], m["thang"]
        ds.unpersist()
        d = {
            "rank": int(tham_so["rank"]), "reg_param": float(tham_so["regParam"]), "alpha": float(tham_so["alpha"]),
            "ndcg_val": m_val[NDCG], "ndcg_kiem_tra": m_kt[NDCG], "ndcg_ca_thang": m_thang[NDCG],
            "recall_kiem_tra": m_kt["recall_at_{}".format(K)], "so_giay": round(time.time() - t0, 1),
        }
        dong.append(d)
        print("[val] rank={rank} reg={reg_param} alpha={alpha} val={ndcg_val:.5f} kiem_tra={ndcg_kiem_tra:.5f} "
              "ca_thang={ndcg_ca_thang:.5f} ({so_giay}s)".format(**d), flush=True)
        if dong_chon is None or d["ndcg_val"] > dong_chon["ndcg_val"]:
            dong_chon, mo_hinh_chon = d, mo_hinh

    tot_kt = chon_theo(dong, "ndcg_kiem_tra")
    tot_thang = chon_theo(dong, "ndcg_ca_thang")

    # 2. Baseline trên đúng kỳ kiểm tra.
    ds_b1 = baseline_toan_san(spark, thuc_te_kt, pho_bien, K).cache()
    m_b1 = tinh_chi_so(ds_b1, thuc_te_kt, pho_bien, so_item, so_nguoi_train, K)
    ds_b2 = baseline_danh_muc(spark, train, thuc_te_kt, pho_bien, bang_danh_muc_item(spark, ghi_chu), K).cache()
    m_b2 = tinh_chi_so(ds_b2, thuc_te_kt, pho_bien, so_item, so_nguoi_train, K)

    # 3. β của xếp lại novelty, chọn trên validation với mô hình đã chọn trên validation.
    nov = {
        r["item_idx"]: -math.log(max(r["so_nguoi"], 1) / float(so_nguoi_train), 2)
        for r in pho_bien.select("item_idx", "so_nguoi").collect()
    }
    ung_vien = mo_hinh_chon.recommendForUserSubset(nguoi, args.so_ung_vien).cache()
    nguoi_kt = thuc_te_kt.select("user_idx")
    dong_beta = []
    for beta in [float(x) for x in args.cac_beta.split(",")]:
        ds = xep_lai_tu_ung_vien(ung_vien, nov, beta, spark).cache()
        m_val = chi_so_xep_hang(ds, thuc_te_val, K)
        m_kt = tinh_chi_so(ds.join(nguoi_kt, "user_idx"), thuc_te_kt, pho_bien, so_item, so_nguoi_train, K)
        dong_beta.append({
            "beta": beta, "ndcg_val": m_val[NDCG], "ndcg_kiem_tra": m_kt[NDCG],
            "coverage_kiem_tra": m_kt["coverage"], "so_item_kiem_tra": m_kt["so_item_duoc_goi_y"],
        })
        print("[val] beta={beta} val={ndcg_val:.5f} kiem_tra={ndcg_kiem_tra:.5f} coverage={coverage_kiem_tra:.4f}".format(
            **dong_beta[-1]), flush=True)
        ds.unpersist()
    beta_chon = chon_theo(dong_beta, "ndcg_val")
    beta_tot_kt = chon_theo(dong_beta, "ndcg_kiem_tra")

    # 4. Bảng Markdown.
    print()
    print("## Tuning trên kỳ validation riêng — features v{}, validation {} ngày đầu của tháng test, {} tổ hợp".format(
        phien_ban, args.so_ngay_val, len(luoi)))
    print()
    print("| Kỳ | User |")
    print("|---|---|")
    print("| Cả tháng test | {:,} |".format(quy_mo["so_user_ca_thang"]))
    print("| Validation ({} ngày đầu) | {:,} |".format(args.so_ngay_val, quy_mo["so_user_val"]))
    print("| Kiểm tra (phần còn lại) | {:,} |".format(quy_mo["so_user_kiem_tra"]))
    print("| Có mặt ở cả hai kỳ | {:,} |".format(quy_mo["so_user_ca_hai_ky"]))
    print()
    print("| rank | regParam | alpha | NDCG@10 validation | NDCG@10 kiểm tra | NDCG@10 cả tháng |")
    print("|---|---|---|---|---|---|")
    for d in sorted(dong, key=lambda x: -x["ndcg_val"]):
        dau = " **(chọn)**" if d is dong_chon else ""
        print("| {} | {} | {} | {:.4f}{} | {:.4f} | {:.4f} |".format(
            d["rank"], d["reg_param"], d["alpha"], d["ndcg_val"], dau, d["ndcg_kiem_tra"], d["ndcg_ca_thang"]))
    print()
    print("| Trên kỳ kiểm tra | NDCG@10 | So với baseline toàn sàn | So với baseline danh mục |")
    print("|---|---|---|---|")

    def tang(x, nen):
        return "{:+.0%}".format(x / nen - 1.0) if nen else "—"

    print("| Baseline toàn sàn | {:.4f} | — | — |".format(m_b1[NDCG]))
    print("| Baseline danh mục | {:.4f} | — | — |".format(m_b2[NDCG]))
    print("| ALS chọn trên validation (rank {}, reg {}, alpha {}) | {:.4f} | {} | {} |".format(
        dong_chon["rank"], dong_chon["reg_param"], dong_chon["alpha"], dong_chon["ndcg_kiem_tra"],
        tang(dong_chon["ndcg_kiem_tra"], m_b1[NDCG]), tang(dong_chon["ndcg_kiem_tra"], m_b2[NDCG])))
    print("| ALS tốt nhất nếu chọn trên chính kỳ kiểm tra (rank {}, reg {}, alpha {}) | {:.4f} | {} | {} |".format(
        tot_kt["rank"], tot_kt["reg_param"], tot_kt["alpha"], tot_kt["ndcg_kiem_tra"],
        tang(tot_kt["ndcg_kiem_tra"], m_b1[NDCG]), tang(tot_kt["ndcg_kiem_tra"], m_b2[NDCG])))
    print("| Xếp lại β = {} (chọn trên validation) | {:.4f} | {} | {} |".format(
        beta_chon["beta"], beta_chon["ndcg_kiem_tra"],
        tang(beta_chon["ndcg_kiem_tra"], m_b1[NDCG]), tang(beta_chon["ndcg_kiem_tra"], m_b2[NDCG])))
    print()
    print("| β | NDCG@10 validation | NDCG@10 kiểm tra | Coverage kiểm tra | Số item |")
    print("|---|---|---|---|---|")
    for d in dong_beta:
        dau = " **(chọn)**" if d is beta_chon else ""
        print("| {} | {:.4f}{} | {:.4f} | {:.2%} | {:,} |".format(
            d["beta"], d["ndcg_val"], dau, d["ndcg_kiem_tra"], d["coverage_kiem_tra"], d["so_item_kiem_tra"]))
    print()

    tom_tat = {
        "quy_mo": quy_mo,
        "chon_tren_validation": dong_chon,
        "tot_nhat_tren_kiem_tra": tot_kt,
        "tot_nhat_tren_ca_thang": tot_thang,
        "baseline_kiem_tra": {"toan_san": m_b1[NDCG], "danh_muc": m_b2[NDCG],
                              "coverage_toan_san": m_b1["coverage"], "coverage_danh_muc": m_b2["coverage"]},
        "beta_chon_tren_validation": beta_chon,
        "beta_tot_nhat_tren_kiem_tra": beta_tot_kt,
        "cac_beta": dong_beta,
        "luoi": dong,
        "so_phut": round((time.time() - bat_dau_job) / 60.0, 1),
    }
    print("```json")
    print(json.dumps(tom_tat, ensure_ascii=False, indent=1, default=str))
    print("```")


if __name__ == "__main__":
    main()
