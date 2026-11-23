"""kiem_tra/thu_nguon_ung_vien.py - thêm nguồn ứng viên ngoài ALS trước khi chốt danh sách (bổ sung 11/11/2026).

Cấu hình đang phục vụ (ALS + xếp lại novelty β = 1,0 trên 200 ứng viên) đạt coverage 6,47 %, chưa
tới ngưỡng 10 %: mọi ứng viên vẫn đến từ ALS, mà ALS thiên về item phổ biến. Job này thử dành vài ô
cuối của top-10 cho hai nguồn không đi qua ALS, cả hai tính CHỈ từ dữ liệu train:

  * dong_xuat_hien: item hay được xem cùng ngày với các món user đã tương tác nhiều nhất (cosine
    trên số người, đúng công thức của khối "Sản phẩm liên quan" - ml/san_pham_lien_quan.py);
  * danh_muc: bán chạy trong danh mục user xem nhiều nhất (đúng baseline 2).

Cấu hình (q_c, q_d): giữ 10 − q_c − q_d ô đầu của danh sách đang phục vụ, rồi thêm q_c món đồng
xuất hiện và q_d món danh mục chưa có trong danh sách (thiếu thì bù bằng các món kế tiếp của danh
sách phục vụ). Chọn cấu hình trên kỳ VALIDATION (15 ngày đầu tháng test) theo luật đặt trước:
coverage cao nhất trong các cấu hình có NDCG@10 validation ≥ 95 % của cấu hình đang phục vụ. Báo
cáo NDCG@10 trên kỳ KIỂM TRA (phần còn lại) và trên cả tháng; coverage tính trên danh sách của mọi
user test (không dùng nhãn, nên không phải chọn trên tập test).

Chỉ đọc, không ghi bảng, không đổi gì ở hệ thống đang phục vụ.

    ssh labsoe-11 "cd ~/bd03_khuyen_nghi && tmux new -d -s ung_vien 'bash ha_tang/spark/submit.sh als kiem_tra/thu_nguon_ung_vien.py 2>/dev/null > ~/nguon_ung_vien.md'"
"""
import argparse
import json
import math
import time

from pyspark.ml.feature import StringIndexerModel
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.als_lib import (
    bang_danh_muc_item,
    baseline_danh_muc,
    baseline_toan_san,
    chi_so_xep_hang,
    chia_ky_validation,
    danh_chi_so_bang_indexer_da_luu,
    doc_features,
    gop_nhan_tren_driver,
    phan_phoi_pho_bien,
    thong_ke_pho_bien,
    thuc_te_cua,
    thuc_te_tu_su_kien,
    tinh_chi_so,
)
from common.spark_utils import doc_delta, tao_spark
from common.tron import xep_lai_novelty
from ml.san_pham_lien_quan import tinh_lien_quan

K = ch.K_DANH_GIA
NDCG = "ndcg_at_{}".format(K)
CAU_HINH = [(0, 0), (1, 0), (2, 0), (3, 0), (0, 1), (0, 2), (1, 1), (2, 1), (3, 1), (2, 2)]


def ghep_danh_sach(phuc_vu, dong_xuat, danh_muc, q_c, q_d, k=K):
    """Giữ k − q_c − q_d món đầu của danh sách phục vụ, thêm q_c món đồng xuất hiện rồi q_d món
    danh mục chưa có trong danh sách; thiếu thì bù bằng các món kế tiếp của danh sách phục vụ."""
    giu = max(k - q_c - q_d, 0)
    kq = list(phuc_vu[:giu])
    da_co = set(kq)
    for nguon, so in ((dong_xuat or [], q_c), (danh_muc or [], q_d)):
        them = 0
        for i in nguon:
            if them >= so:
                break
            if i not in da_co:
                kq.append(i)
                da_co.add(i)
                them += 1
    for i in phuc_vu[giu:]:
        if len(kq) >= k:
            break
        if i not in da_co:
            kq.append(i)
            da_co.add(i)
    return kq[:k]


def chon_cau_hinh(cac_dong, ty_le_giu=0.95):
    """Luật đặt trước: coverage cao nhất trong các cấu hình có NDCG validation ≥ ty_le_giu x cấu hình
    gốc (0, 0); hòa thì lấy cấu hình đứng trước. Cấu hình gốc luôn đủ điều kiện."""
    goc = next(d for d in cac_dong if (d["q_c"], d["q_d"]) == (0, 0))
    nguong = goc["ndcg_val"] * ty_le_giu
    tot = None
    for d in cac_dong:
        if d["ndcg_val"] >= nguong and (tot is None or d["coverage_val"] > tot["coverage_val"]):
            tot = d
    return tot


def ung_vien_dong_xuat_hien(spark, silver_train, train, nguoi, so_hat=10, so_ung_vien=20):
    """(user_idx, ds_c): món hay được xem cùng ngày với so_hat món điểm cao nhất của user trong train."""
    lq = tinh_lien_quan(silver_train, so_lien_quan=20)
    nhan = StringIndexerModel.load(ch.duong_dan_mo_hinh("item_indexer")).labels
    anh_xa = spark.createDataFrame([(i, str(v)) for i, v in enumerate(nhan)], "item_idx int, ma string")
    lq_idx = (
        lq.join(anh_xa.select(F.col("ma").alias("ma_san_pham"), F.col("item_idx").alias("i")), "ma_san_pham")
        .join(anh_xa.select(F.col("ma").alias("ma_san_pham_lien_quan"), F.col("item_idx").alias("j")),
              "ma_san_pham_lien_quan")
        .select("i", "j", "do_lien_quan")
    )
    hat = (
        train.join(nguoi, "user_idx")
        .withColumn("h", F.row_number().over(Window.partitionBy("user_idx").orderBy(F.desc("diem"), "item_idx")))
        .filter(F.col("h") <= so_hat)
        .select("user_idx", F.col("item_idx").alias("i"), "diem")
    )
    diem = (
        hat.join(lq_idx, "i")
        .groupBy("user_idx", "j")
        .agg(F.sum(F.col("diem") * F.col("do_lien_quan")).alias("s"))
        .withColumn("h", F.row_number().over(Window.partitionBy("user_idx").orderBy(F.desc("s"), "j")))
        .filter(F.col("h") <= so_ung_vien)
    )
    return diem.groupBy("user_idx").agg(
        F.sort_array(F.collect_list(F.struct("h", "j"))).alias("x")
    ).select("user_idx", F.col("x.j").alias("ds_c"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ma-chay", default=None, help="Mặc định: mô hình đang triển khai")
    ap.add_argument("--so-ngay-val", type=int, default=15)
    ap.add_argument("--beta", type=float, default=ch.BETA_XEP_LAI)
    ap.add_argument("--so-ung-vien", type=int, default=ch.SO_UNG_VIEN_XEP_LAI)
    args = ap.parse_args()
    t0 = time.time()

    spark = tao_spark("bd03-thu-nguon-ung-vien")
    ls = doc_delta(spark, "gold.lich_su_huan_luyen")
    if args.ma_chay:
        ls = ls.filter(F.col("ma_chay") == args.ma_chay)
    elif "trang_thai" in ls.columns:
        ls = ls.filter(F.col("trang_thai") == "trien_khai")
    lich_su = ls.orderBy(F.desc("thoi_diem")).first()
    if lich_su is None:
        raise SystemExit("Không tìm thấy lần huấn luyện phù hợp trong gold.lich_su_huan_luyen")

    train, thuc_te, ghi_chu, phien_ban = doc_features(spark, int(lich_su["phien_ban_features"]))
    train, thuc_te = train.cache(), thuc_te.cache()
    silver = doc_delta(spark, "silver.tuong_tac", ghi_chu["phien_ban_silver"]).filter(F.col("_nguon") != "web")
    ngay_cat = F.lit(ghi_chu["ngay_cat"]).cast("date")
    silver_train = silver.filter(F.col("ngay") < ngay_cat)
    test_tho = silver.filter(F.col("ngay") >= ngay_cat)
    if ghi_chu.get("ngay_cuoi"):
        test_tho = test_tho.filter(F.col("ngay") <= F.lit(ghi_chu["ngay_cuoi"]).cast("date"))
    val_tho, kt_tho = chia_ky_validation(test_tho, ghi_chu["ngay_cat"], args.so_ngay_val)
    # Ground truth gom về driver: bảng nhỏ không còn lineage, cache bị đẩy ra cũng không phải đọc lại silver.
    nhan = gop_nhan_tren_driver(spark, {
        "thang": thuc_te,
        "val": thuc_te_tu_su_kien(danh_chi_so_bang_indexer_da_luu(val_tho), train),
        "kt": thuc_te_tu_su_kien(danh_chi_so_bang_indexer_da_luu(kt_tho), train),
    }).cache()
    thuc_te = thuc_te_cua(nhan, "thang").cache()
    thuc_te_val, thuc_te_kt = thuc_te_cua(nhan, "val").cache(), thuc_te_cua(nhan, "kt").cache()
    nguoi = nhan.select("user_idx").cache()

    pho_bien = thong_ke_pho_bien(train).cache()
    so_item = pho_bien.count()
    so_nguoi_train = train.select("user_idx").distinct().count()
    nov = {
        r["item_idx"]: -math.log(max(r["so_nguoi"], 1) / float(so_nguoi_train), 2)
        for r in pho_bien.select("item_idx", "so_nguoi").collect()
    }
    nov_bc = spark.sparkContext.broadcast(nov)
    print("[ung_vien] mô hình {} (rank {}, reg {}, alpha {}), features v{}".format(
        lich_su["phien_ban_mo_hinh"], lich_su["rank"], lich_su["reg_param"], lich_su["alpha"], phien_ban), flush=True)

    # Ba nguồn ứng viên.
    mo_hinh = ALSModel.load(ch.duong_dan_mo_hinh_hien_tai(lich_su["duong_dan_mo_hinh"]))
    als = mo_hinh.recommendForUserSubset(nguoi, args.so_ung_vien)
    dong_xuat = ung_vien_dong_xuat_hien(spark, silver_train, train, nguoi)
    danh_muc = baseline_danh_muc(spark, train, nguoi, pho_bien, bang_danh_muc_item(spark, ghi_chu), K) \
        .select("user_idx", F.col("ds").alias("ds_d"))
    gop = als.join(dong_xuat, "user_idx", "left").join(danh_muc, "user_idx", "left")
    beta = args.beta

    def dung(r):
        ds = [(x["item_idx"], float(x["rating"])) for x in r["recommendations"]]
        phuc_vu = [int(i) for i in xep_lai_novelty(ds, nov_bc.value, beta, 3 * K)]
        c = [int(i) for i in (r["ds_c"] or [])]
        d = [int(i) for i in (r["ds_d"] or [])]
        return [(r["user_idx"], "{}_{}".format(qc, qd), ghep_danh_sach(phuc_vu, c, d, qc, qd), bool(c))
                for qc, qd in CAU_HINH]

    # Tính ba nguồn MỘT lần (đồng xuất hiện là phép tự nối nặng nhất), thu danh sách về driver rồi
    # tạo lại bảng không còn lineage: các lần đo sau không bao giờ phải tính lại ba nguồn.
    dong_tat_ca = gop.rdd.flatMap(dung).collect()
    co_dong_xuat = sum(1 for u, cau_hinh, _, co in dong_tat_ca if cau_hinh == "0_0" and co)
    tat_ca = spark.createDataFrame([(u, h, ds) for u, h, ds, _ in dong_tat_ca],
                                   "user_idx int, cau_hinh string, ds array<int>").cache()
    print("[ung_vien] {} user, {} user có ứng viên đồng xuất hiện ({:.1f} phút)".format(
        len(dong_tat_ca) // len(CAU_HINH), co_dong_xuat, (time.time() - t0) / 60.0), flush=True)

    nguoi_val = thuc_te_val.select("user_idx")
    dong = []
    for qc, qd in CAU_HINH:
        ds = tat_ca.filter(F.col("cau_hinh") == "{}_{}".format(qc, qd)).select("user_idx", "ds").cache()
        m_thang = tinh_chi_so(ds.join(thuc_te.select("user_idx"), "user_idx"), thuc_te, pho_bien, so_item, so_nguoi_train, K)
        m_val = tinh_chi_so(ds.join(nguoi_val, "user_idx"), thuc_te_val, pho_bien, so_item, so_nguoi_train, K)
        m_kt = chi_so_xep_hang(ds, thuc_te_kt, K)
        n1 = phan_phoi_pho_bien(ds.join(thuc_te.select("user_idx"), "user_idx"), pho_bien, K)[0]
        d = {
            "q_c": qc, "q_d": qd,
            "ndcg_val": m_val[NDCG], "coverage_val": m_val["coverage"],
            "ndcg_kiem_tra": m_kt[NDCG],
            "ndcg_ca_thang": m_thang[NDCG], "coverage_ca_thang": m_thang["coverage"],
            "so_item_ca_thang": m_thang["so_item_duoc_goi_y"], "novelty_ca_thang": m_thang["novelty"],
            "ty_le_n1": n1,
        }
        dong.append(d)
        print("[ung_vien] q_c={q_c} q_d={q_d} val={ndcg_val:.5f} kiem_tra={ndcg_kiem_tra:.5f} "
              "ca_thang={ndcg_ca_thang:.5f} coverage={coverage_ca_thang:.4f}".format(**d), flush=True)
        ds.unpersist()

    chon = chon_cau_hinh(dong)
    goc = dong[0]
    ds_b1 = baseline_toan_san(spark, thuc_te, pho_bien, K)
    ndcg_b1 = tinh_chi_so(ds_b1, thuc_te, pho_bien, so_item, so_nguoi_train, K)[NDCG]
    ndcg_b1_kt = chi_so_xep_hang(baseline_toan_san(spark, thuc_te_kt, pho_bien, K), thuc_te_kt, K)[NDCG]

    print()
    print("## Thêm nguồn ứng viên ngoài ALS — mô hình `{}`, features v{}, xếp lại β = {} trên {} ứng viên".format(
        lich_su["phien_ban_mo_hinh"], phien_ban, beta, args.so_ung_vien))
    print()
    print("Mỗi cấu hình (q_c, q_d) giữ 10 − q_c − q_d ô đầu của danh sách đang phục vụ, thêm q_c món đồng xuất "
          "hiện và q_d món bán chạy theo danh mục. Coverage trên {:,} item của train.".format(so_item))
    print()
    print("| q_c | q_d | NDCG@10 validation | NDCG@10 kiểm tra | NDCG@10 cả tháng | Coverage cả tháng | Số item | Suất vào N1 | Novelty |")
    print("|---|---|---|---|---|---|---|---|---|")
    for d in dong:
        dau = " **(chọn)**" if d is chon else ""
        print("| {} | {} | {:.4f}{} | {:.4f} | {:.4f} | {:.2%} | {:,} | {:.1%} | {:.2f} |".format(
            d["q_c"], d["q_d"], d["ndcg_val"], dau, d["ndcg_kiem_tra"], d["ndcg_ca_thang"],
            d["coverage_ca_thang"], d["so_item_ca_thang"], d["ty_le_n1"], d["novelty_ca_thang"]))
    print()
    print("Baseline toàn sàn: NDCG@10 cả tháng {:.4f}, kỳ kiểm tra {:.4f}. Luật chọn: coverage cao nhất trong các "
          "cấu hình có NDCG@10 validation ≥ 95 % cấu hình (0, 0).".format(ndcg_b1, ndcg_b1_kt))
    print()
    tom_tat = {
        "mo_hinh": lich_su["phien_ban_mo_hinh"], "phien_ban_features": phien_ban, "beta": beta,
        "so_item_train": so_item, "so_user_ca_thang": thuc_te.count(),
        "so_user_val": thuc_te_val.count(), "so_user_kiem_tra": thuc_te_kt.count(),
        "user_co_dong_xuat_hien": co_dong_xuat,
        "baseline_toan_san": {"ca_thang": ndcg_b1, "kiem_tra": ndcg_b1_kt},
        "goc": goc, "chon": chon,
        "dat_nguong_coverage": chon["coverage_ca_thang"] >= ch.NGUONG_COVERAGE,
        "uplift_ca_thang_so_voi_baseline": chon["ndcg_ca_thang"] / ndcg_b1 - 1.0 if ndcg_b1 else None,
        "cac_cau_hinh": dong,
        "so_phut": round((time.time() - t0) / 60.0, 1),
    }
    print("```json")
    print(json.dumps(tom_tat, ensure_ascii=False, indent=1, default=str))
    print("```")


if __name__ == "__main__":
    main()
