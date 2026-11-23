"""kiem_tra/bootstrap_ab.py - kiểm định A/B lấy USER làm đơn vị (bổ sung 11/11/2026). 

Kết quả chính của báo cáo (CTR A 2,094 % – B 1,978 %, p = 0,015) dùng z-test hai tỷ lệ theo
impression, tức coi 350.160 impression là độc lập. Trong mô phỏng, mỗi user có nhiều phiên và
xác suất click phụ thuộc vào việc danh sách của chính user đó có món "đúng" hay không, nên các
impression của cùng một user tương quan. Job này tính lại trên đúng ảnh chụp đó bằng:

  * z-test với sai số chuẩn theo cụm user (delta method),
  * bootstrap lấy mẫu lại user (2.000 lần, hạt giống cố định),
  * hệ số thiết kế (design effect) cho biết z-test theo impression lạc quan bao nhiêu.

Chỉ đọc, không ghi bảng nào. Mặc định chỉ lấy traffic mô phỏng của chiến lược ALS, đúng phạm vi
phân tích chính; --ca-web để gộp cả impression thật ghi từ web.

    ssh labsoe-11 "cd ~/bd03_khuyen_nghi && bash ha_tang/spark/submit.sh nhanh kiem_tra/bootstrap_ab.py 2>/dev/null" > bootstrap_ab.md
"""
import argparse
import json

from pyspark.sql import functions as F

from common.spark_utils import doc_delta, phien_ban_moi_nhat, tao_spark
from common.thong_ke import bootstrap_theo_user, he_so_thiet_ke, ztest_hai_ty_le, ztest_theo_cum


def theo_user(nhat_ky, chi_mo_phong=True):
    """(nhom_ab, ma_nguoi_dung) -> số impression, click, mua; cùng bộ lọc với common/ab.py."""
    df = nhat_ky.filter(F.col("chien_luoc").isNull() | (F.col("chien_luoc") == "als"))
    if chi_mo_phong:
        df = df.filter(F.col("nguon_traffic") == "mo_phong")
    return (
        df.groupBy("nhom_ab", "ma_nguoi_dung")
        .agg(
            F.count("*").alias("n"),
            F.sum(F.col("da_click").cast("long")).alias("click"),
            F.sum(F.col("da_mua").cast("long")).alias("mua"),
        )
    )


def phan_tich(dong, so_lan, hat_giong):
    """dong: list dict (nhom_ab, n, click, mua). Trả về dict kết quả cho CTR và chuyển đổi."""
    nhom = {"A": [], "B": []}
    for d in dong:
        if d["nhom_ab"] in nhom:
            nhom[d["nhom_ab"]].append(d)
    ket_qua = {}
    for ten, cot in (("ctr", "click"), ("chuyen_doi", "mua")):
        a = [(int(d["n"]), int(d["click"] if cot == "click" else d["mua"])) for d in nhom["A"]]
        b = [(int(d["n"]), int(d["click"] if cot == "click" else d["mua"])) for d in nhom["B"]]
        z_imp, p_imp = ztest_hai_ty_le(sum(c for _, c in a), sum(n for n, _ in a),
                                       sum(c for _, c in b), sum(n for n, _ in b))
        ket_qua[ten] = {
            "theo_impression": {"z": z_imp, "p_value": p_imp},
            "theo_cum_user": ztest_theo_cum(a, b),
            "bootstrap_user": bootstrap_theo_user(a, b, so_lan=so_lan, hat_giong=hat_giong),
            "he_so_thiet_ke": he_so_thiet_ke(a, b),
        }
    ket_qua["quy_mo"] = {
        g: {
            "so_user": len(nhom[g]),
            "so_hien_thi": int(sum(d["n"] for d in nhom[g])),
            "so_click": int(sum(d["click"] for d in nhom[g])),
            "so_mua": int(sum(d["mua"] for d in nhom[g])),
        }
        for g in ("A", "B")
    }
    return ket_qua


def in_markdown(kq, phien_ban, chi_mo_phong):
    q = kq["quy_mo"]
    print("## Kiểm định A/B theo user — gold.nhat_ky_hien_thi phiên bản {} ({})".format(
        phien_ban, "chỉ traffic mô phỏng" if chi_mo_phong else "gồm cả traffic web"))
    print()
    print("| Nhóm | User | Impression | Click | Mua | Impression/user |")
    print("|---|---|---|---|---|---|")
    for g in ("A", "B"):
        print("| {} | {:,} | {:,} | {:,} | {:,} | {:.1f} |".format(
            g, q[g]["so_user"], q[g]["so_hien_thi"], q[g]["so_click"], q[g]["so_mua"],
            q[g]["so_hien_thi"] / max(q[g]["so_user"], 1)))
    print()
    print("| Chỉ số | Tỷ lệ A | Tỷ lệ B | Hiệu tương đối | p theo impression | p theo cụm user | p bootstrap user | KTC 95 % hiệu tương đối (bootstrap) | Design effect |")
    print("|---|---|---|---|---|---|---|---|---|")
    for ten in ("ctr", "chuyen_doi"):
        r = kq[ten]
        c, b = r["theo_cum_user"], r["bootstrap_user"]
        if c is None or b is None:
            print("| {} | thiếu dữ liệu |".format(ten))
            continue
        print("| {} | {:.4%} | {:.4%} | {:+.2%} | {:.4f} | {:.4f} | {:.4f} | [{:+.2%} ; {:+.2%}] | {:.2f} |".format(
            ten, c["ty_le_a"], c["ty_le_b"], b["hieu_tuong_doi"], r["theo_impression"]["p_value"],
            c["p_value"], b["p_value"], b["ci_hieu_tuong_doi"][0], b["ci_hieu_tuong_doi"][1],
            r["he_so_thiet_ke"]))
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phien-ban", type=int, default=None, help="Phiên bản impression log; mặc định mới nhất")
    ap.add_argument("--ca-web", action="store_true", help="Gộp cả impression thật từ web")
    ap.add_argument("--so-lan", type=int, default=2000)
    ap.add_argument("--hat-giong", type=int, default=42)
    args = ap.parse_args()

    spark = tao_spark("bd03-bootstrap-ab")
    phien_ban = args.phien_ban if args.phien_ban is not None else phien_ban_moi_nhat(spark, "gold.nhat_ky_hien_thi")
    nhat_ky = doc_delta(spark, "gold.nhat_ky_hien_thi", phien_ban)
    dong = [r.asDict() for r in theo_user(nhat_ky, not args.ca_web).collect()]
    kq = phan_tich(dong, args.so_lan, args.hat_giong)
    in_markdown(kq, phien_ban, not args.ca_web)
    kq["phien_ban_delta_du_lieu"] = int(phien_ban)
    kq["chi_mo_phong"] = not args.ca_web
    print("```json")
    print(json.dumps(kq, ensure_ascii=False, indent=1, default=str))
    print("```")


if __name__ == "__main__":
    main()
