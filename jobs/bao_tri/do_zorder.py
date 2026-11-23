"""bao_tri/do_zorder.py - số liệu OPTIMIZE/ZORDER trên truy vấn lấy gợi ý theo user.

Truy vấn được đo chính là truy vấn nóng của web:
    SELECT * FROM gold.goi_y_nguoi_dung WHERE ma_nguoi_dung = ?

Quy trình (3 giai đoạn, cùng một tập user ngẫu nhiên):
  1. (tùy chọn) --phan-manh N: viết lại bảng thành N tệp nhỏ trộn ngẫu nhiên user, mô phỏng
     trạng thái sau nhiều ngày MERGE. Ghi bằng dataChange=false nên không đổi dữ liệu.
     Đo "truoc_toi_uu": khởi động 3 truy vấn, rồi đo --so-truy-van truy vấn.
  2. OPTIMIZE (chỉ gộp tệp, không sắp xếp), đo "sau_optimize".
  3. OPTIMIZE ... ZORDER BY (ma_nguoi_dung), đo "sau_zorder"; ghi 3 dòng vào gold.do_hieu_nang_zorder.

Vì sao cần giai đoạn 2: nếu bảng nhỏ, OPTIMIZE gộp hết thành 1 tệp và truy vấn nhanh lên chỉ vì
ít tệp, không nói được gì về ZORDER. Giới hạn cỡ tệp sau OPTIMIZE bằng --tep-toi-da-mb (mặc
định 1 MB) để bảng còn nhiều tệp: chỉ gộp thì mỗi tệp vẫn chứa user rải khắp dải giá trị, truy
vấn phải đọc gần hết các tệp; ZORDER xếp user liền nhau nên min/max của mỗi tệp hẹp lại và
Delta bỏ qua được các tệp không chứa user cần tìm (data skipping). So "sau_optimize" với
"sau_zorder" mới là tác dụng riêng của ZORDER.

Kèm số tệp Spark thực sự phải đọc cho mỗi truy vấn (lấy từ metric "number of files read"
của bước quét), vì thời gian trên cụm dao động còn số tệp thì không.
"""
import argparse
import statistics
import time
from datetime import datetime

from pyspark.sql import functions as F

from common import cau_hinh as ch
from common.luoc_do import tao_df
from common.spark_utils import doc_delta, ghi_delta, in_ket_qua, so_tep, sql_bang, tao_spark


def phan_vi(gia_tri, p):
    """Phân vị p (0..1) theo nội suy tuyến tính."""
    ds = sorted(gia_tri)
    if not ds:
        return None
    vt = (len(ds) - 1) * p
    duoi = int(vt)
    tren = min(duoi + 1, len(ds) - 1)
    return ds[duoi] + (ds[tren] - ds[duoi]) * (vt - duoi)


def so_tep_da_doc(df):
    """Số tệp đã đọc của lần thực thi vừa rồi, lấy từ metric của kế hoạch vật lý."""
    try:
        ke_hoach = df._jdf.queryExecution().executedPlan()
        tong = 0
        hang_doi = [ke_hoach]
        while hang_doi:
            nut = hang_doi.pop()
            metric = nut.metrics()
            if metric.contains("numFiles"):
                tong += int(metric.apply("numFiles").value())
            con = nut.children()
            for i in range(con.size()):
                hang_doi.append(con.apply(i))
            # Kế hoạch có AQE bọc bên ngoài: đi vào kế hoạch thật bên trong.
            if nut.nodeName() == "AdaptiveSparkPlan":
                hang_doi.append(nut.executedPlan())
        return tong
    except Exception:
        return None


def do_truy_van(spark, duong_dan, cac_user):
    """Trả về (danh sách ms, danh sách số tệp đã đọc)."""
    thoi_gian, so_tep_doc = [], []
    for u in cac_user:
        df = spark.read.format("delta").load(duong_dan).filter(F.col("ma_nguoi_dung") == u)
        bat_dau = time.perf_counter()
        df.collect()
        thoi_gian.append((time.perf_counter() - bat_dau) * 1000.0)
        so_tep_doc.append(so_tep_da_doc(df))
    return thoi_gian, so_tep_doc


def tong_hop(giai_doan, so_tep_bang, thoi_gian, so_tep_doc):
    """Một dòng của gold.do_hieu_nang_zorder (cộng số tệp đã đọc để in ra)."""
    doc_duoc = [x for x in so_tep_doc if x is not None]
    return {
        "thoi_diem": datetime.utcnow(),
        "giai_doan": giai_doan,
        "so_tep": so_tep_bang,
        "so_truy_van": len(thoi_gian),
        "trung_binh_ms": statistics.mean(thoi_gian),
        "trung_vi_ms": statistics.median(thoi_gian),
        "p95_ms": phan_vi(thoi_gian, 0.95),
        "tep_doc_trung_binh": float(statistics.mean(doc_duoc)) if doc_duoc else None,
    }


def chon_user_mau(goi_y, n, hat_giong=ch.HAT_GIONG):
    """Chọn n user ngẫu nhiên nhưng cố định theo hạt giống, NGAY TRÊN CỤM.

    Bản cũ collect toàn bộ ~180 nghìn mã user về driver rồi random.sample. Bản này băm mã user kèm
    hạt giống, sắp theo giá trị băm và lấy n dòng đầu: Spark chỉ gửi về driver đúng n mã cần đo
    (sort + limit được thực hiện thành takeOrdered trên từng phân vùng).
    """
    return [
        r["ma_nguoi_dung"]
        for r in goi_y.select("ma_nguoi_dung").distinct()
        .withColumn("_k", F.xxhash64("ma_nguoi_dung", F.lit(hat_giong)))
        .orderBy("_k", "ma_nguoi_dung").limit(n).collect()
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--so-truy-van", type=int, default=30)
    ap.add_argument("--phan-manh", type=int, default=0, help="Viết lại thành N tệp nhỏ trước khi đo")
    ap.add_argument("--tep-toi-da-mb", type=float, default=1.0, help="Cỡ tệp tối đa sau OPTIMIZE (MB)")
    args = ap.parse_args()

    spark = tao_spark("bd03-do-zorder")
    co_tep = str(int(args.tep_toi_da_mb * 1024 * 1024))
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", co_tep)
    # Tệp nhỏ hơn ngưỡng này mới bị gộp lại; đặt bằng cỡ tối đa để lần OPTIMIZE ZORDER sau
    # không gộp tiếp các tệp đã đủ cỡ thành một tệp lớn.
    spark.conf.set("spark.databricks.delta.optimize.minFileSize", co_tep)
    duong_dan = ch.duong_dan("gold.goi_y_nguoi_dung")

    if args.phan_manh > 0:
        (
            doc_delta(spark, "gold.goi_y_nguoi_dung")
            .repartition(args.phan_manh)
            .write.format("delta")
            .mode("overwrite")
            .option("dataChange", "false")
            .save(duong_dan)
        )

    cac_user = chon_user_mau(doc_delta(spark, "gold.goi_y_nguoi_dung"), args.so_truy_van)

    bang = sql_bang("gold.goi_y_nguoi_dung")

    def do_giai_doan(ten):
        do_truy_van(spark, duong_dan, cac_user[:3])  # khởi động JVM, bộ nhớ đệm nhật ký Delta
        dong = tong_hop(ten, so_tep(spark, "gold.goi_y_nguoi_dung"), *do_truy_van(spark, duong_dan, cac_user))
        print("So do ZORDER: " + str(dong), flush=True)  # in ngay, lỡ bước sau lỗi vẫn còn số đo
        return dong

    truoc = do_giai_doan("truoc_toi_uu")
    spark.sql("OPTIMIZE {}".format(bang))
    chi_gop = do_giai_doan("sau_optimize")
    spark.sql("OPTIMIZE {} ZORDER BY (ma_nguoi_dung)".format(bang))
    sau = do_giai_doan("sau_zorder")

    ghi_delta(tao_df(spark, "gold.do_hieu_nang_zorder", [truoc, chi_gop, sau]), "gold.do_hieu_nang_zorder", "append")
    trung_vi = lambda d: max(d["trung_vi_ms"], 1e-9)  # noqa: E731
    in_ket_qua({
        "truoc": truoc, "sau_optimize": chi_gop, "sau_zorder": sau,
        "nhanh_hon_lan_tong": truoc["trung_vi_ms"] / trung_vi(sau),
        "nhanh_hon_lan_rieng_zorder": chi_gop["trung_vi_ms"] / trung_vi(sau),
    })


if __name__ == "__main__":
    main()
