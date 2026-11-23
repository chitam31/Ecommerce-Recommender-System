"""Thư viện ALS dùng chung cho huấn luyện, đánh giá và tái lập.

Nguyên tắc tái lập: mọi thứ ảnh hưởng tới metric (dữ liệu, siêu tham số, maxIter,
hạt giống, cách tính metric) đều đi qua đúng các hàm ở đây. Job tái lập chỉ khác ở
chỗ đọc features bằng VERSION AS OF và không ghi kết quả vào bảng gold.
"""
import math
import time

from pyspark.ml.feature import StringIndexerModel
from pyspark.ml.recommendation import ALS
from pyspark.sql import Window
from pyspark.sql import functions as F

from . import cau_hinh as ch
from .chi_so import chi_so_mot_nguoi, cong_chi_so, trung_binh_chi_so
from .spark_utils import bang_ton_tai, doc_delta, doc_ghi_chu_commit, phien_ban_moi_nhat
from .tron import loc_theo_tam_gia, xep_lai_novelty

# ---------------------------------------------------------------------------
# 1. Đọc features theo phiên bản
# ---------------------------------------------------------------------------


def doc_features(spark, phien_ban=None):
    """Trả về (train, thuc_te, ghi_chu, phien_ban).

    train: user_idx, item_idx, diem (tập 4 tháng đầu, đã suy giảm theo thời gian)
    thuc_te: user_idx, thuc_te (mảng item của tháng cuối, chỉ user và item có trong train)
    """
    if phien_ban is None:
        phien_ban = phien_ban_moi_nhat(spark, "features.ma_tran_tuong_tac")
    df = doc_delta(spark, "features.ma_tran_tuong_tac", phien_ban)
    ghi_chu = doc_ghi_chu_commit(spark, "features.ma_tran_tuong_tac", phien_ban) or {}

    train = df.filter(F.col("tap") == "train").select("user_idx", "item_idx", "diem")
    test = df.filter(F.col("tap") == "test").select("user_idx", "item_idx")
    # ALS không thể xếp hạng user hoặc item chưa từng thấy: phần đó thuộc về cold start
    # và được đánh giá riêng, không trộn vào bảng metric xếp hạng.
    test = test.join(train.select("user_idx").distinct(), "user_idx").join(
        train.select("item_idx").distinct(), "item_idx"
    )
    thuc_te = test.groupBy("user_idx").agg(F.sort_array(F.collect_set("item_idx")).alias("thuc_te"))
    return train, thuc_te, ghi_chu, int(phien_ban)


def thong_ke_pho_bien(train):
    """Độ phổ biến của item trong train: số người dùng tương tác và tổng điểm."""
    return train.groupBy("item_idx").agg(
        F.countDistinct("user_idx").alias("so_nguoi"),
        F.sum("diem").alias("tong_diem"),
    )


# ---------------------------------------------------------------------------
# 2. Huấn luyện
# ---------------------------------------------------------------------------


def tao_als(tham_so, max_iter, hat_giong=ch.HAT_GIONG, so_khoi=None):
    """Khởi tạo ALS implicit với coldStartStrategy='drop' như đặc tả 3.6."""
    als = ALS(
        userCol="user_idx",
        itemCol="item_idx",
        ratingCol="diem",
        implicitPrefs=True,
        rank=int(tham_so["rank"]),
        regParam=float(tham_so["regParam"]),
        alpha=float(tham_so["alpha"]),
        maxIter=int(max_iter),
        seed=int(hat_giong),
        coldStartStrategy="drop",
    )
    if so_khoi:
        # Tăng numBlocks là một biện pháp chống OOM ở mục 3.10.
        als = als.setNumUserBlocks(int(so_khoi)).setNumItemBlocks(int(so_khoi))
    return als


# ---------------------------------------------------------------------------
# 3. Metric
# ---------------------------------------------------------------------------


def _danh_sach_tu_khuyen_nghi(df_khuyen_nghi):
    """Đổi cột recommendations (mảng struct) thành mảng item_idx theo thứ hạng."""
    return df_khuyen_nghi.select(
        "user_idx", F.col("recommendations.item_idx").alias("ds")
    )


def tinh_chi_so(ds_goi_y, thuc_te, pho_bien, so_item_catalog, so_nguoi_train, k=ch.K_DANH_GIA):
    """Tính 4 metric xếp hạng + coverage + novelty cho một bảng gợi ý (user_idx, ds).

    Người dùng có trong thuc_te mà không có gợi ý vẫn được tính với điểm 0.
    """
    du_lieu = thuc_te.join(ds_goi_y, "user_idx", "left").select(
        F.coalesce(F.col("ds"), F.array().cast("array<int>")).alias("ds"), "thuc_te"
    )
    tong = du_lieu.rdd.map(lambda r: chi_so_mot_nguoi(list(r["ds"]), list(r["thuc_te"]), k)).fold(
        (0.0, 0.0, 0.0, 0.0, 0), cong_chi_so
    )
    ket_qua = trung_binh_chi_so(tong, k)

    da_goi_y = ds_goi_y.select("user_idx", F.posexplode(F.slice("ds", 1, k)).alias("vi_tri", "item_idx"))
    so_item_khac_nhau = da_goi_y.select("item_idx").distinct().count()
    ket_qua["coverage"] = so_item_khac_nhau / float(max(so_item_catalog, 1))
    ket_qua["so_item_duoc_goi_y"] = so_item_khac_nhau

    # Novelty = trung bình -log2(tỷ lệ người dùng đã tương tác với item).
    nov = (
        da_goi_y.join(pho_bien.select("item_idx", "so_nguoi"), "item_idx", "left")
        .select((-F.log2(F.coalesce("so_nguoi", F.lit(1)) / F.lit(float(so_nguoi_train)))).alias("nov"))
        .agg(F.avg("nov"))
        .first()[0]
    )
    ket_qua["novelty"] = float(nov or 0.0)
    return ket_qua


def phan_phoi_pho_bien(ds_goi_y, pho_bien, k=ch.K_DANH_GIA, so_nhom=10):
    """Tỷ lệ suất gợi ý rơi vào từng nhóm 10% item theo độ phổ biến (nhóm 1 = phổ biến nhất).

    Nếu không có popularity bias, mỗi nhóm nhận khoảng 10%.
    """
    hang = pho_bien.withColumn(
        "nhom",
        F.ntile(so_nhom).over(Window.orderBy(F.desc("so_nguoi"), F.asc("item_idx"))),
    ).select("item_idx", "nhom")
    dem = (
        ds_goi_y.select(F.explode(F.slice("ds", 1, k)).alias("item_idx"))
        .join(hang, "item_idx")
        .groupBy("nhom")
        .count()
        .collect()
    )
    tong = float(sum(d["count"] for d in dem)) or 1.0
    theo_nhom = {d["nhom"]: d["count"] / tong for d in dem}
    return [round(theo_nhom.get(i, 0.0), 4) for i in range(1, so_nhom + 1)]


def danh_gia_als(mo_hinh, thuc_te, pho_bien, so_item_catalog, so_nguoi_train, k=ch.K_DANH_GIA):
    """Sinh top-k cho đúng các user trong tập test rồi tính metric."""
    ds = _danh_sach_tu_khuyen_nghi(mo_hinh.recommendForUserSubset(thuc_te.select("user_idx"), k)).cache()
    ket_qua = tinh_chi_so(ds, thuc_te, pho_bien, so_item_catalog, so_nguoi_train, k)
    return ket_qua, ds


# ---------------------------------------------------------------------------
# 4. Baseline bắt buộc
# ---------------------------------------------------------------------------


def baseline_toan_san(spark, thuc_te, pho_bien, k=ch.K_DANH_GIA):
    """Baseline 1: top-k bán chạy toàn sàn (theo tổng điểm tương tác trong train)."""
    top = [
        r["item_idx"]
        for r in pho_bien.orderBy(F.desc("tong_diem"), F.asc("item_idx")).limit(k).collect()
    ]
    return thuc_te.select("user_idx").withColumn("ds", F.array(*[F.lit(int(i)) for i in top]))


def bang_danh_muc_item(spark, ghi_chu):
    """Ánh xạ item_idx -> ma_danh_muc qua StringIndexer và silver.san_pham.

    Dùng đúng phiên bản silver.san_pham đã ghi trong features để baseline tái lập được.
    """
    if not bang_ton_tai(spark, "silver.san_pham"):
        return None
    nhan = StringIndexerModel.load(ch.duong_dan_mo_hinh("item_indexer")).labels
    anh_xa = spark.createDataFrame(
        [(i, str(v)) for i, v in enumerate(nhan)], "item_idx int, ma_san_pham string"
    )
    san_pham = doc_delta(spark, "silver.san_pham", ghi_chu.get("phien_ban_san_pham"))
    return anh_xa.join(san_pham.select("ma_san_pham", "ma_danh_muc"), "ma_san_pham").select(
        "item_idx", "ma_danh_muc"
    )


def baseline_danh_muc(spark, train, thuc_te, pho_bien, danh_muc_item, k=ch.K_DANH_GIA):
    """Baseline 2: top-k bán chạy trong danh mục user xem nhiều nhất; thiếu thì bù toàn sàn."""
    if danh_muc_item is None:
        return baseline_toan_san(spark, thuc_te, pho_bien, k)
    nguoi = thuc_te.select("user_idx")
    dm_nguoi = (
        train.join(nguoi, "user_idx")
        .join(danh_muc_item, "item_idx")
        .groupBy("user_idx", "ma_danh_muc")
        .agg(F.sum("diem").alias("d"))
        .withColumn(
            "h",
            F.row_number().over(
                Window.partitionBy("user_idx").orderBy(F.desc("d"), F.asc("ma_danh_muc"))
            ),
        )
        .filter(F.col("h") == 1)
        .select("user_idx", "ma_danh_muc")
    )
    top_dm = (
        pho_bien.join(danh_muc_item, "item_idx")
        .withColumn(
            "h",
            F.row_number().over(
                Window.partitionBy("ma_danh_muc").orderBy(F.desc("tong_diem"), F.asc("item_idx"))
            ),
        )
        .filter(F.col("h") <= k)
        .groupBy("ma_danh_muc")
        .agg(F.sort_array(F.collect_list(F.struct("h", "item_idx"))).alias("x"))
        .select("ma_danh_muc", F.col("x.item_idx").alias("ds_dm"))
    )
    top_toan_san = [
        r["item_idx"]
        for r in pho_bien.orderBy(F.desc("tong_diem"), F.asc("item_idx")).limit(k * 2).collect()
    ]
    bu = F.array(*[F.lit(int(i)) for i in top_toan_san])
    return (
        nguoi.join(dm_nguoi, "user_idx", "left")
        .join(top_dm, "ma_danh_muc", "left")
        .select(
            "user_idx",
            F.slice(
                F.array_distinct(F.concat(F.coalesce("ds_dm", F.array().cast("array<int>")), bu)),
                1,
                k,
            ).alias("ds"),
        )
    )


# ---------------------------------------------------------------------------
# 5. Giảm popularity bias bằng re-ranking theo novelty
# ---------------------------------------------------------------------------


def xep_lai_theo_novelty(mo_hinh, thuc_te, pho_bien, so_nguoi_train, beta, k=ch.K_DANH_GIA, so_ung_vien=50):
    """Lấy dư top-50 rồi xếp lại theo điểm ALS + beta x novelty."""
    nov = {
        r["item_idx"]: -math.log(max(r["so_nguoi"], 1) / float(so_nguoi_train), 2)
        for r in pho_bien.select("item_idx", "so_nguoi").collect()
    }
    nov_bc = thuc_te.sparkSession.sparkContext.broadcast(nov)
    ung_vien = mo_hinh.recommendForUserSubset(thuc_te.select("user_idx"), so_ung_vien)

    def xep(r):
        ds = [(x["item_idx"], float(x["rating"])) for x in r["recommendations"]]
        return (r["user_idx"], [int(i) for i in xep_lai_novelty(ds, nov_bc.value, beta, k)])

    return thuc_te.sparkSession.createDataFrame(ung_vien.rdd.map(xep), "user_idx int, ds array<int>")


# ---------------------------------------------------------------------------
# 5b. Mô hình v2: ràng buộc lọc theo tầm giá (sau schema evolution tuần 5)
# ---------------------------------------------------------------------------


def dung_mo_hinh_v2(spark, ghi_chu):
    """v2 bật khi phiên bản silver.tuong_tac dùng để dựng features đã có cột gia_san_pham.

    Quyết định dựa trên phiên bản silver ghi trong features nên tái lập cho cùng kết quả.
    """
    if not bang_ton_tai(spark, "silver.tuong_tac"):
        return False
    return "gia_san_pham" in doc_delta(spark, "silver.tuong_tac", ghi_chu.get("phien_ban_silver")).columns


def bang_gia_item(spark, ghi_chu):
    """item_idx -> gia, lấy từ đúng phiên bản silver.san_pham đã ghi trong features."""
    if not bang_ton_tai(spark, "silver.san_pham"):
        return None
    san_pham = doc_delta(spark, "silver.san_pham", ghi_chu.get("phien_ban_san_pham"))
    if "gia_san_pham" not in san_pham.columns:
        return None
    nhan = StringIndexerModel.load(ch.duong_dan_mo_hinh("item_indexer")).labels
    anh_xa = spark.createDataFrame(
        [(i, str(v)) for i, v in enumerate(nhan)], "item_idx int, ma_san_pham string"
    )
    return (
        anh_xa.join(san_pham.select("ma_san_pham", "gia_san_pham"), "ma_san_pham")
        .filter(F.col("gia_san_pham") > 0)
        .select("item_idx", F.col("gia_san_pham").alias("gia"))
    )


def tam_gia_nguoi_dung(tuong_tac, cot_user, cot_gia="gia"):
    """Tầm giá mỗi user: [(1 - BIEN) x P10 ; (1 + BIEN) x P90] giá các món đã tương tác.

    Dùng phân vị thay cho min/max để một lần lỡ tay xem món rất đắt không làm tầm giá
    rộng vô nghĩa. User có ít hơn SO_GIA_TOI_THIEU tương tác có giá thì không lọc.
    """
    return (
        tuong_tac.filter(F.col(cot_gia).isNotNull() & (F.col(cot_gia) > 0))
        .groupBy(cot_user)
        .agg(
            F.count(F.lit(1)).alias("so_gia"),
            F.percentile_approx(cot_gia, 0.1).alias("p10"),
            F.percentile_approx(cot_gia, 0.9).alias("p90"),
        )
        .filter(F.col("so_gia") >= ch.SO_GIA_TOI_THIEU)
        .select(
            cot_user,
            (F.col("p10") * (1.0 - ch.BIEN_TAM_GIA)).alias("gia_thap"),
            (F.col("p90") * (1.0 + ch.BIEN_TAM_GIA)).alias("gia_cao"),
        )
    )


def loc_tam_gia_tren_test(mo_hinh, train, thuc_te, gia_item, k=ch.K_DANH_GIA):
    """Đánh giá v2: lấy dư top-(k x HE_SO_UNG_VIEN_V2) rồi lọc theo tầm giá, cắt còn k.

    Đi qua đúng hàm common.tron.loc_theo_tam_gia mà job trộn gợi ý dùng khi phục vụ.
    """
    tam_gia = tam_gia_nguoi_dung(train.join(gia_item, "item_idx"), "user_idx")
    ung_vien = (
        mo_hinh.recommendForUserSubset(thuc_te.select("user_idx"), k * ch.HE_SO_UNG_VIEN_V2)
        .select("user_idx", F.posexplode("recommendations").alias("vt", "r"))
        .select("user_idx", "vt", F.col("r.item_idx").alias("item_idx"))
        .join(gia_item, "item_idx", "left")
        .groupBy("user_idx")
        .agg(F.sort_array(F.collect_list(F.struct("vt", "item_idx", "gia"))).alias("ds"))
        .join(tam_gia, "user_idx", "left")
    )

    def loc(r):
        ds = [{"ma_san_pham": x["item_idx"], "gia": x["gia"]} for x in r["ds"]]
        giu = loc_theo_tam_gia(ds, r["gia_thap"], r["gia_cao"], k)[:k]
        return (r["user_idx"], [int(g["ma_san_pham"]) for g in giu])

    return thuc_te.sparkSession.createDataFrame(ung_vien.rdd.map(loc), "user_idx int, ds array<int>")


# ---------------------------------------------------------------------------
# 6. Đánh giá đầy đủ một mô hình so với 2 baseline (dùng cho danh_gia và tái lập)
# ---------------------------------------------------------------------------


def danh_gia_day_du(spark, mo_hinh, train, thuc_te, ghi_chu, cac_beta=(0.1, 0.3), k=ch.K_DANH_GIA):
    """Trả về dict gồm metric ALS, 2 baseline, re-ranking và phân phối độ phổ biến."""
    pho_bien = thong_ke_pho_bien(train).cache()
    so_item = pho_bien.count()
    so_nguoi_train = train.select("user_idx").distinct().count()

    m_als, ds_als = danh_gia_als(mo_hinh, thuc_te, pho_bien, so_item, so_nguoi_train, k)
    ds_b1 = baseline_toan_san(spark, thuc_te, pho_bien, k).cache()
    m_b1 = tinh_chi_so(ds_b1, thuc_te, pho_bien, so_item, so_nguoi_train, k)
    dm_item = bang_danh_muc_item(spark, ghi_chu)
    ds_b2 = baseline_danh_muc(spark, train, thuc_te, pho_bien, dm_item, k).cache()
    m_b2 = tinh_chi_so(ds_b2, thuc_te, pho_bien, so_item, so_nguoi_train, k)

    xep_lai = {}
    phan_phoi = {
        "als": phan_phoi_pho_bien(ds_als, pho_bien, k),
        "baseline_toan_san": phan_phoi_pho_bien(ds_b1, pho_bien, k),
        "baseline_danh_muc": phan_phoi_pho_bien(ds_b2, pho_bien, k),
    }
    for beta in cac_beta:
        ds_r = xep_lai_theo_novelty(mo_hinh, thuc_te, pho_bien, so_nguoi_train, beta, k).cache()
        xep_lai[str(beta)] = tinh_chi_so(ds_r, thuc_te, pho_bien, so_item, so_nguoi_train, k)
        phan_phoi["xep_lai_beta_{}".format(beta)] = phan_phoi_pho_bien(ds_r, pho_bien, k)

    # Đúng cấu hình xếp lại mà sinh_goi_y_toan_bo.py dùng để phục vụ (MO_HINH.md mục 6).
    phuc_vu = None
    if ch.BETA_XEP_LAI > 0:
        ds_p = xep_lai_theo_novelty(
            mo_hinh, thuc_te, pho_bien, so_nguoi_train, ch.BETA_XEP_LAI, k, ch.SO_UNG_VIEN_XEP_LAI
        ).cache()
        phuc_vu = tinh_chi_so(ds_p, thuc_te, pho_bien, so_item, so_nguoi_train, k)
        phuc_vu.update({"beta": ch.BETA_XEP_LAI, "so_ung_vien": ch.SO_UNG_VIEN_XEP_LAI})
        phan_phoi["phuc_vu"] = phan_phoi_pho_bien(ds_p, pho_bien, k)

    # Tổng toàn catalog để tính coverage trên toàn sàn (235.000 sản phẩm ở dữ liệu thật).
    so_item_toan_san = so_item
    if bang_ton_tai(spark, "silver.san_pham"):
        so_item_toan_san = max(
            doc_delta(spark, "silver.san_pham", ghi_chu.get("phien_ban_san_pham")).count(), so_item
        )
    m_als["coverage_toan_san"] = m_als["so_item_duoc_goi_y"] / float(so_item_toan_san)

    # Mô hình v2: cùng mô hình ALS, thêm ràng buộc lọc theo tầm giá ở bước sinh danh sách.
    m_v2 = None
    gia_item = bang_gia_item(spark, ghi_chu) if dung_mo_hinh_v2(spark, ghi_chu) else None
    if gia_item is not None:
        ds_v2 = loc_tam_gia_tren_test(mo_hinh, train, thuc_te, gia_item.cache(), k).cache()
        m_v2 = tinh_chi_so(ds_v2, thuc_te, pho_bien, so_item, so_nguoi_train, k)
        phan_phoi["loc_tam_gia_v2"] = phan_phoi_pho_bien(ds_v2, pho_bien, k)

    return {
        "als": m_als,
        "baseline_toan_san": m_b1,
        "baseline_danh_muc": m_b2,
        "loc_tam_gia_v2": m_v2,
        "xep_lai_novelty": xep_lai,
        "phuc_vu": phuc_vu,
        "phan_phoi_pho_bien": phan_phoi,
        "so_item_catalog_als": so_item,
        "so_item_toan_san": so_item_toan_san,
        "so_nguoi_train": so_nguoi_train,
        "so_nguoi_test": m_als["so_nguoi_danh_gia"],
    }


def chon_tot_nhat(cac_dong):
    """NDCG@10 cao nhất; hòa thì ưu tiên rank nhỏ rồi regParam lớn (mô hình đơn giản hơn)."""
    return sorted(cac_dong, key=lambda d: (-d["ndcg_at_10"], d["rank"], -d["reg_param"], d["alpha"]))[0]


def ty_le_tang(moi, nen):
    """(moi - nen) / nen; trả về None nếu nền bằng 0."""
    if not nen:
        return None
    return (moi - nen) / nen


def mo_hinh_dang_trien_khai(spark):
    """Dòng lịch sử của mô hình đang phục vụ (trang_thai = trien_khai) gần nhất, hoặc None."""
    if not bang_ton_tai(spark, "gold.lich_su_huan_luyen"):
        return None
    dong = (
        doc_delta(spark, "gold.lich_su_huan_luyen")
        .filter(F.col("trang_thai") == "trien_khai")
        .orderBy(F.desc("thoi_diem"))
        .first()
    )
    return dong.asDict() if dong else None


def them_to_hop_dang_phuc_vu(cac_to_hop, dong_cu):
    """Thêm siêu tham số của mô hình đang phục vụ vào CUỐI lưới nếu lưới chưa có.

    Lưới tối thiểu của DAG hằng ngày cố định alpha = 10, còn mô hình đang phục vụ được chọn từ
    lưới đầy đủ (alpha = 40). Nếu không đánh giá lại đúng tổ hợp đó, lần chạy hằng ngày trên
    cùng dữ liệu không bao giờ bằng mô hình cũ và luôn rẽ sang giu_mo_hinh_cu, kể cả khi dữ liệu
    hay lược đồ (mô hình v2) đã đổi. Thêm vào cuối để thứ tự các tổ hợp cũ không đổi (tái lập).
    Trả về (danh_sach_moi, to_hop_da_them hoặc None).
    """
    ds = list(cac_to_hop)
    if not dong_cu or dong_cu.get("rank") is None:
        return ds, None
    to_hop = {
        "rank": int(dong_cu["rank"]),
        "regParam": float(dong_cu["reg_param"]),
        "alpha": float(dong_cu["alpha"]),
    }
    khoa = lambda t: (int(t["rank"]), float(t["regParam"]), float(t["alpha"]))  # noqa: E731
    if any(khoa(t) == khoa(to_hop) for t in ds):
        return ds, None
    return ds + [to_hop], to_hop


# ---------------------------------------------------------------------------
# 7. Vòng tuning dùng chung cho huấn luyện và tái lập
# ---------------------------------------------------------------------------


def chay_tuning(spark, phien_ban, cac_to_hop, max_iter, hat_giong, so_khoi=None, luu_mo_hinh_vao=None):
    """Huấn luyện từng tổ hợp và đánh giá trên tập test của đúng phiên bản features.

    Trả về (danh sách dòng kết quả, phiên bản features thực sự đã đọc).
    """
    train, thuc_te, _, phien_ban = doc_features(spark, phien_ban)
    train = train.cache()
    thuc_te = thuc_te.cache()
    pho_bien = thong_ke_pho_bien(train).cache()
    so_item = pho_bien.count()
    so_nguoi_train = train.select("user_idx").distinct().count()

    ket_qua = []
    tot_nhat = None
    mo_hinh_tot = None
    for tham_so in cac_to_hop:
        bat_dau = time.time()
        mo_hinh = tao_als(tham_so, max_iter, hat_giong, so_khoi).fit(train)
        m, ds = danh_gia_als(mo_hinh, thuc_te, pho_bien, so_item, so_nguoi_train)
        ds.unpersist()
        dong = {
            "rank": int(tham_so["rank"]),
            "reg_param": float(tham_so["regParam"]),
            "alpha": float(tham_so["alpha"]),
            "precision_at_10": m["precision_at_10"],
            "recall_at_10": m["recall_at_10"],
            "ndcg_at_10": m["ndcg_at_10"],
            "map_at_10": m["map_at_10"],
            "coverage": m["coverage"],
            "coverage_toan_san": None,
            "novelty": m["novelty"],
            "so_giay": round(time.time() - bat_dau, 1),
        }
        ket_qua.append(dong)
        print(
            "[tuning] rank={rank} reg={reg_param} alpha={alpha} ndcg@10={ndcg_at_10:.5f} "
            "coverage={coverage:.4f} ({so_giay}s)".format(**dong),
            flush=True,
        )
        if luu_mo_hinh_vao and (tot_nhat is None or chon_tot_nhat([tot_nhat, dong]) is dong):
            tot_nhat, mo_hinh_tot = dong, mo_hinh
    if luu_mo_hinh_vao and mo_hinh_tot is not None:
        mo_hinh_tot.write().overwrite().save(luu_mo_hinh_vao)
    return ket_qua, phien_ban


# ---------------------------------------------------------------------------
# 8. Kỳ validation riêng (bổ sung 11/11/2026)
# ---------------------------------------------------------------------------
# Kết quả chính chọn siêu tham số trên chính tháng test (MO_HINH mục 13). Các hàm dưới đây
# tách tháng đó theo thời gian: nửa đầu để CHỌN, nửa sau chỉ để BÁO CÁO. Không đổi gì ở
# features, huấn luyện hay phục vụ: train vẫn là 4 tháng đầu của đúng phiên bản features.


def chi_so_xep_hang(ds_goi_y, thuc_te, k=ch.K_DANH_GIA):
    """Chỉ 4 metric xếp hạng (không coverage, novelty): phần đầu của tinh_chi_so, cùng công thức."""
    du_lieu = thuc_te.join(ds_goi_y, "user_idx", "left").select(
        F.coalesce(F.col("ds"), F.array().cast("array<int>")).alias("ds"), "thuc_te"
    )
    tong = du_lieu.rdd.map(lambda r: chi_so_mot_nguoi(list(r["ds"]), list(r["thuc_te"]), k)).fold(
        (0.0, 0.0, 0.0, 0.0, 0), cong_chi_so
    )
    return trung_binh_chi_so(tong, k)


def chia_ky_validation(su_kien, ngay_cat, so_ngay_val):
    """Tách sự kiện của tháng test theo ngày: [ngay_cat, ngay_cat + so_ngay_val) là validation,
    phần còn lại là kiểm tra. Trả về (validation, kiem_tra)."""
    moc = F.date_add(F.lit(ngay_cat).cast("date"), int(so_ngay_val))
    return su_kien.filter(F.col("ngay") < moc), su_kien.filter(F.col("ngay") >= moc)


def thuc_te_tu_su_kien(su_kien_idx, train):
    """(user_idx, item_idx) của một kỳ -> (user_idx, thuc_te) như doc_features: chỉ giữ user và
    item có trong train, vì ALS không thể xếp hạng phần còn lại (thuộc về cold start)."""
    return (
        su_kien_idx.select("user_idx", "item_idx")
        .join(train.select("user_idx").distinct(), "user_idx")
        .join(train.select("item_idx").distinct(), "item_idx")
        .groupBy("user_idx")
        .agg(F.sort_array(F.collect_set("item_idx")).alias("thuc_te"))
    )


def danh_chi_so_bang_indexer_da_luu(su_kien):
    """Gán user_idx/item_idx bằng hai StringIndexer đã lưu, CHỈ ĐỌC (không nối nhãn, không ghi).

    Indexer chỉ nối thêm nhãn nên chỉ số trùng với mọi phiên bản features; nhãn lạ bị bỏ qua.
    """
    idx_user = StringIndexerModel.load(ch.duong_dan_mo_hinh("user_indexer")).setHandleInvalid("skip")
    idx_item = StringIndexerModel.load(ch.duong_dan_mo_hinh("item_indexer")).setHandleInvalid("skip")
    df = su_kien.select(
        F.col("ma_nguoi_dung").cast("string").alias("ma_nguoi_dung"),
        F.col("ma_san_pham").cast("string").alias("ma_san_pham"),
    )
    return (
        idx_item.transform(idx_user.transform(df))
        .select(F.col("user_idx").cast("int").alias("user_idx"), F.col("item_idx").cast("int").alias("item_idx"))
    )


def gop_nhan_tren_driver(spark, cac_bang):
    """cac_bang: {ten_cot: DataFrame (user_idx, thuc_te)}. Thu về driver rồi tạo lại MỘT bảng nhỏ
    (user_idx, <ten_cot> array<int> ...; null nếu user không có mặt ở kỳ đó) KHÔNG còn lineage.

    Lý do (lần chạy 11/11): ground truth dựng từ silver rất nhỏ (khoảng 9.500 user) nhưng có lineage
    dài (đọc silver từ HDFS, StringIndexer, join). Khi ALS chiếm gần hết bộ nhớ executor, bản cache bị
    đẩy ra và Spark tính lại từ HDFS ở mỗi lần đo, làm mỗi tổ hợp chậm 5-6 lần.
    """
    gop = {}
    for ten, df in cac_bang.items():
        for r in df.collect():
            gop.setdefault(int(r["user_idx"]), {})[ten] = [int(i) for i in r["thuc_te"]]
    ten_cot = list(cac_bang)
    luoc_do = "user_idx int, " + ", ".join("{} array<int>".format(t) for t in ten_cot)
    dong = [tuple([u] + [v.get(t) for t in ten_cot]) for u, v in sorted(gop.items())]
    return spark.createDataFrame(dong, luoc_do)


def thuc_te_cua(nhan, ten_cot):
    """Lấy một bộ ground truth (user_idx, thuc_te) từ bảng gop_nhan_tren_driver."""
    return nhan.filter(F.col(ten_cot).isNotNull()).select("user_idx", F.col(ten_cot).alias("thuc_te"))


def chi_so_nhieu_ky(ds_goi_y, nhan, cac_cot, k=ch.K_DANH_GIA):
    """4 metric xếp hạng cho nhiều bộ ground truth trong MỘT lượt duyệt.

    nhan: bảng của gop_nhan_tren_driver. User không có mặt ở một kỳ (null) không được tính cho kỳ
    đó; user có mặt mà không có gợi ý được tính với điểm 0, như tinh_chi_so.
    """
    cot = list(cac_cot)
    rong = tuple((0.0, 0.0, 0.0, 0.0, 0) for _ in cot)

    def mot(r):
        ds = list(r["ds"]) if r["ds"] is not None else []
        return tuple(
            chi_so_mot_nguoi(ds, list(r[c]), k) if r[c] is not None else (0.0, 0.0, 0.0, 0.0, 0) for c in cot
        )

    def cong(a, b):
        return tuple(cong_chi_so(x, y) for x, y in zip(a, b))

    tong = nhan.join(ds_goi_y, "user_idx", "left").rdd.map(mot).fold(rong, cong)
    return {c: trung_binh_chi_so(t, k) for c, t in zip(cot, tong)}
