"""Phần học máy: ma trận đặc trưng, StringIndexer nối nhãn, metric, baseline, tính tái lập của ALS."""
from datetime import date, datetime, timedelta

import pytest

from common import cau_hinh as ch
from common.als_lib import (
    baseline_danh_muc,
    baseline_toan_san,
    chon_tot_nhat,
    danh_gia_als,
    loc_tam_gia_tren_test,
    phan_phoi_pho_bien,
    tam_gia_nguoi_dung,
    tao_als,
    thong_ke_pho_bien,
    them_to_hop_dang_phuc_vu,
    tinh_chi_so,
    ty_le_tang,
)
from ml.chuan_bi_ma_tran import dung_ma_tran, tinh_diem
from ml.ghi_gold import dung_nguon_merge
from ml.san_pham_lien_quan import tinh_lien_quan
from ml.sinh_goi_y_toan_bo import gop_toan_bo

SILVER = "ma_nguoi_dung string, ma_san_pham string, thoi_diem timestamp, loai_su_kien string, trong_so double, ngay date, _nguon string"


@pytest.fixture
def lake_tam(tmp_path, monkeypatch):
    monkeypatch.setattr(ch, "LAKE_GOC", "file://" + str(tmp_path / "lake"))
    return tmp_path


def silver_mau(spark, cac_dong):
    return spark.createDataFrame(
        [(u, sp, datetime(n.year, n.month, n.day, 12), "xem", w, n, nguon) for u, sp, n, w, nguon in cac_dong], SILVER
    )


def test_suy_giam_thoi_gian(spark):
    df = spark.createDataFrame([(0, 0, 1.0, date(2015, 6, 1)), (0, 0, 5.0, date(2015, 6, 11))],
                               "user_idx int, item_idx int, trong_so double, ngay date")
    kq = tinh_diem(df, date(2015, 6, 11)).first()["diem"]
    import math
    assert kq == pytest.approx(1.0 * math.exp(-0.01 * 10) + 5.0)


def test_dung_ma_tran_va_indexer_noi_nhan(spark, lake_tam):
    d0 = date(2015, 5, 1)
    dong = []
    for i in range(3):  # u1 đủ 3 tương tác trong train, u2 chỉ 1 -> bị lọc
        dong.append(("u1", "sp{}".format(i), d0 + timedelta(days=i), 1.0, "phat_lai"))
    dong.append(("u2", "sp1", d0, 1.0, "phat_lai"))
    dong.append(("u1", "sp9", d0 + timedelta(days=60), 3.0, "phat_lai"))  # tháng cuối -> test
    dong.append(("w", "sp1", date(2026, 11, 10), 1.0, "web"))  # sự kiện web không vào chia train/test
    f1, tt1 = dung_ma_tran(spark, silver_mau(spark, dong), min_tuong_tac=3)
    assert tt1["ngay_cuoi"] == str(d0 + timedelta(days=60))
    assert tt1["ngay_cat"] == str(d0 + timedelta(days=30))
    dem = {r["tap"]: r["count"] for r in f1.groupBy("tap").count().collect()}
    assert dem == {"train": 3, "test": 1}
    assert f1.select("user_idx").distinct().count() == 1  # chỉ còn u1
    anh_xa_cu = {(r["item_idx"]) for r in f1.collect()}

    # Lần chạy sau có thêm sản phẩm mới: chỉ số cũ giữ nguyên, nhãn mới nối vào cuối.
    dong2 = dong + [("u1", "aaa_moi", d0 + timedelta(days=59), 1.0, "phat_lai")]
    f2, tt2 = dung_ma_tran(spark, silver_mau(spark, dong2), min_tuong_tac=3)
    assert tt2["so_item_nhan_moi"] == 1 and tt2["so_item_nhan_cu"] == tt1["so_item_nhan_cu"] + tt1["so_item_nhan_moi"]
    from pyspark.ml.feature import StringIndexerModel
    nhan = StringIndexerModel.load(ch.duong_dan_mo_hinh("item_indexer")).labels
    assert nhan[-1] == "aaa_moi"  # dù "aaa" đứng đầu bảng chữ cái, vẫn nằm cuối danh sách nhãn
    assert anh_xa_cu <= {r["item_idx"] for r in f2.collect()}


def test_gop_toan_bo_dung_cong_thuc(spark):
    f = spark.createDataFrame([(0, 0, 2.0, "train"), (0, 0, 1.0, "test")], "user_idx int, item_idx int, diem double, tap string")
    import math
    kq = gop_toan_bo(f, date(2015, 8, 1), date(2015, 8, 31)).first()["diem"]
    assert kq == pytest.approx(2.0 * math.exp(-0.01 * 30) + 1.0)


def du_lieu_als(spark):
    """Bốn cụm sở thích: user u thích 10 item của cụm u % 4. Train thấy 4 item, test là 2 item khác."""
    import random

    rnd = random.Random(1)
    train, test = [], {}
    for u in range(120):
        nhom = list(range((u % 4) * 10, (u % 4) * 10 + 10))
        cac_item = rnd.sample(nhom, 6)
        for i in cac_item[:4]:
            train.append((u, i, 1.0 + rnd.random()))
        train.append((u, 40 + u % 5, 1.0))  # chút nhiễu phổ biến
        test[u] = sorted(cac_item[4:])
    tr = spark.createDataFrame(train, "user_idx int, item_idx int, diem double")
    tt = spark.createDataFrame([(u, v) for u, v in test.items()], "user_idx int, thuc_te array<int>")
    return tr, tt


def test_als_vuot_baseline_va_tai_lap_duoc(spark):
    train, thuc_te = du_lieu_als(spark)
    pho_bien = thong_ke_pho_bien(train).cache()
    so_item, so_nguoi = pho_bien.count(), train.select("user_idx").distinct().count()
    tham_so = {"rank": 10, "regParam": 0.1, "alpha": 10.0}
    m1, _ = danh_gia_als(tao_als(tham_so, 10).fit(train), thuc_te, pho_bien, so_item, so_nguoi)
    m2, _ = danh_gia_als(tao_als(tham_so, 10).fit(train), thuc_te, pho_bien, so_item, so_nguoi)
    # Cùng dữ liệu, cùng hạt giống -> metric giống hệt: nền tảng của "tái lập thí nghiệm".
    for k in ("precision_at_10", "recall_at_10", "ndcg_at_10", "map_at_10", "coverage", "novelty"):
        assert m1[k] == m2[k], k
    b1 = tinh_chi_so(baseline_toan_san(spark, thuc_te, pho_bien), thuc_te, pho_bien, so_item, so_nguoi)
    assert m1["ndcg_at_10"] > b1["ndcg_at_10"]
    assert m1["coverage"] > b1["coverage"]
    assert 0 < m1["coverage"] <= 1 and m1["novelty"] > 0


def test_baseline_danh_muc_va_phan_phoi(spark):
    train, thuc_te = du_lieu_als(spark)
    pho_bien = thong_ke_pho_bien(train)
    dm = spark.createDataFrame([(i, "dm{}".format(i // 10)) for i in range(45)], "item_idx int, ma_danh_muc string")
    ds = {r["user_idx"]: r["ds"] for r in baseline_danh_muc(spark, train, thuc_te, pho_bien, dm).collect()}
    assert all(i < 10 for i in ds[0][:10]) and all(10 <= i < 20 for i in ds[1][:10])  # đúng danh mục của user
    pp = phan_phoi_pho_bien(baseline_toan_san(spark, thuc_te, pho_bien), pho_bien)
    assert len(pp) == 10 and sum(pp) == pytest.approx(1.0, abs=1e-3)


def test_tinh_chi_so_khop_ham_python(spark):
    from common.chi_so import ndcg_at_k

    tt = spark.createDataFrame([(1, [3, 4]), (2, [9])], "user_idx int, thuc_te array<int>")
    ds = spark.createDataFrame([(1, [4, 1, 3]), (2, [1, 2])], "user_idx int, ds array<int>")
    pb = spark.createDataFrame([(i, i, float(i)) for i in range(1, 10)], "item_idx int, so_nguoi long, tong_diem double")
    kq = tinh_chi_so(ds, tt, pb, 9, 10)
    assert kq["ndcg_at_10"] == pytest.approx((ndcg_at_k([4, 1, 3], [3, 4], 10) + 0) / 2)
    assert kq["coverage"] == pytest.approx(4 / 9)


def test_chon_tot_nhat_va_ty_le_tang():
    dong = [{"ndcg_at_10": 0.1, "rank": 50, "reg_param": 0.1, "alpha": 1.0},
            {"ndcg_at_10": 0.1, "rank": 10, "reg_param": 0.1, "alpha": 1.0},
            {"ndcg_at_10": 0.05, "rank": 10, "reg_param": 1.0, "alpha": 1.0}]
    assert chon_tot_nhat(dong)["rank"] == 10
    assert ty_le_tang(0.13, 0.10) == pytest.approx(0.30)
    assert ty_le_tang(0.1, 0) is None


def test_them_to_hop_dang_phuc_vu():
    """Lưới tối thiểu (alpha = 10) phải có thêm tổ hợp của mô hình đang phục vụ (alpha = 40) ở cuối,
    nếu không lần chạy hằng ngày không bao giờ bằng được mô hình cũ trên cùng dữ liệu."""
    from ml.huan_luyen_als import to_hop_tham_so

    luoi = to_hop_tham_so("toi_thieu")
    dang_phuc_vu = {"rank": 50, "reg_param": 1.0, "alpha": 40.0, "ndcg_at_10": 0.068}
    moi, da_them = them_to_hop_dang_phuc_vu(luoi, dang_phuc_vu)
    assert len(moi) == 10 and moi[:9] == luoi
    assert moi[-1] == {"rank": 50, "regParam": 1.0, "alpha": 40.0} and da_them == moi[-1]
    # đã có trong lưới (lưới đầy đủ) thì giữ nguyên
    du = to_hop_tham_so("day_du")
    assert them_to_hop_dang_phuc_vu(du, dang_phuc_vu) == (du, None)
    # chưa có mô hình nào phục vụ
    assert them_to_hop_dang_phuc_vu(luoi, None) == (luoi, None)


def test_ghi_gold_danh_dau_hang_thua(spark):
    cot = "ma_nguoi_dung string, xep_hang int, ma_san_pham string, diem_du_doan double, nguon string, phien_ban_mo_hinh string, sinh_luc timestamp"
    bay_gio = datetime(2026, 11, 10)
    moi = spark.createDataFrame([("u", 1, "a", 1.0, "als", "v2", bay_gio), ("u", 2, "b", 0.9, "als", "v2", bay_gio)], cot)
    cu = spark.createDataFrame([("u", r, "x", 0.1, "als", "v1", bay_gio) for r in (1, 2, 3, 4)] +
                               [("khac", 1, "y", 0.1, "als", "v1", bay_gio)], cot)
    nguon = {(r["ma_nguoi_dung"], r["xep_hang"]): r["_xoa"] for r in dung_nguon_merge(moi, cu).collect()}
    assert nguon == {("u", 1): False, ("u", 2): False, ("u", 3): True, ("u", 4): True}  # "khac" không bị đụng tới


def test_san_pham_lien_quan(spark):
    d = date(2015, 6, 1)
    dong = [("u{}".format(i), sp, d, 1.0, "phat_lai") for i in range(5) for sp in ("a", "b")]
    dong += [("u9", "a", d, 1.0, "phat_lai"), ("u9", "c", d, 1.0, "phat_lai"), ("u8", "c", d, 1.0, "phat_lai")]
    kq = {(r["ma_san_pham"], r["ma_san_pham_lien_quan"]): r["do_lien_quan"] for r in tinh_lien_quan(silver_mau(spark, dong)).collect()}
    import math
    assert kq[("a", "b")] == pytest.approx(5 / math.sqrt(6 * 5))
    assert ("a", "c") not in kq  # chỉ 1 người cùng xem -> dưới ngưỡng 2 người


def test_tam_gia_nguoi_dung(spark):
    df = spark.createDataFrame(
        [("u1", float(g)) for g in range(100, 1100, 100)] + [("u2", 500.0), ("u2", 600.0), ("u3", None)],
        "ma_nguoi_dung string, gia double",
    )
    kq = {r["ma_nguoi_dung"]: r for r in tam_gia_nguoi_dung(df, "ma_nguoi_dung").collect()}
    assert set(kq) == {"u1"}  # u2 chỉ 2 món có giá, u3 không có giá -> không lọc
    assert kq["u1"]["gia_thap"] == pytest.approx(100 * (1 - ch.BIEN_TAM_GIA))
    assert kq["u1"]["gia_cao"] == pytest.approx(900 * (1 + ch.BIEN_TAM_GIA))


def test_loc_tam_gia_tren_test_chi_giu_mon_trong_tam(spark):
    train, thuc_te = du_lieu_als(spark)
    mo_hinh = tao_als({"rank": 10, "regParam": 0.1, "alpha": 10.0}, 10).fit(train)
    # Item chẵn rẻ (100), item lẻ đắt (10000): user nào cũng có tầm giá rộng vì đã xem cả hai loại,
    # nên ép thêm item lẻ đắt tới mức ngoài tầm giá để kiểm tra phép lọc.
    gia = spark.createDataFrame([(i, 100.0 if i % 2 == 0 else 1e9) for i in range(45)], "item_idx int, gia double")
    train_re = train.join(gia.filter("gia < 1000"), "item_idx").select("user_idx", "item_idx", "diem")
    ds = {r["user_idx"]: r["ds"] for r in loc_tam_gia_tren_test(mo_hinh, train_re, thuc_te, gia, 10).collect()}
    assert ds and all(len(v) <= 10 for v in ds.values())
    co_tam_gia = [v for v in ds.values() if len(v) == 10 and all(i % 2 == 0 for i in v)]
    assert co_tam_gia, "phải có user được lọc chỉ còn item trong tầm giá"


def test_phan_tich_loi_theo_nhom(spark):
    """kiem_tra/phan_tich_loi: NDCG từng người, gom theo số tương tác / độ phổ biến / số danh mục."""
    from kiem_tra.phan_tich_loi import dac_trung_nguoi, ndcg_tung_nguoi, phan_tich

    thuc_te = spark.createDataFrame([(1, [10]), (2, [20]), (3, [30, 31])], "user_idx int, thuc_te array<int>")
    ds = {
        "als": spark.createDataFrame([(1, [10, 11]), (2, [99]), (3, [31])], "user_idx int, ds array<int>"),
        "b1": spark.createDataFrame([(u, [10]) for u in (1, 2, 3)], "user_idx int, ds array<int>"),
        "b2": spark.createDataFrame([(1, [11]), (2, [20]), (3, [30])], "user_idx int, ds array<int>"),
    }
    train = spark.createDataFrame([(1, 10, 1.0)] * 3 + [(2, 20, 1.0)] * 12 + [(3, 30, 1.0)] * 60,
                                  "user_idx int, item_idx int, diem double")
    pho_bien = spark.createDataFrame([(10, 50), (20, 1), (30, 1), (31, 1), (11, 2)], "item_idx int, so_nguoi long")
    dm = spark.createDataFrame([(10, "a"), (20, "b"), (30, "c")], "item_idx int, ma_danh_muc string")
    df = ndcg_tung_nguoi(thuc_te, ds, 10).join(dac_trung_nguoi(train, thuc_te, pho_bien, dm), "user_idx")
    kq = phan_tich(df)
    assert kq["tong"]["so_nguoi"] == 3
    assert kq["tong"]["ty_le_als_thua_b2"] == pytest.approx(1 / 3, abs=1e-4)   # user 2: ALS 0, B2 1
    assert [d["nhom_tuong_tac"] for d in kq["theo_so_tuong_tac"]] == ["3-4", "10-19", ">=50"]
    assert kq["theo_so_tuong_tac"][0]["ndcg_als"] == 1.0 and kq["theo_so_tuong_tac"][1]["ndcg_als"] == 0.0
    assert {d["nhom_pho_bien_test"] for d in kq["theo_pho_bien_test"]} == {"dau (>=50% SP test pho bien)",
                                                                           "duoi dai (0% SP test pho bien)"}
