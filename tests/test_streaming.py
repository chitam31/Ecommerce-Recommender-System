"""Job streaming: chuẩn hóa sự kiện, cửa sổ thịnh hành, gộp impression - chạy thật bằng
Structured Streaming với nguồn tệp (thay Kafka) để kiểm tra đúng ngữ nghĩa luồng."""
import json
import os
from datetime import datetime, timedelta

import pytest

from streaming.ghi_hien_thi import gop_theo_hien_thi
from streaming.nap_su_kien import chuan_hoa_su_kien
from streaming.tinh_thinh_hanh import gom_cua_so, lay_su_kien, tinh_diem

TS = 1433152800000  # 2015-06-01 10:00 UTC


def dong_kafka(spark, cac_gia_tri, ts=None):
    ts = ts or datetime(2026, 11, 10, 10, 0, 0)
    return spark.createDataFrame(
        [(json.dumps(v) if isinstance(v, dict) else v, "retailrocket-events", 0, i, ts) for i, v in enumerate(cac_gia_tri)],
        "value string, kafka_topic string, kafka_partition int, kafka_offset long, kafka_ts timestamp",
    )


def test_chuan_hoa_su_kien(spark):
    tho = dong_kafka(
        spark,
        [
            {"timestamp": TS, "visitorid": 257597, "event": "view", "itemid": 355908, "transactionid": None},
            {"timestamp": TS + 1000, "visitorid": "moi_7", "event": "addtocart", "itemid": "355908", "nguon": "nguoi_moi"},
            {"timestamp": TS + 2000, "visitorid": 1, "event": "transaction", "itemid": 9, "transactionid": 42},
            "khong phai json",
            {"timestamp": TS, "visitorid": None, "event": "view", "itemid": 1},
        ],
    )
    kq = {r["ma_nguoi_dung"]: r for r in chuan_hoa_su_kien(tho).collect()}
    assert set(kq) == {"257597", "moi_7", "1"}  # dòng hỏng và thiếu user bị loại
    assert kq["257597"]["loai_su_kien"] == "xem"
    assert kq["moi_7"]["loai_su_kien"] == "them_gio" and kq["moi_7"]["_nguon"] == "nguoi_moi"
    assert kq["1"]["loai_su_kien"] == "mua" and kq["1"]["ma_giao_dich"] == "42"
    assert kq["257597"]["thoi_diem"] == datetime(2015, 6, 1, 10, 0, 0)
    assert str(kq["257597"]["ngay"]) == "2015-06-01"
    assert kq["257597"]["_nguon"] == "cu"  # thông điệp của bộ phát cũ không có trường nguon
    # mã phiên: cùng user trong cùng khung 30 phút -> cùng phiên
    hai = chuan_hoa_su_kien(dong_kafka(spark, [
        {"timestamp": TS, "visitorid": 5, "event": "view", "itemid": 1},
        {"timestamp": TS + 60000, "visitorid": 5, "event": "view", "itemid": 2}])).collect()
    assert hai[0]["ma_phien"] == hai[1]["ma_phien"]


def test_bo_sung_danh_muc(spark):
    tho = dong_kafka(spark, [{"timestamp": TS, "visitorid": 1, "event": "view", "itemid": 9}])
    dm = spark.createDataFrame([("9", "1051")], "ma_san_pham string, ma_danh_muc_sp string")
    assert chuan_hoa_su_kien(tho, dm).first()["ma_danh_muc"] == "1051"


def test_cua_so_truot_15_phut(spark, monkeypatch):
    t = datetime(2026, 11, 10, 10, 7, 30)
    tho = dong_kafka(spark, [{"timestamp": TS, "visitorid": 1, "event": "view", "itemid": 9},
                             {"timestamp": TS, "visitorid": 2, "event": "addtocart", "itemid": 9},
                             {"timestamp": TS, "visitorid": 3, "event": "transaction", "itemid": 9}], ts=t)
    cua_so = gom_cua_so(lay_su_kien(tho)).collect()
    assert len(cua_so) == 15  # cửa sổ 15 phút, bước 1 phút -> mỗi sự kiện thuộc 15 cửa sổ
    assert all(r["so_luot_xem"] == 1 and r["so_luot_them_gio"] == 1 for r in cua_so)
    assert all(r["cua_so_ket_thuc"] - r["cua_so_bat_dau"] == timedelta(minutes=15) for r in cua_so)

    # Web đọc gold.thinh_hanh_15p: cửa sổ đang chạy và cửa sổ của lần bấm (buổi tập 17/11).
    # 8 lượt thêm giỏ lúc 02:08 (buổi trước), bấm thêm 1 lượt lúc 02:20:05. Cửa sổ mới nhất theo
    # cua_so_ket_thuc chỉ còn 1 lượt nên trang cũ (so với 8) không bao giờ báo "ĐÃ CẬP NHẬT";
    # trang mới đo trên cửa sổ bắt đầu ở phút của lần bấm.
    from pyspark.sql import functions as F

    from dich_vu import kho_du_lieu
    from dich_vu.kho_du_lieu import KhoDelta

    goc = 1794880800  # 2026-11-17 02:00:00 UTC, epoch giây
    su_kien = [(goc + 8 * 60 + 10, "addtocart")] * 8 + [(goc + 20 * 60 + 5, "addtocart"), (goc + 20 * 60 + 6, "view")]
    tho = spark.createDataFrame(
        [(json.dumps({"timestamp": TS, "visitorid": 1, "event": e, "itemid": "102605"}), "t", 0, i, s)
         for i, (s, e) in enumerate(su_kien)],
        "value string, kafka_topic string, kafka_partition int, kafka_offset long, giay long",
    ).withColumn("kafka_ts", F.timestamp_seconds("giay"))
    bang = gom_cua_so(lay_su_kien(tho))

    kho = KhoDelta()
    monkeypatch.setattr(kho, "_co_bang", lambda ten: True)
    monkeypatch.setattr(kho, "_doc", lambda ten, phien_ban=None: bang)
    bay_gio = [goc + 20 * 60 + 30]
    monkeypatch.setattr(kho_du_lieu.time, "time", lambda: bay_gio[0])

    # Cửa sổ đang chạy [02:06, 02:21): đủ 15 phút gần nhất, cả lượt cũ lẫn lượt mới.
    hien = kho.thinh_hanh_san_pham("102605")
    assert hien["cua_so_bat_dau_ms"] == (goc + 6 * 60) * 1000 and hien["cua_so_ket_thuc_ms"] == (goc + 21 * 60) * 1000
    assert (hien["so_luot_them_gio"], hien["so_luot_xem"]) == (9, 1)
    # Cửa sổ của lần bấm (trang lùi 10 giây: 02:19:55 -> phút 02:19): chỉ có lượt mới.
    lan_bam = kho.thinh_hanh_san_pham("102605", bat_dau_ms=(goc + 19 * 60 + 55) * 1000)
    assert lan_bam["so_luot_them_gio"] == 1 and lan_bam["cua_so_bat_dau_ms"] == (goc + 19 * 60) * 1000
    assert kho.thinh_hanh_san_pham("102605", bat_dau_ms=(goc + 25 * 60) * 1000) is None
    assert kho.thinh_hanh_san_pham("999999") is None
    # 15 phút sau lượt cuối thì không còn cửa sổ đang chạy.
    bay_gio[0] = goc + 36 * 60
    assert kho.thinh_hanh_san_pham("102605") is None


def test_cong_thuc_diem(spark):
    lo = spark.createDataFrame([("a", 20, 2), ("b", 20, 2)], "ma_san_pham string, so_luot_xem int, so_luot_them_gio int")
    tb = spark.createDataFrame([("a", 30.0)], "ma_san_pham string, trung_binh_24h double")
    diem = {r["ma_san_pham"]: r["diem_thinh_hanh"] for r in tinh_diem(lo, tb).collect()}
    assert diem["a"] == pytest.approx((20 + 6) / (30 + 10))
    assert diem["b"] == pytest.approx((20 + 6) / 10)  # sản phẩm ít phổ biến được đẩy lên


def test_luong_that_voi_nguon_tep(spark, tmp_path):
    """Chạy Structured Streaming thật: nguồn tệp -> chuẩn hóa -> cửa sổ có watermark, chế độ update."""
    from common.nguon_luong import doc_luong

    vao = tmp_path / "vao"
    vao.mkdir()
    with open(vao / "a.json", "w") as f:
        for i in range(5):
            f.write(json.dumps({"timestamp": TS + i, "visitorid": i, "event": "view" if i else "addtocart", "itemid": 7}) + "\n")
    luong = doc_luong(spark, None, nguon="tep", thu_muc_tep=str(vao))

    bronze = str(tmp_path / "bronze")
    q1 = (chuan_hoa_su_kien(luong).writeStream.format("parquet").option("checkpointLocation", str(tmp_path / "ck1"))
          .partitionBy("ngay").trigger(availableNow=True).start(bronze))
    q1.awaitTermination(120)
    assert spark.read.parquet(bronze).count() == 5

    ket_qua = []
    q2 = (gom_cua_so(lay_su_kien(luong)).writeStream.outputMode("update")
          .foreachBatch(lambda lo, _: ket_qua.extend(lo.collect()))
          .option("checkpointLocation", str(tmp_path / "ck2")).trigger(availableNow=True).start())
    q2.awaitTermination(120)
    assert ket_qua and {r["ma_san_pham"] for r in ket_qua} == {"7"}
    assert max(r["so_luot_xem"] for r in ket_qua) == 4 and max(r["so_luot_them_gio"] for r in ket_qua) == 1


def test_gop_impression(spark):
    msg = [
        {"loai": "hien_thi", "ma_hien_thi": "h1", "ma_nguoi_dung": "u", "nhom_ab": "B", "vi_tri": 1, "ma_san_pham": "a",
         "thoi_diem_ms": TS, "nguon_traffic": "web", "chien_luoc": "als"},
        {"loai": "phan_hoi", "ma_hien_thi": "h1", "ma_nguoi_dung": "u", "hanh_dong": "mua", "thoi_diem_ms": TS + 5},
        {"loai": "hien_thi", "ma_hien_thi": "h2", "ma_nguoi_dung": "u", "nhom_ab": "B", "vi_tri": 2, "ma_san_pham": "b",
         "thoi_diem_ms": TS},
        {"loai": "phan_hoi", "ma_hien_thi": "h3", "ma_nguoi_dung": "u", "hanh_dong": "click", "thoi_diem_ms": TS},
    ]
    kq = {r["ma_hien_thi"]: r for r in gop_theo_hien_thi(dong_kafka(spark, msg)).collect()}
    assert kq["h1"]["da_click"] and kq["h1"]["da_mua"]  # mua cũng tính là click
    assert not kq["h2"]["da_click"] and kq["h2"]["nguon_traffic"] == "web"
    assert kq["h3"]["thoi_diem_hien_thi"] is None  # phản hồi không có impression -> MERGE bỏ qua
    assert kq["h1"]["chien_luoc"] == "als"


def test_khong_doc_tep_dang_ghi(tmp_path):
    from phat_lai_su_kien import GhiTep

    g = GhiTep(str(tmp_path))
    for i in range(3):
        g.send("t", value={"i": i})
    assert os.listdir(tmp_path) == [".dang_ghi.json"]  # chưa đổi tên -> Spark bỏ qua tệp ẩn
    g.flush()
    assert os.listdir(tmp_path) == ["su_kien_00001.json"]


def test_tom_tat_tien_do_nap():
    from streaming.nap_su_kien import tom_tat_tien_do

    assert tom_tat_tien_do([]) == {"so_lo": 0, "so_su_kien_moi": 0}
    lo = [{"numInputRows": 1200}, {"numInputRows": 800}, {"numInputRows": None}]
    assert tom_tat_tien_do(lo) == {"so_lo": 3, "so_su_kien_moi": 2000}
