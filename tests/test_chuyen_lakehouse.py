"""Chuyển hồ dữ liệu /lake -> /lakehouse/bd03_khuyen_nghi (scripts/cum/chuyen_lakehouse.py).

Phần quan trọng nhất được kiểm bằng Spark thật: file sink Parquet của bronze ghi đường dẫn
tuyệt đối vào _spark_metadata. Sau khi dời thư mục và viết lại tiền tố, (1) đọc bronze ở gốc mới
ra đủ số dòng, (2) luồng chạy tiếp với checkpoint đã dời không xử lý lại lô cũ, (3) hoàn tác
đưa mọi thứ về như cũ.
"""
import json
import os
import sys

import pytest

GOC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(GOC, "scripts", "cum"))

import chuyen_lakehouse as cl  # noqa: E402
from common import cau_hinh as ch  # noqa: E402


def test_goc_moi_dung_quy_uoc_A4():
    assert ch.LAKE_DUONG_DAN == "/lakehouse/bd03_khuyen_nghi"
    assert ch.duong_dan("silver.tuong_tac").endswith("/lakehouse/bd03_khuyen_nghi/silver/tuong_tac")
    assert ch.duong_dan("bronze.su_kien").endswith("/lakehouse/bd03_khuyen_nghi/bronze/su_kien")
    assert ch.duong_dan("_quarantine.tuong_tac").endswith("/lakehouse/bd03_khuyen_nghi/_quarantine/tuong_tac")
    assert ch.duong_dan_checkpoint("x").endswith("/lakehouse/bd03_khuyen_nghi/checkpoints/x")
    assert ch.duong_dan_mo_hinh("als/r1").endswith("/lakehouse/bd03_khuyen_nghi/models/als/r1")


def test_ke_hoach_chi_chuyen_du_lieu_cua_du_an():
    cap = dict(cl.ke_hoach(checkpoint=["nap_su_kien_v2", "tinh_thinh_hanh_v1"]))
    assert cap["/lake/bronze/su_kien_v2"] == "/lakehouse/bd03_khuyen_nghi/bronze/su_kien"
    assert cap["/lake/silver/tuong_tac"] == "/lakehouse/bd03_khuyen_nghi/silver/tuong_tac"
    assert cap["/lake/gold/goi_y_nguoi_dung"] == "/lakehouse/bd03_khuyen_nghi/gold/goi_y_nguoi_dung"
    assert cap["/lake/checkpoints/nap_su_kien_v2"] == "/lakehouse/bd03_khuyen_nghi/checkpoints/nap_su_kien_v2"
    assert cap["/lake/models"] == "/lakehouse/bd03_khuyen_nghi/models"
    assert cap["/lake/landing/retailrocket"] == "/lakehouse/bd03_khuyen_nghi/landing/retailrocket"
    # Dữ liệu cũ của nhóm và chính thư mục /lake không nằm trong kế hoạch.
    assert "/lake/bronze/su_kien" not in cap and "/lake" not in cap and "/lake/bronze" not in cap
    assert not any(n.startswith("/lake/cu_nhom") for n in cap)
    # Bảng quarantine chỉ có ở gốc mới.
    assert not any("_quarantine" in n for n in cap)


def test_checkpoint_lay_tu_ma_nguon():
    assert {"nap_su_kien_v2", "tinh_thinh_hanh_v1", "ghi_hien_thi_v1"} <= set(cl.ten_checkpoint_trong_ma())


def test_doi_tien_to_giu_scheme_va_host():
    dong = [
        "v1",
        json.dumps({"path": "hdfs://10.68.71.113:8020/lake/bronze/su_kien_v2/ngay=2015-05-03/part-0.parquet",
                    "size": 10, "action": "add"}),
        json.dumps({"path": "hdfs://10.68.71.113:8020/lake/bronze/su_kien_v2x/khac.parquet", "size": 1}),
        json.dumps({"path": "file:/tmp/a/lake/bronze/su_kien_v2/ngay=2015-05-04/p.parquet", "size": 2}),
    ]
    moi, so = cl.doi_tien_to("\n".join(dong), "/lake/bronze/su_kien_v2", "/lakehouse/bd03_khuyen_nghi/bronze/su_kien")
    d = moi.splitlines()
    assert so == 1 and d[0] == "v1"
    assert json.loads(d[1])["path"] == ("hdfs://10.68.71.113:8020/lakehouse/bd03_khuyen_nghi/bronze/su_kien/"
                                        "ngay=2015-05-03/part-0.parquet")
    assert "su_kien_v2x" in d[2]          # thư mục chỉ trùng tiền tố chữ, không phải thư mục con: không đổi
    assert "/tmp/a/lake/" in d[3]         # tiền tố cũ không đứng ngay sau scheme/host: không đổi
    assert cl.kiem_tra_metadata(moi, "/lake/bronze/su_kien_v2") == (3, 0)
    assert cl.kiem_tra_metadata("\n".join(dong), "/lake/bronze/su_kien_v2") == (3, 1)


def test_hdfs_qua_docker_ghi_de_an_toan():
    lenh = []

    class KQ:
        returncode, stdout, stderr = 0, b"", b""

    fs = cl.HdfsQuaDocker(chay=lambda l, **k: lenh.append((l, k.get("input"))) or KQ())
    fs.ghi_de("/x/_spark_metadata/9.compact", "v1\n{}")
    thu_tu = [" ".join(l[0][5:]) for l in lenh]
    assert thu_tu == ["dfs -put -f - /x/_spark_metadata/9.compact._chuyen_tam",
                      "dfs -rm -f -skipTrash /x/_spark_metadata/9.compact",
                      "dfs -mv /x/_spark_metadata/9.compact._chuyen_tam /x/_spark_metadata/9.compact"]
    assert lenh[0][1] == b"v1\n{}"


def test_dich_ton_tai_thi_khong_chuyen(tmp_path):
    fs = cl.HeTepCucBo(str(tmp_path))
    (tmp_path / "lake" / "silver" / "tuong_tac").mkdir(parents=True)
    (tmp_path / "lakehouse" / "bd03_khuyen_nghi" / "silver" / "tuong_tac").mkdir(parents=True)
    with pytest.raises(RuntimeError, match="Đích đã tồn tại"):
        cl.thuc_hien(fs, cl.ke_hoach(checkpoint=[]), cl.bang_bronze(), ch.LAKE_DUONG_DAN)
    assert (tmp_path / "lake" / "silver" / "tuong_tac").exists()


def _chay_luong(spark, nguon, dich, checkpoint):
    """Một lượt file sink Parquet phân vùng theo ngay, trigger availableNow (như --mot-lan)."""
    from pyspark.sql import functions as F

    df = (spark.readStream.schema("ma_su_kien STRING, thoi_diem TIMESTAMP").json(nguon)
          .withColumn("ngay", F.to_date("thoi_diem")))
    q = (df.writeStream.format("parquet").option("checkpointLocation", checkpoint)
         .partitionBy("ngay").trigger(availableNow=True).start(dich))
    q.awaitTermination()


def test_chuyen_bronze_that_roi_chay_tiep_va_hoan_tac(spark, tmp_path):
    goc = str(tmp_path)
    vao = tmp_path / "vao"
    vao.mkdir()
    (vao / "a.json").write_text("\n".join(
        json.dumps({"ma_su_kien": "e{}".format(i), "thoi_diem": "2015-05-0{} 10:00:00".format(1 + i % 3)})
        for i in range(30)))
    bronze_cu = "file://{}/lake/bronze/su_kien_v2".format(goc)
    ck_cu = "file://{}/lake/checkpoints/nap_su_kien_v2".format(goc)
    _chay_luong(spark, str(vao), bronze_cu, ck_cu)
    assert spark.read.parquet(bronze_cu).count() == 30
    (tmp_path / "lake" / "bronze" / "su_kien").mkdir()          # dữ liệu cũ của nhóm: phải giữ nguyên
    (tmp_path / "lake" / "gold" / "ket_qua_ab" / "_delta_log").mkdir(parents=True)

    fs = cl.HeTepCucBo(goc)
    cap = cl.ke_hoach(checkpoint=["nap_su_kien_v2"])
    nhat_ky = []
    da = cl.thuc_hien(fs, cap, cl.bang_bronze(), ch.LAKE_DUONG_DAN, in_ra=nhat_ky.append)
    assert len(da) == 3 and (tmp_path / "lake" / "bronze" / "su_kien").exists()
    assert (tmp_path / "lakehouse" / "bd03_khuyen_nghi" / "_quarantine").is_dir()
    assert (tmp_path / "lakehouse" / "bd03_khuyen_nghi" / "gold" / "ket_qua_ab" / "_delta_log").is_dir()
    meta = tmp_path / "lakehouse" / "bd03_khuyen_nghi" / "bronze" / "su_kien" / "_spark_metadata"
    for t in meta.iterdir():
        if not t.name.startswith("."):
            assert "/lake/bronze/su_kien_v2/" not in t.read_text()

    bronze_moi = "file://{}/lakehouse/bd03_khuyen_nghi/bronze/su_kien".format(goc)
    ck_moi = "file://{}/lakehouse/bd03_khuyen_nghi/checkpoints/nap_su_kien_v2".format(goc)
    assert spark.read.parquet(bronze_moi).count() == 30           # đọc qua _spark_metadata đã viết lại
    (vao / "b.json").write_text("\n".join(
        json.dumps({"ma_su_kien": "m{}".format(i), "thoi_diem": "2015-05-04 10:00:00"}) for i in range(5)))
    _chay_luong(spark, str(vao), bronze_moi, ck_moi)              # luồng chạy tiếp ở gốc mới
    df = spark.read.parquet(bronze_moi)
    assert df.count() == 35 and df.select("ma_su_kien").distinct().count() == 35   # không xử lý lại lô cũ

    tra = cl.hoan_tac(fs, cap, cl.bang_bronze(), in_ra=nhat_ky.append)
    assert len(tra) == 3 and not os.path.exists(bronze_moi[len("file://"):])
    assert spark.read.parquet(bronze_cu).count() == 35


def test_duong_dan_mo_hinh_cu_doi_sang_goc_moi():
    cu = "hdfs://10.68.71.113:8020/lake/models/als/r20261110T010203"
    assert ch.duong_dan_mo_hinh_hien_tai(cu) == ch.LAKE_GOC + "/models/als/r20261110T010203"
    assert ch.duong_dan_mo_hinh_hien_tai(ch.duong_dan_mo_hinh("als/r1")) == ch.duong_dan_mo_hinh("als/r1")
