"""thu_lai_khi_xung_dot: luồng streaming và DAG bảo trì cùng ghi một bảng Delta không làm hỏng nhau."""
import pytest

from common.spark_utils import la_xung_dot_delta, thu_lai_khi_xung_dot


class LoiGiaPy4J(Exception):
    """Giống cách PySpark báo lỗi Java: tên lớp Delta nằm trong thông điệp."""


def test_nhan_ra_xung_dot_delta():
    assert la_xung_dot_delta(LoiGiaPy4J("io.delta.exceptions.ConcurrentAppendException: Files were added"))
    assert la_xung_dot_delta(LoiGiaPy4J("An error occurred: ConcurrentDeleteReadException ..."))
    assert not la_xung_dot_delta(LoiGiaPy4J("AnalysisException: Table not found"))
    assert not la_xung_dot_delta(ValueError("bad"))


def test_thu_lai_roi_thanh_cong():
    dem, cho = [], []

    def ham():
        dem.append(1)
        if len(dem) < 3:
            raise LoiGiaPy4J("io.delta.exceptions.ConcurrentDeleteReadException")
        return "xong"

    assert thu_lai_khi_xung_dot(ham, so_lan=5, cho_giay=2, ngu=cho.append) == "xong"
    assert len(dem) == 3 and cho == [2, 4]  # chờ tăng dần theo số lần


def test_loi_khac_nem_ngay_va_het_lan_thi_nem():
    dem = []

    def loi_khac():
        dem.append(1)
        raise LoiGiaPy4J("AnalysisException: cột không tồn tại")

    with pytest.raises(LoiGiaPy4J):
        thu_lai_khi_xung_dot(loi_khac, ngu=lambda _: None)
    assert len(dem) == 1

    dem.clear()

    def luon_xung_dot():
        dem.append(1)
        raise LoiGiaPy4J("ConcurrentAppendException")

    with pytest.raises(LoiGiaPy4J):
        thu_lai_khi_xung_dot(luon_xung_dot, so_lan=4, ngu=lambda _: None)
    assert len(dem) == 4


def test_xoa_goi_y_cu_giu_lan_trien_khai_moi_nhat():
    """DAG 4 chỉ xóa dòng cũ hơn lần triển khai mới nhất 7 ngày: sau 7 ngày không triển khai mới
    (giữ mô hình cũ, không có dữ liệu mới), danh sách đang phục vụ vẫn còn nguyên."""
    from datetime import datetime

    from bao_tri.toi_uu_delta import lenh_xoa_goi_y_cu

    lenh = lenh_xoa_goi_y_cu("delta.`/lake/gold/goi_y_nguoi_dung`", datetime(2026, 11, 11, 9, 30, 5), 7)
    assert lenh == ("DELETE FROM delta.`/lake/gold/goi_y_nguoi_dung` "
                    "WHERE sinh_luc < timestamp'2026-11-11 09:30:05' - INTERVAL 7 DAYS")
    assert "current_timestamp" not in lenh
    assert lenh_xoa_goi_y_cu("t", None, 7) is None


def test_bao_tri_gioi_han_co_tep_bang_goi_y():
    """Lỗi 16/11: OPTIMIZE hằng đêm dùng cỡ tệp mặc định 1 GB gộp bảng gợi ý (~40 MB) thành một tệp,
    ZORDER mất tác dụng và web tra một user mất ~9 giây. Bảng gợi ý phải giữ tệp 1 MB như thí nghiệm."""
    from bao_tri.toi_uu_delta import BANG_TOI_UU, cau_hinh_co_tep

    ch = cau_hinh_co_tep("gold.goi_y_nguoi_dung")
    assert ch["spark.databricks.delta.optimize.maxFileSize"] == str(1024 * 1024)
    assert ch["spark.databricks.delta.optimize.minFileSize"] == str(1024 * 1024)
    assert ("gold.goi_y_nguoi_dung", "ma_nguoi_dung") in BANG_TOI_UU
    assert cau_hinh_co_tep("silver.tuong_tac") == {}
