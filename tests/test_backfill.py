"""Backfill: tính lại một khoảng ngày trong quá khứ, chạy lại không nhân đôi dữ liệu (mục A.8, C.3)."""
from datetime import date, datetime

import pytest


def test_ngay_trong_khoang_cua_impression(spark):
    from transform.tinh_ket_qua_ab import ngay_trong_khoang

    df = spark.createDataFrame(
        [(datetime(2026, 11, d, 9, 0),) for d in (10, 10, 11, 12, 14)], "thoi_diem_hien_thi timestamp")
    assert ngay_trong_khoang(df, "2026-11-11", "2026-11-14") == [date(2026, 11, 11), date(2026, 11, 12),
                                                                  date(2026, 11, 14)]
    assert ngay_trong_khoang(df, "2026-11-10", None) == [date(2026, 11, 10)]
    assert ngay_trong_khoang(df, None, "2026-11-13") == []
    with pytest.raises(ValueError):
        ngay_trong_khoang(df, "2026-11-14", "2026-11-10")
