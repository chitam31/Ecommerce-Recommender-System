"""Kiểm tra metric xếp hạng: tính tay và đối chiếu với RankingMetrics của Spark."""
import math

import pytest

from common.chi_so import (
    chi_so_mot_nguoi,
    cong_chi_so,
    map_at_k,
    ndcg_at_k,
    novelty_item,
    precision_at_k,
    recall_at_k,
    trung_binh_chi_so,
)


def test_tinh_tay():
    goi_y = [1, 2, 3, 4, 5]
    thuc_te = [2, 5, 9]
    assert precision_at_k(goi_y, thuc_te, 5) == pytest.approx(2 / 5)
    assert recall_at_k(goi_y, thuc_te, 5) == pytest.approx(2 / 3)
    dcg = 1 / math.log2(3) + 1 / math.log2(6)
    idcg = 1 + 1 / math.log2(3) + 1 / math.log2(4)
    assert ndcg_at_k(goi_y, thuc_te, 5) == pytest.approx(dcg / idcg)
    assert map_at_k(goi_y, thuc_te, 5) == pytest.approx((1 / 2 + 2 / 5) / 3)


def test_truong_hop_bien():
    assert precision_at_k([1, 2], [], 2) == 0.0
    assert ndcg_at_k([], [1], 10) == 0.0
    assert ndcg_at_k([7], [7], 10) == pytest.approx(1.0)
    assert novelty_item(1, 8) == pytest.approx(3.0)
    assert novelty_item(0, 8) == 0.0


def test_cong_don():
    tong = cong_chi_so(chi_so_mot_nguoi([1, 2], [1], 2), chi_so_mot_nguoi([3, 4], [9], 2))
    kq = trung_binh_chi_so(tong, 2)
    assert kq["so_nguoi_danh_gia"] == 2
    assert kq["precision_at_2"] == pytest.approx(0.25)
    assert trung_binh_chi_so((0, 0, 0, 0, 0), 10)["ndcg_at_10"] == 0.0


def test_khop_ranking_metrics_cua_spark(spark):
    """Định nghĩa của dự án phải khớp pyspark.mllib.evaluation.RankingMetrics."""
    from pyspark.mllib.evaluation import RankingMetrics

    du_lieu = [
        ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], [1, 3, 11]),
        ([5, 4, 3, 2, 1, 6, 7, 8, 9, 10], [10, 12]),
        ([3, 1, 2, 9, 8, 7, 6, 5, 4, 10], [3, 2, 1, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
    ]
    rm = RankingMetrics(spark.sparkContext.parallelize(du_lieu))
    k = 10
    tong = (0.0, 0.0, 0.0, 0.0, 0)
    for goi_y, thuc_te in du_lieu:
        tong = cong_chi_so(tong, chi_so_mot_nguoi(goi_y, thuc_te, k))
    kq = trung_binh_chi_so(tong, k)
    assert kq["precision_at_10"] == pytest.approx(rm.precisionAt(k))
    assert kq["ndcg_at_10"] == pytest.approx(rm.ndcgAt(k))
    assert kq["recall_at_10"] == pytest.approx(rm.recallAt(k))
    assert kq["map_at_10"] == pytest.approx(rm.meanAveragePrecisionAt(k))
