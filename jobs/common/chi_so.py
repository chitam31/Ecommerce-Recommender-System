"""Chỉ số xếp hạng viết bằng Python thuần để dễ kiểm thử.

Định nghĩa khớp pyspark.mllib.evaluation.RankingMetrics (có test đối chiếu):
độ liên quan nhị phân, log cơ số 2, MAP@K chia cho min(|thực tế|, K).
"""
import math


def precision_at_k(goi_y, thuc_te, k):
    """Tỷ lệ item đúng trong K vị trí đầu."""
    tap = set(thuc_te)
    if not tap:
        return 0.0
    return sum(1 for item in goi_y[:k] if item in tap) / float(k)


def recall_at_k(goi_y, thuc_te, k):
    """Tỷ lệ item thực tế được tìm thấy trong K vị trí đầu."""
    tap = set(thuc_te)
    if not tap:
        return 0.0
    return sum(1 for item in goi_y[:k] if item in tap) / float(len(tap))


def ndcg_at_k(goi_y, thuc_te, k):
    """NDCG@K với độ liên quan nhị phân."""
    tap = set(thuc_te)
    if not tap:
        return 0.0
    dcg = sum(
        1.0 / math.log(i + 2, 2) for i, item in enumerate(goi_y[:k]) if item in tap
    )
    idcg = sum(1.0 / math.log(i + 2, 2) for i in range(min(len(tap), k)))
    return dcg / idcg


def map_at_k(goi_y, thuc_te, k):
    """Average Precision@K của một người dùng."""
    tap = set(thuc_te)
    if not tap:
        return 0.0
    so_dung = 0
    tong = 0.0
    for i, item in enumerate(goi_y[:k]):
        if item in tap:
            so_dung += 1
            tong += so_dung / float(i + 1)
    return tong / float(min(len(tap), k))


def chi_so_mot_nguoi(goi_y, thuc_te, k):
    """Trả về (precision, recall, ndcg, map, 1) để cộng dồn trên RDD."""
    return (
        precision_at_k(goi_y, thuc_te, k),
        recall_at_k(goi_y, thuc_te, k),
        ndcg_at_k(goi_y, thuc_te, k),
        map_at_k(goi_y, thuc_te, k),
        1,
    )


def cong_chi_so(a, b):
    """Cộng từng thành phần của hai bộ chỉ số."""
    return tuple(x + y for x, y in zip(a, b))


def trung_binh_chi_so(tong, k):
    """Đổi tổng cộng dồn thành dict chỉ số trung bình."""
    so_nguoi = tong[4]
    if so_nguoi == 0:
        return {
            "precision_at_{}".format(k): 0.0,
            "recall_at_{}".format(k): 0.0,
            "ndcg_at_{}".format(k): 0.0,
            "map_at_{}".format(k): 0.0,
            "so_nguoi_danh_gia": 0,
        }
    return {
        "precision_at_{}".format(k): tong[0] / so_nguoi,
        "recall_at_{}".format(k): tong[1] / so_nguoi,
        "ndcg_at_{}".format(k): tong[2] / so_nguoi,
        "map_at_{}".format(k): tong[3] / so_nguoi,
        "so_nguoi_danh_gia": so_nguoi,
    }


def novelty_item(so_luot, tong_luot):
    """Lượng tin của một item: càng hiếm thì càng cao (-log2 thị phần)."""
    if so_luot <= 0 or tong_luot <= 0:
        return 0.0
    return -math.log(float(so_luot) / float(tong_luot), 2)
