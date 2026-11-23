"""Lược đồ dữ liệu: JSON trên Kafka và DDL các bảng Delta (mục 3.4 của đề tài)."""
from pyspark.sql.types import LongType, StringType, StructField, StructType

from . import cau_hinh as ch

# Thông điệp clickstream. Giữ nguyên 5 trường gốc của topic retailrocket-events để
# thông điệp cũ vẫn đọc được; visitorid/itemid đọc thành chuỗi vì người dùng mới
# sinh thêm có mã dạng "moi_123".
LUOC_DO_SU_KIEN = StructType([
    StructField("timestamp", LongType()),
    StructField("visitorid", StringType()),
    StructField("event", StringType()),
    StructField("itemid", StringType()),
    StructField("transactionid", StringType()),
    StructField("ma_phien", StringType()),
    StructField("ma_danh_muc", StringType()),
    StructField("thiet_bi", StringType()),
    StructField("nguon", StringType()),
])

# Thông điệp trên topic hien-thi: "hien_thi" là một dòng impression,
# "phan_hoi" báo click hoặc mua của một impression đã có.
LUOC_DO_HIEN_THI = StructType([
    StructField("loai", StringType()),
    StructField("ma_hien_thi", StringType()),
    StructField("ma_nguoi_dung", StringType()),
    StructField("nhom_ab", StringType()),
    StructField("vi_tri", LongType()),
    StructField("ma_san_pham", StringType()),
    StructField("thoi_diem_ms", LongType()),
    StructField("hanh_dong", StringType()),
    StructField("nguon_traffic", StringType()),
    StructField("chien_luoc", StringType()),
])

_GIU_LICH_SU = {
    "delta.deletedFileRetentionDuration": ch.GIU_FILE_DA_XOA,
    "delta.logRetentionDuration": ch.GIU_NHAT_KY_DELTA,
}

def ten_cot(ten_bang):
    """Danh sách tên cột của một bảng trong BANG_DELTA, đúng thứ tự DDL."""
    return [phan.split()[0] for phan in " ".join(BANG_DELTA[ten_bang]["cot"].split()).split(", ")]


def kieu_cot(ten_bang):
    """Tên cột -> kiểu (chữ hoa) theo DDL, ví dụ {"so_tep": "LONG", "p95_ms": "DOUBLE"}."""
    phan = " ".join(BANG_DELTA[ten_bang]["cot"].split()).split(", ")
    return {p.split()[0]: p.split()[1].upper() for p in phan}


def _ep_kieu(gia_tri, kieu):
    # Spark từ chối int trong cột DOUBLE (statistics.mean([1, 1]) trả về int 1, không phải 1.0).
    if kieu in ("DOUBLE", "FLOAT") and isinstance(gia_tri, int) and not isinstance(gia_tri, bool):
        return float(gia_tri)
    return gia_tri


def tao_df(spark, ten_bang, cac_dong):
    """Tạo DataFrame từ list dict theo đúng lược đồ DDL; khóa thiếu nhận None."""
    kieu = kieu_cot(ten_bang)
    cot = ten_cot(ten_bang)
    return spark.createDataFrame(
        [tuple(_ep_kieu(d.get(c), kieu[c]) for c in cot) for d in cac_dong], BANG_DELTA[ten_bang]["cot"]
    )


# Bảng cần tồn tại trước khi luồng và web chạy. Tên cột tuân đúng đặc tả; cột thêm
# vào đều có lý do và được giải thích trong docs/KIEN_TRUC.md.
BANG_DELTA = {
    "gold.thinh_hanh_15p": {
        "cot": """cua_so_bat_dau TIMESTAMP, cua_so_ket_thuc TIMESTAMP, ma_san_pham STRING,
                  so_luot_xem INT, so_luot_them_gio INT, diem_thinh_hanh DOUBLE""",
        # Bảng nhận một commit mỗi 20 giây và chỉ giữ 2 ngày dữ liệu: không cần lịch sử dài.
        "thuoc_tinh": {
            "delta.deletedFileRetentionDuration": "interval 2 days",
            "delta.logRetentionDuration": "interval 7 days",
        },
    },
    "gold.goi_y_nguoi_dung": {
        "cot": """ma_nguoi_dung STRING, xep_hang INT, ma_san_pham STRING, diem_du_doan DOUBLE,
                  nguon STRING, phien_ban_mo_hinh STRING, sinh_luc TIMESTAMP""",
        "thuoc_tinh": {},
    },
    "gold.nhat_ky_hien_thi": {
        "cot": """ma_hien_thi STRING, ma_nguoi_dung STRING, nhom_ab STRING, vi_tri INT,
                  ma_san_pham STRING, thoi_diem_hien_thi TIMESTAMP, da_click BOOLEAN,
                  da_mua BOOLEAN, nguon_traffic STRING, chien_luoc STRING""",
        "thuoc_tinh": dict(_GIU_LICH_SU, **{"delta.enableChangeDataFeed": "true"}),
    },
    "gold.ket_qua_ab": {
        "cot": """ngay DATE, nhom_ab STRING, so_hien_thi LONG, so_click LONG, so_mua LONG,
                  ctr DOUBLE, ty_le_chuyen_doi DOUBLE, p_value DOUBLE, p_value_chuyen_doi DOUBLE,
                  ty_le_mo_phong DOUBLE, phien_ban_delta_du_lieu LONG""",
        "thuoc_tinh": _GIU_LICH_SU,
    },
    "gold.ket_qua_tuning": {
        "cot": """ma_chay STRING, phien_ban_features LONG, rank INT, reg_param DOUBLE, alpha DOUBLE,
                  precision_at_10 DOUBLE, recall_at_10 DOUBLE, ndcg_at_10 DOUBLE, map_at_10 DOUBLE,
                  coverage DOUBLE, coverage_toan_san DOUBLE, novelty DOUBLE, so_giay DOUBLE""",
        "thuoc_tinh": {},
    },
    "gold.lich_su_huan_luyen": {
        "cot": """ma_chay STRING, phien_ban_mo_hinh STRING, thoi_diem TIMESTAMP,
                  phien_ban_features LONG, phien_ban_silver LONG, ngay_cat DATE,
                  rank INT, reg_param DOUBLE, alpha DOUBLE, max_iter INT, hat_giong INT,
                  precision_at_10 DOUBLE, recall_at_10 DOUBLE, ndcg_at_10 DOUBLE, map_at_10 DOUBLE,
                  coverage DOUBLE, coverage_toan_san DOUBLE, novelty DOUBLE,
                  ndcg_baseline_toan_san DOUBLE, ndcg_baseline_danh_muc DOUBLE, uplift_ndcg DOUBLE,
                  dat_nguong BOOLEAN, cai_thien BOOLEAN, trang_thai STRING,
                  duong_dan_mo_hinh STRING, chi_tiet_json STRING""",
        "thuoc_tinh": _GIU_LICH_SU,
    },
    "gold.ket_qua_tai_lap": {
        "cot": """thoi_diem TIMESTAMP, loai STRING, khoa STRING, phien_ban_delta LONG,
                  chi_so_cu_json STRING, chi_so_moi_json STRING, lech_lon_nhat DOUBLE, trung_khop BOOLEAN""",
        "thuoc_tinh": {},
    },
    "gold.do_hieu_nang_zorder": {
        "cot": """thoi_diem TIMESTAMP, giai_doan STRING, so_tep LONG, so_truy_van INT,
                  trung_binh_ms DOUBLE, trung_vi_ms DOUBLE, p95_ms DOUBLE, tep_doc_trung_binh DOUBLE""",
        "thuoc_tinh": {},
    },
}
