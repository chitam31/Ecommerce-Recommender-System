"""Cấu hình trung tâm: địa chỉ cụm, đường dẫn hồ dữ liệu, hằng số nghiệp vụ.

Giá trị mặc định khớp đúng cụm hiện tại của nhóm (repo spark-cluster,
hadoop-cluster, kafka-docker). Muốn chạy thử ở máy cá nhân chỉ cần đặt biến
môi trường LAKE_GOC=file:///tmp/lake.

Gốc hồ dữ liệu theo quy ước mục A.4 của đề cương: hdfs:///lakehouse/<ma_de_tai>/ với các thư
mục bronze, silver, gold, features, models, checkpoints, _quarantine. Trước ngày 15/11/2026 hồ
dữ liệu nằm ở /lake; scripts/cum/chuyen_lakehouse.sh chuyển dữ liệu cũ sang gốc mới.

Lưu ý: cụm Spark standalone của nhóm KHÔNG có Hive Metastore, nên tên bảng dạng
"silver.tuong_tac" không tồn tại trong catalog giữa các phiên. Vì vậy dự án
truy cập bảng theo đường dẫn, còn tên bảng chỉ là khóa tra cứu trong _BANG.
"""
import os

# ----- Địa chỉ cụm qua ZeroTier -----
HDFS_GOC = os.environ.get("HDFS_GOC", "hdfs://10.68.71.113:8020")
MA_DE_TAI = "bd03_khuyen_nghi"
# Đường dẫn trên HDFS (không kèm hdfs://host:port) của gốc hồ dữ liệu, và gốc cũ trước khi chuyển.
LAKE_DUONG_DAN = os.environ.get("LAKE_DUONG_DAN", "/lakehouse/" + MA_DE_TAI)
LAKE_DUONG_DAN_CU = "/lake"
LAKE_GOC = os.environ.get("LAKE_GOC", HDFS_GOC + LAKE_DUONG_DAN)
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "10.68.71.204:9092")
TOPIC_SU_KIEN = os.environ.get("TOPIC_SU_KIEN", "retailrocket-events")
TOPIC_HIEN_THI = os.environ.get("TOPIC_HIEN_THI", "hien-thi")

# ----- Ánh xạ tên bảng logic -> thư mục con trong hồ dữ liệu -----
_BANG = {
    # Bronze để Parquet thuần (lựa chọn của nhóm, giữ job bronze cũ và Bảng 6.2 giáo trình;
    # đặc tả 3.4 chỉ quy định cột). Ở gốc cũ /lake bảng này nằm ở bronze/su_kien_v2 vì
    # /lake/bronze/su_kien là dữ liệu cũ của nhóm với lược đồ khác; gốc mới không còn trùng tên.
    "bronze.su_kien": "bronze/su_kien",
    "silver.tuong_tac": "silver/tuong_tac",
    "silver.san_pham": "silver/san_pham",
    "features.ma_tran_tuong_tac": "features/ma_tran_tuong_tac",
    "gold.thong_ke_san_pham": "gold/thong_ke_san_pham",
    "gold.thinh_hanh_15p": "gold/thinh_hanh_15p",
    "gold.goi_y_als_tam": "gold/goi_y_als_tam",
    "gold.goi_y_tam": "gold/goi_y_tam",
    "gold.goi_y_nguoi_dung": "gold/goi_y_nguoi_dung",
    "gold.san_pham_lien_quan": "gold/san_pham_lien_quan",
    "gold.nhat_ky_hien_thi": "gold/nhat_ky_hien_thi",
    "gold.ket_qua_ab": "gold/ket_qua_ab",
    "gold.ket_qua_tuning": "gold/ket_qua_tuning",
    "gold.lich_su_huan_luyen": "gold/lich_su_huan_luyen",
    "gold.ket_qua_tai_lap": "gold/ket_qua_tai_lap",
    "gold.do_hieu_nang_zorder": "gold/do_hieu_nang_zorder",
    # Mục A.4: bản ghi lỗi bị loại khỏi silver, kèm lý do (bronze_to_silver ghi).
    "_quarantine.tuong_tac": "_quarantine/tuong_tac",
}

# Vị trí của từng bảng ở gốc cũ /lake, chỉ dùng cho scripts/cum/chuyen_lakehouse.py.
_BANG_GOC_CU = dict(_BANG, **{"bronze.su_kien": "bronze/su_kien_v2"})
_BANG_GOC_CU.pop("_quarantine.tuong_tac")


def duong_dan(ten_bang):
    """Trả về đường dẫn đầy đủ của một bảng logic."""
    return "{}/{}".format(LAKE_GOC, _BANG[ten_bang])


def duong_dan_checkpoint(ten_job):
    """Mỗi luồng một thư mục checkpoint riêng trên HDFS."""
    return "{}/checkpoints/{}".format(LAKE_GOC, ten_job)


def duong_dan_mo_hinh(ten):
    """Vị trí lưu mô hình và StringIndexer trên HDFS."""
    return "{}/models/{}".format(LAKE_GOC, ten)


def duong_dan_mo_hinh_hien_tai(duong_dan_luu):
    """Đổi đường dẫn mô hình đã lưu trong gold.lich_su_huan_luyen sang gốc hồ dữ liệu hiện tại.

    Các lượt huấn luyện trước khi chuyển gốc ghi hdfs://.../lake/models/als/<ma_chay>; thư mục
    models đã được chuyển nguyên vẹn sang gốc mới nên chỉ cần thay phần trước "/models/".
    """
    vi_tri = (duong_dan_luu or "").find("/models/")
    return LAKE_GOC + duong_dan_luu[vi_tri:] if vi_tri >= 0 else duong_dan_luu


def duong_dan_landing(ten_tep=""):
    """Vùng chứa CSV gốc của RetailRocket sau khi hdfs dfs -put."""
    return "{}/landing/retailrocket/{}".format(LAKE_GOC, ten_tep)


# ----- Hằng số nghiệp vụ theo đề tài (mục 3.4 và 3.6) -----
TRONG_SO = {"xem": 1.0, "them_gio": 3.0, "mua": 5.0}
LOAI_SU_KIEN = {
    "view": "xem",
    "addtocart": "them_gio",
    "transaction": "mua",
    "removefromcart": "bo_gio",
}
LAMBDA_SUY_GIAM = 0.01
GIAY_KHU_TRUNG = 5
NGUONG_BOT_MOI_GIO = 500

SO_NGAY_TEST = 30
K_DANH_GIA = 10
TOP_N_GOLD = 20
NGUONG_TANG_NDCG = 0.30
NGUONG_COVERAGE = 0.10
HAT_GIONG = 42

LUOI_DAY_DU = {
    "rank": [10, 30, 50],
    "regParam": [0.01, 0.1, 1.0],
    "alpha": [1.0, 10.0, 40.0],
}
# Lưới tối thiểu vẫn đủ 9 tổ hợp theo đề, dùng khi cụm thiếu thời gian.
LUOI_TOI_THIEU = {
    "rank": [10, 30, 50],
    "regParam": [0.01, 0.1, 1.0],
    "alpha": [10.0],
}

# Cold start
NGUONG_USER_IT_TUONG_TAC = 5
TY_LE_THINH_HANH_NHOM_B = 0.30
TY_LE_ITEM_MOI = 0.20

# Mô hình v2 (sau schema evolution tuần 5): ràng buộc lọc theo tầm giá.
# Tầm giá của user = [(1 - BIEN) x P10 ; (1 + BIEN) x P90] giá các món họ đã tương tác.
SO_GIA_TOI_THIEU = 3        # cần ít nhất 3 tương tác có giá mới xác định được tầm giá
BIEN_TAM_GIA = 0.5
HE_SO_UNG_VIEN_V2 = 2       # lấy dư gấp đôi ứng viên ALS để sau khi lọc vẫn đủ danh sách

# Xếp lại danh sách ALS phục vụ theo độ mới (MO_HINH.md mục 6, 12.2): lấy dư 200 ứng viên,
# điểm xếp lại = điểm ALS chuẩn hóa + 1,0 x novelty chuẩn hóa. Trên dữ liệu thật 10/11/2026,
# cấu hình này cho NDCG@10 +27% và coverage +88% so với ALS thuần. BETA_XEP_LAI = 0 thì quay
# về ALS thuần (xếp theo điểm ALS, đúng số ứng viên cần dùng).
BETA_XEP_LAI = 1.0
SO_UNG_VIEN_XEP_LAI = 200

# Thời gian lưu lịch sử Delta: phải dài hơn khoảng cần tái lập thí nghiệm,
# nếu không VACUUM mặc định 7 ngày sẽ xóa mất file của phiên bản cũ.
GIU_FILE_DA_XOA = "interval 30 days"
GIU_NHAT_KY_DELTA = "interval 60 days"
