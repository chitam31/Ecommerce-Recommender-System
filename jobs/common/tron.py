"""Logic phân nhóm A/B và trộn gợi ý, dùng chung cho job batch, web và test.

Mỗi gợi ý là một dict có các khóa: ma_san_pham, diem_du_doan, nguon.

Danh sách cuối gồm 20 vị trí, chia thành 2 khối 10. Web hiển thị đúng khối đầu, nên
tỷ lệ của từng nguồn được bảo đảm ngay trên phần người dùng nhìn thấy:

    Nhóm A (ALS thuần):        ALS x8, item mới x2 (vị trí 9-10)
    Nhóm B (ALS + 30% hot):    ALS ALS TH ALS ALS TH ALS TH, item mới x2
    User < 5 tương tác:        danh mục / thịnh hành xen kẽ 50/50
    User mới hoàn toàn:        toàn bộ thịnh hành

Item mới (không có trong ALS) luôn nằm ở 20% vị trí cuối mỗi khối, áp dụng như nhau
cho cả A và B nên khác biệt giữa hai nhóm chỉ còn là phần trộn thịnh hành.
"""
import hashlib

NGUON_ALS = "als"
NGUON_DANH_MUC = "pho_bien_danh_muc"
NGUON_THINH_HANH = "thinh_hanh"

# Ký hiệu ô trong mẫu: a = ALS, t = thịnh hành, m = item mới, d = bán chạy danh mục
MAU_KHOI = {
    "A": "aaaaaaaamm",
    "B": "aataatatmm",
    "IT": "dtdtdtdtdt",
    "MOI": "tttttttttt",
}
# Hết hàng ở một nguồn thì lấy bù theo thứ tự này.
THU_TU_BU = {
    "a": "adt",
    "t": "tad",
    "m": "mad",
    "d": "dta",
}


def chon_nhom(ma_nguoi_dung):
    """Hash cố định nên cùng một người dùng luôn vào cùng một nhóm."""
    bam = hashlib.md5(str(ma_nguoi_dung).encode("utf-8")).hexdigest()
    return "A" if int(bam[:8], 16) % 2 == 0 else "B"


def lap_theo_mau(mau, nguon, so_khoi=2):
    """Điền danh sách theo mẫu khối 10 ô, bỏ trùng sản phẩm, lấy bù khi một nguồn cạn.

    nguon: dict ký hiệu -> list gợi ý đã xếp hạng, ví dụ {"a": [...], "t": [...]}.
    """
    vi_tri_doc = {k: 0 for k in nguon}
    da_co = set()
    ket_qua = []

    def lay_tu(ky_hieu):
        ds = nguon.get(ky_hieu) or []
        i = vi_tri_doc.get(ky_hieu, 0)
        while i < len(ds) and ds[i]["ma_san_pham"] in da_co:
            i += 1
        vi_tri_doc[ky_hieu] = i
        if i < len(ds):
            vi_tri_doc[ky_hieu] = i + 1
            return ds[i]
        return None

    for _ in range(so_khoi):
        for o in mau:
            duoc_chon = None
            for ky_hieu in THU_TU_BU[o]:
                duoc_chon = lay_tu(ky_hieu)
                if duoc_chon is not None:
                    break
            if duoc_chon is None:
                continue
            ket_qua.append(duoc_chon)
            da_co.add(duoc_chon["ma_san_pham"])
    return ket_qua


def danh_sach_nguoi_quen(nhom, als, thinh_hanh, item_moi, so_khoi=2):
    """User có lịch sử trong ALS: mẫu A hoặc B tùy nhóm."""
    return lap_theo_mau(MAU_KHOI[nhom], {"a": als, "t": thinh_hanh, "m": item_moi, "d": []}, so_khoi)


def danh_sach_it_tuong_tac(ban_chay_danh_muc, thinh_hanh, so_khoi=2):
    """User có dưới 5 tương tác: bán chạy theo danh mục đã xem, trộn 50/50 thịnh hành."""
    return lap_theo_mau(MAU_KHOI["IT"], {"d": ban_chay_danh_muc, "t": thinh_hanh, "a": [], "m": []}, so_khoi)


def danh_sach_nguoi_moi(thinh_hanh, so_khoi=2):
    """User mới hoàn toàn: top thịnh hành 15 phút gần nhất."""
    return lap_theo_mau(MAU_KHOI["MOI"], {"t": thinh_hanh, "a": [], "m": [], "d": []}, so_khoi)


TIEN_TO_V2 = "als_v2_"


def ten_phien_ban_mo_hinh(ma_chay, la_v2):
    """Tên phiên bản mô hình ghi vào gold: als_<ma_chay> hoặc als_v2_<ma_chay>."""
    return (TIEN_TO_V2 if la_v2 else "als_") + str(ma_chay)


def la_mo_hinh_v2(phien_ban_mo_hinh):
    return str(phien_ban_mo_hinh or "").startswith(TIEN_TO_V2)


def loc_theo_tam_gia(als, gia_thap, gia_cao, toi_thieu=10):
    """Mô hình v2: chỉ giữ gợi ý ALS nằm trong tầm giá của user, giữ nguyên thứ hạng.

    als: list gợi ý đã xếp hạng, mỗi dict có thể có khóa "gia" (None = chưa biết giá).
    - User chưa xác định được tầm giá (gia_thap/gia_cao là None): không lọc.
    - Item chưa biết giá: giữ lại, vì không đủ thông tin để loại.
    - Nếu sau khi lọc còn ít hơn toi_thieu món, nối các món ngoài tầm giá vào cuối theo
      thứ hạng cũ, để khối hiển thị 10 ô không bị thiếu (ràng buộc khi đó thành ưu tiên).
    """
    if gia_thap is None or gia_cao is None:
        return list(als)
    trong, ngoai = [], []
    for goi_y in als:
        gia = goi_y.get("gia")
        if gia is None or gia_thap <= gia <= gia_cao:
            trong.append(goi_y)
        else:
            ngoai.append(goi_y)
    if len(trong) >= toi_thieu:
        return trong
    return trong + ngoai


def bo_sung_thinh_hanh(tu_luong, du_phong, so_luong):
    """Danh sách thịnh hành cho bước trộn batch.

    Cửa sổ 15 phút mới nhất có thể chỉ còn vài sản phẩm khi luồng vắng (producer dừng, chỉ còn
    vài click thử): khi đó cả 265.000 danh sách sẽ lưu cùng một món ở ô thịnh hành, các ô còn
    lại bị lấp bằng nguồn khác. Thiếu bao nhiêu thì bù bằng danh sách xem nhiều 24 giờ, không
    trùng, giữ thứ tự của luồng trước.
    """
    ket_qua = list(tu_luong[:so_luong])
    da_co = set(g["ma_san_pham"] for g in ket_qua)
    for g in du_phong:
        if len(ket_qua) >= so_luong:
            break
        if g["ma_san_pham"] not in da_co:
            ket_qua.append(g)
            da_co.add(g["ma_san_pham"])
    return ket_qua


def thay_o_thinh_hanh(danh_sach, thinh_hanh_moi):
    """Web thay các ô nguồn thịnh hành bằng dữ liệu mới nhất từ luồng, giữ nguyên vị trí."""
    da_co = set(g["ma_san_pham"] for g in danh_sach if g["nguon"] != NGUON_THINH_HANH)
    con = [g for g in thinh_hanh_moi if g["ma_san_pham"] not in da_co]
    ket_qua = []
    for goi_y in danh_sach:
        if goi_y["nguon"] == NGUON_THINH_HANH and con:
            moi = con.pop(0)
            da_co.add(moi["ma_san_pham"])
            ket_qua.append(moi)
        else:
            ket_qua.append(goi_y)
    return ket_qua


def xep_ung_vien_phuc_vu(ung_vien, novelty_theo_item, beta, so_lay):
    """Danh sách ALS phục vụ: (item, diem_als) đã sắp theo điểm ALS -> so_lay món.

    beta > 0: xếp lại theo xep_lai_novelty trên toàn bộ ứng viên lấy dư; beta = 0: giữ thứ tự
    ALS. Điểm trả về luôn là điểm ALS gốc (diem_du_doan), thứ tự mới nằm ở vị trí trong list.
    """
    if beta <= 0:
        return list(ung_vien[:so_lay])
    diem = dict(ung_vien)
    return [(i, diem[i]) for i in xep_lai_novelty(ung_vien, novelty_theo_item, beta, so_lay)]


def xep_lai_novelty(ung_vien, novelty_theo_item, beta, k):
    """Giảm popularity bias: điểm mới = điểm ALS chuẩn hóa + beta x novelty chuẩn hóa.

    ung_vien: list (item, diem_als) đã lấy dư (ví dụ top-50).
    """
    if not ung_vien:
        return []

    def chuan_hoa(gia_tri):
        nho = min(gia_tri)
        lon = max(gia_tri)
        if lon <= nho:
            return [0.0 for _ in gia_tri]
        return [(x - nho) / (lon - nho) for x in gia_tri]

    diem = chuan_hoa([d for _, d in ung_vien])
    nov = chuan_hoa([novelty_theo_item.get(i, 0.0) for i, _ in ung_vien])
    xep = sorted(
        zip(ung_vien, diem, nov),
        key=lambda x: (-(x[1] + beta * x[2]), x[0][0]),
    )
    return [item for (item, _), _, _ in xep[:k]]
