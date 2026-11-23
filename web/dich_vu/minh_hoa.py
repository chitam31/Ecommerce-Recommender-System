"""Lớp hiển thị minh họa cho sản phẩm (tên ngành hàng, tên hiển thị, ảnh SVG).

RetailRocket đã ẩn danh: không có tên sản phẩm, không có ảnh, danh mục chỉ là mã số và các
thuộc tính khác đã băm. Để giao diện trông như một sàn TMĐT, mỗi danh mục CẤP 2 trong cây danh
mục (con trực tiếp của một gốc) được gán cố định một ngành hàng minh họa (điện thoại, thời trang...), và mỗi sản phẩm có
ảnh SVG vẽ tại chỗ theo ngành hàng của nó. Mọi thứ ở đây chỉ để trình bày:
  - không đi vào bất kỳ bảng dữ liệu, mô hình hay số đo nào;
  - tất định (cùng mã -> cùng ngành hàng, cùng ảnh), không cần Internet;
  - ảnh luôn có chữ "ảnh minh họa", chân trang web nói rõ nguồn gốc.
"""
import hashlib
import math
from xml.sax.saxutils import escape

# khóa, tên ngành hàng, danh từ sản phẩm, màu đậm, màu nhạt
NGANH_HANG = [
    ("dien_thoai", "Điện thoại & Phụ kiện", "Điện thoại", "#3554A5", "#DCE4F7"),
    ("may_tinh", "Máy tính & Laptop", "Laptop", "#2F4858", "#DDE6EC"),
    ("am_thanh", "Thiết bị âm thanh", "Tai nghe", "#6B3FA0", "#E9E0F5"),
    ("dong_ho", "Đồng hồ", "Đồng hồ", "#7A5C1E", "#F3EAD3"),
    ("may_anh", "Máy ảnh & Quay phim", "Máy ảnh", "#374151", "#E5E7EB"),
    ("thoi_trang_nam", "Thời trang nam", "Áo thun", "#1F6F8B", "#D8EDF4"),
    ("thoi_trang_nu", "Thời trang nữ", "Đầm", "#B03A6B", "#F7DDE8"),
    ("giay_dep", "Giày dép", "Giày", "#C2541B", "#FBE3D3"),
    ("tui_xach", "Túi xách & Ví", "Túi xách", "#8A4B2A", "#F1E1D6"),
    ("my_pham", "Mỹ phẩm & Làm đẹp", "Mỹ phẩm", "#B4466F", "#F8E1EA"),
    ("noi_that", "Nhà cửa & Nội thất", "Đèn trang trí", "#5B6B2F", "#E8EDD8"),
    ("nha_bep", "Đồ dùng nhà bếp", "Nồi", "#9B2F2F", "#F5DEDE"),
    ("me_be", "Mẹ & Bé", "Bình sữa", "#2E8B7A", "#D9F0EB"),
    ("the_thao", "Thể thao & Dã ngoại", "Tạ tay", "#2D6A4F", "#DCEFE4"),
    ("sach", "Sách & Văn phòng phẩm", "Sách", "#4A4E8C", "#E2E3F3"),
    ("do_choi", "Đồ chơi & Game", "Tay cầm game", "#C0392B", "#F9E0DC"),
]

NHAN_NGUON = {
    "als": "Gợi ý ALS",
    "thinh_hanh": "Thịnh hành",
    "pho_bien_danh_muc": "Theo danh mục",
    "dong_xuat_hien": "Hay xem cùng",
}


def _so(chuoi):
    return int(hashlib.md5(str(chuoi).encode("utf-8")).hexdigest()[:8], 16)


def _dict_nganh(dong):
    k, ten, loai, dam, nhat = dong
    return {"khoa": k, "ten": ten, "loai": loai, "mau_dam": dam, "mau_nhat": nhat}


_THEO_KHOA = {d[0]: _dict_nganh(d) for d in NGANH_HANG}


def co_nganh(khoa):
    return khoa in _THEO_KHOA


def nganh_hang(ma_danh_muc_goc, ma_san_pham=None):
    """Ngành hàng minh họa của một danh mục (thiếu danh mục thì theo mã sản phẩm)."""
    khoa = ma_danh_muc_goc if ma_danh_muc_goc not in (None, "") else "sp:" + str(ma_san_pham)
    return _dict_nganh(NGANH_HANG[_so(khoa) % len(NGANH_HANG)])


def trang_tri(sp, ma_danh_muc_goc=None):
    """Các trường hiển thị thêm cho một sản phẩm (dict có ma_san_pham, ma_danh_muc, ...)."""
    ma = str(sp.get("ma_san_pham"))
    nh = nganh_hang(ma_danh_muc_goc if ma_danh_muc_goc is not None else sp.get("ma_danh_muc"), ma)
    return {
        "nganh": nh,
        "ten_hien_thi": "{} {}".format(nh["loai"], ma),
        # Khóa ngành hàng nằm ngay trong địa chỉ ảnh: chữ trên thẻ và ảnh không thể lệch nhau,
        # kể cả khi trình duyệt còn giữ ảnh vẽ theo cách gán cũ trong bộ nhớ đệm.
        "anh": "/anh/{}/{}.svg".format(nh["khoa"], ma),
        "nhan_nguon": NHAN_NGUON.get(sp.get("nguon"), sp.get("nguon")),
    }


def dinh_dang_gia(gia):
    """479880.0 -> '479.880 ₫' (giá của RetailRocket đã ẩn danh, chỉ để so sánh tương đối)."""
    if gia is None:
        return None
    return "{:,.0f} ₫".format(float(gia)).replace(",", ".")


# ---------------------------------------------------------------- hình vẽ (hệ tọa độ 0..100)
def _hinh(khoa, dam, sang):
    """Các phần tử SVG của biểu tượng ngành hàng, vẽ trong ô 100 x 100."""
    w = "#FFFFFF"
    h = {
        "dien_thoai": [
            '<rect x="30" y="8" width="40" height="84" rx="8" fill="{d}"/>',
            '<rect x="34" y="16" width="32" height="62" rx="3" fill="{s}"/>',
            '<rect x="44" y="11" width="12" height="2.5" rx="1.2" fill="{w}"/>',
            '<circle cx="50" cy="85" r="3" fill="{w}"/>',
            '<rect x="38" y="22" width="24" height="14" rx="2" fill="{w}" opacity=".55"/>',
            '<rect x="38" y="40" width="16" height="3" rx="1.5" fill="{w}" opacity=".55"/>',
        ],
        "may_tinh": [
            '<rect x="16" y="20" width="68" height="46" rx="4" fill="{d}"/>',
            '<rect x="21" y="25" width="58" height="36" rx="2" fill="{s}"/>',
            '<path d="M8 70 H92 L86 80 H14 Z" fill="{d}"/>',
            '<rect x="42" y="72" width="16" height="3" rx="1.5" fill="{w}" opacity=".6"/>',
            '<rect x="27" y="31" width="30" height="4" rx="2" fill="{w}" opacity=".6"/>',
            '<rect x="27" y="39" width="20" height="4" rx="2" fill="{w}" opacity=".6"/>',
        ],
        "am_thanh": [
            '<path d="M20 56 V48 A30 30 0 0 1 80 48 V56" fill="none" stroke="{d}" stroke-width="7" stroke-linecap="round"/>',
            '<rect x="12" y="52" width="18" height="30" rx="8" fill="{d}"/>',
            '<rect x="70" y="52" width="18" height="30" rx="8" fill="{d}"/>',
            '<rect x="17" y="58" width="8" height="18" rx="4" fill="{s}"/>',
            '<rect x="75" y="58" width="8" height="18" rx="4" fill="{s}"/>',
        ],
        "dong_ho": [
            '<rect x="38" y="6" width="24" height="24" rx="5" fill="{d}"/>',
            '<rect x="38" y="70" width="24" height="24" rx="5" fill="{d}"/>',
            '<circle cx="50" cy="50" r="27" fill="{d}"/>',
            '<circle cx="50" cy="50" r="21" fill="{s}"/>',
            '<path d="M50 50 V35 M50 50 L60 56" stroke="{d}" stroke-width="3.5" stroke-linecap="round"/>',
            '<rect x="77" y="46" width="6" height="8" rx="2" fill="{d}"/>',
        ],
        "may_anh": [
            '<rect x="10" y="28" width="80" height="54" rx="9" fill="{d}"/>',
            '<path d="M34 28 L40 18 H60 L66 28 Z" fill="{d}"/>',
            '<circle cx="50" cy="55" r="19" fill="{s}"/>',
            '<circle cx="50" cy="55" r="11" fill="{d}"/>',
            '<circle cx="45" cy="50" r="3.5" fill="{w}" opacity=".7"/>',
            '<rect x="72" y="35" width="10" height="6" rx="2" fill="{w}" opacity=".6"/>',
        ],
        "thoi_trang_nam": [
            '<path d="M34 12 L20 18 L6 36 L20 46 L26 38 V90 H74 V38 L80 46 L94 36 L80 18 L66 12 '
            'Q50 26 34 12 Z" fill="{d}"/>',
            '<path d="M38 14 Q50 24 62 14" fill="none" stroke="{s}" stroke-width="3"/>',
            '<rect x="56" y="44" width="10" height="10" rx="2" fill="{s}" opacity=".8"/>',
        ],
        "thoi_trang_nu": [
            '<path d="M40 8 H60 L62 28 Q70 40 66 46 L86 90 H14 L34 46 Q30 40 38 28 Z" fill="{d}"/>',
            '<path d="M34 46 H66" stroke="{s}" stroke-width="4"/>',
            '<path d="M44 8 Q50 18 56 8" fill="none" stroke="{s}" stroke-width="3"/>',
            '<path d="M30 70 Q50 76 70 70" fill="none" stroke="{s}" stroke-width="2.5" opacity=".7"/>',
        ],
        "giay_dep": [
            '<path d="M8 66 Q8 46 22 44 L40 42 Q48 30 58 34 L70 52 Q90 56 92 68 V74 H8 Z" fill="{d}"/>',
            '<rect x="8" y="74" width="84" height="8" rx="3" fill="{s}"/>',
            '<path d="M44 44 L52 54 M50 40 L58 50" stroke="{s}" stroke-width="3" stroke-linecap="round"/>',
            '<path d="M14 62 H60" stroke="{w}" stroke-width="2.5" opacity=".5"/>',
        ],
        "tui_xach": [
            '<path d="M34 34 V26 A16 16 0 0 1 66 26 V34" fill="none" stroke="{d}" stroke-width="6" stroke-linecap="round"/>',
            '<path d="M14 34 H86 L80 88 H20 Z" fill="{d}"/>',
            '<rect x="14" y="34" width="72" height="14" fill="{s}" opacity=".35"/>',
            '<rect x="44" y="50" width="12" height="9" rx="2" fill="{s}"/>',
        ],
        "my_pham": [
            '<rect x="16" y="40" width="30" height="50" rx="7" fill="{d}"/>',
            '<rect x="23" y="26" width="16" height="16" rx="3" fill="{s}"/>',
            '<rect x="20" y="56" width="22" height="14" rx="3" fill="{w}" opacity=".6"/>',
            '<rect x="58" y="44" width="22" height="46" rx="4" fill="{d}"/>',
            '<path d="M61 44 V24 Q69 12 77 24 V44 Z" fill="{s}"/>',
        ],
        "noi_that": [
            '<path d="M30 14 H70 L82 50 H18 Z" fill="{d}"/>',
            '<rect x="47" y="50" width="6" height="30" fill="{d}"/>',
            '<rect x="30" y="80" width="40" height="8" rx="4" fill="{d}"/>',
            '<path d="M36 22 H64" stroke="{s}" stroke-width="3" opacity=".8"/>',
            '<circle cx="50" cy="58" r="4" fill="{s}"/>',
        ],
        "nha_bep": [
            '<path d="M14 42 H86 V70 Q86 86 70 86 H30 Q14 86 14 70 Z" fill="{d}"/>',
            '<rect x="10" y="36" width="80" height="8" rx="4" fill="{d}"/>',
            '<rect x="4" y="50" width="12" height="7" rx="3" fill="{d}"/>',
            '<rect x="84" y="50" width="12" height="7" rx="3" fill="{d}"/>',
            '<rect x="44" y="28" width="12" height="8" rx="3" fill="{s}"/>',
            '<path d="M22 56 H78" stroke="{s}" stroke-width="3" opacity=".6"/>',
        ],
        "me_be": [
            '<rect x="34" y="34" width="32" height="56" rx="10" fill="{d}"/>',
            '<rect x="31" y="26" width="38" height="10" rx="4" fill="{s}"/>',
            '<path d="M42 26 Q42 8 50 8 Q58 8 58 26 Z" fill="{s}"/>',
            '<path d="M40 50 H48 M40 62 H52 M40 74 H48" stroke="{w}" stroke-width="3" stroke-linecap="round" opacity=".7"/>',
        ],
        "the_thao": [
            '<rect x="30" y="46" width="40" height="8" rx="3" fill="{d}"/>',
            '<rect x="16" y="30" width="12" height="40" rx="4" fill="{d}"/>',
            '<rect x="72" y="30" width="12" height="40" rx="4" fill="{d}"/>',
            '<rect x="6" y="38" width="10" height="24" rx="4" fill="{s}"/>',
            '<rect x="84" y="38" width="10" height="24" rx="4" fill="{s}"/>',
        ],
        "sach": [
            '<path d="M50 22 Q34 14 12 18 V82 Q34 78 50 86 Z" fill="{d}"/>',
            '<path d="M50 22 Q66 14 88 18 V82 Q66 78 50 86 Z" fill="{d}" opacity=".85"/>',
            '<path d="M20 32 Q32 30 42 34 M20 44 Q32 42 42 46 M58 34 Q68 30 80 32 M58 46 Q68 42 80 44" '
            'fill="none" stroke="{s}" stroke-width="2.5" stroke-linecap="round"/>',
        ],
        "do_choi": [
            '<path d="M22 36 H78 Q94 36 94 56 Q94 80 80 80 Q72 80 66 70 H34 Q28 80 20 80 Q6 80 6 56 Q6 36 22 36 Z" fill="{d}"/>',
            '<path d="M24 50 V62 M18 56 H30" stroke="{s}" stroke-width="4" stroke-linecap="round"/>',
            '<circle cx="70" cy="51" r="4" fill="{s}"/>',
            '<circle cx="78" cy="59" r="4" fill="{s}"/>',
            '<circle cx="62" cy="59" r="4" fill="{s}" opacity=".7"/>',
        ],
    }[khoa]
    return "".join(x.format(d=dam, s=sang, w=w) for x in h)


def _pha(mau_hex, ty_le):
    """Pha màu với trắng (ty_le > 0) hoặc đen (ty_le < 0)."""
    r, g, b = (int(mau_hex[i:i + 2], 16) for i in (1, 3, 5))
    if ty_le >= 0:
        r, g, b = (round(c + (255 - c) * ty_le) for c in (r, g, b))
    else:
        r, g, b = (round(c * (1 + ty_le)) for c in (r, g, b))
    return "#{:02X}{:02X}{:02X}".format(r, g, b)


def anh_svg(ma_san_pham, ma_danh_muc_goc=None, khoa=None):
    """Ảnh 320 x 320 của một sản phẩm: nền theo ngành hàng, biến thể màu theo mã sản phẩm."""
    nh = _THEO_KHOA[khoa] if khoa else nganh_hang(ma_danh_muc_goc, ma_san_pham)
    so = _so("anh:" + str(ma_san_pham))
    sac = (-0.18, 0.0, 0.22)[so % 3]           # ba biến thể độ đậm của biểu tượng
    dam = _pha(nh["mau_dam"], sac)
    sang = _pha(nh["mau_dam"], 0.62)
    nen1, nen2 = nh["mau_nhat"], _pha(nh["mau_nhat"], 0.55)
    goc = math.radians((so >> 3) % 360)
    x1, y1 = 0.5 - 0.5 * math.cos(goc), 0.5 - 0.5 * math.sin(goc)
    cx, cy = 80 + (so >> 5) % 160, 70 + (so >> 9) % 120
    ma_id = "g" + hashlib.md5(str(ma_san_pham).encode("utf-8")).hexdigest()[:8]
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 320 320" width="320" height="320" role="img" '
        'aria-label="Ảnh minh họa: {ten}">'
        '<defs><linearGradient id="{gid}" x1="{x1:.3f}" y1="{y1:.3f}" x2="{x2:.3f}" y2="{y2:.3f}">'
        '<stop offset="0" stop-color="{n1}"/><stop offset="1" stop-color="{n2}"/></linearGradient></defs>'
        '<rect width="320" height="320" fill="url(#{gid})"/>'
        '<circle cx="{cx}" cy="{cy}" r="120" fill="#FFFFFF" opacity=".35"/>'
        '<ellipse cx="160" cy="262" rx="88" ry="12" fill="{dam}" opacity=".12"/>'
        '<g transform="translate(70 60) scale(1.8)">{hinh}</g>'
        '<text x="308" y="306" text-anchor="end" font-family="Arial, sans-serif" font-size="13" '
        'fill="{dam}" opacity=".55">ảnh minh họa</text>'
        "</svg>"
    ).format(ten=escape(nh["ten"]), gid=ma_id, x1=x1, y1=y1, x2=1 - x1, y2=1 - y1, n1=nen1, n2=nen2, cx=cx, cy=cy, dam=dam, hinh=_hinh(nh["khoa"], dam, sang))
