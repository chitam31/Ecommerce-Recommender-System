"""Web giả lập sàn TMĐT: 5 màn hình, API theo đặc tả 3.8, impression log, vòng khép kín."""
import json

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from app import create_app  # noqa: E402
from common.luoc_do import LUOC_DO_HIEN_THI, LUOC_DO_SU_KIEN  # noqa: E402
from dich_vu.gui_su_kien import GuiBoNho  # noqa: E402
from dich_vu.kho_gia import KhoGia  # noqa: E402


@pytest.fixture
def web():
    gui = GuiBoNho()
    return TestClient(create_app(kho=KhoGia(), gui=gui)), gui


def test_nam_man_hinh(web):
    c, _ = web
    for url in ("/", "/?u=u_als_1", "/san-pham/sp3", "/ab", "/mo-hinh", "/tai-lap"):
        r = c.get(url)
        assert r.status_code == 200, url
        assert "DỮ LIỆU GIẢ LẬP" in r.text


def test_moi_lan_render_ghi_impression(web):
    c, gui = web
    c.get("/?u=u_als_3")
    hien_thi = [m for t, _, m in gui.da_gui if t == "hien-thi"]
    assert len(hien_thi) == 10
    assert [m["vi_tri"] for m in hien_thi] == list(range(1, 11))
    ten_truong = {f.name for f in LUOC_DO_HIEN_THI.fields}
    assert all(set(m) <= ten_truong for m in hien_thi)  # job ghi_hien_thi đọc được mọi trường
    assert len({m["nhom_ab"] for m in hien_thi}) == 1


def test_su_kien_vao_kafka_dung_luoc_do(web):
    c, gui = web
    r = c.post("/api/su-kien", json={"ma_nguoi_dung": "u_als_1", "ma_san_pham": "sp5", "loai": "them_gio", "ma_hien_thi": "h1"})
    assert r.status_code == 200
    topic, khoa, m = gui.da_gui[-1]
    assert topic == "retailrocket-events" and khoa == "u_als_1"
    assert m["event"] == "addtocart" and m["nguon"] == "web" and m["ma_danh_muc"] == "dm5"
    assert set(m) <= {f.name for f in LUOC_DO_SU_KIEN.fields}
    assert c.post("/api/su-kien", json={"ma_nguoi_dung": "u", "ma_san_pham": "sp5", "loai": "xoa"}).status_code == 400


def test_mua_tu_impression_ghi_phan_hoi(web):
    c, gui = web
    c.get("/san-pham/sp5?u=u_als_1&mh=h9")
    c.post("/api/su-kien", json={"ma_nguoi_dung": "u_als_1", "ma_san_pham": "sp5", "loai": "mua", "ma_hien_thi": "h9"})
    phan_hoi = [m["hanh_dong"] for t, _, m in gui.da_gui if t == "hien-thi" and m["loai"] == "phan_hoi"]
    assert phan_hoi == ["click", "mua"]


def test_ba_chien_luoc_cold_start(web):
    c, _ = web
    assert c.get("/api/goi-y/u_als_2").json()["chien_luoc"] == "als"
    assert c.get("/api/goi-y/u_it_1").json()["chien_luoc"] == "it_tuong_tac"
    moi = c.get("/nguoi-dung-moi", follow_redirects=False).headers["location"].split("u=")[1]
    kq = c.get("/api/goi-y/" + moi).json()
    assert kq["chien_luoc"] == "nguoi_moi" and all(g["nguon"] == "thinh_hanh" for g in kq["goi_y"])
    for sp in ("sp3", "sp13"):
        c.post("/api/su-kien", json={"ma_nguoi_dung": moi, "ma_san_pham": sp, "loai": "xem"})
    kq = c.get("/api/goi-y/" + moi).json()
    assert kq["chien_luoc"] == "it_tuong_tac"
    assert sum(g["nguon"] == "thinh_hanh" for g in kq["goi_y"]) == 5
    assert {g.get("ma_danh_muc") for g in kq["goi_y"] if g["nguon"] == "pho_bien_danh_muc"} == {"dm3"}


def test_nhom_ab_on_dinh_va_dung_ty_le(web):
    c, _ = web
    lan = [c.get("/api/goi-y/u_als_4").json() for _ in range(3)]
    assert len({k["nhom_ab"] for k in lan}) == 1
    for u in ("u_als_{}".format(i) for i in range(1, 9)):
        kq = c.get("/api/goi-y/" + u).json()
        so_th = sum(g["nguon"] == "thinh_hanh" for g in kq["goi_y"])
        assert so_th == (3 if kq["nhom_ab"] == "B" else 0)


def test_api_ab_mo_hinh_tai_lap(web):
    c, _ = web
    ab = c.get("/api/ab/ket-qua").json()
    assert ab["tong_hop"]["p_value"] is not None and "ket_luan" in ab["tong_hop"]
    assert len(c.get("/api/mo-hinh/chi-so").json()["tuning"]) >= 9
    r = c.post("/api/tai-lap/2026-11-04")
    assert r.status_code == 200 and r.json()["trung_khop"] is True
    assert c.post("/api/tai-lap/2000-01-01").status_code == 404


def test_thinh_hanh_cap_nhat_sau_them_gio(web):
    c, _ = web
    truoc = c.get("/api/thinh-hanh/sp200").json()["so_luot_them_gio"]
    c.post("/api/su-kien", json={"ma_nguoi_dung": "x", "ma_san_pham": "sp200", "loai": "them_gio"})
    assert c.get("/api/thinh-hanh/sp200").json()["so_luot_them_gio"] == truoc + 1
    assert json.dumps(c.get("/api/thinh-hanh").json())

    # Trang sản phẩm đo độ trễ trên cửa sổ bắt đầu ở phút của lần bấm (tham số bat_dau_ms).
    phut = 60000
    r = c.post("/api/su-kien", json={"ma_nguoi_dung": "x", "ma_san_pham": "sp201", "loai": "them_gio"}).json()
    bd = (r["luc_ms"] - 10000) // phut * phut
    d = c.get("/api/thinh-hanh/sp201", params={"bat_dau_ms": bd}).json()
    assert d["so_luot_them_gio"] == 1 and d["cua_so_bat_dau_ms"] == bd
    assert d["cua_so_ket_thuc_ms"] - d["cua_so_bat_dau_ms"] == 15 * phut
    # Cửa sổ bắt đầu sau lần bấm chưa có lượt nào; sản phẩm chưa ai bấm cũng vậy.
    assert c.get("/api/thinh-hanh/sp201", params={"bat_dau_ms": bd + 20 * phut}).json() == {}
    assert c.get("/api/thinh-hanh/sp202", params={"bat_dau_ms": bd}).json() == {}
    # Không tham số: cửa sổ đang chạy, có mốc epoch để trình duyệt đổi sang giờ địa phương.
    hien = c.get("/api/thinh-hanh/sp201").json()
    assert hien["cua_so_ket_thuc_ms"] > r["luc_ms"] - 1000 and hien["cua_so_bat_dau_ms"] <= r["luc_ms"]
    # Mã trang: đo bằng cửa sổ của lần bấm và giữ dòng kết quả (lỗi buổi tập 17/11).
    html = c.get("/san-pham/sp201?u=x").text
    assert "bat_dau_ms=" in html and 'id="do-tre"' in html and "ĐÃ CẬP NHẬT sau" in html
    assert "soThemGioTruoc" not in html


def test_anh_minh_hoa_va_ghi_chu(web):
    """Lớp minh họa (RetailRocket ẩn danh): ảnh SVG hợp lệ, tất định, có chữ "ảnh minh họa", chân trang nói rõ."""
    import xml.etree.ElementTree as ET

    c, _ = web
    r = c.get("/anh/sach/sp3.svg")
    assert r.status_code == 200 and r.headers["content-type"].startswith("image/svg+xml")
    ET.fromstring(r.text)                                  # XML hợp lệ (tên ngành có "&" đã được escape)
    assert "ảnh minh họa" in r.text and r.text == c.get("/anh/sach/sp3.svg").text
    assert c.get("/anh/khong_co/sp3.svg").status_code == 404
    trang = c.get("/?u=u_als_1").text
    assert "/anh/" in trang and "minh họa" in trang
    assert "/static/style.css?v=" in trang                 # trình duyệt không dùng lại CSS cũ sau khi cập nhật


def test_anh_luon_khop_ten_nganh_tren_the(web):
    """Lỗi 30: ảnh cũ trong bộ nhớ đệm trình duyệt hiện quyển sách dưới thẻ "Tạ tay". Địa chỉ ảnh
    mang khóa ngành hàng nên mọi thẻ và trang sản phẩm có ảnh đúng ngành ghi trên thẻ."""
    import html
    import re

    from dich_vu import minh_hoa

    c, _ = web
    ten = {k: t for k, t, *_ in minh_hoa.NGANH_HANG}
    for url in ("/?u=u_als_1", "/?u=u_it_2", "/?u=moi_x1", "/san-pham/sp3"):
        trang = c.get(url).text
        cap = re.findall(r'<img src="/anh/([a-z_]+)/[^"]+\.svg".*?class="nganh"[^>]*>([^<]+)<', trang, re.S)
        assert cap, url
        assert all(ten[k] == html.unescape(t).strip() for k, t in cap), url


def test_nguoi_dung_ngoai_thi_nghiem_khong_hien_nhan_nhom(web):
    """User mới và user ít tương tác không vào A/B (common/ab chỉ đếm chiến lược als)."""
    c, _ = web
    assert "ngoài thí nghiệm A/B" in c.get("/?u=moi_x2").text
    assert "ngoài thí nghiệm A/B" not in c.get("/?u=u_als_1").text


def test_nganh_hang_theo_danh_muc_cap_2():
    from dich_vu import minh_hoa
    from dich_vu.kho_du_lieu import goc_cua

    cha = {"5": "2", "2": "1", "1": "", "7": "1", "9": "9x", "9x": "9"}   # 9 <-> 9x là vòng lặp lỗi
    assert goc_cua("5", cha) == "2" and goc_cua("2", cha) == "2"   # cấp 2: con trực tiếp của gốc 1
    assert goc_cua("7", cha) == "7" and goc_cua("1", cha) == "1"
    assert goc_cua("9", cha) in ("9", "9x")                         # không treo khi cây có vòng
    a, b = minh_hoa.trang_tri({"ma_san_pham": "1", "ma_danh_muc": "5"}, "1"), minh_hoa.trang_tri({"ma_san_pham": "2"}, "1")
    assert a["nganh"] == b["nganh"]                               # cùng danh mục cấp 2 -> cùng ngành hàng
    assert a["ten_hien_thi"].endswith(" 1") and a["anh"] == "/anh/{}/1.svg".format(a["nganh"]["khoa"])
    assert minh_hoa.dinh_dang_gia(479880.0) == "479.880 ₫" and minh_hoa.dinh_dang_gia(None) is None
    import xml.etree.ElementTree as ET
    for i in range(64):
        ET.fromstring(minh_hoa.anh_svg(str(i), str(i)))


def test_ghi_nguoc_delta_khi_chay_lai_tai_lap(web):
    """Mục A.6.4: "Chạy lại" ghi một dòng loai='ab_web' vào gold.ket_qua_tai_lap (KhoGia giữ trong bộ nhớ)."""
    c, _ = web
    ngay = c.get("/api/ab/ket-qua").json()["theo_ngay"][0]["ngay"]
    r = c.post("/api/tai-lap/" + ngay)
    assert r.status_code == 200 and r.json()["ghi_delta"]["bang"] == "gold.ket_qua_tai_lap"
    ghi = c.app.state.kho.ket_qua_tai_lap()            # tác vụ nền đã chạy xong khi TestClient trả phản hồi
    assert len(ghi) == 1 and ghi[0]["loai"] == "ab_web" and ghi[0]["khoa"] == ngay and ghi[0]["trung_khop"] is True
    assert json.loads(ghi[0]["chi_so_cu_json"]) and isinstance(ghi[0]["phien_ban_delta"], int)
    assert "ab_web" in c.get("/tai-lap").text


def test_do_thoi_gian_phan_hoi(web):
    """Mục A.8: header X-Thoi-Gian-Xu-Ly-Ms và /api/hieu-nang theo mẫu route, ngưỡng 3 giây."""
    c, _ = web
    for u in ("u_als_1", "u_als_2", "moi_x"):
        r = c.get("/api/goi-y/" + u)
        assert float(r.headers["X-Thoi-Gian-Xu-Ly-Ms"]) >= 0
    c.get("/static/style.css")
    hn = c.get("/api/hieu-nang").json()
    assert hn["nguong_ms"] == 3000.0
    route = hn["theo_route"]["GET /api/goi-y/{user_id}"]
    assert route["so_yeu_cau"] == 3 and route["ty_le_duoi_nguong"] == 1.0 and route["p95_ms"] >= route["trung_vi_ms"]
    assert not any("/static" in k for k in hn["theo_route"])


def test_chart_js_dong_goi_san(web):
    c, _ = web
    r = c.get("/static/vendor/chart.umd.min.js")
    assert r.status_code == 200 and "Chart.js v4" in r.text[:200]
    assert "/static/vendor/chart.umd.min.js" in c.get("/ab").text
    assert "new Chart(" in c.get("/static/bieu_do.js").text


def test_lich_su_mo_hinh_khong_sua_bo_dem():
    """Lỗi 16/11 trên cụm: lich_su_mo_hinh pop("chi_tiet_json") thẳng trên danh sách trong bộ đệm,
    nên tải lại /mo-hinh, /api/mo-hinh/chi-so hay mở /tai-lap trong 60 giây sau đều lỗi 500."""
    from dich_vu.kho_du_lieu import KhoDelta

    bo_dem = [{"ma_chay": "r1", "thoi_diem": "2026-11-11", "chi_tiet_json": '{"ndcg": 0.08}'}]
    kho = KhoDelta.__new__(KhoDelta)
    kho.dem_bao_cao = None
    kho._bang_nho = lambda *a, **k: bo_dem
    for _ in range(3):
        ds = kho.lich_su_mo_hinh()
        assert ds[0]["chi_tiet"] == {"ndcg": 0.08} and "chi_tiet_json" not in ds[0]
    assert bo_dem[0]["chi_tiet_json"] == '{"ndcg": 0.08}' and "chi_tiet" not in bo_dem[0]


def test_lam_nong_chay_moi_buoc_va_khong_lam_hong_web():
    """Làm nóng bộ đệm lúc khởi động: gọi đủ các truy vấn chậm; một bước lỗi không chặn bước sau."""
    from dich_vu.kho_du_lieu import KhoDelta

    kho = KhoDelta.__new__(KhoDelta)
    goi = []

    def ghi(ten, kq=None, loi=False):
        def ham(*a):
            goi.append(ten)
            if loi:
                raise RuntimeError("bảng chưa có")
            return kq
        return ham

    kho.nguoi_dung_mau = ghi("mau", loi=True)
    kho.lich_su_mo_hinh = ghi("lich_su", [{"ma_chay": "r1"}])
    kho.tuning = ghi("tuning")
    for ten in ("do_zorder", "ket_qua_ab", "ket_qua_tai_lap", "thinh_hanh", "_danh_muc_san_pham", "_cay_danh_muc"):
        setattr(kho, ten, ghi(ten))
    xong = kho.lam_nong(in_ra=lambda *a: None)
    assert "nguoi_dung_mau" not in xong and len(xong) == 8
    assert goi[0] == "mau" and "tuning" in goi and goi[-1] == "_cay_danh_muc"


def test_lam_nong_chi_khi_may_chu_khoi_dong():
    """Tạo app (kể cả lúc import web/app.py) không được chạy làm nóng, vì làm nóng mở phiên Spark;
    chỉ sự kiện startup của máy chủ mới chạy. Kho giả không có lam_nong thì không có gì chạy."""
    import threading

    da_goi = threading.Event()

    class KhoCoLamNong(KhoGia):
        def lam_nong(self):
            da_goi.set()

    app = create_app(kho=KhoCoLamNong(), gui=GuiBoNho())
    assert not da_goi.wait(0.3)
    with TestClient(app):
        assert da_goi.wait(5)
    create_app(kho=KhoGia(), gui=GuiBoNho())
