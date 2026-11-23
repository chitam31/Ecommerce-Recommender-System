"""Web giả lập sàn TMĐT của đề tài 3 (mục 3.8). 

Màn hình             API
-------------------  ------------------------------------------------------------
Trang chủ  /         GET /api/goi-y/{user_id}, POST /api/hien-thi
Sản phẩm   /san-pham POST /api/su-kien  (Xem / Thêm giỏ / Mua -> Kafka)
A/B        /ab       GET /api/ab/ket-qua
Mô hình    /mo-hinh  GET /api/mo-hinh/chi-so
Tái lập    /tai-lap  POST /api/tai-lap/{ngay}  (ghi kết quả chạy lại vào Delta gold.ket_qua_tai_lap)

Ghi ngược vào Delta (mục A.6.4 đề cương), hai đường:
  - Trực tiếp: "Chạy lại" ở màn hình Tái lập ghi một dòng loai='ab_web' vào gold.ket_qua_tai_lap
    bằng phiên Spark + Delta ngay trong tiến trình web (ghi nền, không làm chậm phản hồi).
  - Qua Kafka: mỗi lần hiển thị/nhấp/mua -> topic hien-thi -> luồng ghi_hien_thi MERGE vào
    gold.nhat_ky_hien_thi; Xem/Thêm giỏ/Mua -> topic retailrocket-events -> bronze -> thịnh hành.
Hiệu năng (mục A.8, phản hồi < 3 giây): mọi phản hồi có header X-Thoi-Gian-Xu-Ly-Ms,
GET /api/hieu-nang trả trung vị, p95 theo từng API.

Chạy:  uvicorn app:app --host 0.0.0.0 --port 8000   (thư mục web, PYTHONPATH có jobs/)
Biến môi trường:
  WEB_CHE_DO = delta (mặc định, đọc cụm thật) | gia (dữ liệu giả lập, không cần cụm)
  WEB_KAFKA  = bat (mặc định ở chế độ delta) | tat (chỉ ghi log, dùng khi thử giao diện)
"""
import hashlib
import logging
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from common import cau_hinh as ch
from common.thong_ke import co_mau_can_thiet, ket_luan, ztest_hai_ty_le
from common.tron import chon_nhom
from dich_vu.goi_y import goi_y_trang_chu
from dich_vu.gui_su_kien import GuiBoNho, GuiKafka, thong_diep_click_stream
from dich_vu import minh_hoa
from dich_vu.do_hieu_nang import ThongKeThoiGian
from dich_vu.kho_du_lieu import lay_kho
from dich_vu.phien import BoNhoPhien

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
THU_MUC = Path(__file__).resolve().parent


class YeuCauHienThi(BaseModel):
    ma_nguoi_dung: str
    danh_sach: List[str]
    chien_luoc: Optional[str] = None
    nguon_traffic: str = "web"


class YeuCauSuKien(BaseModel):
    ma_nguoi_dung: str
    ma_san_pham: str
    loai: str
    ma_hien_thi: Optional[str] = None


def create_app(kho=None, gui=None, phien=None):
    che_do = os.environ.get("WEB_CHE_DO", "delta")
    kho = kho or lay_kho(che_do)
    if gui is None:
        bat_kafka = os.environ.get("WEB_KAFKA", "tat" if che_do == "gia" else "bat") == "bat"
        gui = GuiKafka() if bat_kafka else GuiBoNho()
    phien = phien or BoNhoPhien()

    @asynccontextmanager
    async def vong_doi(_app):
        """Làm nóng bộ đệm trong luồng nền khi máy chủ khởi động (chỉ với kho Delta thật; tắt bằng
        WEB_LAM_NONG=0). Đặt trong lifespan chứ không chạy ngay trong create_app: module này tạo
        `app` lúc import, và import (vd. khi pytest thu thập kiểm thử) không được mở phiên Spark."""
        if hasattr(kho, "lam_nong") and os.environ.get("WEB_LAM_NONG", "1") != "0":
            threading.Thread(target=kho.lam_nong, name="lam-nong", daemon=True).start()
        yield

    app = FastAPI(title="bd03 - Hệ khuyến nghị sản phẩm TMĐT", lifespan=vong_doi)
    app.mount("/static", StaticFiles(directory=str(THU_MUC / "static")), name="static")
    mau = Jinja2Templates(directory=str(THU_MUC / "templates"))
    mau.env.globals["gia_lap"] = bool(getattr(kho, "la_gia_lap", False))
    mau.env.filters["gia"] = minh_hoa.dinh_dang_gia
    # Phiên bản tệp tĩnh = băm nội dung: trình duyệt đã lưu bản CSS cũ vẫn tải bản mới sau khi cập nhật.
    mau.env.globals["phien_ban_tinh"] = hashlib.md5(
        b"".join((THU_MUC / "static" / t).read_bytes() for t in ("style.css", "bieu_do.js"))
    ).hexdigest()[:8]
    goc_danh_muc = getattr(kho, "danh_muc_goc", lambda ma_danh_muc: None)

    def trang_tri(sp):
        """Thêm ngành hàng, tên hiển thị và ảnh minh họa (RetailRocket ẩn danh, xem dich_vu/minh_hoa.py)."""
        return dict(sp, **minh_hoa.trang_tri(sp, goc_danh_muc(sp.get("ma_danh_muc"))))
    app.state.kho, app.state.gui, app.state.phien = kho, gui, phien
    hieu_nang = ThongKeThoiGian()
    app.state.hieu_nang = hieu_nang

    @app.middleware("http")
    async def do_thoi_gian(request: Request, call_next):
        """Đo thời gian xử lý mỗi yêu cầu (không tính tệp tĩnh) theo mẫu route."""
        bat_dau = time.perf_counter()
        phan_hoi = await call_next(request)
        ms = (time.perf_counter() - bat_dau) * 1000
        phan_hoi.headers["X-Thoi-Gian-Xu-Ly-Ms"] = "{:.1f}".format(ms)
        route = request.scope.get("route")
        duong = getattr(route, "path", None) or request.url.path
        if not duong.startswith("/static"):
            hieu_nang.ghi(duong, request.method, ms)
        return phan_hoi

    # ------------------------------------------------------------ nghiệp vụ
    def ghi_hien_thi(ma_nguoi_dung, danh_sach, chien_luoc, nguon_traffic):
        """Mỗi sản phẩm được hiển thị là một dòng impression; trả về mã của từng dòng."""
        nhom = chon_nhom(ma_nguoi_dung)
        bay_gio = int(time.time() * 1000)
        ma = []
        for vi_tri, ma_san_pham in enumerate(danh_sach, start=1):
            ma_hien_thi = uuid.uuid4().hex
            gui.gui(
                ch.TOPIC_HIEN_THI,
                ma_nguoi_dung,
                {
                    "loai": "hien_thi",
                    "ma_hien_thi": ma_hien_thi,
                    "ma_nguoi_dung": ma_nguoi_dung,
                    "nhom_ab": nhom,
                    "vi_tri": vi_tri,
                    "ma_san_pham": ma_san_pham,
                    "thoi_diem_ms": bay_gio,
                    "nguon_traffic": nguon_traffic,
                    "chien_luoc": chien_luoc,
                },
            )
            ma.append(ma_hien_thi)
        return nhom, ma

    def phan_hoi(ma_nguoi_dung, ma_hien_thi, hanh_dong):
        gui.gui(
            ch.TOPIC_HIEN_THI,
            ma_nguoi_dung,
            {"loai": "phan_hoi", "ma_hien_thi": ma_hien_thi, "ma_nguoi_dung": ma_nguoi_dung,
             "hanh_dong": hanh_dong, "thoi_diem_ms": int(time.time() * 1000)},
        )

    def lam_giau(ds):
        """Gắn thông tin danh mục và giá cho từng gợi ý để hiển thị."""
        return [trang_tri(dict(g, **{k: v for k, v in kho.san_pham(g["ma_san_pham"]).items() if k != "ma_san_pham"})) for g in ds]

    def goi_y_va_ghi(ma_nguoi_dung, ghi, nguon_traffic):
        kq = goi_y_trang_chu(ma_nguoi_dung, kho, phien)
        kq["goi_y"] = lam_giau(kq["goi_y"])
        kq["thinh_hanh"] = lam_giau(kq["thinh_hanh"])
        if ghi:
            _, ma = ghi_hien_thi(ma_nguoi_dung, [g["ma_san_pham"] for g in kq["goi_y"]], kq["chien_luoc"], nguon_traffic)
            for g, m in zip(kq["goi_y"], ma):
                g["ma_hien_thi"] = m
            gui.day_ngay()
        return kq

    def tong_hop_ab(dong):
        """Gộp toàn bộ các ngày theo nhóm và kiểm định z hai tỷ lệ trên tổng."""
        tong = {}
        for d in dong:
            t = tong.setdefault(d["nhom_ab"], {"so_hien_thi": 0, "so_click": 0, "so_mua": 0, "mo_phong": 0.0})
            t["so_hien_thi"] += d["so_hien_thi"]
            t["so_click"] += d["so_click"]
            t["so_mua"] += d["so_mua"]
            t["mo_phong"] += (d.get("ty_le_mo_phong") or 0.0) * d["so_hien_thi"]
        for t in tong.values():
            t["ctr"] = t["so_click"] / t["so_hien_thi"] if t["so_hien_thi"] else None
            t["ty_le_chuyen_doi"] = t["so_mua"] / t["so_hien_thi"] if t["so_hien_thi"] else None
            t["ty_le_mo_phong"] = t.pop("mo_phong") / t["so_hien_thi"] if t["so_hien_thi"] else None
        kq = {"theo_nhom": tong, "p_value": None, "p_value_chuyen_doi": None}
        if "A" in tong and "B" in tong:
            a, b = tong["A"], tong["B"]
            kq["z"], kq["p_value"] = ztest_hai_ty_le(a["so_click"], a["so_hien_thi"], b["so_click"], b["so_hien_thi"])
            _, kq["p_value_chuyen_doi"] = ztest_hai_ty_le(a["so_mua"], a["so_hien_thi"], b["so_mua"], b["so_hien_thi"])
            if a["ctr"]:
                kq["co_mau_can_thiet_moi_nhom"] = co_mau_can_thiet(a["ctr"], 0.10)
                kq["chenh_lech_ctr_tuong_doi"] = (b["ctr"] - a["ctr"]) / a["ctr"]
        kq["ket_luan"] = ket_luan(kq["p_value"])
        kq["ket_luan_chuyen_doi"] = ket_luan(kq["p_value_chuyen_doi"])
        return kq

    # ------------------------------------------------------------ màn hình
    @app.get("/", response_class=HTMLResponse)
    def trang_chu(request: Request, u: Optional[str] = None, nguon: str = "web"):
        mau_nguoi = kho.nguoi_dung_mau()
        kq = goi_y_va_ghi(u, True, nguon) if u else None
        return mau.TemplateResponse(request, "trang_chu.html", {"u": u, "kq": kq, "mau_nguoi": mau_nguoi})

    @app.get("/nguoi-dung-moi")
    def nguoi_dung_moi():
        """Tạo user mới hoàn toàn tại chỗ để demo cold start."""
        return RedirectResponse("/?u=moi_{}".format(uuid.uuid4().hex[:8]), status_code=303)

    @app.get("/san-pham/{ma_san_pham}", response_class=HTMLResponse)
    def trang_san_pham(request: Request, ma_san_pham: str, u: Optional[str] = None, mh: Optional[str] = None):
        if u and mh:
            phan_hoi(u, mh, "click")
            gui.day_ngay()
        return mau.TemplateResponse(
            request,
            "san_pham.html",
            {
                "u": u,
                "mh": mh,
                "sp": trang_tri(kho.san_pham(ma_san_pham)),
                "lien_quan": lam_giau(kho.lien_quan(ma_san_pham)),
                "thinh_hanh": kho.thinh_hanh_san_pham(ma_san_pham),
            },
        )

    @app.get("/anh/{khoa}/{ma_san_pham}.svg")
    def anh_san_pham(khoa: str, ma_san_pham: str):
        """Ảnh minh họa vẽ tại chỗ theo ngành hàng ghi trong địa chỉ; không phải ảnh thật của sản phẩm."""
        if not minh_hoa.co_nganh(khoa):
            raise HTTPException(status_code=404, detail="Không có ngành hàng minh họa " + khoa)
        return Response(minh_hoa.anh_svg(ma_san_pham, khoa=khoa), media_type="image/svg+xml",
                        headers={"Cache-Control": "public, max-age=86400"})

    @app.get("/ab", response_class=HTMLResponse)
    def trang_ab(request: Request):
        dong = kho.ket_qua_ab()
        return mau.TemplateResponse(request, "ab.html", {"dong": dong, "tong": tong_hop_ab(dong)})

    @app.get("/mo-hinh", response_class=HTMLResponse)
    def trang_mo_hinh(request: Request):
        return mau.TemplateResponse(request, "mo_hinh.html", chi_so_mo_hinh())

    @app.get("/tai-lap", response_class=HTMLResponse)
    def trang_tai_lap(request: Request):
        ngay = sorted({(d["ngay"], d["phien_ban_delta_du_lieu"]) for d in kho.ket_qua_ab()}, reverse=True)
        return mau.TemplateResponse(
            request, "tai_lap.html", {"cac_ngay": ngay, "tai_lap_mo_hinh": kho.ket_qua_tai_lap(),
                                      "lich_su": kho.lich_su_mo_hinh()}
        )

    # ------------------------------------------------------------ API
    @app.get("/api/goi-y/{user_id}")
    def api_goi_y(user_id: str, ghi_hien_thi: bool = False, nguon_traffic: str = "web"):
        return goi_y_va_ghi(user_id, ghi_hien_thi, nguon_traffic)

    @app.post("/api/hien-thi")
    def api_hien_thi(yc: YeuCauHienThi):
        nhom, ma = ghi_hien_thi(yc.ma_nguoi_dung, yc.danh_sach, yc.chien_luoc, yc.nguon_traffic)
        gui.day_ngay()
        return {"nhom_ab": nhom, "ma_hien_thi": ma}

    @app.post("/api/su-kien")
    def api_su_kien(yc: YeuCauSuKien):
        if yc.loai not in ("xem", "them_gio", "mua"):
            raise HTTPException(400, "loai phải là xem, them_gio hoặc mua")
        dm = kho.san_pham(yc.ma_san_pham).get("ma_danh_muc")
        gui.gui(ch.TOPIC_SU_KIEN, yc.ma_nguoi_dung, thong_diep_click_stream(yc.ma_nguoi_dung, yc.ma_san_pham, yc.loai, dm))
        if yc.ma_hien_thi and yc.loai == "mua":
            phan_hoi(yc.ma_nguoi_dung, yc.ma_hien_thi, "mua")
        gui.day_ngay()
        phien.ghi(yc.ma_nguoi_dung, yc.ma_san_pham, yc.loai, dm)
        if yc.loai == "them_gio" and hasattr(kho, "ghi_nhan_them_gio"):
            kho.ghi_nhan_them_gio(yc.ma_san_pham)
        return {"da_gui": True, "luc_ms": int(time.time() * 1000), "so_tuong_tac_phien": phien.so_tuong_tac(yc.ma_nguoi_dung)}

    @app.get("/api/thinh-hanh")
    def api_thinh_hanh(n: int = 10):
        return lam_giau(kho.thinh_hanh(n))

    @app.get("/api/thinh-hanh/{ma_san_pham}")
    def api_thinh_hanh_san_pham(ma_san_pham: str, bat_dau_ms: Optional[int] = None):
        """Không tham số: cửa sổ 15 phút đang chạy. bat_dau_ms: cửa sổ bắt đầu ở phút đó (đo độ trễ)."""
        return kho.thinh_hanh_san_pham(ma_san_pham, bat_dau_ms) or {}

    @app.get("/api/ab/ket-qua")
    def api_ab():
        dong = kho.ket_qua_ab()
        return {"theo_ngay": dong, "tong_hop": tong_hop_ab(dong)}

    def chi_so_mo_hinh():
        lich_su = kho.lich_su_mo_hinh()
        moi_nhat = lich_su[-1] if lich_su else None
        return {
            "lich_su": [{k: v for k, v in d.items() if k != "chi_tiet"} for d in lich_su],
            "moi_nhat": moi_nhat,
            "tuning": kho.tuning(moi_nhat["ma_chay"]) if moi_nhat else [],
            "zorder": kho.do_zorder(),
            "nguong": {"tang_ndcg": ch.NGUONG_TANG_NDCG, "coverage": ch.NGUONG_COVERAGE},
        }

    @app.get("/api/mo-hinh/chi-so")
    def api_mo_hinh():
        return chi_so_mo_hinh()

    def ghi_tai_lap_vao_delta(kq):
        """Ghi kết quả "Chạy lại" vào Delta gold.ket_qua_tai_lap (chạy nền sau khi trả phản hồi)."""
        import json as _json

        dong = {
            "thoi_diem": datetime.utcnow(),
            "loai": "ab_web",
            "khoa": kq["ngay"],
            "phien_ban_delta": int(kq["phien_ban_delta_du_lieu"]),
            "chi_so_cu_json": _json.dumps(kq["cu"], ensure_ascii=False, default=str),
            "chi_so_moi_json": _json.dumps(kq["moi"], ensure_ascii=False, default=str),
            "lech_lon_nhat": float(kq["lech_lon_nhat"]),
            "trung_khop": bool(kq["trung_khop"]),
        }
        try:
            phien_ban = kho.ghi_ket_qua_tai_lap(dong)
            logging.getLogger("web").info("Đã ghi gold.ket_qua_tai_lap phiên bản %s (ngày %s)", phien_ban, kq["ngay"])
        except Exception:  # ghi nền: lỗi chỉ vào log, phản hồi đã trả cho người dùng
            logging.getLogger("web").exception("Ghi gold.ket_qua_tai_lap thất bại")

    @app.post("/api/tai-lap/{ngay}")
    def api_tai_lap(ngay: str, nen: BackgroundTasks):
        kq = kho.tai_lap_ab(ngay)
        if kq is None:
            raise HTTPException(404, "Chưa có kết quả A/B cho ngày {}".format(ngay))
        cu = {d["nhom_ab"]: d for d in kq["cu"]}
        so_sanh = []
        lech = 0.0
        for d in kq["moi"]:
            for k in ("so_hien_thi", "so_click", "so_mua", "ctr", "ty_le_chuyen_doi", "p_value"):
                a, b = cu.get(d["nhom_ab"], {}).get(k), d.get(k)
                khac = None if a is None or b is None else abs(float(a) - float(b))
                if khac is not None:
                    lech = max(lech, khac)
                so_sanh.append({"nhom_ab": d["nhom_ab"], "chi_so": k, "cu": a, "moi": b, "lech": khac})
        kq["so_sanh"] = so_sanh
        kq["lech_lon_nhat"] = lech
        kq["trung_khop"] = lech == 0.0 and len(kq["moi"]) == len(kq["cu"])
        if hasattr(kho, "ghi_ket_qua_tai_lap"):
            nen.add_task(ghi_tai_lap_vao_delta, dict(kq))
            kq["ghi_delta"] = {"bang": "gold.ket_qua_tai_lap", "loai": "ab_web", "trang_thai": "dang_ghi_nen"}
        return kq

    @app.post("/api/tai-lap-mo-hinh")
    def api_tai_lap_mo_hinh(phien_ban_delta: int, che_do: str = "mo_hinh_chon"):
        """Kích hoạt DAG kn_tai_lap qua REST API của Airflow (nếu đã cấu hình AIRFLOW_URL)."""
        from dich_vu.airflow_api import kich_hoat_dag

        try:
            return kich_hoat_dag("kn_tai_lap", {"phien_ban_delta": phien_ban_delta, "che_do": che_do})
        except Exception as loi:  # trả lỗi rõ ràng thay vì 500 trống
            return JSONResponse({"loi": str(loi)}, status_code=502)

    @app.get("/api/nguoi-dung-mau")
    def api_nguoi_dung_mau(n: int = 12):
        return kho.nguoi_dung_mau(n)

    @app.get("/api/hieu-nang")
    def api_hieu_nang():
        """Thời gian phản hồi đo trong tiến trình web, theo từng API (ngưỡng 3 giây)."""
        return hieu_nang.tom_tat()

    @app.get("/api/suc-khoe")
    def suc_khoe():
        return {"che_do": "gia" if getattr(kho, "la_gia_lap", False) else "delta", "kafka": type(gui).__name__}

    return app


app = create_app()
