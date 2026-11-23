"""Ba sơ đồ do nhóm thiết kế (ngày 22–23/11/2026, theo góp ý của GVHD), dựng lại bằng HTML + SVG để chữ và nét là vector:
  tong_quan(nn)  : Kiến trúc tổng thể và vai trò các công nghệ (slide 5, Hình 2.1 báo cáo)
  nam_dag(nn)    : Airflow điều phối năm DAG (slide 6, Hình 2.2 báo cáo)
  lakehouse(nn)  : Lakehouse trên HDFS, luồng lưu trữ và luồng tính toán (slide 8, Hình 4.1 báo cáo)
nn = "vi" hoặc "en". Mỗi hàm trả về một khối HTML kích thước cố định (toạ độ theo bản thiết kế gốc), thu phóng bằng tham số
ti_le. gen_vi.py / gen.py chèn khối này vào slide; ../hinh/xuat_so_do.py chụp khối này ở độ phân giải cao cho báo cáo.
Biểu tượng là hình vẽ chung; riêng logo Apache Airflow ở dải điều phối là tệp logo do nhóm cung cấp.
"""

F = "'Times New Roman', Tinos, 'Liberation Serif', Times, serif"
NAVY, TEXT, MUTED = "#14213D", "#232A37", "#5F6B7A"
XANH_LA, XANH_NGOC, XANH_DUONG, DO = "#2E8B3A", "#1A9BA1", "#2F64B5", "#B5403A"

# ---------------------------------------------------------------- biểu tượng chung (24 x 24, nét)
_ICON = {
    "db": '<ellipse cx="12" cy="5.5" rx="7.5" ry="3"/><path d="M4.5 5.5v13c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3v-13"/>'
          '<path d="M4.5 12c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3"/>',
    "play": '<circle cx="12" cy="12" r="9"/><path d="M10 8.5l5 3.5-5 3.5z" fill="currentColor"/><path d="M3 9.5h2M19 14.5h2"/>',
    "nodes": '<circle cx="12" cy="5" r="2.2"/><circle cx="12" cy="19" r="2.2"/><circle cx="5.5" cy="9" r="2"/><circle cx="18.5" cy="9" r="2"/>'
             '<circle cx="5.5" cy="15.5" r="2"/><circle cx="18.5" cy="15.5" r="2"/><circle cx="12" cy="12" r="2.4"/>'
             '<path d="M12 7.2v2.4M12 14.4v2.4M7.3 9.9l2.6 1.2M16.7 9.9l-2.6 1.2M7.3 14.6l2.6-1.3M16.7 14.6l-2.6-1.3"/>',
    "star": '<path d="M12 2.8l2.7 5.9 6.4.7-4.8 4.3 1.4 6.3L12 16.8 6.3 20l1.4-6.3L2.9 9.4l6.4-.7z"/>',
    "layers": '<path d="M12 3l9 4.5-9 4.5-9-4.5z"/><path d="M3 12l9 4.5 9-4.5"/><path d="M3 16.5l9 4.5 9-4.5"/>',
    "code": '<rect x="2.5" y="4" width="19" height="13" rx="1.5"/><path d="M8 21h8M12 17v4"/><path d="M9.5 8.5L7 10.8l2.5 2.2M14.5 8.5l2.5 2.3-2.5 2.2M12.8 8l-1.6 5.6"/>',
    "users": '<circle cx="9" cy="8" r="3.5"/><path d="M2.5 20c0-3.6 2.9-6 6.5-6s6.5 2.4 6.5 6"/><circle cx="17" cy="9" r="2.8"/>'
             '<path d="M16.5 14.2c2.9.2 5 2.3 5 5.3"/>',
    "wifi": '<path d="M2.5 9a14 14 0 0 1 19 0"/><path d="M5.5 12.5a9.5 9.5 0 0 1 13 0"/><path d="M8.6 16a5 5 0 0 1 6.8 0"/>'
            '<circle cx="12" cy="19.3" r="1.2" fill="currentColor"/>',
    "gear": '<circle cx="12" cy="12" r="3.2"/><path d="M12 2.5v3M12 18.5v3M2.5 12h3M18.5 12h3M5.3 5.3l2.1 2.1M16.6 16.6l2.1 2.1'
            'M5.3 18.7l2.1-2.1M16.6 7.4l2.1-2.1"/><circle cx="12" cy="12" r="6.5"/>',
    "calendar": '<rect x="3" y="4.5" width="18" height="16" rx="2"/><path d="M3 9h18M8 2.5v4M16 2.5v4"/>'
                '<path d="M7 12.5h2M11 12.5h2M15 12.5h2M7 16h2M11 16h2M15 16h2"/>',
    "refresh": '<path d="M19.5 9A8 8 0 0 0 5 7.5L3.5 9"/><path d="M3.5 4.5V9H8"/><path d="M4.5 15A8 8 0 0 0 19 16.5l1.5-1.5"/>'
               '<path d="M20.5 19.5V15H16"/>',
    "dag": '<rect x="2.5" y="9" width="5" height="5" rx="1"/><rect x="16.5" y="3" width="5" height="5" rx="1"/>'
           '<rect x="16.5" y="16" width="5" height="5" rx="1"/><rect x="9.5" y="9" width="5" height="5" rx="1"/>'
           '<path d="M7.5 11.5h2M14.5 11l2-4.5M14.5 12.5l2 5"/>',
}


def logo_airflow(co=62):
    """Logo Apache Airflow do nhóm cung cấp (tệp ../hinh/logo_airflow.png, cắt từ logo chính thức của dự án), nhúng thẳng vào HTML."""
    import base64
    import os
    tep = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "hinh", "logo_airflow.png")
    with open(tep, "rb") as f:
        du_lieu = base64.b64encode(f.read()).decode()
    return '<img src="data:image/png;base64,%s" alt="Apache Airflow" style="width:%dpx;height:%dpx;display:block">' % (du_lieu, co, co)


def icon(ten, co=36, mau=NAVY, net=1.6):
    return ('<svg width="{c}" height="{c}" viewBox="0 0 24 24" fill="none" stroke="{m}" stroke-width="{n}" '
            'stroke-linecap="round" stroke-linejoin="round" style="color:{m};display:block">{p}</svg>'
            ).format(c=co, m=mau, n=net, p=_ICON[ten])


def o(x, y, w, h, style="", inner=""):
    """Khối định vị tuyệt đối trong hệ toạ độ của sơ đồ."""
    return ('<div style="position:absolute;left:{x}px;top:{y}px;width:{w}px;height:{h}px;box-sizing:border-box;{s}">{i}</div>'
            ).format(x=x, y=y, w=w, h=h, s=style, i=inner)


def chu(x, y, w, noi_dung, co=20, mau=TEXT, dam=False, nghieng=False, canh="center", style=""):
    """Dòng chữ (có thể nhiều dòng qua <br>) đặt theo góc trên trái."""
    return ('<div style="position:absolute;left:{x}px;top:{y}px;width:{w}px;font-size:{c}px;line-height:1.28;color:{m};'
            'font-weight:{d};font-style:{n};text-align:{a};{s}">{t}</div>'
            ).format(x=x, y=y, w=w, c=co, m=mau, d=700 if dam else 400, n="italic" if nghieng else "normal", a=canh, s=style, t=noi_dung)


def vien_nhan(cx, cy, w, h, noi_dung, nen, vien=None, co=24, mau=NAVY, nghieng=False, r=None):
    """Nhãn dạng viên thuốc, căn giữa tại (cx, cy)."""
    return o(cx - w / 2, cy - h / 2, w, h,
             "background:{n};border:{v};border-radius:{r}px;display:flex;align-items:center;justify-content:center;"
             "font-size:{c}px;font-weight:700;color:{m};font-style:{i};white-space:nowrap"
             .format(n=nen, v=("1.5px solid " + vien) if vien else "none", r=r if r is not None else h / 2, c=co, m=mau,
                     i="italic" if nghieng else "normal"), "<span>%s</span>" % noi_dung)


class Mui:
    """Lớp SVG phủ trên sơ đồ để vẽ mũi tên, đường đứt, hình trụ."""

    def __init__(self, tien_to, w, h):
        self.id, self.w, self.h, self.p = tien_to, w, h, []
        self.duoi = []   # hình nằm dưới chữ (trụ, mũi tên khối)
        self.mau = {}

    def _dau(self, mau):
        if mau not in self.mau:
            self.mau[mau] = "%s_m%d" % (self.id, len(self.mau))
        return self.mau[mau]

    def duong(self, diem, mau, net=2.6, dut=False, dau=True, hai_dau=False):
        m = self._dau(mau)
        self.p.append('<polyline points="{d}" fill="none" stroke="{c}" stroke-width="{n}" {da} stroke-linejoin="round" {me} {ms}/>'
                      .format(d=" ".join("%g,%g" % p for p in diem), c=mau, n=net,
                              da='stroke-dasharray="9 6"' if dut else "",
                              me='marker-end="url(#%s)"' % m if dau else "",
                              ms='marker-start="url(#%s)"' % m if hai_dau else ""))

    def tru(self, x, y, w, h, nen, vien, ry=None, net=1.5):
        """Hình trụ nằm (đĩa / topic): trả về một khối SVG định vị tuyệt đối, chèn vào giữa nền thẻ và chữ."""
        ry = ry or min(9, h * 0.22)
        p = 2
        return ('<svg width="{W}" height="{H}" style="position:absolute;left:{L}px;top:{T}px;overflow:visible">'
                '<path d="M{x0} {y1} v{hh} a{rx} {ry} 0 0 0 {w} 0 v-{hh}" fill="{n}" stroke="{v}" stroke-width="{t}"/>'
                '<ellipse cx="{cx}" cy="{y1}" rx="{rx}" ry="{ry}" fill="{n}" stroke="{v}" stroke-width="{t}"/></svg>'
                ).format(W=w + 2 * p, H=h + 2 * p, L=x - p, T=y - p, x0=p, y1=p + ry, hh=h - 2 * ry, rx=w / 2, ry=ry, w=w,
                         n=nen, v=vien, t=net, cx=p + w / 2)

    def khoi_mui_ten(self, x, y, w, h, mau="#D3D6CE"):
        """Mũi tên khối màu xám nằm ngang (khối SVG định vị tuyệt đối)."""
        t = h * 0.32
        return ('<svg width="{W}" height="{H}" style="position:absolute;left:{L}px;top:{T}px">'
                '<polygon points="0,{b} {c},{b} {c},0 {W},{f} {c},{H} {c},{h} 0,{h}" fill="{m}"/></svg>'
                ).format(W=w, H=h, L=x, T=y, b=t, c=w * 0.62, f=h / 2, h=h - t, m=mau)

    def html_duoi(self):
        return ('<svg width="{w}" height="{h}" style="position:absolute;left:0;top:0;overflow:visible" '
                'xmlns="http://www.w3.org/2000/svg">{p}</svg>').format(w=self.w, h=self.h, p="".join(self.duoi))

    def html(self):
        defs = "".join('<marker id="{i}" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="5.5" markerHeight="5.5" '
                       'orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="{c}"/></marker>'.format(i=i, c=c)
                       for c, i in self.mau.items())
        return ('<svg width="{w}" height="{h}" style="position:absolute;left:0;top:0;overflow:visible" '
                'xmlns="http://www.w3.org/2000/svg"><defs>{d}</defs>{p}</svg>').format(w=self.w, h=self.h, d=defs, p="".join(self.p))


def _boc(w, h, ti_le, khoi, mui, id_, tren=""):
    """Bọc sơ đồ: khung ngoài đúng kích thước sau thu phóng, khung trong theo toạ độ thiết kế.
    Thứ tự lớp: hình dưới (trụ, mũi tên khối) → khối và chữ → mũi tên → nhãn đặt trên đường."""
    return ('<div class="sodo" id="{i}" style="position:relative;width:{W}px;height:{H}px;flex:none">'
            '<div style="position:absolute;left:0;top:0;width:{w}px;height:{h}px;transform:scale({k});transform-origin:0 0;'
            'font-family:{f};color:{t}">{d}{a}{b}{c}</div></div>'
            ).format(i=id_, W=round(w * ti_le), H=round(h * ti_le), w=w, h=h, k=ti_le, f=F, t=TEXT,
                     d=mui.html_duoi(), a=khoi, b=mui.html(), c=tren)


# ======================================================================== 1. KIẾN TRÚC TỔNG QUAN
T1 = {
    "vi": dict(nguon="Nguồn &amp; Sự kiện", rr="RetailRocket", rr2="2,76 triệu sự kiện<br>(139 ngày)",
               pr="Producer phát lại", pr2="1 ngày → 30 s<br>+5 % người dùng mới",
               dp="Xử lý dữ liệu và lưu trữ (Data Plane)", kf="Apache Kafka", kf2="Thu nhận, đệm và<br>phân phối sự kiện",
               sp="Apache Spark", sp2="Xử lý dữ liệu phân tán", st="Streaming", st2="Xử lý gần thời gian thực",
               st3="(nạp sự kiện, thịnh hành, nhật ký)", bt="Batch &amp; ML", bt2="ETL, huấn luyện ALS,<br>đánh giá và sinh gợi ý",
               lk="Lakehouse<br>(HDFS + Delta Lake)", lk2="Lưu trữ phân tán,<br>quản lý phiên bản",
               sv="Phục vụ và phản hồi (Serving)", web="Web FastAPI", web2="Đọc dữ liệu từ Gold<br>để hiển thị gợi ý, thịnh hành<br>và ghi nhận tương tác",
               br="Browser / Người dùng", br2="Xem gợi ý, nhấp, thêm giỏ,<br>mua sản phẩm",
               ph="Sự kiện tương tác (view / add to cart / purchase / impression)",
               dk='Điều phối <span style="color:%s">(spark-submit, schedule,</span> monitor)' % DO,
               af="Apache Airflow", af2="Điều phối và giám sát pipeline",
               i1="Lập lịch 5 DAG", i2="Gọi và giám sát Spark jobs", i3="Retry, cảnh báo và backfill",
               i4="Bảo trì dữ liệu (OPTIMIZE, Z-ORDER, VACUUM)",
               l1="Luồng dữ liệu (sự kiện)", l2="Đọc dữ liệu phục vụ", l3="Phản hồi từ người dùng", l4="Điều phối / điều khiển"),
    "en": dict(nguon="Sources &amp; events", rr="RetailRocket", rr2="2.76 million events<br>(139 days)",
               pr="Replay producer", pr2="1 day → 30 s<br>+5 % new users",
               dp="Data processing and storage (Data Plane)", kf="Apache Kafka", kf2="Ingests, buffers and<br>distributes events",
               sp="Apache Spark", sp2="Distributed processing", st="Streaming", st2="Near-real-time processing",
               st3="(event ingest, trending, impression log)", bt="Batch &amp; ML", bt2="ETL, ALS training,<br>evaluation, recommendations",
               lk="Lakehouse<br>(HDFS + Delta Lake)", lk2="Distributed storage,<br>version management",
               sv="Serving and feedback (Serving)", web="FastAPI web", web2="Reads Gold to show<br>recommendations and trending,<br>and records interactions",
               br="Browser / User", br2="Sees recommendations, clicks,<br>adds to cart, buys",
               ph="Interaction events (view / add to cart / purchase / impression)",
               dk='Orchestration <span style="color:%s">(spark-submit, schedule,</span> monitor)' % DO,
               af="Apache Airflow", af2="Orchestration and monitoring",
               i1="Schedules 5 DAGs", i2="Calls and monitors Spark jobs", i3="Retry, alerts and backfill",
               i4="Data maintenance (OPTIMIZE, Z-ORDER, VACUUM)",
               l1="Data flow (events)", l2="Read for serving", l3="User feedback", l4="Orchestration / control"),
}


def tong_quan(nn="vi", ti_le=1.0):
    t = T1[nn]
    W, H = 1570, 692
    m = Mui("tq" + nn, W, H)
    a = []
    the = "background:#FFFFFF;border:1.6px solid #6B7380;border-radius:10px"
    # --- Nguồn & sự kiện
    a.append(o(0, 20, 285, 397, "background:#FAFAF8;border:1.6px solid #9AA0A6;border-radius:14px"))
    a.append(vien_nhan(143, 48, 244, 42, t["nguon"], "#ECEEEA", co=26.5))
    for y, ic, tieu, mo in ((100, "db", t["rr"], t["rr2"]), (260, "play", t["pr"], t["pr2"])):
        a.append(o(18, y, 249, 124, the))
        a.append(o(30, y + 22, 46, 46, "", icon(ic, 46, NAVY, 1.5)))
        a.append(chu(89, y + 13, 176, tieu, 23 if len(tieu) > 14 else 25, NAVY, dam=True, canh="left", style="white-space:nowrap"))
        a.append(chu(89, y + 48, 176, mo, 19.5, TEXT, canh="left"))
    # --- Data plane
    a.append(o(303, 2, 864, 425, "background:#F7FAF2;border:1.6px solid #A8D08D;border-radius:14px"))
    a.append(vien_nhan(735, 29, 470, 42, t["dp"], "#DDEFCB", co=25))
    # Kafka
    a.append(o(323, 74, 243, 318, "background:#EAF2FB;border:1.6px solid #9DBDE6;border-radius:12px"))
    a.append(o(422.5, 82, 44, 44, "", icon("nodes", 44, XANH_DUONG, 1.6)))
    a.append(chu(323, 130, 243, t["kf"], 26, NAVY, dam=True))
    a.append(chu(323, 170, 243, t["kf2"], 20.5, TEXT))
    for y, w, ten in ((248, 203, "retailrocket-events"), (318, 196, "hien-thi")):
        x = 323 + (243 - w) / 2                     # giữa thẻ Kafka
        a.append(m.tru(x, y, w, 48, "#DCE9F8", "#7FA8DC", ry=6.5))
        a.append(chu(x, y + 12, w, ten, 19, NAVY, dam=True))   # chữ nằm giữa chiều cao trụ như bản gốc
    # Spark
    a.append(o(585, 70, 311, 324, "background:#FFF8E6;border:1.6px solid #F0CD6E;border-radius:12px"))
    a.append(o(718.5, 77, 44, 44, "", icon("star", 44, "#E3A21A", 1.6)))
    a.append(chu(585, 124, 311, t["sp"], 26, NAVY, dam=True))
    a.append(chu(585, 160, 311, t["sp2"], 20.5, TEXT))
    the_con = "background:#FFFFFF;border:1.5px solid #E2C462;border-radius:9px"
    a.append(o(600, 197, 281, 81, the_con))       # giữa thẻ Spark (585..896)
    a.append(o(613, 213, 34, 34, "", icon("wifi", 34, NAVY, 1.6)))
    a.append(chu(662, 203, 216, t["st"], 19.5, NAVY, dam=True, canh="left"))
    a.append(chu(662, 229, 216, t["st2"], 17, TEXT, canh="left"))
    a.append(chu(662, 252, 216, t["st3"], 13.5, TEXT, canh="left", style="white-space:nowrap"))
    a.append(o(600, 292, 281, 85, the_con))
    a.append(o(613, 309, 34, 34, "", icon("gear", 34, NAVY, 1.6)))
    a.append(chu(662, 298, 216, t["bt"], 19.5, NAVY, dam=True, canh="left"))
    a.append(chu(662, 324, 216, t["bt2"], 17, TEXT, canh="left"))
    # Lakehouse
    a.append(o(927, 74, 235, 321, "background:#F3EEF9;border:1.6px solid #B9A4DA;border-radius:12px"))
    a.append(o(1022.5, 80, 44, 44, "", icon("layers", 44, "#5B4B8A", 1.6)))
    a.append(chu(927, 124, 235, t["lk"], 22.5, NAVY, dam=True))
    a.append(chu(927, 188, 235, t["lk2"], 18.5, TEXT))
    for y, ten, nen, vien in ((248, "Bronze", "#F6CBA9", "#C9875A"), (295, "Silver", "#E4E4E4", "#A0A0A0"),
                              (342, "Gold", "#FFE7A0", "#D9B44A")):
        x = 927 + (235 - 180) / 2                    # giữa thẻ Lakehouse
        a.append(m.tru(x, y, 180, 40, nen, vien, ry=5.5))
        a.append(chu(x, y + 8.5, 180, ten, 19, NAVY, dam=True))
    # --- Serving
    a.append(o(1185, 2, 382, 425, "background:#F1FAFA;border:1.6px solid #86C9CB;border-radius:14px"))
    a.append(vien_nhan(1377, 29, 362, 42, t["sv"], "#CDEDEE", co=25))
    a.append(o(1229, 74, 321, 148, the))
    a.append(o(1268, 82, 56, 56, "", icon("code", 56, NAVY, 1.35)))
    a.append(chu(1332, 92, 190, t["web"], 24.5, NAVY, dam=True, style="white-space:nowrap"))
    a.append(chu(1229, 132, 321, t["web2"], 18.5, TEXT))
    a.append(o(1232, 277, 318, 115, the))
    a.append(o(1250, 290, 48, 48, "", icon("users", 48, NAVY, 1.35)))
    a.append(chu(1310, 293, 238, t["br"], 23 if nn == "vi" else 24, NAVY, dam=True, canh="left", style="white-space:nowrap"))
    a.append(chu(1310, 332, 238, t["br2"], 18.5, TEXT, canh="left"))
    # --- Mũi tên dữ liệu
    m.duong([(287, 257), (327, 257)], XANH_LA, 2.8)
    m.duong([(569, 257), (600, 257)], XANH_LA, 2.8)
    m.duong([(889, 257), (934, 257)], XANH_LA, 2.4, hai_dau=True)
    m.duong([(1150, 350), (1190, 350), (1190, 152), (1226, 152)], XANH_NGOC, 2.8)
    m.duong([(1390, 225), (1390, 274)], XANH_NGOC, 2.8, hai_dau=True)
    # Phản hồi người dùng → Kafka (đường đứt xanh)
    m.duong([(1390, 393), (1390, 440), (447, 440), (447, 368)], XANH_DUONG, 2.4, dut=True)
    tren = [vien_nhan(995, 440, 476, 33, t["ph"], "#FFFFFF", "#7FA0D6", co=16.5, mau=XANH_DUONG, r=16).replace(
        "font-weight:700", "font-weight:400")]
    # Điều phối Airflow → Spark (đường đứt đỏ)
    m.duong([(712, 482), (712, 398)], DO, 2.4, dut=True)
    tren.append(vien_nhan(788, 500, 396, 36, t["dk"], "#FFF7F6", "#D98C86", co=17.5, mau=NAVY, r=18).replace(
        "font-weight:700", "font-weight:400"))
    # --- Dải Airflow
    a.append(o(2, 530, 1560, 90, "background:#FCEAE9;border:1.6px solid #D98C86;border-radius:12px"))
    a.append(o(76, 545, 60, 60, "", logo_airflow(60)))
    a.append(chu(150, 551, 270, t["af"], 27, NAVY, dam=True, canh="left"))
    a.append(chu(150, 586, 300, t["af2"], 19.5, TEXT, canh="left"))
    a.append(o(423, 547, 1.5, 56, "background:#D9A6A2"))
    for x, ic, txt in ((450, "calendar", t["i1"]), (632, "gear", t["i2"]), (890, "refresh", t["i3"]), (1148, "layers", t["i4"])):
        a.append(o(x, 561, 28, 28, "", icon(ic, 28, NAVY, 1.6)))
        a.append(chu(x + 36, 564, 400, txt, 16.8, TEXT, canh="left", style="white-space:nowrap;letter-spacing:-0.2px"))
    # --- Chú giải
    a.append(o(98, 638, 1367, 48, "background:#FFFFFF;border:1.4px solid #9AA0A6;border-radius:8px"))
    for x, mau, dut, nhan in ((146, XANH_LA, False, t["l1"]), (486, XANH_NGOC, False, t["l2"]),
                              (794, XANH_DUONG, True, t["l3"]), (1154, DO, True, t["l4"])):
        m.duong([(x, 662), (x + 70, 662)], mau, 2.6, dut=dut)
        a.append(chu(x + 88, 650, 280, nhan, 17.5, TEXT, canh="left", style="white-space:nowrap"))
    return _boc(W, H, ti_le, "".join(a), m, "sodo_tq_" + nn, "".join(tren))


# ======================================================================== 2. NĂM DAG AIRFLOW
T2 = {
    "vi": dict(tieu="Apache Airflow 3.3",
               phu="Airflow không xử lý dữ liệu trực tiếp; Airflow lập lịch, gọi <b>spark-submit</b>, giám sát, retry, rẽ nhánh và backfill.",
               lich="Lịch chạy", viec="Công việc", anh="Ảnh hưởng",
               d=[("DAG 1 · kn_pipeline_hang_ngay", "02:00 mỗi ngày",
                   ["Batch chính của hệ thống", "bronze_to_silver → thống kê → ALS → ghi gợi ý"],
                   [("Spark<br>batch", "g"), ("Bronze", "b"), ("Silver", "s"), ("Gold.goi_y_<br>nguoi_dung", "y")]),
                  ("DAG 2 · kn_giam_sat_streaming", "mỗi 15 phút",
                   ["Kiểm tra luồng thịnh hành", "Khởi động lại nếu stale hoặc chết"],
                   [("Spark<br>streaming", "g"), ("Gold.thinh_hanh_15p", "y")]),
                  ("DAG 3 · kn_tinh_ab", "mỗi giờ",
                   ["Đọc nhật ký hiển thị", "Tính CTR / conversion / p-value", "Ghi kết quả A/B"],
                   [("Gold.nhat_ky_<br>hien_thi", "y"), ("Gold.ket_qua_ab", "y")]),
                  ("DAG 4 · kn_bao_tri", "04:00 mỗi ngày",
                   ["OPTIMIZE · Z-ORDER · VACUUM", "Dọn dữ liệu cũ và giảm tệp nhỏ"],
                   [("Silver", "s"), ("Gold", "y")]),
                  ("DAG 5 · kn_tai_lap", "kích hoạt thủ công",
                   ["Time travel theo phiên bản Delta", "Chạy lại huấn luyện và so với lịch sử"],
                   [("VERSION AS OF", "g"), ("Gold.ket_qua_tai_lap", "y")])],
               chan="Các DAG gọi Spark jobs trên cụm; dữ liệu nằm trong lakehouse Bronze – Silver – Gold.",
               cg=["DAG Airflow", "Điều phối", "Thành phần / bảng bị tác động", "Spark job / streaming"]),
    "en": dict(tieu="Apache Airflow 3.3",
               phu="Airflow does not process data itself; it schedules, calls <b>spark-submit</b>, monitors, retries, branches and backfills.",
               lich="Schedule", viec="Tasks", anh="Affects",
               d=[("DAG 1 · kn_pipeline_hang_ngay", "02:00 every day",
                   ["The system's main batch", "bronze_to_silver → statistics → ALS → write recs"],
                   [("Spark<br>batch", "g"), ("Bronze", "b"), ("Silver", "s"), ("Gold.goi_y_<br>nguoi_dung", "y")]),
                  ("DAG 2 · kn_giam_sat_streaming", "every 15 minutes",
                   ["Checks the trending stream", "Restarts it if stale or dead"],
                   [("Spark<br>streaming", "g"), ("Gold.thinh_hanh_15p", "y")]),
                  ("DAG 3 · kn_tinh_ab", "every hour",
                   ["Reads the impression log", "Computes CTR / conversion / p-value", "Writes the A/B result"],
                   [("Gold.nhat_ky_<br>hien_thi", "y"), ("Gold.ket_qua_ab", "y")]),
                  ("DAG 4 · kn_bao_tri", "04:00 every day",
                   ["OPTIMIZE · Z-ORDER · VACUUM", "Cleans old data, compacts small files"],
                   [("Silver", "s"), ("Gold", "y")]),
                  ("DAG 5 · kn_tai_lap", "manual trigger",
                   ["Time travel by Delta version", "Retrains and compares with history"],
                   [("VERSION AS OF", "g"), ("Gold.ket_qua_tai_lap", "y")])],
               chan="The DAGs call Spark jobs on the cluster; data lives in the Bronze – Silver – Gold lakehouse.",
               cg=["Airflow DAG", "Orchestration", "Affected component / table", "Spark job / streaming"]),
}
CHIP = {"g": ("#E7F5DF", "#7DBE6A"), "b": ("#FBE7D8", "#D9A27A"), "s": ("#EFEFEF", "#9E9E9E"), "y": ("#FFF3C4", "#E2B93B")}
MAU_DAG = [("#EAF2FB", "#8DB3E2", "#DCE7F5"), ("#EEF7EA", "#9BCB8A", "#DDEDD6"), ("#FFF6E0", "#E9C46A", "#F4E8C6"),
           ("#F3F3F3", "#BDBDBD", "#E4E4E4"), ("#E8F6F7", "#7CC8CC", "#D3ECEE")]


def nam_dag(nn="vi", ti_le=1.0):
    t = T2[nn]
    W, H = 1557, 672
    m = Mui("dag" + nn, W, H)
    a = [o(0, 0, W, H, "background:#FFFFFF;border:2px solid #222")]
    a.append(o(44, 19, 1470, 90, "background:#FBE3E3;border:1.6px solid #D98080;border-radius:10px"))
    a.append(chu(44, 28, 1470, t["tieu"], 34, "#111", dam=True))
    a.append(chu(44, 72, 1470, t["phu"], 20, TEXT))
    # đường điều phối
    m.duong([(812, 109), (812, 133)], DO, 2.2, dut=True, dau=False)
    m.duong([(300, 133), (1304, 133)], DO, 2.2, dut=True, dau=False)
    for x in (300, 832, 1304):
        m.duong([(x, 133), (x, 162)], DO, 2.2, dut=True)
    m.duong([(462, 401), (1090, 401)], DO, 2.2, dut=True, dau=False)
    for x in (462, 1090):
        m.duong([(x, 401), (x, 424)], DO, 2.2, dut=True)
    vi_tri = [(15, 165, 563, 216), (592, 165, 448, 216), (1055, 165, 487, 216), (153, 425, 585, 180), (763, 425, 616, 180)]
    for (x, y, w, h), (ten, lich, viec, anh), (nen, vien, nhan) in zip(vi_tri, t["d"], MAU_DAG):
        a.append(o(x, y, w, h, "background:%s;border:1.6px solid %s;border-radius:10px" % (nen, vien)))
        a.append(chu(x, y + 9, w, ten, 23.5, "#111", dam=True))
        lx, cx, vx = x + 13, x + 132, x + 150
        hang = [(y + 47, t["lich"]), (y + 84, t["viec"]), (y + h - 46, t["anh"])]
        for yy, nhan_t in hang:
            a.append(o(lx, yy, 110, 30, "background:%s;border:1.2px solid %s;border-radius:7px;display:flex;align-items:center;"
                       "justify-content:center;font-size:18.5px;font-weight:700;color:#222" % (nhan, vien), nhan_t))
            a.append(chu(cx - 6, yy + 2, 16, ":", 19, "#222"))
        a.append(chu(vx, y + 50, w - 160, lich, 18.5, "#222", canh="left"))
        li = "".join('<div style="display:flex;gap:8px"><span>•</span><span>%s</span></div>' % v for v in viec)
        a.append(chu(vx, y + 82, w - 160, li, 18, "#222", canh="left", style="line-height:1.25"))
        # chuỗi ảnh hưởng
        xx, yy = vx - 4, y + h - 52
        ws = {"Spark<br>batch": [70, 72, 68, 108], "Spark<br>streaming": [96, 166], "VERSION AS OF": [148, 178],
              "Silver": [96, 96]}.get(anh[0][0], [118, 150])
        cao = 42
        for i, ((txt, loai), wc) in enumerate(zip(anh, ws)):
            nen_c, vien_c = CHIP[loai]
            a.append(o(xx, yy, wc, cao, "background:%s;border:1.4px solid %s;border-radius:7px;display:flex;align-items:center;"
                       "justify-content:center;text-align:center;font-size:%spx;line-height:1.12;color:#222"
                       % (nen_c, vien_c, 15.5 if "<br>" in txt else 16.5), txt))
            if i < len(anh) - 1:
                m.duong([(xx + wc + 3, yy + cao / 2), (xx + wc + 23, yy + cao / 2)], "#333", 1.8)
            xx += wc + 26
    a.append(chu(24, 628, 760, t["chan"], 18, "#333", nghieng=True, canh="left"))
    muc = []
    for i, nhan in enumerate(t["cg"]):
        if i == 1:
            mau_cg = ('<svg width="44" height="10"><line x1="0" y1="5" x2="34" y2="5" stroke="%s" stroke-width="2" '
                      'stroke-dasharray="7 5"/><path d="M34 0L44 5L34 10z" fill="%s"/></svg>' % (DO, DO))
        else:
            nen_c, vien_c = (MAU_DAG[0][0], MAU_DAG[0][1]) if i == 0 else CHIP["y" if i == 2 else "g"]
            mau_cg = '<span style="display:inline-block;width:38px;height:19px;background:%s;border:1.4px solid %s;border-radius:5px"></span>' % (nen_c, vien_c)
        muc.append('<span style="display:flex;align-items:center;gap:8px">%s<span>%s</span></span>' % (mau_cg, nhan))
    a.append(o(802, 620, 740, 40, "background:#FFFFFF;border:1.4px solid #8A8A8A;border-radius:6px;display:flex;align-items:center;"
               "justify-content:space-evenly;font-size:15px;color:#222;white-space:nowrap", "".join(muc)))
    return _boc(W, H, ti_le, "".join(a), m, "sodo_dag_" + nn)


# ======================================================================== 3. LAKEHOUSE: LƯU TRỮ VÀ TÍNH TOÁN
T3 = {
    "vi": dict(lt="Luồng lưu trữ", tt="Luồng tính toán",
               the=[("Bronze", "#9A5B2A", '<span style="color:#A0522D">su_kien</span>', "Sự kiện thô từ Kafka, Parquet, chỉ<br>ghi thêm, theo ngày"),
                    ("Silver", "#6A7486", '<i style="color:#6A7486">tuong_tac, san_pham</i>', "Dữ liệu đã làm sạch, chuẩn hóa<br>và sẵn sàng cho phân tích"),
                    ("Gold", "#B8541E", '<span style="color:#B8541E">goi_y_nguoi_dung, <i>thinh_hanh_15p,</i><br><i>nhat_ky_hien_thi, ket_qua_ab</i></span>',
                     "Bảng phục vụ web, dashboard, A/B<br>và phân tích cuối")],
               job=[("Bronze → Silver", "#6E7C93", '<span style="color:#5B6B86">bronze_to_silver</span>',
                     ["Khử trùng theo mã và nội dung", "Lọc bot", "Gán trọng số", "Ghi vào silver"]),
                    ("Đặc trưng &amp; thống kê", "#2459B8", '<span style="color:#2459B8">features.ma_tran_tuong_tac</span>',
                     ["Chia train / test theo thời gian", "Time decay + indexer", "Thống kê sản phẩm, đồng xuất hiện"]),
                    ("ML &amp; phục vụ", "#B8541E", '<span style="color:#333">huan_luyen_als · tron_goi_y · ghi_gold</span>',
                     ["Huấn luyện ALS + đánh giá", "Cold start, re-rank, blending", "Ghi kết quả cuối vào gold"])],
               so="<b>2.894.238</b> sự kiện → <b>2.882.479</b> dòng silver (139 ngày) → <b>746.350</b> cặp người dùng–sản phẩm (features) → "
                  "<b>5.300.860</b> dòng gold phục vụ cho 265.043 người dùng"),
    "en": dict(lt="Storage flow", tt="Compute flow",
               the=[("Bronze", "#9A5B2A", '<span style="color:#A0522D">su_kien</span>', "Raw events from Kafka, Parquet,<br>append-only, by day"),
                    ("Silver", "#6A7486", '<i style="color:#6A7486">tuong_tac, san_pham</i>', "Cleaned, normalised data<br>ready for analysis"),
                    ("Gold", "#B8541E", '<span style="color:#B8541E">goi_y_nguoi_dung, <i>thinh_hanh_15p,</i><br><i>nhat_ky_hien_thi, ket_qua_ab</i></span>',
                     "Tables served to the web, dashboard,<br>A/B and final analysis")],
               job=[("Bronze → Silver", "#6E7C93", '<span style="color:#5B6B86">bronze_to_silver</span>',
                     ["Dedup by id and by content", "Bot filter", "Assign weights", "Write to silver"]),
                    ("Features &amp; statistics", "#2459B8", '<span style="color:#2459B8">features.ma_tran_tuong_tac</span>',
                     ["Time-based train / test split", "Time decay + indexers", "Item statistics, co-occurrence"]),
                    ("ML &amp; serving", "#B8541E", '<span style="color:#333">huan_luyen_als · tron_goi_y · ghi_gold</span>',
                     ["ALS training + evaluation", "Cold start, re-rank, blending", "Write final results to gold"])],
               so="<b>2,894,238</b> events → <b>2,882,479</b> silver rows (139 days) → <b>746,350</b> user–item pairs (features) → "
                  "<b>5,300,860</b> gold rows served to 265,043 users"),
}


def lakehouse(nn="vi", ti_le=1.0, co_dai_so=True):
    t = T3[nn]
    W, H = 1520, 666 if co_dai_so else 584
    m = Mui("lk" + nn, W, H)
    a = []
    khung = "background:#F2F3EE;border:1.4px solid #D9DBD3;border-radius:16px"
    a.append(o(1, 18, 1518, 272, khung))
    a.append(vien_nhan(99, 18, 180, 32, t["lt"], "#E1E4DE", co=20, mau="#2B3445", r=10))
    for (ten, vien, sub, mo), x in zip(t["the"], (44, 548, 1053)):
        w = 419 if x != 1053 else 427
        a.append(o(x, 52, w, 211, "background:#FFFFFF;border:1.2px solid #DDE0D8;border-top:6px solid %s;border-radius:10px;"
                   "box-shadow:0 1px 3px rgba(0,0,0,.08)" % vien))
        a.append(chu(x + 27, 68, w - 40, ten, 36, NAVY, dam=True, canh="left"))
        a.append(chu(x + 27, 122, w - 34, sub, 21.5, TEXT, canh="left", style="line-height:1.32"))
        a.append(chu(x + 27, 166 if x != 1053 else 184, w - 34, mo, 21.5, TEXT, canh="left", style="line-height:1.36"))
    a.append(m.khoi_mui_ten(483, 147, 50, 26))
    a.append(m.khoi_mui_ten(986, 147, 50, 26))
    a.append(o(1, 325, 1518, 255, khung))
    a.append(vien_nhan(108, 324, 198, 32, t["tt"], "#E1E4DE", co=20, mau="#2B3445", nghieng=True, r=10))
    for (ten, vien, sub, ds), (x, w) in zip(t["job"], ((121, 378), (565, 406), (1040, 407))):
        a.append(o(x, 358, w, 200, "background:#FFFFFF;border:1.2px solid #DDE0D8;border-top:6px solid %s;border-radius:10px;"
                   "box-shadow:0 1px 3px rgba(0,0,0,.08)" % vien))
        a.append(chu(x + 25, 372, w - 30, ten, 30, NAVY, dam=True, canh="left"))
        a.append(chu(x + 25, 411, w - 25, sub, 19.5, TEXT, canh="left", style="white-space:nowrap"))
        li = "".join('<div style="display:flex;gap:12px;align-items:baseline"><span style="color:#9AA0A6;font-size:14px">●</span>'
                     '<span>%s</span></div>' % v for v in ds)
        a.append(chu(x + 27, 442, w - 30, li, 19.5, TEXT, canh="left", style="line-height:1.38"))
    a.append(m.khoi_mui_ten(515, 446, 40, 24))
    a.append(m.khoi_mui_ten(986, 446, 42, 24))
    m.duong([(356, 263), (356, 354)], "#6B7380", 1.8)
    m.duong([(1263, 356), (1263, 266)], "#6B7380", 1.8)
    if co_dai_so:
        a.append(o(1, 605, 1519, 58, "background:#E9EBE3;border-radius:12px;display:flex;align-items:center;justify-content:center;"
                   "font-size:21.5px;color:%s;white-space:nowrap" % NAVY, "<span>%s</span>" % t["so"]))
    return _boc(W, H, ti_le, "".join(a), m, "sodo_lk_" + nn)


def trang_xem(*khoi, nen="#F5F6F1"):
    """Trang HTML độc lập để xem thử hoặc chụp ảnh."""
    return ('<!DOCTYPE html><html><head><meta charset="utf-8"><style>body{margin:0;background:%s}'
            '.wrap{display:flex;flex-direction:column;gap:40px;padding:20px;align-items:flex-start}</style></head>'
            '<body><div class="wrap">%s</div></body></html>') % (nen, "".join(khoi))


if __name__ == "__main__":
    import sys
    ra = sys.argv[1] if len(sys.argv) > 1 else "/tmp/so_do_xem.html"
    with open(ra, "w", encoding="utf-8") as f:
        f.write(trang_xem(tong_quan("vi"), nam_dag("vi"), lakehouse("vi"), tong_quan("en"), nam_dag("en"), lakehouse("en")))
    print(ra)
