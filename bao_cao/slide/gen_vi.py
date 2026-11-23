"""Bản tiếng Việt của deck bảo vệ BD03: sinh project_vi/slides/*.html và project_vi/deck.json.

Cùng bố cục, số liệu và hình với gen.py (bản tiếng Anh); hình giữ nhãn tiếng Anh, chú thích bằng tiếng Việt.
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import so_do  # noqa: E402  (ba sơ đồ nhóm thiết kế, dựng bằng HTML + SVG vector)

GOC = os.path.dirname(os.path.abspath(__file__))
SL = os.path.join(GOC, "project_vi", "slides")
os.makedirs(SL, exist_ok=True)

NAVY, BG, BG2, CARD = "#14213D", "#F5F6F1", "#ECEEE6", "#FFFFFF"
TEXT, MUTED, RULE = "#3D4A5C", "#5F6B7A", "#D5D9CF"
BLUE, ORANGE, LIGHT_ACC = "#2459B8", "#A84F17", "#F2B66D"
# Toàn bộ chữ: Times New Roman (máy Windows/macOS có sẵn); Tinos là bản Google Fonts cùng số đo
# với Times New Roman, dùng khi máy xem không có phông này (Linux, Android, máy xuất PDF).
TNR = "'Times New Roman', Tinos, 'Liberation Serif', Times, serif"
H = TNR
B = TNR
M = TNR

# Hình: (tài sản đã tải lên deck tiếng Việt, rộng, cao theo tỉ lệ, tệp nguồn trong bao_cao/hinh). Sơ đồ và biểu đồ là SVG vector
# (chữ đã đổi thành đường cong) nên chiếu lên màn hình lớn hay phóng to PDF vẫn nét; ảnh chụp màn hình giữ PNG.
IMG = {
    "f02": ("/_blob/32e0721e5ceac86d3534e998b05bdaf4", 2220, 966, "f02_cum.svg"),
    "f05b": ("/_blob/8ff58e383f84e4760ab24b3d0ba7f72f", 2220, 798, "f05b_dag1_ngang.svg"),
    "f07": ("/_blob/a24f5c1ebd90391999e6ffa0baea8e0b", 2220, 1026, "f07_luong.svg"),
    "f08": ("/_blob/cf4d122e73d01a0909f1042377511441", 2220, 1050, "f08_vong_khep_kin.svg"),
    "f09": ("/_blob/1708668e3be465b51a25f23f45789f4c", 2220, 930, "f09_tai_lap.svg"),
    "f11": ("/_blob/6bfd20df6a2c5a43a3d5a06672ca8fcb", 2220, 816, "f11_mau_khoi.svg"),
    "c01": ("/_blob/28f3a633faa16b640d390606d28c38b4", 1245, 416, "c01_tuning.svg"),
    "c02": ("/_blob/99b971940d702a1691a01175fb491ab8", 1544, 554, "c02_phuong_phap.svg"),
    "c03": ("/_blob/894937be4820d4fe2f39411b1da3a1f6", 1518, 556, "c03_pho_bien.svg"),
    "c04": ("/_blob/b1cd37cb6e3692e43b85b1e4cf0dbda6", 1155, 587, "c04_xep_lai.svg"),
    "c05": ("/_blob/32b87e1158c5ad93e8a9cc3c8b063b4b", 1172, 509, "c05_zorder.svg"),
    "c06": ("/_blob/6cdb639ca6760ecff359c107f6374cdd", 1925, 722, "c06_ab.svg"),
    "c09": ("/_blob/a448bb2ffe66ef5123b5975b1952a05b", 1544, 526, "c09_phan_tich_loi_en.svg"),
    "s01": ("/_blob/c19192fe48e48a7f0a3442b9decda7ac", 1350, 1034, "s01_cold_start_web.png"),
    "s03": ("/_blob/63c6969edb66ff879becb57f345592d0", 1350, 870, "s03_ab_web.png"),
    "eq_als": ("/_blob/e5e58e0526c6af9f08829cc01db65050", 975, 113, "eq_als.png"),
    "eq_pref": ("/_blob/fef3751751f9cae8520ecdde54a758e0", 671, 63, "eq_pref.png"),
}

THU_TU = []
TONG = 28


SO_HINH = [0]


def hinh(khoa, alt, rong=None, cao=None, dem=16, the=True):
    """Ảnh trong thẻ trắng kèm chú thích đánh số kiểu bài báo ("Figure n. ...") ngay dưới ảnh.

    Cho rong HOẶC cao, cạnh kia theo tỉ lệ. Phần chú thích chiếm thêm khoảng 34 px chiều cao.
    """
    src, w, h = IMG[khoa][:3]
    if rong is not None:
        cao = round(rong * h / w)
    else:
        rong = round(cao * w / h)
    SO_HINH[0] += 1
    if not the:  # sơ đồ đã có viền ngoài riêng: không bọc thẻ
        return (
            '<div style="display:flex;flex-direction:column;align-items:center;gap:8px">'
            '<img src="{s}" alt="{a}" style="width:{w}px;height:{h}px;object-fit:contain">'
            '<p style="font-family:{f};font-size:20px;line-height:1.25;color:{m};text-align:center;max-width:{w}px">'
            '<b>Hình {n}.</b> <i>{a}</i></p></div>'
        ).format(s=src, a=alt, w=rong, h=cao, f=TNR, m=MUTED, n=SO_HINH[0])
    return (
        '<div style="background:{c};border:1px solid {r};border-radius:6px;padding:{d}px {d}px 10px;'
        'display:flex;flex-direction:column;align-items:center;justify-content:center;gap:6px">'
        '<img src="{s}" alt="{a}" style="width:{w}px;height:{h}px;object-fit:contain">'
        '<p style="font-family:{f};font-size:20px;line-height:1.25;color:{m};text-align:center;max-width:{w}px">'
        '<b>Hình {n}.</b> <i>{a}</i></p></div>'
    ).format(c=CARD, r=RULE, d=dem, s=src, a=alt, w=rong, h=cao, f=TNR, m=MUTED, n=SO_HINH[0])


def chu_thich(alt):
    """Chú thích đánh số cho sơ đồ dựng bằng HTML (cùng kiểu với hinh())."""
    SO_HINH[0] += 1
    return ('<p style="font-family:{f};font-size:20px;line-height:1.25;color:{m};text-align:center">'
            '<b>Hình {n}.</b> <i>{a}</i></p>').format(f=TNR, m=MUTED, n=SO_HINH[0], a=alt)


DEM_SO_DO = "72px 64px 112px"   # lề hẹp cho ba slide sơ đồ để sơ đồ đủ lớn

def tieu_de(eyebrow, tieu):
    return (
        '<div style="display:flex;flex-direction:column;gap:8px">'
        '<p style="font-size:24px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:{o}">{e}</p>'
        '<h2 style="font-family:{h};font-size:64px;font-weight:600;line-height:1.15;color:{n}">{t}</h2></div>'
    ).format(o=ORANGE, e=eyebrow, h=H, n=NAVY, t=tieu)


def chan(so):
    return (
        '<p style="position:absolute;left:128px;bottom:64px;width:1300px;font-size:24px;color:{m}">'
        'BD03 · Hệ thống gợi ý sản phẩm thương mại điện tử · CS411V Dữ liệu lớn</p>'
        '<p style="position:absolute;right:128px;bottom:64px;width:200px;text-align:right;font-size:24px;color:{m}">{s} / {t}</p>'
    ).format(m=MUTED, s=so, t=TONG)


def slide(id_, noi_dung, ghi_chu, nen=BG, chuyen="fade", dem="128px 128px 160px", khoang=40):
    THU_TU.append(id_)
    so = len(THU_TU)
    html = (
        '<section id="{i}" data-transition="{tr}" style="background:{bg};color:{tx};font-family:{b};'
        'padding:{dm};display:flex;flex-direction:column;gap:{kh}px">\n{nd}\n{ch}\n<aside>{gc}</aside>\n</section>\n'
    ).format(i=id_, tr=chuyen, bg=nen, tx=TEXT, b=B, nd=noi_dung, ch=chan(so), gc=ghi_chu, dm=dem, kh=khoang)
    with open(os.path.join(SL, id_ + ".html"), "w", encoding="utf-8") as f:
        f.write(html)


def dong(nhan, mo_ta, co=28, rong_nhan=None):
    """Một dòng: nhãn đậm + mô tả."""
    w = "width:{}px;".format(rong_nhan) if rong_nhan else ""
    return (
        '<div style="display:flex;gap:24px;align-items:baseline;border-top:1px solid {r};padding:14px 0px 0px 0px">'
        '<p style="{w}font-size:{c}px;font-weight:600;color:{n};min-width:min-content">{l}</p>'
        '<p style="flex:1;font-size:{c}px;line-height:1.35;color:{t}">{m}</p></div>'
    ).format(r=RULE, w=w, c=co, n=NAVY, l=nhan, t=TEXT, m=mo_ta)


def so_lon(gia_tri, nhan, mau=BLUE, co=72):
    return (
        '<div style="display:flex;flex-direction:column;gap:8px">'
        '<p style="font-family:{h};font-size:{c}px;font-weight:700;line-height:1.05;color:{m}">{g}</p>'
        '<p style="font-size:28px;line-height:1.35;color:{t}">{n}</p></div>'
    ).format(h=H, c=co, m=mau, g=gia_tri, t=TEXT, n=nhan)


def bang(cot, dong_s, rong, co=26):
    tr = ['<tr style="background:{}">'.format(BG2) + "".join(
        '<th style="width:{}%;text-align:left">{}</th>'.format(w, c) for c, w in zip(cot, rong)) + "</tr>"]
    for d in dong_s:
        tr.append("<tr>" + "".join("<td>{}</td>".format(x) for x in d) + "</tr>")
    return '<table style="font-size:{}px;color:{};width:100%">{}</table>'.format(co, TEXT, "".join(tr))


# ------------------------------------------------------------------ 1. Bìa
slide("cover", (
    '<p style="font-size:24px;font-weight:600;letter-spacing:3px;text-transform:uppercase;color:{la}">CS411V Dữ liệu lớn · Đồ án cuối kỳ · Đề tài 3</p>'
    '<h1 style="font-family:{h};font-size:96px;font-weight:700;line-height:1.08;color:{bg}">Hệ thống gợi ý sản phẩm<br>thương mại điện tử</h1>'
    '<p style="font-size:32px;line-height:1.4;color:#C9D4E5;width:1400px">ALS phản hồi ngầm, xu hướng theo luồng và thử nghiệm A/B trên cụm Spark, Kafka, Delta Lake gồm sáu máy phòng lab</p>'
    '<div style="flex:1"></div>'
    '<div style="display:flex;gap:96px">'
    '<div style="display:flex;flex-direction:column;gap:6px"><p style="font-size:32px;font-weight:600;color:{bg}">Lê Chí Tâm</p><p style="font-size:24px;color:#C9D4E5">MSSV 2302220</p></div>'
    '<div style="display:flex;flex-direction:column;gap:6px"><p style="font-size:32px;font-weight:600;color:{bg}">Lê Nguyễn Quốc Anh</p><p style="font-size:24px;color:#C9D4E5">MSSV 2302283</p></div>'
    '</div>'
    '<p style="font-size:24px;color:#C9D4E5">Trường Đại học Tân Tạo · Bảo vệ ngày 19/11/2026 · GVHD: TS. Cao Tiến Dũng</p>'
).format(la=LIGHT_ACC, h=H, bg=BG),
    "Kính chào Thầy. Chúng em là Lê Chí Tâm và Lê Nguyễn Quốc Anh. Đề tài của nhóm là Đề tài 3 của học phần CS411V: hệ thống "
    "gợi ý sản phẩm cho một sàn thương mại điện tử mô phỏng. Thầy vừa xem hệ thống chạy trực tiếp trên cụm sáu máy phòng lab; "
    "các slide này trình bày cách hệ thống được xây dựng, mô hình đạt được gì trên dữ liệu thật, và từng tiêu chí trong tám tiêu "
    "chí nghiệm thu được chứng minh ra sao.", nen=NAVY)
# chân trang trên nền tối: sửa màu trong tệp bìa
p = os.path.join(SL, "cover.html")
s = open(p, encoding="utf-8").read().replace("color:{}\">BD03".format(MUTED), "color:#9FB0C8\">BD03").replace(
    "text-align:right;font-size:24px;color:{}".format(MUTED), "text-align:right;font-size:24px;color:#9FB0C8")
open(p, "w", encoding="utf-8").write(s)

# ------------------------------------------------------------------ 2. Đề bài
muc_tieu = [
    ("O1", "Nền tảng dữ liệu", "Thu nhận qua Kafka và lakehouse bronze → silver → gold trên HDFS với Delta Lake"),
    ("O2", "Mô hình", "ALS phản hồi ngầm, lưới tinh chỉnh, hai baseline và ba chiến lược khởi đầu lạnh"),
    ("O3", "Luồng", "Khối “Đang thịnh hành” phản ứng với cú nhấp trong vòng 90 giây"),
    ("O4", "Thực nghiệm", "Thử nghiệm A/B có ghi lượt hiển thị và kết luận thống kê"),
    ("O5", "Vận hành", "Năm DAG Airflow, ứng dụng web năm màn hình và thực nghiệm tái lập được"),
]
hang = "".join(
    '<div style="display:flex;gap:28px;align-items:center;border-top:1px solid {r};padding:18px 0px 0px 0px">'
    '<p style="font-family:{h};font-size:32px;font-weight:700;color:{bg};background:{n};padding:8px 18px;border-radius:12px">{k}</p>'
    '<p style="width:300px;font-size:32px;font-weight:600;color:{n}">{t}</p>'
    '<p style="flex:1;font-size:28px;line-height:1.35;color:{tx}">{m}</p></div>'.format(
        r=RULE, h=H, bg=BG, n=NAVY, k=k, t=t, tx=TEXT, m=m) for k, t, m in muc_tieu)
slide("assignment", tieu_de("Giới thiệu", "Đề bài yêu cầu gì") +
      '<p style="font-size:32px;line-height:1.4;color:{t};width:1500px">Học từ cú nhấp, phản ứng trong vài giây, chứng minh bằng thực nghiệm.</p>'.format(t=TEXT) +
      '<div style="display:flex;flex-direction:column;gap:12px">' + hang + "</div>",
      "Đề bài có năm mục tiêu. O1 là nền tảng dữ liệu: sự kiện đến qua Kafka và được lưu trong lakehouse trên HDFS với Delta Lake. "
      "O2 là mô hình: ALS cho phản hồi ngầm, lưới tinh chỉnh, hai baseline và khởi đầu lạnh. O3 là xử lý luồng: khối thịnh hành phản "
      "ứng trong chín mươi giây. O4 là thực nghiệm A/B, và O5 là vận hành: Airflow, ứng dụng web và khả năng tái lập.")

# ------------------------------------------------------------------ 3. Tám tiêu chí
tieu_chi = [
    "Bảng tinh chỉnh ít nhất 9 tổ hợp ALS, kèm NDCG@10",
    "ALS so với 2 baseline trên 4 chỉ số xếp hạng, độ phủ và độ mới lạ",
    "Ba chiến lược khởi đầu lạnh, minh họa bằng người dùng tạo tại chỗ",
    "Khối thịnh hành cập nhật tối đa 90 giây sau cú nhấp",
    "Vòng khép kín: cú nhấp trên web đến Kafka, bronze và bảng thịnh hành",
    "Kết quả A/B có p-value và kết luận thống kê",
    "Tái lập thực nghiệm bằng Delta <span style=\"color:{}\">VERSION AS OF</span>".format(BLUE),
    "Số liệu OPTIMIZE / Z-ORDER cho truy vấn theo người dùng",
]
the = "".join(
    '<div style="display:flex;gap:20px;align-items:center;background:{c};border:1px solid {r};border-radius:14px;padding:20px 24px">'
    '<p style="font-family:{h};font-size:44px;font-weight:700;color:{b}">{i}</p>'
    '<p style="flex:1;font-size:28px;line-height:1.3;color:{n}">{t}</p></div>'.format(
        c=CARD, r=RULE, h=H, b=BLUE, i=i + 1, n=NAVY, t=t) for i, t in enumerate(tieu_chi))
slide("criteria", tieu_de("Giới thiệu", "Tám tiêu chí nghiệm thu") +
      '<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px 28px">' + the + "</div>",
      "Đặc tả chuyển các mục tiêu này thành tám tiêu chí nghiệm thu đo được. Cuối bài chúng em sẽ quay lại danh sách này và chỉ "
      "bằng chứng cho từng tiêu chí. Cả tám tiêu chí đều đã được minh họa trên cụm với dữ liệu thật.")

# ------------------------------------------------------------------ 4. Dữ liệu
slide("dataset", tieu_de("Dữ liệu và kiến trúc", "Dữ liệu nhấp chuột RetailRocket") +
      '<div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:32px">' +
      so_lon("2,89 triệu", "sự kiện trong 139 ngày: xem, thêm giỏ, mua") +
      so_lon("417.053", "sản phẩm trong danh mục (126.230 có trong tập huấn luyện)") +
      so_lon("179.999", "người dùng huấn luyện có ít nhất 3 tương tác") +
      so_lon("3,3×10<sup style=\"font-size:0.6em;vertical-align:0.75em;line-height:0\">−5</sup>", "mật độ ma trận huấn luyện (179.999 × 126.230)") +
      "</div>" +
      '<div style="display:flex;flex-direction:column;gap:12px">' +
      dong("Phát lại", "Producer Kafka phát lại nhật ký, nén mỗi ngày thành 30 giây; 4,8 % người dùng là người mới tổng hợp để thử khởi đầu lạnh", 28, 260) +
      dong("Chia dữ liệu", "Theo thời gian: bốn tháng để huấn luyện, tháng cuối (19/8 – 18/9/2015) để kiểm tra; 9.467 người dùng kiểm tra", 28, 260) +
      dong("Trọng số", "Xem 1, thêm giỏ 3, mua 5, suy giảm theo tuổi (λ = 0,01 mỗi ngày)", 28, 260) +
      "</div>",
      "Nhóm dùng bộ dữ liệu RetailRocket: gần 2,9 triệu sự kiện trong 139 ngày. Ma trận cực thưa, mật độ 3,3 nhân mười mũ trừ năm, "
      "và phần lớn khách chỉ xuất hiện một lần; điều này chi phối mọi kết quả về sau. Producer phát lại nhật ký qua Kafka, mỗi ngày "
      "trong ba mươi giây, và thêm khoảng năm phần trăm người dùng mới tổng hợp để thử khởi đầu lạnh. Đánh giá dùng cách chia theo "
      "thời gian: tháng cuối là tập kiểm tra.")

# ------------------------------------------------------------------ 6. Cụm
slide("cluster", tieu_de("Dữ liệu và kiến trúc", "Triển khai trên sáu máy phòng lab") +
      '<div style="display:flex;justify-content:center">' + hinh("f02", "Triển khai vật lý: máy, container, cổng và tài nguyên", cao=606, the=False) + "</div>",
      "Cụm gồm sáu máy phòng lab trên mạng ZeroTier. Máy master chạy NameNode, Spark Master, mọi driver và Airflow. Năm máy worker "
      "cho tổng cộng 34 lõi và 13 GB bộ nhớ executor. Mỗi máy chỉ có 7 GB RAM và dùng chung với nhóm khác, nên mọi thiết lập bộ nhớ "
      "đều được định cỡ từ số đo thật.")

# ------------------------------------------------------------------ 5. Kiến trúc tổng thể (thiết kế của nhóm, 22/11/2026)
# Theo góp ý của GVHD ngày 19/11 và thống nhất của nhóm: slide 5 tách thành hai slide, tổng quan công nghệ rồi năm DAG;
# thứ tự (23/11): cụm → tổng quan → ba job luồng → năm DAG → vòng khép kín → lakehouse → DAG 1.
slide("architecture", tieu_de("Dữ liệu và kiến trúc", "Kiến trúc tổng thể và vai trò các công nghệ") +
      '<div style="display:flex;justify-content:center">' + so_do.tong_quan("vi", ti_le=1.09) + "</div>",
      "Đây là bức tranh chung nhất: hệ thống dùng công nghệ gì và mỗi công nghệ làm gì. Bên trái là nguồn: dữ liệu RetailRocket 2,76 triệu sự kiện trong 139 ngày, được producer phát lại với nhịp một ngày bằng 30 giây. Ở giữa là phần xử lý và lưu trữ: Kafka thu nhận và đệm sự kiện ở hai topic; Spark xử lý, gồm các job streaming gần thời gian thực và các job batch, học máy; lakehouse trên HDFS với Delta Lake lưu trữ theo ba lớp bronze, silver, gold. Bên phải là phần phục vụ: web FastAPI đọc Gold để hiển thị gợi ý và thịnh hành; mọi lượt xem, thêm giỏ, mua và hiển thị quay lại Kafka theo đường nét đứt xanh. Dải dưới cùng là Airflow: lập lịch năm DAG, gọi và giám sát job Spark, thử lại, cảnh báo và bảo trì dữ liệu. Các slide sau đi vào chi tiết từng phần.", dem=DEM_SO_DO, khoang=28)

# ------------------------------------------------------------------ 8. Streaming
slide("streaming", tieu_de("Dữ liệu và kiến trúc", "Ba job Structured Streaming") +
      '<div style="display:flex;justify-content:center">' + hinh("f07", "Ba job luồng và ngân sách độ trễ của khối thịnh hành", cao=606, the=False) + "</div>",
      "Ba job luồng chạy liên tục. Job thứ nhất đưa mọi sự kiện vào bronze. Job thứ hai tính bảng thịnh hành: cửa sổ mười lăm phút "
      "trượt mỗi phút, MERGE vào gold mỗi hai mươi giây. Job thứ ba ghi nhật ký hiển thị cho thử nghiệm A/B. Một điểm thiết kế: cửa sổ "
      "tính theo thời điểm của Kafka chứ không theo thời điểm gốc của sự kiện, vì dữ liệu phát lại là từ năm 2015 còn cú nhấp thật là "
      "từ năm 2026.")

# ------------------------------------------------------------------ 5b. Năm DAG Airflow (thiết kế của nhóm, 22/11/2026)
slide("dags", tieu_de("Dữ liệu và kiến trúc", "Airflow điều phối năm DAG") +
      '<div style="display:flex;flex-direction:column;align-items:center;gap:8px">' + so_do.nam_dag("vi", ti_le=1.07) +
      chu_thich("Năm DAG Airflow và phạm vi điều phối trong hệ thống.") + "</div>",
      "Airflow không tự xử lý dữ liệu; nó lập lịch và gọi spark-submit. DAG 1 chạy lúc 2 giờ sáng: pipeline batch chính từ bronze đến danh sách gợi ý trong gold. DAG 2 mỗi 15 phút kiểm tra luồng thịnh hành, khởi động lại nếu luồng chết hoặc dữ liệu cũ. DAG 3 mỗi giờ đọc nhật ký hiển thị, tính CTR, tỷ lệ chuyển đổi và p-value rồi ghi kết quả A/B. DAG 4 lúc 4 giờ sáng chạy OPTIMIZE, Z-ORDER và VACUUM. DAG 5 kích hoạt tay, dùng time travel để huấn luyện lại trên đúng phiên bản dữ liệu cũ và so với lịch sử. Hàng Ảnh hưởng trong mỗi ô cho biết DAG đó chạm vào bảng nào.", dem=DEM_SO_DO, khoang=28)

# ------------------------------------------------------------------ 21. Vòng khép kín
slide("loop", tieu_de("Hệ thống thật và vận hành", "Vòng khép kín, đo trực tiếp") +
      '<div style="display:flex;gap:40px;flex:1">'
      '<div style="width:290px;display:flex;flex-direction:column;gap:28px">' +
      so_lon("10,3 s", "Kafka → bronze cho ba cú nhấp “Thêm giỏ”", BLUE, 56) +
      so_lon("51 s", "cú nhấp → bảng thịnh hành cập nhật; mục tiêu&nbsp;90&nbsp;s", ORANGE, 56) +
      '<p style="font-size:24px;line-height:1.35;color:{m}">Cú nhấp gửi khi một luồng đang dừng vẫn được tính sau khi luồng chạy lại.</p>'.format(m=MUTED) +
      "</div>" + hinh("f08", "Vòng khép kín giữa ứng dụng web, Kafka, các luồng và pipeline A/B", cao=606, the=False) + "</div>",
      "Hai tiêu chí này được đo ngày 11/11 qua API của ứng dụng web. Ba cú nhấp thêm giỏ đến bronze khoảng mười giây sau khi Kafka nhận, "
      "và đến bảng thịnh hành năm mươi mốt giây sau cú nhấp: hai mươi giây chu kỳ kích hoạt, cộng micro-batch và chu kỳ đọc. Ở lần thử "
      "đầu, một luồng đã chết; sau khi khởi động lại, các cú nhấp trước đó vẫn được tính, cho thấy luồng phục hồi đúng một lần từ "
      "checkpoint.")

# ------------------------------------------------------------------ 7. Lakehouse (thiết kế của nhóm, 22/11/2026)
# Luồng lưu trữ (Bronze, Silver, Gold) ở trên, luồng tính toán (job Spark) ở dưới, theo góp ý của GVHD ngày 19/11.
slide("lakehouse", tieu_de("Dữ liệu và kiến trúc", "Lakehouse trên HDFS: luồng lưu trữ và luồng tính toán") +
      '<div style="display:flex;justify-content:center">' + so_do.lakehouse("vi", ti_le=1.13) + "</div>",
      "Lakehouse tách hai luồng. Luồng lưu trữ ở trên chỉ gồm ba lớp: bronze giữ sự kiện thô từ Kafka dạng Parquet, chỉ ghi thêm; silver giữ dữ liệu đã làm sạch; gold giữ các bảng phục vụ web, dashboard và A/B. Luồng tính toán ở dưới là các job Spark: bronze_to_silver khử trùng, lọc bot, gán trọng số rồi ghi silver; bước đặc trưng dựng ma trận tương tác, chia train và test theo thời gian, áp suy giảm theo thời gian, cùng thống kê sản phẩm và đồng xuất hiện; cuối cùng là huấn luyện ALS, khởi đầu lạnh, xếp lại, trộn, rồi ghi kết quả vào gold. Dải số bên dưới cho thấy dữ liệu đi qua từng bước.", dem=DEM_SO_DO, khoang=28)

# ------------------------------------------------------------------ 9. Airflow
slide("airflow", tieu_de("Dữ liệu và kiến trúc", "DAG 1 chi tiết: pipeline hằng đêm") +
      '<p style="font-size:26px;line-height:1.35;color:{t}"><b>DAG 2</b> giám sát luồng (mỗi 15 phút) · <b>DAG 3</b> A/B (mỗi giờ) · <b>DAG 4</b> bảo trì (04:00) · <b>DAG 5</b> tái lập (thủ công)</p>'.format(t=TEXT) +
      '<div style="display:flex;justify-content:center">' + hinh("f05b", "DAG 1 kn_pipeline_hang_ngay, pipeline hằng đêm; mọi job đều lũy đẳng", rong=1488, the=False) + "</div>",
      "Slide 8 đã giới thiệu năm DAG; đây là chi tiết DAG 1, pipeline hằng đêm: chờ dữ liệu mới, dựng lại silver và ma "
      "trận, tinh chỉnh ALS, đánh giá rồi rẽ nhánh — chỉ công bố gợi ý mới nếu NDCG không kém mô hình đang phục vụ. Bốn DAG còn lại "
      "giám sát luồng, tính A/B mỗi giờ, bảo trì các bảng Delta và tái lập thực nghiệm.")

# ------------------------------------------------------------------ 10. ALS
src_p, wp, hp = IMG["eq_pref"][:3]
src_a, wa, ha = IMG["eq_als"][:3]
slide("als", tieu_de("Mô hình và đánh giá", "ALS cho phản hồi ngầm") +
      '<div style="display:flex;gap:32px;align-items:center;background:{c};border:1px solid {r};border-radius:16px;padding:28px 40px">'
      '<img src="{sp}" alt="Sở thích và độ tin cậy của một quan sát" style="width:560px;height:{hp}px;object-fit:contain">'
      '<img src="{sa}" alt="Hàm mục tiêu ALS cho phản hồi ngầm" style="width:900px;height:{ha}px;object-fit:contain"></div>'.format(
          c=CARD, r=RULE, sp=src_p, hp=round(560 * hp / wp), sa=src_a, ha=round(900 * ha / wa)) +
      '<div style="display:flex;flex-direction:column;gap:14px">' +
      dong("Mọi ô đều tính", "Cặp đã quan sát có sở thích 1, độ tin cậy 1 + α·r; cặp chưa thấy có sở thích 0, độ tin cậy 1", 28, 330) +
      dong("Luân phiên", "Cố định sản phẩm, giải từng người dùng bằng công thức đóng, rồi đổi vai; Spark song song hóa theo khối", 28, 330) +
      dong("Lưới (27)", "rank {10, 30, 50} × regParam {0,01; 0,1; 1,0} × alpha {1; 10; 40}, chọn theo NDCG@10", 28, 330) +
      "</div>",
      "Nhóm dùng ALS ở dạng phản hồi ngầm của Hu, Koren và Volinsky. Một cú nhấp không phải điểm đánh giá: nó cho biết độ tin cậy, "
      "không phải sở thích. Mọi ô của ma trận đều vào hàm mất mát: cặp đã quan sát có sở thích một và độ tin cậy tăng theo alpha nhân "
      "điểm đã suy giảm, cặp chưa thấy có sở thích không. Bình phương tối thiểu luân phiên biến mỗi nửa bước thành các bài toán bình "
      "phương tối thiểu độc lập mà Spark phân tán được. Nhóm đã tinh chỉnh đủ 27 tổ hợp.")

# ------------------------------------------------------------------ 11. Tuning
slide("tuning", tieu_de("Mô hình và đánh giá", "Tinh chỉnh: rank lớn hơn, alpha cao hơn") +
      '<p style="font-size:32px;line-height:1.35;color:{n}">Tốt nhất trong 27: <b>rank 50 · regParam 1,0 · alpha 40 → NDCG@10 = 0,0680</b> <span style="color:{m}">(78 phút trên 34 lõi)</span></p>'.format(n=NAVY, m=MUTED) +
      '<div style="display:flex;justify-content:center">' + hinh("c01", "NDCG@10 trên lưới tinh chỉnh, mỗi rank một bản đồ nhiệt", rong=1290) + "</div>" +
      '<p style="font-size:26px;color:{m}">Kiểm tra ngày 11/11: chọn trên 19/8 – 2/9, báo cáo trên 3–18/9 → cùng cấu hình (độ lạc quan 0,0002).</p>'.format(m=MUTED),
      "NDCG tăng đơn điệu theo rank và theo alpha. Với phản hồi ngầm, alpha lớn cho các cặp đã quan sát, nhất là lượt mua, trọng số "
      "lớn hơn hẳn các ô trống, nên mô hình rút được nhiều tín hiệu hơn từ dữ liệu rất thưa. Điểm tối ưu nằm ở biên của lưới, nên bước "
      "tiếp theo tự nhiên là rank 100 và alpha 80. Nhóm cũng kiểm tra phản biện dễ thấy, rằng lưới được chọn trên tháng kiểm tra: chọn "
      "trên mười lăm ngày đầu và báo cáo trên mười sáu ngày cuối vẫn ra đúng cấu hình đó, độ lạc quan chỉ 0,0002.")

# ------------------------------------------------------------------ 12. Baselines
slide("baselines", tieu_de("Mô hình và đánh giá", "ALS vượt cả hai baseline") +
      '<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:48px">' +
      so_lon("+1.237 %", "NDCG@10 so với bán chạy toàn sàn (0,0051 → 0,0680); mục tiêu +30 %") +
      so_lon("+43 %", "NDCG@10 so với bán chạy theo danh mục (0,0475)") +
      so_lon("0,0865", "NDCG@10 của cấu hình phục vụ (ALS + xếp lại)", ORANGE) +
      "</div>" +
      '<div style="display:flex;justify-content:center">' + hinh("c02", "NDCG@10 và độ phủ của các baseline, ALS và các biến thể xếp lại", rong=1070) + "</div>",
      "So với hai baseline bắt buộc, ALS thắng cách biệt: gấp mười ba lần danh sách bán chạy toàn sàn, và hơn 43 phần trăm so với bán "
      "chạy trong danh mục của người dùng — một baseline mạnh hơn nhiều vì khách RetailRocket thường ở lại trong một danh mục. ALS "
      "cũng thắng ở precision, recall và MAP. Cấu hình phục vụ, có xếp lại theo độ mới lạ, còn tốt hơn nữa.")

# ------------------------------------------------------------------ 12b. Phân tích lỗi
slide("errors", tieu_de("Mô hình và đánh giá", "ALS sai ở đâu: phân tích lỗi") +
      '<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:48px">' +
      so_lon("Mọi nhóm", "ALS hơn bán chạy theo danh mục ở cả 12 nhóm người dùng, 29–54 %") +
      so_lon("41 %", "người dùng kiểm tra chỉ chạm sản phẩm đuôi dài; NDCG@10 của họ bằng 1/9 phần còn lại", ORANGE) +
      so_lon("84,8 %", "người dùng kiểm tra nhận NDCG@10 = 0 từ cả ALS lẫn baseline danh mục") +
      "</div>" +
      '<div style="display:flex;justify-content:center">' + hinh("c09", "NDCG@10 theo nhóm người dùng của ALS và hai baseline (9.467 người dùng kiểm tra, chạy trên cụm 16/11)", rong=1100) + "</div>",
      "Nhóm dự đoán ALS sẽ thua baseline danh mục ở người dùng ít tương tác và ở người dùng chỉ xem một danh mục. Cả hai giả thuyết "
      "đều sai: ALS thắng ở mọi nhóm, kể cả nhóm ba đến bốn tương tác chiếm hai phần ba tập kiểm tra, và người dùng một danh mục lại "
      "là trường hợp dễ nhất cho cả hai phương pháp. Điểm yếu thật là đuôi dài: khi các sản phẩm người dùng chạm tới trong tháng sau "
      "đều ít phổ biến, cả hai phương pháp gần như luôn cho điểm không. Đó chính là thiên lệch độ phổ biến ở slide tiếp theo, và nó "
      "chỉ ra hướng cần thêm nguồn ứng viên cho đuôi dài, thay vì tinh chỉnh thêm ALS.")

# ------------------------------------------------------------------ 13. Thiên lệch độ phổ biến
slide("bias", tieu_de("Mô hình và đánh giá", "Độ phủ bị giới hạn bởi thiên lệch độ phổ biến") +
      '<p style="font-size:32px;line-height:1.35;color:{n};width:1600px"><b>99,2 %</b> số ô của ALS rơi vào 10 % sản phẩm phổ biến nhất. Độ phủ <b>3,44 %</b> so với trần cấu trúc 75 % — nguyên nhân là thiên lệch, không phải số ô.</p>'.format(n=NAVY) +
      '<div style="display:flex;justify-content:center">' + hinh("c03", "Phân bố ô gợi ý theo nhóm độ phổ biến", rong=1245) + "</div>",
      "Độ phủ là ngưỡng duy nhất ALS thuần chưa đạt: 3,44 phần trăm so với mục tiêu 10. Đặc tả chấp nhận điều này nếu có phân tích. "
      "Mười gợi ý cho 9.467 người dùng kiểm tra cho gần 95 nghìn ô trên 126 nghìn sản phẩm, nên trần là 75 phần trăm. Nguyên nhân "
      "thật là thiên lệch độ phổ biến: sản phẩm ít tương tác có vector ẩn yếu và hiếm khi lọt vào top mười.")

# ------------------------------------------------------------------ 14. Xếp lại
slide("rerank", tieu_de("Mô hình và đánh giá", "Xếp lại theo độ mới lạ: chính xác và đa dạng cùng tăng") +
      '<div style="display:flex;gap:48px;flex:1">'
      '<div style="flex:1;display:flex;flex-direction:column;gap:28px">' +
      so_lon("+27 %", "NDCG@10 với β = 1,0 trên 200 ứng viên (0,0680 → 0,0865)", ORANGE, 64) +
      so_lon("+88 %", "độ phủ (3,44 % → 6,47 %); phần ô thuộc top 10 % giảm từ 99,2 % xuống 80,8 %", ORANGE, 64) +
      '<p style="font-size:26px;line-height:1.4;color:{m}">Đã triển khai để phục vụ; không β nào một mình đạt 10 %. Offline, 3 ô đồng xuất hiện cho độ phủ <b style="color:{o}">14,5 %</b> và NDCG@10 0,0892.</p>'.format(m=MUTED, o=ORANGE) +
      "</div>" + hinh("c04", "Đánh đổi độ chính xác – độ phủ khi xếp lại theo độ mới lạ", cao=560) + "</div>",
      "Để giảm thiên lệch, nhóm xếp lại 200 ứng viên của ALS theo điểm ALS chuẩn hóa cộng beta nhân độ mới lạ chuẩn hóa. Với beta "
      "đến một, độ chính xác và độ đa dạng cùng tăng, vì tháng kiểm tra chứa nhiều sản phẩm hiếm trong tập huấn luyện. Nhóm đã triển "
      "khai cấu hình đó. Để đạt mười phần trăm cần nguồn ứng viên ngoài ALS, và nhóm đã thử offline: dành ba ô cuối cho sản phẩm được "
      "xem cùng với sản phẩm của chính người dùng thì độ phủ lên 14,5 phần trăm và NDCG cũng tăng. Cách này chưa được triển khai.")

# ------------------------------------------------------------------ 15. Khởi đầu lạnh
slide("coldstart", tieu_de("Mô hình và đánh giá", "Khởi đầu lạnh và khối “Dành cho bạn” trộn nguồn") +
      '<div style="display:flex;gap:44px;flex:1">'
      '<div style="width:1040px;display:flex;flex-direction:column;gap:18px">' +
      hinh("f11", "Mẫu mười ô hiển thị theo loại người dùng và nhóm A/B", rong=1040, the=False) +
      dong("Người dùng mới", "Top 10 của cửa sổ thịnh hành mới nhất", 26, 250) +
      dong("&lt; 5 tương tác", "Bán chạy của các danh mục đã xem + thịnh hành", 26, 250) +
      dong("Sản phẩm mới", "Sản phẩm ALS chưa biết được đưa vào ô 9–10", 26, 250) +
      "</div>" + hinh("s01", "Trang chủ của người dùng vừa tạo tại chỗ", rong=540) + "</div>",
      "Ba chiến lược khởi đầu lạnh là các hàm thuần, dùng chung cho job lô và ứng dụng web. Người dùng mới hoàn toàn thấy các sản "
      "phẩm thịnh hành mới nhất; người dùng dưới năm tương tác thấy hàng bán chạy của các danh mục đã xem, xen kẽ với thịnh hành; còn "
      "sản phẩm mới mà ALS chưa biết đi vào hai ô cuối. Ảnh chụp là hệ thống thật: người dùng tạo bằng một cú nhấp lập tức nhận chiến "
      "lược người mới, và trang ghi rõ người dùng này nằm ngoài thử nghiệm A/B. Tên và ảnh sản phẩm chỉ để minh họa, vì RetailRocket "
      "đã ẩn danh.")

# ------------------------------------------------------------------ 16. A/B
slide("abtest", tieu_de("Thực nghiệm và lakehouse", "A/B: trộn sản phẩm thịnh hành làm giảm CTR") +
      '<div style="display:flex;gap:40px;align-items:center">'
      '<p style="font-family:{h};font-size:64px;font-weight:700;line-height:1;color:{o}">p = 0,015</p>'
      '<p style="flex:1;font-size:26px;line-height:1.35;color:{t}">CTR 2,094 % (A) so với 1,978 % (B), z = −2,43: B kém hơn 5,5 %; chuyển đổi p = 0,75. '
      '350.160 lượt hiển thị mô phỏng trong 7 ngày (cần 76.972 mỗi nhóm); bootstrap theo người dùng p = 0,029.</p></div>'.format(h=H, o=ORANGE, t=TEXT) +
      '<div style="display:flex;justify-content:center">' + hinh("c06", "CTR và tỷ lệ chuyển đổi theo nhóm, khoảng tin cậy 95 %", rong=1250) + "</div>",
      "Nhóm A chỉ thấy ALS, nhóm B thấy ALS trộn ba mươi phần trăm sản phẩm thịnh hành. Khách thật quá ít, nên lượt hiển thị được mô "
      "phỏng bằng mô hình nhấp phụ thuộc vị trí và gắn nhãn là mô phỏng. Trên 350 nghìn lượt hiển thị, B có tỷ lệ nhấp thấp hơn có ý "
      "nghĩa, p bằng 0,015, và không khác biệt về chuyển đổi. Coi người dùng là đơn vị, bootstrap theo người dùng vẫn cho p bằng 0,029. "
      "Kết luận chỉ đúng trong giả định của mô phỏng, vốn không ưu đãi sản phẩm thịnh hành. Bảng điều khiển trực tiếp, đã xem trong "
      "demo, còn tính cả các lượt hiển thị thật trên web từ ngày 11/11, nên p hơi khác, khoảng 0,014.")

# ------------------------------------------------------------------ 17. Delta Lake
slide("delta", tieu_de("Thực nghiệm và lakehouse", "Các tính năng Delta Lake được dùng") +
      bang(["Tính năng", "Cách dự án dùng"], [
          ["<b>MERGE</b>", "Upsert danh sách gợi ý, cửa sổ thịnh hành và lượt hiển thị; xóa các thứ hạng thừa"],
          ["<b>Time travel</b>", "Mỗi lần huấn luyện ghi phiên bản đặc trưng; kết quả A/B tổng hợp lại đúng phiên bản đã ghi"],
          ["<b>Schema evolution</b>", "ALTER TABLE ADD COLUMNS giá và số sao → mô hình phiên bản 2"],
          ["<b>Change Data Feed</b>", "Nhật ký hiển thị → job A/B hằng giờ chỉ tính lại các ngày bị ảnh hưởng"],
          ["<b>OPTIMIZE / Z-ORDER</b>", "Gộp tệp hằng đêm; Z-ORDER BY người dùng trên bảng gợi ý đang phục vụ"],
          ["<b>Phân vùng, lưu giữ</b>", "Silver theo ngày, mỗi ngày một tệp; giữ tệp 30 ngày, nhật ký 60 ngày"],
          ["<b>Tương tranh</b>", "Luồng và bảo trì cùng ghi một bảng → thử lại khi gặp lỗi xung đột Delta"],
      ], [30, 70], 24),
      "Mọi yêu cầu về Delta Lake của đặc tả đều được dùng cho một mục đích thật. MERGE làm mọi bước ghi trở nên lũy đẳng. Time travel "
      "phục vụ tái lập. Schema evolution sinh ra mô hình phiên bản 2. Change Data Feed cho job A/B chỉ đọc lượt hiển thị mới. Và vì luồng "
      "và bảo trì cùng ghi một bảng, tương tranh lạc quan phải được xử lý bằng cơ chế thử lại tường minh.")

# ------------------------------------------------------------------ 18. Z-ORDER
slide("zorder", tieu_de("Thực nghiệm và lakehouse", "Z-ORDER: mỗi lần tra cứu chỉ đọc một tệp") +
      '<div style="display:flex;gap:48px;flex:1">'
      '<div style="flex:1;display:flex;flex-direction:column;gap:32px">' +
      so_lon("67 → 1,07", "tệp đọc mỗi lần tra cứu, chỉ gộp tệp so với Z-ORDER BY người dùng", BLUE, 56) +
      so_lon("8.280 → 980 ms", "độ trễ trung vị khi tra cứu theo người dùng", BLUE, 56) +
      so_lon("8,4×", "nhanh hơn chỉ gộp tệp (11× so với bảng phân mảnh)", ORANGE, 56) +
      "</div>" + hinh("c05", "Độ trễ và số tệp đọc trong ba pha", rong=1040) + "</div>",
      "Truy vấn nóng của ứng dụng web là tra cứu theo người dùng. Nhóm đo ba pha trên bản sao của bảng 5,3 triệu dòng. Chỉ gộp tệp "
      "gần như không giúp: mỗi tệp sau khi gộp vẫn trải trên toàn dải mã người dùng. Z-ORDER gom các dòng cùng người dùng, thu hẹp "
      "thống kê min-max của từng tệp, và cho data skipping chỉ đọc khoảng một tệp. Số tệp đọc là con số đáng tin; độ trễ thay đổi "
      "theo Wi-Fi.")

# ------------------------------------------------------------------ 19. Tái lập
slide("reproduce", tieu_de("Thực nghiệm và lakehouse", "Tái lập thực nghiệm bằng time travel") +
      '<div style="display:flex;gap:40px;align-items:center">'
      '<p style="font-family:{h};font-size:64px;font-weight:700;line-height:1;color:{b}">0,000000</p>'
      '<p style="flex:1;font-size:26px;line-height:1.35;color:{t}">độ lệch lớn nhất khi huấn luyện lại trên features VERSION AS OF 0 (10/11) và mô hình đang phục vụ ở phiên bản 3 (11/11); seed cố định, indexer chỉ nối thêm, tập kiểm tra lưu trong cùng phiên bản.</p></div>'.format(h=H, b=BLUE, t=TEXT) +
      '<div style="display:flex;justify-content:center">' + hinh("f09", "Tái lập bằng Delta time travel (DAG 5); các phiên bản bảng về sau không ảnh hưởng", cao=498, the=False) + "</div>",
      "DAG 5 chạy lại thực nghiệm đã chọn trên phiên bản không của bảng đặc trưng và so kết quả với chỉ số đã ghi: độ lệch bằng không "
      "đến sáu chữ số thập phân. Đây là một kiểm thử có ý nghĩa, vì trong lúc chạy các bảng nguồn đã thay đổi. Kết quả trùng khớp vì "
      "job chỉ đọc đúng phiên bản đó, ALS có seed cố định, và các indexer chỉ nối thêm, nên một chỉ số luôn chỉ cùng một người dùng hay "
      "sản phẩm ở mọi phiên bản. Ngày 11/11 nhóm lặp lại từ nút trên web cho mô hình đang phục vụ ở phiên bản ba: lại trùng khớp hoàn toàn.")

# ------------------------------------------------------------------ 20. v2
slide("v2", tieu_de("Thực nghiệm và lakehouse", "Schema evolution và mô hình phiên bản 2") +
      '<div style="display:flex;gap:56px;flex:1">'
      '<div style="flex:1;display:flex;flex-direction:column;gap:16px">' +
      dong("1", "ALTER TABLE silver ADD COLUMNS (giá, số sao): chỉ đổi metadata", 28, 40) +
      dong("2", "MERGE điền cả hai cột từ catalog; dòng mới tự có giá trị", 28, 40) +
      dong("3", "Phiên bản 2 giữ ứng viên trong tầm giá của người dùng [0,5·P10; 1,5·P90]", 28, 40) +
      dong("4", "Cổng hằng đêm: chỉ triển khai nếu NDCG@10 không kém mô hình đang phục vụ", 28, 40) +
      "</div>"
      '<div style="width:720px;display:flex;flex-direction:column;gap:20px">'
      '<p style="font-size:28px;font-weight:600;color:{n}">Mô hình triển khai (rank 50, reg 1,0, alpha 40)</p>'.format(n=NAVY) +
      bang(["Chỉ số", "v1", "v2"], [
          ["NDCG@10", "0,0680", "<b>0,0694</b> (+2,1 %)"],
          ["Độ phủ", "3,44 %", "<b>3,72 %</b>"],
          ["Số SP khác nhau", "4.346", "<b>4.699</b>"],
      ], [40, 25, 35], 28) +
      '<p style="font-size:26px;line-height:1.4;color:{m}">11/11: lượt 02:00, được đánh thức lúc 05:22 bởi các cú nhấp thử trên web, '
      'tự triển khai v2 lúc 06:07; một lượt chạy tay xác nhận lại. Với alpha 10, mức tăng tương tự (+2,3 %).</p>'.format(m=MUTED) +
      "</div></div>",
      "Tuần năm của đặc tả yêu cầu một thay đổi lược đồ. Nhóm thêm giá và số sao vào silver bằng ALTER TABLE, điền bằng MERGE, rồi dựng "
      "mô hình phiên bản 2, giữ gợi ý trong tầm giá quen thuộc của người dùng. Cùng siêu tham số, phiên bản 2 tốt hơn ở mọi chỉ số, "
      "khoảng hai phần trăm — nhỏ so với xếp lại. Lượt chạy đêm đầu tiên giữ mô hình cũ vì một lý do thiết kế mà chúng em giải thích "
      "ở slide vận hành; sau khi sửa, cổng so sánh đúng cấu hình: ngày 11/11, lượt chạy theo lịch, được đánh thức bởi chính các cú nhấp "
      "thử trên web, đã huấn luyện lại và triển khai phiên bản 2 mà không ai can thiệp.")

# ------------------------------------------------------------------ 22. Vận hành
van_de = [
    ("Hố đen Path MTU", "Từ mạng ở nhà, gói 1.400 byte biến mất: SSH treo ở bước trao đổi khóa.",
     "Máy trung gian, MTU ZeroTier 1.280"),
    ("Driver luồng “thây ma”", "Hủy ứng dụng vẫn để driver chế độ client sống; hai driver tranh một checkpoint.",
     "Dừng = hủy ứng dụng + kill driver"),
    ("Cổng không bao giờ mở", "Lưới hằng đêm thiếu alpha đang phục vụ, nên mô hình mới không thể thắng.",
     "Luôn thử lại cấu hình đang phục vụ"),
    ("Tệp nhỏ, bộ nhớ nhỏ", "7.334 tệp cho 20.624 dòng; một master 7,4 GB cho mọi driver.",
     "Mỗi ngày một tệp; pool 1 chỗ ban đêm"),
]
the = "".join(
    '<div style="display:flex;flex-direction:column;gap:12px;background:{c};border:1px solid {r};border-radius:14px;padding:26px 30px">'
    '<h3 style="font-family:{h};font-size:36px;font-weight:600;color:{n}">{t}</h3>'
    '<p style="font-size:26px;line-height:1.4;color:{tx}">{m}</p>'
    '<p style="font-size:26px;font-weight:600;color:{b}">→ {f}</p></div>'.format(
        c=CARD, r=RULE, h=H, n=NAVY, t=t, tx=TEXT, m=m, b=BLUE, f=f) for t, m, f in van_de)
slide("operations", tieu_de("Hệ thống thật và vận hành", "Bài học từ một cụm dùng chung, tài nguyên hạn chế") +
      '<div style="display:grid;grid-template-columns:1fr 1fr;gap:24px 28px">' + the + "</div>" +
      '<p style="font-size:26px;color:{m}">33 sự cố được phát hiện và sửa, mỗi sự cố một kiểm thử hồi quy — tổng cộng 143 kiểm thử tự động.</p>'.format(m=MUTED),
      "Một phần lớn công sức nằm ở vận hành: ba mươi ba sự cố, mỗi sự cố một kiểm thử hồi quy. Bốn ví dụ. Hố đen path-MTU trên mạng ở "
      "nhà lặng lẽ làm rơi các gói lớn. Hủy một ứng dụng Spark qua Master không dừng driver chế độ client, và driver “thây ma” tranh "
      "checkpoint với luồng mới. Cổng triển khai hằng đêm không bao giờ mở được, vì lưới hằng đêm không chứa siêu tham số đang phục vụ. "
      "Và tệp nhỏ cùng bộ nhớ nhỏ buộc nhóm phải phân vùng lại và lên lịch cẩn thận.")

# ------------------------------------------------------------------ 23. Kết quả
slide("results", tieu_de("Kết luận", "Cả tám tiêu chí đã chứng minh trên cụm") +
      bang(["#", "Tiêu chí", "Bằng chứng", "Tình trạng"], [
          ["1", "Bảng tinh chỉnh, ≥ 9 tổ hợp", "27 tổ hợp; NDCG@10 tốt nhất = 0,0680", "Đạt"],
          ["2", "ALS so với 2 baseline", "+1.237 % / +43 %; độ phủ 3,44 %, 6,47 % khi phục vụ, 14,5 % offline", "Đạt, có phân tích độ phủ"],
          ["3", "Ba chiến lược khởi đầu lạnh", "Người dùng mới minh họa trực tiếp; cả ba có kiểm thử đơn vị", "Đạt"],
          ["4", "Thịnh hành ≤ 90 s sau cú nhấp", "51 s", "Đạt"],
          ["5", "Vòng khép kín web → bronze → thịnh hành", "10,3 s đến bronze", "Đạt"],
          ["6", "A/B có p-value", "CTR p = 0,015 (B thấp hơn), chuyển đổi p = 0,75", "Đạt"],
          ["7", "Tái lập, VERSION AS OF", "Độ lệch 0,000000", "Đạt"],
          ["8", "OPTIMIZE / Z-ORDER", "Tệp đọc 67 → 1,07, nhanh hơn 8,4×", "Đạt"],
      ], [5, 33, 42, 20], 24),
      "Khép lại danh sách từ đầu bài: cả tám tiêu chí nghiệm thu đã được chứng minh trên cụm với dữ liệu thật. Ngưỡng duy nhất chưa đạt "
      "khi phục vụ, độ phủ catalog, đã được phân tích như đặc tả yêu cầu; xếp lại đang triển khai làm nó tăng gần gấp đôi, và cách trộn "
      "ứng viên đồng xuất hiện chạy offline đã vượt ngưỡng.")

# ------------------------------------------------------------------ 24. Hạn chế
cot = lambda tieu, ds, mau: (
    '<div style="flex:1;display:flex;flex-direction:column;gap:14px;background:{c};border:1px solid {r};border-top:8px solid {m};border-radius:14px;padding:32px">'
    '<h3 style="font-family:{h};font-size:44px;font-weight:600;color:{n}">{t}</h3>'
    '<ul style="font-size:28px;line-height:1.45;color:{tx}">{li}</ul></div>').format(
    c=CARD, r=RULE, m=mau, h=H, n=NAVY, t=tieu, tx=TEXT, li="".join("<li>{}</li>".format(x) for x in ds))
slide("limits", tieu_de("Kết luận", "Hạn chế và hướng phát triển") +
      '<div style="display:flex;gap:32px;flex:1">' +
      cot("Hạn chế", [
          "Độ phủ khi phục vụ 6,5 %; 14,5 % chỉ ở offline",
          "Lưới chính chọn trên tháng kiểm tra (kiểm tra bằng validation: cùng lựa chọn)",
          "Lưu lượng A/B mô phỏng (kết quả vẫn giữ ở mức người dùng)",
          "Một NameNode, một Spark Master, một broker Kafka",
          "Danh sách cá nhân chỉ đổi mỗi đêm",
      ], ORANGE) +
      cot("Hướng phát triển", [
          "Triển khai ô đồng xuất hiện, chạy lại A/B",
          "Lưới validation đầy đủ và cách chia cuốn chiếu",
          "Độ nhạy của kết quả A/B theo sức hút của sản phẩm thịnh hành",
          "Tính sẵn sàng cao cho HDFS và Kafka",
          "Cập nhật mô hình tăng dần (trực tuyến)",
      ], BLUE) + "</div>",
      "Chúng em nói rõ các giới hạn. Độ phủ vẫn dưới mười phần trăm; siêu tham số được chọn trên tháng kiểm tra; lưu lượng A/B là mô "
      "phỏng; và master, NameNode, broker là các điểm lỗi đơn. Các bước tiếp theo giá trị nhất là thêm nguồn ứng viên trước khi xếp "
      "lại, một quy trình validation đúng chuẩn, và phân tích độ nhạy của mô phỏng A/B.")

# ------------------------------------------------------------------ 25. Cảm ơn
demo = ["8 tiêu chí minh họa trực tiếp", "Lượt chạy DAG Airflow", "HDFS + Delta SQL", "Web ghi ngược → Delta", "Luồng chạy ≥ 30 phút"]
slide("thanks", (
    '<p style="font-size:24px;font-weight:600;letter-spacing:3px;text-transform:uppercase;color:{la}">Cảm ơn</p>'
    '<h1 style="font-family:{h};font-size:96px;font-weight:700;line-height:1.08;color:{bg}">Cảm ơn Thầy — xin mời đặt câu hỏi</h1>'
    '<div style="display:flex;gap:20px;flex-wrap:wrap">{d}</div>'
    '<div style="flex:1"></div>'
    '<p style="font-family:{mo};font-size:26px;color:#C9D4E5">github.com/chitam31/CS411V_Final-Project_Ecommerce-Recommender-System</p>'
).format(la=LIGHT_ACC, h=H, bg=BG, mo=M, d="".join(
    '<p style="font-size:28px;font-weight:600;color:{n};background:{la};padding:12px 24px;border-radius:12px">{i}. {t}</p>'.format(
        n=NAVY, la=LIGHT_ACC, i=i + 1, t=t) for i, t in enumerate(demo))),
    "Cảm ơn Thầy đã lắng nghe. Thầy đã xem hệ thống chạy trực tiếp ở phần đầu: tám tiêu chí trên cụm, một lượt chạy DAG, cây "
    "HDFS và Delta SQL, một lần ghi ngược từ web được xác nhận trong Delta, và các luồng đã chạy nhiều giờ. Chúng em xin sẵn sàng trả "
    "lời câu hỏi.",
    nen=NAVY)
p = os.path.join(SL, "thanks.html")
s = open(p, encoding="utf-8").read().replace("color:{}\">BD03".format(MUTED), "color:#9FB0C8\">BD03").replace(
    "text-align:right;font-size:24px;color:{}".format(MUTED), "text-align:right;font-size:24px;color:#9FB0C8")
open(p, "w", encoding="utf-8").write(s)

# ------------------------------------------------------------------ 26. Dự phòng (hỏi đáp)
slide("backup", tieu_de("Dự phòng cho hỏi – đáp", "Thí nghiệm bổ sung (11/11)") +
      '<div style="display:flex;gap:56px;flex:1">'
      '<div style="flex:1.25;display:flex;flex-direction:column;gap:20px">'
      '<p style="font-family:{h};font-size:34px;font-weight:600;color:{n}">Lựa chọn trên kỳ validation</p>'.format(h=H, n=NAVY) +
      '<p style="font-size:24px;color:{m}">Chọn trên 19/8 – 2/9 (6.505 người dùng), báo cáo trên 3–18/9 (4.215 người dùng); giữ nguyên train, seed và lưới</p>'.format(m=MUTED) +
      bang(["Các ngày giữ lại", "NDCG@10", "so với danh mục"], [
          ["Bán chạy theo danh mục", "0,0413", "—"],
          ["ALS chọn trên validation = <b>đang triển khai</b> (rank 50, reg 1,0, alpha 40)", "0,0579", "+40 %"],
          ["ALS tốt nhất trên chính các ngày giữ lại", "0,0581", "+41 %"],
          ["+ xếp lại, β = 1,0 chọn trên validation", "<b>0,0742</b>", "<b>+80 %</b>"],
      ], [62, 19, 19], 24) +
      '<p style="font-size:24px;color:{t}">17 trong 27 tổ hợp; thứ tự giống nhau ở cả hai kỳ (Spearman 0,995)</p>'.format(t=TEXT) +
      "</div>"
      '<div style="flex:1;display:flex;flex-direction:column;gap:20px">'
      '<p style="font-family:{h};font-size:34px;font-weight:600;color:{n}">A/B với đơn vị là người dùng</p>'.format(h=H, n=NAVY) +
      '<p style="font-size:24px;color:{m}">8.592 người dùng mô phỏng, khoảng 41 lượt hiển thị mỗi người</p>'.format(m=MUTED) +
      bang(["CTR, B so với A (−5,5 %)", "p-value"], [
          ["Kiểm định z theo lượt hiển thị (báo cáo)", "0,015"],
          ["Kiểm định z bền vững theo cụm", "<b>0,037</b>"],
          ["Bootstrap theo người dùng, 2.000 lần", "<b>0,029</b>"],
      ], [72, 28], 24) +
      '<p style="font-size:24px;color:{t}">KTC 95 % của chênh lệch tương đối: −10,5 % đến −0,7 %; hệ số thiết kế 1,36; chuyển đổi vẫn p = 0,75</p>'.format(t=TEXT) +
      "</div></div>" +
      '<div style="border-top:1px solid {r};padding-top:18px;display:flex;gap:40px;align-items:baseline">'
      '<p style="font-family:{h};font-size:30px;font-weight:600;color:{n};min-width:max-content">Nguồn ứng viên (offline)</p>'
      '<p style="font-size:24px;line-height:1.4;color:{t}">7 ô phục vụ + 3 ô <b>đồng xuất hiện</b> (chỉ dùng dữ liệu train): độ phủ 6,47 % → '
      '<b style="color:{o}">14,5 %</b> (mục tiêu 10 %), NDCG@10 0,0865 → 0,0892, ngày giữ lại 0,0742 → 0,0764; ô theo danh mục không thêm gì; chưa triển khai</p></div>'.format(
          r=RULE, h=H, n=NAVY, t=TEXT, o=ORANGE),
      "Slide dự phòng cho câu hỏi về quy trình đánh giá và độ phủ. Bên trái: chọn siêu tham số và beta trên mười lăm ngày đầu của tháng "
      "kiểm tra rồi báo cáo trên mười sáu ngày cuối vẫn chọn đúng cấu hình đang triển khai; trên các ngày giữ lại, ALS hơn baseline danh "
      "mục bốn mươi phần trăm và danh sách đã xếp lại hơn tám mươi phần trăm, còn nhìn thẳng vào các ngày giữ lại cũng chỉ được thêm "
      "0,0002. Bên phải: coi người dùng là đơn vị, chênh lệch A/B vẫn có ý nghĩa, p bằng 0,037 với sai số bền vững theo cụm và 0,029 "
      "với bootstrap. Phía dưới: ba ô đồng xuất hiện nâng độ phủ lên 14,5 phần trăm khi chạy offline, vượt mục tiêu, kèm NDCG cao hơn; "
      "cách này chưa được triển khai.")

assert len(THU_TU) == TONG, len(THU_TU)
deck = {
    "v": 4,
    "createdOnFiles": {"v": 1, "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")},
    "lists": "css",
    "title": "BD03 Hệ thống gợi ý – Bảo vệ đồ án",
    "cover": "cover",
    "order": THU_TU,
    "sections": {
        "s1": {"description": "Đề bài, mục tiêu và tám tiêu chí nghiệm thu", "start": "cover"},
        "s2": {"description": "Dữ liệu, kiến trúc, cụm, lakehouse, xử lý luồng và điều phối", "start": "dataset"},
        "s3": {"description": "ALS, tinh chỉnh, baseline, phân tích lỗi, thiên lệch độ phổ biến, xếp lại và khởi đầu lạnh", "start": "als"},
        "s4": {"description": "Thử nghiệm A/B và các tính năng Delta Lake: Z-ORDER, time travel, schema evolution", "start": "abtest"},
        "s5": {"description": "Vòng khép kín chạy thật và bài học vận hành cụm", "start": "loop"},
        "s6": {"description": "Kết quả theo tiêu chí, hạn chế và hướng phát triển", "start": "results"},
    },
    "faces": {
        "tinos": {"family": "Tinos", "href": "https://fonts.googleapis.com/css2?family=Tinos:ital,wght@0,400;0,700;1,400;1,700&display=swap"},
    },
    "designSystems": [],
}
with open(os.path.join(GOC, "project_vi", "deck.json"), "w", encoding="utf-8") as f:
    json.dump(deck, f, ensure_ascii=False, indent=1)
print("OK", len(THU_TU), "slide")
