"""Sinh các tệp slide (project/slides/*.html) và project/deck.json cho deck bảo vệ BD03."""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import so_do  # noqa: E402  (ba sơ đồ nhóm thiết kế, dựng bằng HTML + SVG vector)

GOC = os.path.dirname(os.path.abspath(__file__))
SL = os.path.join(GOC, "project", "slides")
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

# Hình: (đường dẫn tài sản, rộng, cao theo tỉ lệ, tệp nguồn trong bao_cao/hinh). Sơ đồ và biểu đồ là SVG vector
# (chữ đã đổi thành đường cong) nên chiếu lên màn hình lớn hay phóng to PDF vẫn nét; ảnh chụp màn hình giữ PNG.
IMG = {
    "f02": ("/_blob/d898dd0406c5091507c12cc8375e0804", 2220, 966, "f02_cum.svg"),
    "f05b": ("/_blob/b9e878745e8b27b49925a59e9a8ef739", 2220, 798, "f05b_dag1_ngang.svg"),
    "f07": ("/_blob/0a39b751c8359353a6959ec9ec922238", 2220, 1026, "f07_luong.svg"),
    "f08": ("/_blob/d29bb1fa153ddc586d7834b395f1a889", 2220, 1050, "f08_vong_khep_kin.svg"),
    "f09": ("/_blob/0cad98133f1ef2cba96a84f727d0e882", 2220, 930, "f09_tai_lap.svg"),
    "f11": ("/_blob/7a8906490b0cee3486d9a68c21f2a606", 2220, 816, "f11_mau_khoi.svg"),
    "c01": ("/_blob/ee75d3e0922604409cf7e79b99049973", 1245, 416, "c01_tuning.svg"),
    "c02": ("/_blob/0e0553de6cf1b805fbabcb4c7e71a65f", 1544, 554, "c02_phuong_phap.svg"),
    "c03": ("/_blob/77937eb8197783838d6e9e28b731654d", 1518, 556, "c03_pho_bien.svg"),
    "c04": ("/_blob/88e24660e94d702e65d25d205dd48bef", 1155, 587, "c04_xep_lai.svg"),
    "c05": ("/_blob/2d91d56be0f4fc937d653e12756904aa", 1172, 509, "c05_zorder.svg"),
    "c06": ("/_blob/dcdd8358392a4e72022732ccf3a68035", 1925, 722, "c06_ab.svg"),
    "c09": ("/_blob/b42da5d24d6af6998f1233472d502a76", 1544, 526, "c09_phan_tich_loi_en.svg"),
    "s01": ("/_blob/d74fa9afdacef1c40c17f19df041f67c", 1350, 1034, "s01_cold_start_web.png"),
    "s03": ("/_blob/47ac9f5a98784585209076b04e959092", 1350, 870, "s03_ab_web.png"),
    "eq_als": ("/_blob/4576264a7d76e2eb4a8cf05a603503e4", 975, 113, "eq_als.png"),
    "eq_pref": ("/_blob/9b09edcb48b80f018b0ea744ca353cbe", 671, 63, "eq_pref.png"),
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
            '<b>Figure {n}.</b> <i>{a}</i></p></div>'
        ).format(s=src, a=alt, w=rong, h=cao, f=TNR, m=MUTED, n=SO_HINH[0])
    return (
        '<div style="background:{c};border:1px solid {r};border-radius:6px;padding:{d}px {d}px 10px;'
        'display:flex;flex-direction:column;align-items:center;justify-content:center;gap:6px">'
        '<img src="{s}" alt="{a}" style="width:{w}px;height:{h}px;object-fit:contain">'
        '<p style="font-family:{f};font-size:20px;line-height:1.25;color:{m};text-align:center;max-width:{w}px">'
        '<b>Figure {n}.</b> <i>{a}</i></p></div>'
    ).format(c=CARD, r=RULE, d=dem, s=src, a=alt, w=rong, h=cao, f=TNR, m=MUTED, n=SO_HINH[0])


def chu_thich(alt):
    """Chú thích đánh số cho sơ đồ dựng bằng HTML (cùng kiểu với hinh())."""
    SO_HINH[0] += 1
    return ('<p style="font-family:{f};font-size:20px;line-height:1.25;color:{m};text-align:center">'
            '<b>Figure {n}.</b> <i>{a}</i></p>').format(f=TNR, m=MUTED, n=SO_HINH[0], a=alt)


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
        'BD03 · E-commerce Product Recommender System · CS411V Big Data</p>'
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
    '<p style="font-size:24px;font-weight:600;letter-spacing:3px;text-transform:uppercase;color:{la}">CS411V Big Data · Final project · Topic 3</p>'
    '<h1 style="font-family:{h};font-size:96px;font-weight:700;line-height:1.08;color:{bg}">E-commerce Product<br>Recommender System</h1>'
    '<p style="font-size:32px;line-height:1.4;color:#C9D4E5;width:1300px">Implicit-feedback ALS, streaming trends and an A/B test on a Spark, Kafka and Delta Lake cluster of six laboratory machines</p>'
    '<div style="flex:1"></div>'
    '<div style="display:flex;gap:96px">'
    '<div style="display:flex;flex-direction:column;gap:6px"><p style="font-size:32px;font-weight:600;color:{bg}">Le Chi Tam</p><p style="font-size:24px;color:#C9D4E5">Student code 2302220</p></div>'
    '<div style="display:flex;flex-direction:column;gap:6px"><p style="font-size:32px;font-weight:600;color:{bg}">Le Nguyen Quoc Anh</p><p style="font-size:24px;color:#C9D4E5">Student code 2302283</p></div>'
    '</div>'
    '<p style="font-size:24px;color:#C9D4E5">Tan Tao University · Defence 19 November 2026 · Supervisor: Cao Tien Dung, PhD</p>'
).format(la=LIGHT_ACC, h=H, bg=BG),
    "Good morning. We are Le Chi Tam and Le Nguyen Quoc Anh. Our project is Topic 3 of CS411V: a product recommender "
    "for a simulated e-commerce marketplace. We have just shown the system live on a cluster of six laboratory machines; "
    "these slides explain how it is built, what the model achieves on real data, and how each of the eight acceptance "
    "criteria was proved.", nen=NAVY)
# chân trang trên nền tối: sửa màu trong tệp bìa
p = os.path.join(SL, "cover.html")
s = open(p, encoding="utf-8").read().replace("color:{}\">BD03".format(MUTED), "color:#9FB0C8\">BD03").replace(
    "text-align:right;font-size:24px;color:{}".format(MUTED), "text-align:right;font-size:24px;color:#9FB0C8")
open(p, "w", encoding="utf-8").write(s)

# ------------------------------------------------------------------ 2. Đề bài
muc_tieu = [
    ("O1", "Data platform", "Kafka ingestion and a bronze → silver → gold lakehouse on HDFS with Delta Lake"),
    ("O2", "Model", "ALS with implicit feedback, a tuning grid, two baselines and three cold-start strategies"),
    ("O3", "Streaming", "A “trending now” block that reacts to a click within 90 seconds"),
    ("O4", "Experiment", "An A/B test with impression logging and a statistical conclusion"),
    ("O5", "Operations", "Five Airflow DAGs, a five-screen web application and reproducible experiments"),
]
hang = "".join(
    '<div style="display:flex;gap:28px;align-items:center;border-top:1px solid {r};padding:18px 0px 0px 0px">'
    '<p style="font-family:{h};font-size:32px;font-weight:700;color:{bg};background:{n};padding:8px 18px;border-radius:12px">{k}</p>'
    '<p style="width:250px;font-size:32px;font-weight:600;color:{n}">{t}</p>'
    '<p style="flex:1;font-size:28px;line-height:1.35;color:{tx}">{m}</p></div>'.format(
        r=RULE, h=H, bg=BG, n=NAVY, k=k, t=t, tx=TEXT, m=m) for k, t, m in muc_tieu)
slide("assignment", tieu_de("Introduction", "What the assignment asks") +
      '<p style="font-size:32px;line-height:1.4;color:{t};width:1500px">Learn from clicks, react within seconds, prove it with an experiment.</p>'.format(t=TEXT) +
      '<div style="display:flex;flex-direction:column;gap:12px">' + hang + "</div>",
      "The assignment has five objectives. O1 is the data platform: events arrive through Kafka and are stored in a "
      "lakehouse on HDFS with Delta Lake. O2 is the model: ALS for implicit feedback, a tuning grid, two baselines and cold "
      "start. O3 is streaming: a trending block that reacts within ninety seconds. O4 is an A/B experiment, and O5 covers "
      "operations: Airflow, the web application and reproducibility.")

# ------------------------------------------------------------------ 3. Tám tiêu chí
tieu_chi = [
    "Tuning table with at least 9 ALS combinations and NDCG@10",
    "ALS against 2 baselines on 4 ranking metrics, coverage and novelty",
    "Three cold-start strategies, shown with a user created on the spot",
    "Trending block updated at most 90 s after a click",
    "Closed loop: a web click reaches Kafka, bronze and the trending table",
    "A/B result with a p-value and a statistical conclusion",
    "Reproduction of an experiment with Delta <span style=\"color:{}\">VERSION AS OF</span>".format(BLUE),
    "OPTIMIZE / Z-ORDER figures for the per-user lookup",
]
the = "".join(
    '<div style="display:flex;gap:20px;align-items:center;background:{c};border:1px solid {r};border-radius:14px;padding:20px 24px">'
    '<p style="font-family:{h};font-size:44px;font-weight:700;color:{b}">{i}</p>'
    '<p style="flex:1;font-size:28px;line-height:1.3;color:{n}">{t}</p></div>'.format(
        c=CARD, r=RULE, h=H, b=BLUE, i=i + 1, n=NAVY, t=t) for i, t in enumerate(tieu_chi))
slide("criteria", tieu_de("Introduction", "Eight acceptance criteria") +
      '<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px 28px">' + the + "</div>",
      "The specification turns these objectives into eight measurable acceptance criteria. We will come back to this list "
      "at the end and show the evidence for each one. All eight were demonstrated on the cluster with the real data.")

# ------------------------------------------------------------------ 4. Dữ liệu
slide("dataset", tieu_de("Data and architecture", "RetailRocket clickstream") +
      '<div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:32px">' +
      so_lon("2.89 M", "events over 139 days: views, add-to-cart, purchases") +
      so_lon("417,053", "items in the catalogue (126,230 seen in training)") +
      so_lon("179,999", "training users with at least 3 interactions") +
      so_lon("3.3×10<sup style=\"font-size:0.6em;vertical-align:0.75em;line-height:0\">−5</sup>", "density of the training matrix (179,999 × 126,230)") +
      "</div>" +
      '<div style="display:flex;flex-direction:column;gap:12px">' +
      dong("Replay", "A Kafka producer replays the log, compressing one day into 30 seconds; 4.8 % of users are synthetic newcomers for cold start", 28, 260) +
      dong("Split", "By time: four months for training, the last month (19 Aug – 18 Sep 2015) for testing; 9,467 test users", 28, 260) +
      dong("Weights", "View 1, add-to-cart 3, purchase 5, decayed with age (λ = 0.01 per day)", 28, 260) +
      "</div>",
      "We use the RetailRocket dataset: almost 2.9 million events over 139 days. The matrix is extremely sparse, "
      "3.3 times ten to the minus five, and most visitors appear only once, which shapes every result later. A producer "
      "replays the log through Kafka, one day in thirty seconds, and adds about five percent synthetic new users so that "
      "cold start can be exercised. Evaluation uses a temporal split: the last month is the test set.")

# ------------------------------------------------------------------ 6. Cụm
cum = [
    ("Master", "labsoe-11: NameNode, Spark Master, every driver, Airflow"),
    ("Workers", "5 machines, 34 cores and 13 GB of executor memory in total"),
    ("Storage", "5 DataNodes, replication factor 2"),
    ("Kafka", "One broker on labsit; web application on labsoe-18"),
    ("Network", "ZeroTier overlay “ttunet” over the laboratory Wi-Fi"),
]
slide("cluster", tieu_de("Data and architecture", "Deployed on six laboratory machines") +
      '<div style="display:flex;justify-content:center">' + hinh("f02", "Physical deployment: machines, containers, ports and resources", cao=606, the=False) + "</div>",
      "The cluster has six laboratory machines on a ZeroTier network. The master runs the NameNode, the Spark Master, all "
      "drivers and Airflow. Five workers give 34 cores and 13 gigabytes of executor memory. Each machine has only 7 gigabytes "
      "of RAM and is shared with other groups, so every memory setting was sized from measurements.")

# ------------------------------------------------------------------ 5. Kiến trúc tổng thể (thiết kế của nhóm, 22/11/2026)
# Theo góp ý của GVHD ngày 19/11 và thống nhất của nhóm: slide 5 tách thành hai slide, tổng quan công nghệ rồi năm DAG;
# thứ tự (23/11): cụm → tổng quan → ba job luồng → năm DAG → vòng khép kín → lakehouse → DAG 1.
slide("architecture", tieu_de("Data and architecture", "Overall architecture and the role of each technology") +
      '<div style="display:flex;justify-content:center">' + so_do.tong_quan("en", ti_le=1.09) + "</div>",
      "This is the most general picture: which technologies the system uses and what each one does. On the left are the sources: the RetailRocket data, 2.76 million events over 139 days, replayed by the producer at one day per 30 seconds. In the middle is processing and storage: Kafka ingests and buffers events in two topics; Spark processes them with near-real-time streaming jobs and batch and machine-learning jobs; the lakehouse on HDFS with Delta Lake stores them in three tiers, bronze, silver and gold. On the right is serving: the FastAPI web app reads Gold to show recommendations and trending items, and every view, add-to-cart, purchase and impression goes back to Kafka along the blue dashed line. The bottom band is Airflow: it schedules five DAGs, calls and monitors the Spark jobs, retries, alerts and maintains the data. The next slides go into each part.", dem=DEM_SO_DO, khoang=28)

# ------------------------------------------------------------------ 8. Streaming
luong = [
    ("nap_su_kien", "Kafka → bronze, trigger 15 s"),
    ("tinh_thinh_hanh", "15-min window sliding every minute, 2-min watermark, update mode + MERGE, RocksDB state, trigger 20 s"),
    ("ghi_hien_thi", "Impressions and click feedback → impression log with Change Data Feed, trigger 30 s"),
]
slide("streaming", tieu_de("Data and architecture", "Three Structured Streaming jobs") +
      '<div style="display:flex;justify-content:center">' + hinh("f07", "The three streaming jobs and the latency budget of the trending block", cao=606, the=False) + "</div>",
      "Three streaming jobs run continuously. The first lands every event in bronze. The second computes the trending table: a "
      "fifteen-minute window sliding every minute, merged into gold every twenty seconds. The third writes the impression log "
      "used by the A/B test. One design point: windows are keyed on the Kafka timestamp, not the original event time, because "
      "the replayed data is from 2015 and live clicks are from 2026.")

# ------------------------------------------------------------------ 5b. Năm DAG Airflow (thiết kế của nhóm, 22/11/2026)
slide("dags", tieu_de("Data and architecture", "Airflow orchestrates five DAGs") +
      '<div style="display:flex;flex-direction:column;align-items:center;gap:8px">' + so_do.nam_dag("en", ti_le=1.07) +
      chu_thich("The five Airflow DAGs and what each orchestrates in the system.") + "</div>",
      "Airflow processes no data itself; it schedules and calls spark-submit. DAG 1 runs at 2 a.m.: the main batch pipeline from bronze to the recommendation lists in gold. DAG 2 checks the trending stream every 15 minutes and restarts it if it died or went stale. DAG 3 reads the impression log every hour, computes CTR, conversion and the p-value and writes the A/B result. DAG 4 runs OPTIMIZE, Z-ORDER and VACUUM at 4 a.m. DAG 5 is triggered by hand and uses time travel to retrain on the exact old data version and compare with history. The Affects row of each box shows which tables the DAG touches.", dem=DEM_SO_DO, khoang=28)

# ------------------------------------------------------------------ 21. Vòng khép kín
slide("loop", tieu_de("Live system and operations", "The closed loop, measured live") +
      '<div style="display:flex;gap:40px;flex:1">'
      '<div style="width:270px;display:flex;flex-direction:column;gap:28px">' +
      so_lon("10.3 s", "Kafka → bronze for three “Add to cart” clicks", BLUE, 56) +
      so_lon("51 s", "clicks → updated trending table; target&nbsp;90&nbsp;s", ORANGE, 56) +
      '<p style="font-size:24px;line-height:1.35;color:{m}">Clicks sent while a stream was down were counted after its restart.</p>'.format(m=MUTED) +
      "</div>" + hinh("f08", "Closed loop between the web application, Kafka, the streams and the A/B pipeline", cao=606, the=False) + "</div>",
      "These two criteria were measured on 11 November through the web application’s API. Three add-to-cart clicks reached bronze "
      "about ten seconds after Kafka received them, and the trending table fifty-one seconds after the clicks: twenty seconds of "
      "trigger interval, the micro-batch and the polling. During the first attempt a stream had died; after the restart the "
      "earlier clicks were still counted, which shows exactly-once recovery from the checkpoint.")

# ------------------------------------------------------------------ 7. Lakehouse (thiết kế của nhóm, 22/11/2026)
# Luồng lưu trữ (Bronze, Silver, Gold) ở trên, luồng tính toán (job Spark) ở dưới, theo góp ý của GVHD ngày 19/11.
slide("lakehouse", tieu_de("Data and architecture", "Lakehouse on HDFS: storage flow and compute flow") +
      '<div style="display:flex;justify-content:center">' + so_do.lakehouse("en", ti_le=1.13) + "</div>",
      "The lakehouse has two flows. The storage flow on top has only three tiers: bronze keeps raw events from Kafka as append-only Parquet; silver keeps cleaned data; gold keeps the tables served to the web, the dashboard and the A/B test. The compute flow below is made of Spark jobs: bronze_to_silver deduplicates, filters bots, assigns weights and writes silver; the feature step builds the interaction matrix with a time-based train and test split and time decay, plus item statistics and co-occurrence; finally ALS training, cold start, re-ranking and blending, and the result is written to gold. The band of numbers shows the data passing through each step.", dem=DEM_SO_DO, khoang=28)

# ------------------------------------------------------------------ 9. Airflow
slide("airflow", tieu_de("Data and architecture", "DAG 1 in detail: the nightly pipeline") +
      '<p style="font-size:26px;line-height:1.35;color:{t}"><b>DAG 2</b> stream monitor (every 15 min) · <b>DAG 3</b> A/B test (hourly) · <b>DAG 4</b> maintenance (04:00) · <b>DAG 5</b> reproduction (manual)</p>'.format(t=TEXT) +
      '<div style="display:flex;justify-content:center">' + hinh("f05b", "DAG 1 kn_pipeline_hang_ngay, the nightly pipeline; every job is idempotent", rong=1488, the=False) + "</div>",
      "Slide 8 introduced the five DAGs; this is DAG 1 in detail, the nightly pipeline: it waits for new "
      "data, rebuilds silver and the matrix, tunes ALS, evaluates it and branches — new recommendations only if NDCG is not "
      "worse than the served model. The other four monitor the stream, compute the A/B test hourly, maintain the Delta tables "
      "and reproduce experiments.")

# ------------------------------------------------------------------ 10. ALS
src_p, wp, hp = IMG["eq_pref"][:3]
src_a, wa, ha = IMG["eq_als"][:3]
slide("als", tieu_de("Model and evaluation", "ALS for implicit feedback") +
      '<div style="display:flex;gap:32px;align-items:center;background:{c};border:1px solid {r};border-radius:16px;padding:28px 40px">'
      '<img src="{sp}" alt="Preference and confidence of an observation" style="width:560px;height:{hp}px;object-fit:contain">'
      '<img src="{sa}" alt="ALS objective for implicit feedback" style="width:900px;height:{ha}px;object-fit:contain"></div>'.format(
          c=CARD, r=RULE, sp=src_p, hp=round(560 * hp / wp), sa=src_a, ha=round(900 * ha / wa)) +
      '<div style="display:flex;flex-direction:column;gap:14px">' +
      dong("Every cell counts", "Observed pairs get preference 1 with confidence 1 + α·r; unseen pairs preference 0 with confidence 1", 28, 330) +
      dong("Alternating", "Fix the items and solve every user in closed form, then swap; Spark parallelises across blocks", 28, 330) +
      dong("Grid (27)", "rank {10, 30, 50} × regParam {0.01, 0.1, 1.0} × alpha {1, 10, 40}, selected on NDCG@10", 28, 330) +
      "</div>",
      "We use ALS in the implicit-feedback form of Hu, Koren and Volinsky. A click is not a rating: it tells us about "
      "confidence, not preference. Every cell of the matrix enters the loss: observed pairs with preference one and a "
      "confidence that grows with alpha times the decayed score, unseen pairs with preference zero. Alternating least squares "
      "makes each half-step a set of independent least-squares problems, which Spark distributes. We tuned all 27 combinations.")

# ------------------------------------------------------------------ 11. Tuning
slide("tuning", tieu_de("Model and evaluation", "Tuning: bigger rank, higher alpha") +
      '<p style="font-size:32px;line-height:1.35;color:{n}">Best of 27: <b>rank 50 · regParam 1.0 · alpha 40 → NDCG@10 = 0.0680</b> <span style="color:{m}">(78 minutes on 34 cores)</span></p>'.format(n=NAVY, m=MUTED) +
      '<div style="display:flex;justify-content:center">' + hinh("c01", "NDCG@10 over the tuning grid, one heat map per rank", rong=1290) + "</div>" +
      '<p style="font-size:26px;color:{m}">Check on 11 Nov: chosen on 19 Aug – 2 Sep, reported on 3–18 Sep → the same configuration (optimism 0.0002).</p>'.format(m=MUTED),
      "NDCG grows monotonically with rank and with alpha. With implicit feedback a large alpha gives observed pairs, especially "
      "purchases, much more weight than the empty cells, so the model extracts more signal from very sparse data. The optimum "
      "lies on the edge of the grid, so the natural next step is rank 100 and alpha 80. We also checked the obvious objection, that the grid was chosen on the test month: choosing on the first "
      "fifteen days and reporting on the last sixteen selects exactly the same configuration, with an optimism of 0.0002.")

# ------------------------------------------------------------------ 12. Baselines
slide("baselines", tieu_de("Model and evaluation", "ALS beats both baselines") +
      '<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:48px">' +
      so_lon("+1,237 %", "NDCG@10 over global best-sellers (0.0051 → 0.0680); target +30 %") +
      so_lon("+43 %", "NDCG@10 over category best-sellers (0.0475)") +
      so_lon("0.0865", "NDCG@10 of the served configuration (ALS + re-ranking)", ORANGE) +
      "</div>" +
      '<div style="display:flex;justify-content:center">' + hinh("c02", "NDCG@10 and coverage of the baselines, ALS and re-ranked variants", rong=1070) + "</div>",
      "Against the two mandatory baselines ALS wins by a wide margin: thirteen times the global best-seller list, and 43 percent "
      "over best-sellers of the user’s category, which is a much stronger baseline because RetailRocket visitors stay within a "
      "category. ALS also wins on precision, recall and MAP. The served configuration, with novelty re-ranking, is better again.")

# ------------------------------------------------------------------ 12b. Error analysis
slide("errors", tieu_de("Model and evaluation", "Where ALS fails: error analysis") +
      '<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:48px">' +
      so_lon("Every group", "ALS beats category best-sellers in all 12 user groups, by 29–54 %") +
      so_lon("41 %", "of test users only touch long-tail items; their NDCG@10 is 1/9 of the rest", ORANGE) +
      so_lon("84.8 %", "of test users get NDCG@10 = 0 from both ALS and the category baseline") +
      "</div>" +
      '<div style="display:flex;justify-content:center">' + hinh("c09", "NDCG@10 per user group for ALS and both baselines (9,467 test users, cluster run 16 Nov)", rong=1100) + "</div>",
      "We expected ALS to lose to the category baseline for users with few interactions and for users who stay in one category. "
      "Both hypotheses were wrong: ALS wins in every group, including the three-to-four-interaction users who are two thirds of the "
      "test set, and single-category users are the easiest case for both methods. The real failure is the long tail: when the items "
      "a user touches next month are all unpopular, both methods almost always score zero. That is the same popularity bias as on "
      "the next slide, and it points to long-tail candidate sources rather than more ALS tuning.")

# ------------------------------------------------------------------ 13. Popularity bias
slide("bias", tieu_de("Model and evaluation", "Coverage is limited by popularity bias") +
      '<p style="font-size:32px;line-height:1.35;color:{n};width:1600px"><b>99.2 %</b> of ALS slots fall in the 10 % most popular items. Coverage is <b>3.44 %</b> against a structural ceiling of 75 % — the cause is bias, not the number of slots.</p>'.format(n=NAVY) +
      '<div style="display:flex;justify-content:center">' + hinh("c03", "Distribution of recommendation slots over popularity groups", rong=1245) + "</div>",
      "Coverage is the one threshold we did not meet with pure ALS: 3.44 percent against a target of 10. The specification accepts "
      "this if it is analysed. Ten recommendations for 9,467 test users give almost 95 thousand slots for 126 thousand items, so "
      "the ceiling is 75 percent. The real cause is popularity bias: items with few interactions get weak latent vectors and "
      "rarely reach a top ten.")

# ------------------------------------------------------------------ 14. Re-ranking
slide("rerank", tieu_de("Model and evaluation", "Novelty re-ranking: accuracy and diversity together") +
      '<div style="display:flex;gap:48px;flex:1">'
      '<div style="flex:1;display:flex;flex-direction:column;gap:28px">' +
      so_lon("+27 %", "NDCG@10 at β = 1.0 over 200 candidates (0.0680 → 0.0865)", ORANGE, 64) +
      so_lon("+88 %", "coverage (3.44 % → 6.47 %); slots in the top 10 % fall from 99.2 % to 80.8 %", ORANGE, 64) +
      '<p style="font-size:26px;line-height:1.4;color:{m}">Deployed for serving; no β alone reaches 10 %. Offline, 3 co-occurrence slots give <b style="color:{o}">14.5 %</b> coverage and NDCG@10 0.0892.</p>'.format(m=MUTED, o=ORANGE) +
      "</div>" + hinh("c04", "Accuracy–coverage trade-off of novelty re-ranking", cao=560) + "</div>",
      "As a de-biasing method we re-rank 200 ALS candidates by normalised ALS score plus beta times normalised novelty. Up to beta "
      "equal one, accuracy and diversity improve together, because the test month contains many items that are rare in training. "
      "We deployed that configuration. Reaching ten percent needs candidate sources other than ALS, and we tested that offline: giving the "
      "last three slots to items co-viewed with the user's own items raises coverage to 14.5 percent and NDCG as well. It is not deployed yet.")

# ------------------------------------------------------------------ 15. Cold start
slide("coldstart", tieu_de("Model and evaluation", "Cold start and the blended “For you” block") +
      '<div style="display:flex;gap:44px;flex:1">'
      '<div style="width:1040px;display:flex;flex-direction:column;gap:18px">' +
      hinh("f11", "Templates of the ten visible slots by user type and A/B group", rong=1040, the=False) +
      dong("New user", "Top 10 of the newest trending window", 26, 210) +
      dong("&lt; 5 interactions", "Best-sellers of viewed categories + trending", 26, 210) +
      dong("New item", "Items unknown to ALS fill slots 9–10", 26, 210) +
      "</div>" + hinh("s01", "Home page of a user created on the spot", rong=540) + "</div>",
      "Three cold-start strategies are pure functions shared by the batch job and the web application. A brand-new user sees the "
      "newest trending items; a user with fewer than five interactions sees best-sellers of the categories they viewed, "
      "interleaved with trending; and new items that ALS does not know enter through the last two slots. The screenshot is the live "
      "system: a user created with one click immediately gets the new-user strategy, and the page marks that user as outside the A/B "
      "experiment. Product names and pictures are illustrations, because RetailRocket is anonymised.")

# ------------------------------------------------------------------ 16. A/B
slide("abtest", tieu_de("Experiment and lakehouse", "A/B test: blending trending items lowers CTR") +
      '<div style="display:flex;gap:40px;align-items:center">'
      '<p style="font-family:{h};font-size:64px;font-weight:700;line-height:1;color:{o}">p = 0.015</p>'
      '<p style="flex:1;font-size:26px;line-height:1.35;color:{t}">CTR 2.094 % (A) vs 1.978 % (B), z = −2.43: B is 5.5 % worse; conversion p = 0.75. '
      '350,160 simulated impressions over 7 days (76,972 per group needed); user-level bootstrap p = 0.029.</p></div>'.format(h=H, o=ORANGE, t=TEXT) +
      '<div style="display:flex;justify-content:center">' + hinh("c06", "CTR and conversion by group with 95 % confidence intervals", rong=1250) + "</div>",
      "Group A sees ALS only, group B ALS with thirty percent trending items. Real visitors are too few, so impressions are "
      "simulated with a position-dependent click model and labelled as simulated. Over 350 thousand impressions, B has a "
      "significantly lower click-through rate, p equals 0.015, and no difference in conversion. Treating the user as the unit, a "
      "bootstrap over users still gives p equals 0.029. The conclusion holds only under the "
      "assumptions of the simulation, which gives trending items no bonus. The live dashboard, shown in the demo, also counts the 230 real web "
      "impressions logged since 11 November, which is why its p-value is slightly different, about 0.014.")

# ------------------------------------------------------------------ 17. Delta Lake
slide("delta", tieu_de("Experiment and lakehouse", "Delta Lake features in use") +
      bang(["Feature", "How the project uses it"], [
          ["<b>MERGE</b>", "Upserts of recommendation lists, trending windows and impressions; surplus ranks deleted"],
          ["<b>Time travel</b>", "Every training records its feature version; A/B results re-aggregated at the recorded version"],
          ["<b>Schema evolution</b>", "ALTER TABLE ADD COLUMNS price and star rating → model version 2"],
          ["<b>Change Data Feed</b>", "Impression log → the hourly A/B job recomputes only the affected days"],
          ["<b>OPTIMIZE / Z-ORDER</b>", "Nightly compaction; Z-ORDER BY user on the served recommendation table"],
          ["<b>Partitioning, retention</b>", "Silver by date, one file per date; 30 days for files, 60 for the log"],
          ["<b>Concurrency</b>", "Streams and maintenance write the same tables → retry on Delta conflict exceptions"],
      ], [30, 70], 24),
      "Every Delta Lake requirement of the specification is used for a real purpose. MERGE makes every writer idempotent. Time "
      "travel supports reproducibility. Schema evolution produces model version 2. Change Data Feed lets the A/B job read only "
      "new impressions. And because streams and maintenance write the same tables, optimistic concurrency had to be handled "
      "with an explicit retry.")

# ------------------------------------------------------------------ 18. Z-ORDER
slide("zorder", tieu_de("Experiment and lakehouse", "Z-ORDER: one file per user lookup") +
      '<div style="display:flex;gap:48px;flex:1">'
      '<div style="flex:1;display:flex;flex-direction:column;gap:32px">' +
      so_lon("67 → 1.07", "files read per lookup, compaction only vs Z-ORDER BY user", BLUE, 56) +
      so_lon("8,280 → 980 ms", "median latency of the per-user lookup", BLUE, 56) +
      so_lon("8.4×", "faster than compaction alone (11× vs the fragmented table)", ORANGE, 56) +
      "</div>" + hinh("c05", "Latency and files read in the three phases", rong=1040) + "</div>",
      "The hot query of the web application is the per-user lookup. We measured three phases on a copy of the 5.3-million-row "
      "table. Compaction alone barely helps: every compacted file still spans the whole range of user ids. Z-ORDER clusters rows "
      "of the same user, narrows each file’s min-max statistics, and lets data skipping read about one file. Files read is the "
      "reliable figure; latency varies with Wi-Fi.")

# ------------------------------------------------------------------ 19. Tái lập
slide("reproduce", tieu_de("Experiment and lakehouse", "Reproducing an experiment with time travel") +
      '<div style="display:flex;gap:40px;align-items:center">'
      '<p style="font-family:{h};font-size:64px;font-weight:700;line-height:1;color:{b}">0.000000</p>'
      '<p style="flex:1;font-size:26px;line-height:1.35;color:{t}">maximum deviation, re-training on features VERSION AS OF 0 (10 Nov) and the deployed model on version 3 (11 Nov); fixed seed, append-only indexers, test set stored in the same version.</p></div>'.format(h=H, b=BLUE, t=TEXT) +
      '<div style="display:flex;justify-content:center">' + hinh("f09", "Reproduction with Delta time travel (DAG 5); later table versions do not affect it", cao=498, the=False) + "</div>",
      "DAG 5 re-ran the selected experiment on version zero of the feature table and compared the result with the recorded "
      "metrics: the deviation is zero to six decimals. This is a meaningful test, because during the run the source tables had "
      "already evolved. It works because the job reads only that version, ALS has a fixed seed, and the indexers are append-only, "
      "so an index means the same user or item in every version. On 11 November we repeated it from the web button for the deployed model on "
      "version three: again an exact match.")

# ------------------------------------------------------------------ 20. v2
slide("v2", tieu_de("Experiment and lakehouse", "Schema evolution and model version 2") +
      '<div style="display:flex;gap:56px;flex:1">'
      '<div style="flex:1;display:flex;flex-direction:column;gap:16px">' +
      dong("1", "ALTER TABLE silver ADD COLUMNS (price, star rating): metadata only", 28, 40) +
      dong("2", "MERGE fills both from the catalogue; new rows get them automatically", 28, 40) +
      dong("3", "Version 2 keeps candidates in the user’s price range [0.5·P10, 1.5·P90]", 28, 40) +
      dong("4", "Nightly gate: deploy only if NDCG@10 is not worse than the served model", 28, 40) +
      "</div>"
      '<div style="width:720px;display:flex;flex-direction:column;gap:20px">'
      '<p style="font-size:28px;font-weight:600;color:{n}">Deployed model (rank 50, reg 1.0, alpha 40)</p>'.format(n=NAVY) +
      bang(["Metric", "v1", "v2"], [
          ["NDCG@10", "0.0680", "<b>0.0694</b> (+2.1 %)"],
          ["Coverage", "3.44 %", "<b>3.72 %</b>"],
          ["Distinct items", "4,346", "<b>4,699</b>"],
      ], [40, 25, 35], 28) +
      '<p style="font-size:26px;line-height:1.4;color:{m}">11 November: the 02:00 run, woken at 05:22 by our web test clicks, '
      'deployed v2 at 06:07 on its own; a manual run confirmed it. At alpha 10 the gain was the same (+2.3 %).</p>'.format(m=MUTED) +
      "</div></div>",
      "Week five of the specification asks for a schema change. We added price and star rating to silver with ALTER TABLE, filled "
      "them with MERGE, and built model version 2, which keeps recommendations inside the user’s usual price range. At equal "
      "hyper-parameters version 2 is better on every metric, by about two percent — small next to re-ranking. The first nightly "
      "run kept the old model for a design reason we explain on the operations slide; after the fix the gate compares like with "
      "like: on 11 November the scheduled run, woken by our own test clicks on the web page, retrained and deployed version 2 without "
      "anyone touching it.")

# ------------------------------------------------------------------ 22. Vận hành
van_de = [
    ("Path MTU black hole", "From home, 1,400-byte packets vanished: SSH hung at key exchange.",
     "Jump host, ZeroTier MTU 1,280"),
    ("Zombie streaming drivers", "Cancelling an app left the client-mode driver alive; two drivers collided on one checkpoint.",
     "Stop = cancel + kill the driver"),
    ("A gate that never opens", "The nightly grid lacked the served alpha, so no new model could ever win.",
     "Always re-test the served configuration"),
    ("Small files, small memory", "7,334 files for 20,624 rows; one 7.4 GB master for every driver.",
     "One file per date; pool of 1 slot at night"),
]
the = "".join(
    '<div style="display:flex;flex-direction:column;gap:12px;background:{c};border:1px solid {r};border-radius:14px;padding:26px 30px">'
    '<h3 style="font-family:{h};font-size:36px;font-weight:600;color:{n}">{t}</h3>'
    '<p style="font-size:26px;line-height:1.4;color:{tx}">{m}</p>'
    '<p style="font-size:26px;font-weight:600;color:{b}">→ {f}</p></div>'.format(
        c=CARD, r=RULE, h=H, n=NAVY, t=t, tx=TEXT, m=m, b=BLUE, f=f) for t, m, f in van_de)
slide("operations", tieu_de("Live system and operations", "Lessons from a shared, constrained cluster") +
      '<div style="display:grid;grid-template-columns:1fr 1fr;gap:24px 28px">' + the + "</div>" +
      '<p style="font-size:26px;color:{m}">33 issues found and fixed, each with a regression test — 143 automated tests in total.</p>'.format(m=MUTED),
      "A large part of the work was operations: thirty-three issues, each with a regression test. Four examples. A path-MTU black hole on the home network silently dropped large "
      "packets. Cancelling a Spark application through the Master did not stop its client-mode driver, and the zombie collided "
      "with the new stream on the checkpoint. Our nightly deployment gate could never open, because the nightly grid did not "
      "contain the served hyper-parameters. And small files and small memory forced repartitioning and careful scheduling.")

# ------------------------------------------------------------------ 23. Kết quả
slide("results", tieu_de("Conclusion", "All eight criteria demonstrated on the cluster") +
      bang(["#", "Criterion", "Evidence", "Status"], [
          ["1", "Tuning table, ≥ 9 combinations", "27 combinations; best NDCG@10 = 0.0680", "Met"],
          ["2", "ALS vs 2 baselines", "+1,237 % / +43 %; coverage 3.44 %, 6.47 % served, 14.5 % offline", "Met, coverage analysed"],
          ["3", "Three cold-start strategies", "New user shown live; all three unit-tested", "Met"],
          ["4", "Trending ≤ 90 s after a click", "51 s", "Met"],
          ["5", "Closed loop web → bronze → trending", "10.3 s to bronze", "Met"],
          ["6", "A/B with p-value", "CTR p = 0.015 (B lower), conversion p = 0.75", "Met"],
          ["7", "Reproduction, VERSION AS OF", "Deviation 0.000000", "Met"],
          ["8", "OPTIMIZE / Z-ORDER", "Files read 67 → 1.07, 8.4× faster", "Met"],
      ], [5, 33, 42, 20], 24),
      "To close the loop on the list from the beginning: all eight acceptance criteria were demonstrated on the cluster with the "
      "real data. The only threshold not met in production, catalogue coverage, is analysed as the specification requires; the deployed "
      "re-ranking almost doubles it, and an offline mix with co-occurrence candidates passes it.")

# ------------------------------------------------------------------ 24. Hạn chế
cot = lambda tieu, ds, mau: (
    '<div style="flex:1;display:flex;flex-direction:column;gap:14px;background:{c};border:1px solid {r};border-top:8px solid {m};border-radius:14px;padding:32px">'
    '<h3 style="font-family:{h};font-size:44px;font-weight:600;color:{n}">{t}</h3>'
    '<ul style="font-size:28px;line-height:1.45;color:{tx}">{li}</ul></div>').format(
    c=CARD, r=RULE, m=mau, h=H, n=NAVY, t=tieu, tx=TEXT, li="".join("<li>{}</li>".format(x) for x in ds))
slide("limits", tieu_de("Conclusion", "Limitations and future work") +
      '<div style="display:flex;gap:32px;flex:1">' +
      cot("Limitations", [
          "Served coverage 6.5 %; 14.5 % only offline",
          "Main grid chosen on the test month (validation check: same choice)",
          "Simulated A/B traffic (result holds at user level)",
          "Single NameNode, Spark Master and Kafka broker",
          "Personal lists change once per night",
      ], ORANGE) +
      cot("Future work", [
          "Deploy the co-occurrence slots, re-run the A/B test",
          "Full validation grid and rolling splits",
          "Sensitivity of the A/B result to trending attractiveness",
          "High availability for HDFS and Kafka",
          "Incremental (online) model updates",
      ], BLUE) + "</div>",
      "We are clear about the limits. Coverage stays below ten percent; hyper-parameters were chosen on the test month; the A/B "
      "traffic is simulated; and the master, NameNode and broker are single points of failure. The most valuable next steps are "
      "more candidate sources before re-ranking, a proper validation protocol, and a sensitivity analysis of the A/B simulation.")

# ------------------------------------------------------------------ 25. Cảm ơn
demo = ["8 criteria shown live", "Airflow DAG run", "HDFS + Delta SQL", "Web write-back → Delta", "Streams ≥ 30 min"]
slide("thanks", (
    '<p style="font-size:24px;font-weight:600;letter-spacing:3px;text-transform:uppercase;color:{la}">Thank you</p>'
    '<h1 style="font-family:{h};font-size:96px;font-weight:700;line-height:1.08;color:{bg}">Thank you — questions welcome</h1>'
    '<div style="display:flex;gap:20px;flex-wrap:wrap">{d}</div>'
    '<div style="flex:1"></div>'
    '<p style="font-family:{mo};font-size:26px;color:#C9D4E5">github.com/chitam31/CS411V_Final-Project_Ecommerce-Recommender-System</p>'
).format(la=LIGHT_ACC, h=H, bg=BG, mo=M, d="".join(
    '<p style="font-size:28px;font-weight:600;color:{n};background:{la};padding:12px 24px;border-radius:12px">{i}. {t}</p>'.format(
        n=NAVY, la=LIGHT_ACC, i=i + 1, t=t) for i, t in enumerate(demo))),
    "Thank you for your attention. You have already seen the system live at the start: the eight criteria on the cluster, a DAG "
    "run, the HDFS tree and Delta SQL, a write-back from the web confirmed in Delta, and streams that have run for hours. We are "
    "happy to take your questions.",
    nen=NAVY)
p = os.path.join(SL, "thanks.html")
s = open(p, encoding="utf-8").read().replace("color:{}\">BD03".format(MUTED), "color:#9FB0C8\">BD03").replace(
    "text-align:right;font-size:24px;color:{}".format(MUTED), "text-align:right;font-size:24px;color:#9FB0C8")
open(p, "w", encoding="utf-8").write(s)

# ------------------------------------------------------------------ 26. Dự phòng (hỏi đáp)
slide("backup", tieu_de("Backup for questions", "Additional experiments (11 November)") +
      '<div style="display:flex;gap:56px;flex:1">'
      '<div style="flex:1.25;display:flex;flex-direction:column;gap:20px">'
      '<p style="font-family:{h};font-size:34px;font-weight:600;color:{n}">Choices made on a validation period</p>'.format(h=H, n=NAVY) +
      '<p style="font-size:24px;color:{m}">Select on 19 Aug – 2 Sep (6,505 users), report on 3–18 Sep (4,215 users); train, seed and grid unchanged</p>'.format(m=MUTED) +
      bang(["Held-out days", "NDCG@10", "vs category"], [
          ["Category best-sellers", "0.0413", "—"],
          ["ALS selected on validation = <b>deployed</b> (rank 50, reg 1.0, alpha 40)", "0.0579", "+40 %"],
          ["ALS best on the held-out days themselves", "0.0581", "+41 %"],
          ["+ re-ranking, β = 1.0 selected on validation", "<b>0.0742</b>", "<b>+80 %</b>"],
      ], [62, 19, 19], 24) +
      '<p style="font-size:24px;color:{t}">17 of 27 combinations; same ordering in both periods (Spearman 0.995)</p>'.format(t=TEXT) +
      "</div>"
      '<div style="flex:1;display:flex;flex-direction:column;gap:20px">'
      '<p style="font-family:{h};font-size:34px;font-weight:600;color:{n}">A/B with the user as the unit</p>'.format(h=H, n=NAVY) +
      '<p style="font-size:24px;color:{m}">8,592 simulated users, about 41 impressions each</p>'.format(m=MUTED) +
      bang(["CTR, B vs A (−5.5 %)", "p-value"], [
          ["Impression-level z-test (report)", "0.015"],
          ["Cluster-robust z-test", "<b>0.037</b>"],
          ["User bootstrap, 2,000 resamples", "<b>0.029</b>"],
      ], [72, 28], 24) +
      '<p style="font-size:24px;color:{t}">95 % CI of the relative difference: −10.5 % to −0.7 %; design effect 1.36; conversion still p = 0.75</p>'.format(t=TEXT) +
      "</div></div>" +
      '<div style="border-top:1px solid {r};padding-top:18px;display:flex;gap:40px;align-items:baseline">'
      '<p style="font-family:{h};font-size:30px;font-weight:600;color:{n};min-width:max-content">Candidate sources (offline)</p>'
      '<p style="font-size:24px;line-height:1.4;color:{t}">7 served slots + 3 <b>co-occurrence</b> slots (training data only): coverage 6.47 % → '
      '<b style="color:{o}">14.5 %</b> (target 10 %), NDCG@10 0.0865 → 0.0892, held-out 0.0742 → 0.0764; category slots add nothing; not deployed</p></div>'.format(
          r=RULE, h=H, n=NAVY, t=TEXT, o=ORANGE),
      "Backup slide for questions about the evaluation protocol and coverage. On the left: choosing the hyper-parameters and beta on the first "
      "fifteen days of the test month and reporting on the last sixteen selects exactly the deployed configuration; on the held-out "
      "days ALS is forty percent and the re-ranked list eighty percent better than the category baseline, and looking at the held-out "
      "days themselves would only gain 0.0002. On the right: with the user as the unit, the A/B difference stays significant, p equals "
      "0.037 with cluster-robust errors and 0.029 by bootstrap. At the bottom: three co-occurrence slots raise coverage to 14.5 percent "
      "offline, above the target, with higher NDCG; this is not deployed yet.")

assert len(THU_TU) == TONG, len(THU_TU)
deck = {
    "v": 4,
    "createdOnFiles": {"v": 1, "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")},
    "lists": "css",
    "title": "BD03 Recommender System – Defence",
    "cover": "cover",
    "order": THU_TU,
    "sections": {
        "s1": {"description": "The assignment, its objectives and the eight acceptance criteria", "start": "cover"},
        "s2": {"description": "Dataset, architecture, cluster, lakehouse, streaming and orchestration", "start": "dataset"},
        "s3": {"description": "ALS, tuning, baselines, popularity bias, re-ranking and cold start", "start": "als"},
        "s4": {"description": "A/B test and the Delta Lake features: Z-ORDER, time travel, schema evolution", "start": "abtest"},
        "s5": {"description": "The live closed loop and lessons from operating the cluster", "start": "loop"},
        "s6": {"description": "Results against the criteria, limitations and future work", "start": "results"},
    },
    "faces": {
        "tinos": {"family": "Tinos", "href": "https://fonts.googleapis.com/css2?family=Tinos:ital,wght@0,400;0,700;1,400;1,700&display=swap"},
    },
    "designSystems": [],
}
with open(os.path.join(GOC, "project", "deck.json"), "w", encoding="utf-8") as f:
    json.dump(deck, f, ensure_ascii=False, indent=1)
print("OK", len(THU_TU), "slide")
