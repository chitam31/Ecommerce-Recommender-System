"""Hình: ba luồng Structured Streaming và ngân sách độ trễ của khối thịnh hành (thanh tỉ lệ theo giây)."""
from matplotlib.patches import Rectangle
from ve import BANG, So

s = So(7.4, 3.42)
lanes = [
    ("nap_su_kien\n1 core · trigger 15 s\nfrom earliest offset", "retailrocket-\nevents",
     ["parse JSON, drop\nincomplete rows", "Vietnamese schema,\nsha256 event id", "join silver.san_pham\n(category)"],
     "bronze.su_kien\nParquet, by day", "bronze"),
    ("tinh_thinh_hanh\n2 cores · trigger 20 s\nRocksDB state store", "retailrocket-\nevents",
     ["views and carts,\nKafka timestamp", "watermark 2 min,\nwindow 15 / 1 min", "score vs 24-h mean,\nMERGE (window, item)"],
     "gold.thinh_hanh_15p\nDelta, update mode", "gold"),
    ("ghi_hien_thi\n1 core · trigger 30 s\ncheckpoint on HDFS", "hien-thi",
     ["parse impressions,\nclicks, purchases", "aggregate per\nimpression id", "MERGE: insert,\nupdate click / buy"],
     "gold.nhat_ky_hien_thi\nChange Data Feed", "gold"),
]
for i, (dau, topic, buoc, dich, mau_dich) in enumerate(lanes):
    y = 0.1 + i * 0.8
    s.nhom(0.1, y, 7.2, 0.72, None, "luong")
    s.hop("h%d" % i, 0.18, y + 0.1, 1.0, 0.52, dau, "luong", co=6.5)
    s.hop("t%d" % i, 1.32, y + 0.08, 0.8, 0.56, topic, "kafka", co=6.4, kieu="tru", dam_dong_dau=False)
    truoc = "t%d" % i
    for j, b in enumerate(buoc):
        k = "b%d%d" % (i, j)
        s.hop(k, 2.28 + j * 1.22, y + 0.1, 1.06, 0.52, b, "trang", co=6.4, dam_dong_dau=False)
        s.mui(truoc, k, "e", "w", loai="luong")
        truoc = k
    s.hop("d%d" % i, 5.94, y + 0.1, 1.28, 0.52, dich, mau_dich, co=6.4)
    s.mui(truoc, "d%d" % i, "e", "w", loai="luong")

# Ngân sách độ trễ: thanh tỉ lệ theo giây (0–90 s)
y = 2.5
s.nhom(0.1, y, 7.2, 0.84, None, "xam")
s.hop("hb", 0.18, y + 0.12, 1.0, 0.6, "Trending latency\nbudget: click →\nupdated block", "xam", co=6.4)
x0, x1 = 1.32, 7.12
k = (x1 - x0) / 90.0
doan = [(1, "", "kafka"), (20, "wait for the next\ntrigger, ≤ 20 s", "luong"), (30, "micro-batch: aggregate\n+ MERGE, 10–30 s", "gold"),
        (5, "web\n< 5 s", "web"), (34, "margin to the 90 s target", "trang")]
x, by, bh = x0, y + 0.2, 0.32
for giay, nhan, mau in doan:
    nen, vien, _ = BANG[mau]
    s.ax.add_patch(Rectangle((x, by), giay * k, bh, fc=nen, ec=vien, lw=0.8, zorder=3))
    if nhan:
        s.ax.text(x + giay * k / 2, by + bh / 2, nhan, fontsize=6.1, ha="center", va="center", zorder=6,
                  linespacing=1.1)
    x += giay * k
s.chu(x0 + 0.02, by - 0.07, "click → Kafka < 1 s", co=5.9, canh="left", nghieng=True, mau="#444444")
for t in range(0, 91, 15):
    xt = x0 + t * k
    s.ax.plot([xt, xt], [by + bh, by + bh + 0.04], color="#333333", lw=0.6, zorder=4)
    s.chu(xt, by + bh + 0.11, "%d s" % t, co=6.0)
xm = x0 + 51 * k
s.ax.plot([xm, xm], [by - 0.05, by + bh + 0.02], color="#B85450", lw=1.4, zorder=7)
s.chu(xm + 0.04, by - 0.07, "measured: 51 s (11 Nov)", co=6.2, canh="left", dam=True, mau="#B85450")
s.luu("f07_luong.png")
