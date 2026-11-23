"""Hình: đường đi của một job Spark từ task Airflow tới executor và HDFS."""
from ve import MAU, So

s = So(7.55, 5.12)
s.nhom(0.12, 0.12, 4.55, 4.5, "Master host labsoe-11 (10.68.71.113)", MAU["may"], "#6E8BB0")
s.hop("af", 0.27, 0.5, 1.6, 1.0, "airflow-scheduler\nLocalExecutor task:\nBashOperator runs\nsubmit.sh <profile> <job>", MAU["af"], co=6.8)
s.hop("dir", 0.27, 1.95, 1.6, 1.05, "project directory\n~/bd03_khuyen_nghi\n= /opt/bd03 in Airflow\n= /opt/spark/jobs in Spark", MAU["xam"], co=6.6, kieu="tru")
s.hop("sock", 0.27, 3.45, 1.6, 0.6, "/var/run/docker.sock\n(group docker)", MAU["trang"], co=6.8)
s.nhom(2.08, 0.42, 2.45, 4.05, "container spark-master (host network)", MAU["spark"], "#7FB27F", co=7.2)
s.hop("mst", 2.22, 0.78, 2.17, 0.62, "Standalone Master :7077\nUI :8080, applications list", MAU["trang"], co=6.8)
s.hop("drv", 2.22, 1.62, 2.17, 1.35, "Driver (client mode)\nJVM + Python process\ndriver.host = 10.68.71.113\nport per profile, e.g. als 4049/4050\njob UI 4144 · common.zip on --py-files", MAU["trang"], co=6.6)
s.hop("out", 2.22, 3.2, 2.17, 0.62, "stdout: last line = JSON result\n{\"ndcg_at_10\": ..., \"cai_thien\": true}", MAU["trang"], co=6.4)
s.chu(3.3, 4.12, "all drivers share the 7.4 GB of the master host", co=6.3, nghieng=True)

s.nhom(4.85, 0.12, 2.43, 2.62, "Spark workers (5 hosts, host network)", MAU["may"], "#6E8BB0")
for i, ten in enumerate(["labsit12", "labsit-may26", "labsoe-18"]):
    s.hop("ex%d" % i, 5.0, 0.46 + i * 0.7, 2.13, 0.56, "Executor on %s\n2 cores · 2 GB, Python workers" % ten, MAU["spark"], co=6.7)
s.chu(6.06, 2.6, "profile als: cores.max = 10 → 5 executors (3 shown)", co=5.9, nghieng=True, mau="#555555")
s.nhom(4.85, 2.92, 2.43, 1.7, "HDFS (Delta tables, checkpoints, models)", MAU["hdfs"], "#D9A47E")
s.hop("nn", 5.0, 3.28, 2.13, 0.5, "NameNode 10.68.71.113:8020\nmetadata, block locations", MAU["trang"], co=6.6)
s.hop("dn", 5.0, 3.92, 2.13, 0.55, "DataNodes on 5 workers\nreached by ZeroTier IP\n(dfs.client.use.datanode.hostname)", MAU["trang"], co=6.4, dam_dong_dau=False)


def so(x, y, n):
    s.ax.text(x, y, n, fontsize=6.6, ha="center", va="center", color="white", fontweight="bold", zorder=8,
              bbox=dict(boxstyle="circle,pad=0.16", fc="#1F1F1F", ec="none"))


s.mui("af", "dir", "s", "n", ta=0.35, tb=0.35); so(0.62, 1.73, "1")
s.duong([(1.65, 1.5), (1.65, 1.75), (1.98, 1.75), (1.98, 3.75), (1.87, 3.75)]); so(1.98, 2.5, "2")
s.mui("sock", "drv", "e", "w", ta=0.5, tb=0.85); so(2.1, 3.62, "3")
s.mui("drv", "mst", "n", "s", ta=0.5, tb=0.5); so(3.48, 1.51, "4")
s.mui("mst", "ex0", "e", "w", ta=0.5, tb=0.5); so(4.66, 1.17, "5")
s.mui("drv", "ex1", "e", "w", ta=0.45, tb=0.5, hai_chieu=True); so(4.66, 1.83, "6")
# executors <-> DataNodes: khối dữ liệu đi hai chiều (đọc và ghi)
for i in range(3):
    s.ax.plot([7.13, 7.36], [0.74 + i * 0.7] * 2, color="#1F1F1F", lw=0.7, zorder=2.5)
s.ax.plot([7.36, 7.36], [0.74, 4.2], color="#1F1F1F", lw=0.7, zorder=2.5)
s.ax.add_patch(__import__("matplotlib.patches", fromlist=["x"]).FancyArrowPatch(
    (7.36, 4.2), (7.13, 4.2), arrowstyle="<|-|>", mutation_scale=6.5, color="#1F1F1F", lw=0.7, shrinkA=0, shrinkB=0, zorder=2.5))
s.ax.text(7.45, 3.0, "read / write blocks", rotation=270, fontsize=6.0, fontstyle="italic", ha="center", va="center")
s.mui("drv", "nn", "e", "w", ta=0.8, tb=0.5, net="--"); so(4.66, 2.86, "7")
s.duong([(2.4, 3.82), (2.4, 4.42), (1.07, 4.42), (1.07, 4.05)], net="--", mau="#B04A4A"); so(1.72, 4.42, "8")

# chú giải các bước (khớp danh sách trong mục Routing of a Spark job)
buoc = ["submit.sh packs common.zip", "docker exec via the socket", "driver starts (client mode)",
        "driver registers with the Master", "Master launches executors", "tasks and results",
        "HDFS I/O: metadata, blocks", "last stdout line → XCom → branch"]
for k, mo_ta in enumerate(buoc):
    cot, hang = k % 4, k // 4
    x, y = 0.22 + cot * 1.85, 4.78 + hang * 0.2
    so(x, y, str(k + 1))
    s.ax.text(x + 0.1, y, mo_ta, fontsize=6.0, ha="left", va="center", color="#1A1A1A")
s.luu("f03_dinh_tuyen_job.png")
