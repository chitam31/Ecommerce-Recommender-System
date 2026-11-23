"""Hình: DAG 2-5. Mỗi DAG vẽ bằng Graphviz riêng rồi xếp chồng bằng PIL, để bốn khung
cùng bề rộng và không phụ thuộc cách Graphviz xếp các cụm cạnh nhau.
Nội dung lấy từ airflow/dags/kn_giam_sat_streaming.py, kn_tinh_ab.py, kn_bao_tri.py, kn_tai_lap.py."""
import os
import subprocess
import tempfile

from PIL import Image, ImageDraw, ImageFont

DPI = 300
DAU = """digraph G {
  graph [rankdir=LR, fontname="Liberation Serif", nodesep=0.18, ranksep=0.3, pad=0, bgcolor=transparent];
  node  [shape=box, style="rounded,filled", fontname="Liberation Serif", fontsize=9.5, fillcolor="#FFE6CC", color="#D79B00", penwidth=1.2, margin="0.1,0.05"];
  edge  [color="#333333", arrowsize=0.7, penwidth=1.0, fontname="Liberation Serif Italic", fontsize=8.5];
%s
}
"""
DAG = [
    ("DAG 2  kn_giam_sat_streaming  (*/15 * * * *, run timeout 14 min, no retries)", """
    a1 [fillcolor="#FFFFFF", color="#666666", label=<<B>kiem_tra_job_song</B><BR/>Spark Master JSON:<BR/>bd03-tinh-thinh-hanh alive?>];
    a2 [fillcolor="#FFFFFF", color="#666666", label=<<B>kiem_tra_du_lieu_15_phut</B>  [nhanh]<BR/>newest window end ≥ now − 15 min?>];
    a3 [fillcolor="#F8CECC", color="#B85450", label=<<B>khoi_dong_lai_luong</B><BR/>scripts/luong.sh khoi_dong thinh_hanh<BR/>trigger_rule = one_failed>];
    a4 [fillcolor="#F8CECC", color="#B85450", label=<<B>canh_bao</B><BR/>append logs/canh_bao.log<BR/>trigger_rule = one_failed>];
    a1 -> a3; a2 -> a3; a1 -> a4; a2 -> a4;"""),
    ("DAG 3  kn_tinh_ab  (0 * * * *, run timeout 50 min)", """
    b1 [label=<<B>tinh_ket_qua_ab</B>  [lo]<BR/>read the Change Data Feed of gold.nhat_ky_hien_thi since the last processed version<BR/>→ recompute the changed days → two-proportion z-test<BR/>→ MERGE gold.ket_qua_ab with phien_ban_delta_du_lieu>];"""),
    ("DAG 4  kn_bao_tri  (0 4 * * *, run timeout 2 h)", """
    c1 [label=<<B>toi_uu_delta</B>  [bao_tri]<BR/>DELETE recommendation lists generated &gt; 7 days before the latest deployment,<BR/>trending windows ended &gt; 2 days ago → OPTIMIZE (+ ZORDER BY ma_nguoi_dung / ma_san_pham) → VACUUM<BR/>(30-day retention keeps time travel; retries on Delta write conflicts)>];"""),
    ('DAG 5  kn_tai_lap  (manual; conf {"phien_ban_delta": v, "che_do": ...}, run timeout 4 h)', """
    d1 [label=<<B>chay_lai_huan_luyen</B>  [tai_lap]<BR/>features VERSION AS OF v → same tuning / evaluation<BR/>→ compare with ket_qua_tuning and lich_su_huan_luyen<BR/>→ append gold.ket_qua_tai_lap>];
    d2 [fillcolor="#FFFFFF", color="#666666", label=<<B>so_sanh_voi_lich_su</B><BR/>fail if max deviation ≥ 10<SUP>−6</SUP>>];
    d1 -> d2;"""),
]


def ve(than, tep):
    with tempfile.NamedTemporaryFile("w", suffix=".dot", delete=False) as f:
        f.write(DAU % than)
    subprocess.run(["dot", "-Tpng", "-Gdpi=%d" % DPI, f.name, "-o", tep], check=True)
    os.unlink(f.name)
    a = Image.open(tep).convert("RGBA")
    return a.crop(a.getbbox())


# Khung và tiêu đề vẽ bằng PIL để bốn khung cùng bề rộng (giống khung nhóm ở các hình matplotlib).
PX = DPI / 72.0
chu_dam = ImageFont.truetype("/usr/share/fonts/truetype/liberation/LiberationSerif-Bold.ttf", int(10.5 * PX))
le, tren_td, duoi_td, day, khe = int(0.12 * DPI), int(0.07 * DPI), int(0.1 * DPI), int(0.1 * DPI), int(0.08 * DPI)
anh = [ve(b, "/tmp/f06_%d.png" % i) for i, (_, b) in enumerate(DAG)]
cao_td = chu_dam.getbbox("Ag")[3]
rong = max(a.width for a in anh) + 2 * le + 4
cao_khung = [tren_td + cao_td + duoi_td + a.height + day for a in anh]
VIEN = int(0.06 * DPI)
nen = Image.new("RGB", (rong + 2 * VIEN, sum(cao_khung) + khe * (len(anh) - 1) + 4 + 2 * VIEN), "white")
ve2 = ImageDraw.Draw(nen)
y = 1 + VIEN
for (tieu_de, _), a, h in zip(DAG, anh, cao_khung):
    ve2.rounded_rectangle([VIEN + 1, y, VIEN + rong - 2, y + h], radius=int(0.035 * DPI), fill="#FDF4F3", outline="#B85450", width=max(2, int(0.8 * PX)))
    ve2.text((VIEN + le, y + tren_td), tieu_de, font=chu_dam, fill="#1A1A1A")
    nen.paste(a, (VIEN + le, y + tren_td + cao_td + duoi_td), a)
    y += h + khe
ve2.rectangle([2, 2, nen.width - 3, nen.height - 3], outline="#222222", width=int(1.1 * PX))
nen.save("f06_dag_khac.png", dpi=(DPI, DPI))
