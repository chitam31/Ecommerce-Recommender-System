"""Hình: dòng dữ liệu cấp bảng (dựng từ f04_dong_du_lieu.dot, đổi sang bảng màu chung của ve.py)."""
import re
import subprocess

from ve import BANG

src = open("f04_dong_du_lieu.dot", encoding="utf-8").read()


def mau(k, net=""):
    nen, vien, _ = BANG[k]
    return 'fillcolor="%s", color="%s"%s' % (nen, vien, net)


doi = {"#F2F2F2": mau("xam"), "#DCE5F0": mau("kafka"), "#EADBCB": mau("bronze"), "#E6E6E6": mau("silver"),
       "#E2E1EE": mau("feat"), "#F1E7C2": mau("gold"), "#F6F0D8": mau("gold", ', style="filled,dashed"'),
       "#E1ECDD": mau("luong"), "#F0DEDC": mau("af")}
src = re.sub(r'fillcolor="(#[0-9A-Fa-f]{6})"', lambda m: doi.get(m.group(1).upper(), m.group(0)), src)
# mặc định: job theo lô màu cam bo góc, thư mục mô hình màu tím, viền dày hơn; khung ngoài
src = src.replace('node [shape=box, style=filled, fillcolor="#FFFFFF"];',
                  'node [shape=box, style="rounded,filled", %s];' % mau("lo"))
src = src.replace('node [shape=folder, style=filled, fillcolor="#FFFFFF"];',
                  'node [shape=folder, style=filled, %s];' % mau("feat"))
src = src.replace('penwidth=1.0, color="#1F1F1F"]', 'penwidth=1.3]')
src = src.replace('edge  [color="#1F1F1F"', 'edge  [color="#333333"')
i = src.index("node  [")
j = src.rindex("}")
src = src[:src.index("\n", i) + 1] + '  subgraph cluster_khung { label=""; color="#222222"; penwidth=2.0; margin=16;\n' + \
    src[src.index("\n", i) + 1:j] + "  }\n}\n"
open("/tmp/f04.dot", "w", encoding="utf-8").write(src)
subprocess.run(["dot", "-Tpng", "-Gdpi=220", "/tmp/f04.dot", "-o", "f04_dong_du_lieu.png"], check=True)
