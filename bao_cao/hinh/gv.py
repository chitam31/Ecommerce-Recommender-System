"""gv.py - kiểu Graphviz cùng phong cách ve.py (bảng màu draw.io, khối bo góc viền cùng tông, viền ngoài).

ve_dot(than, ten) dựng PNG 300 dpi và SVG (chữ thành đường cong, qua PDF → pdftocairo)."""
import os
import subprocess
import tempfile

from ve import BANG

FONT = "Liberation Serif"


def kieu(khoa, hinh="box", them=""):
    nen, vien, _ = BANG[khoa]
    return 'shape=%s, style="rounded,filled", fillcolor="%s", color="%s", penwidth=1.3%s' % (
        hinh, nen, vien, (", " + them) if them else "")


DAU = """digraph G {
  graph [rankdir=%(rd)s, fontname="%(f)s", nodesep=%(ns)s, ranksep=%(rs)s, pad=0.15, splines=%(sp)s, newrank=true, bgcolor=white];
  node  [fontname="%(f)s", fontsize=%(fs)s, margin="0.10,0.05", penwidth=1.3, fontcolor="#1A1A1A"];
  edge  [color="#333333", arrowsize=0.75, penwidth=1.1, fontname="%(f)s Italic", fontsize=%(es)s, fontcolor="#262626"];
  subgraph cluster_khung { label=""; color="#222222"; penwidth=1.6; margin=14;
%(than)s
  }
}
"""


def ve_dot(than, ten, rankdir="TB", nodesep=0.25, ranksep=0.3, fontsize=10, edgesize=9, splines="true"):
    dot = DAU % dict(rd=rankdir, f=FONT, ns=nodesep, rs=ranksep, sp=splines, fs=fontsize, es=edgesize, than=than)
    with tempfile.NamedTemporaryFile("w", suffix=".dot", delete=False) as f:
        f.write(dot)
    try:
        subprocess.run(["dot", "-Tpng", "-Gdpi=300", f.name, "-o", ten + ".png"], check=True)
        subprocess.run(["dot", "-Tpdf", f.name, "-o", "/tmp/_gv.pdf"], check=True)
        subprocess.run(["pdftocairo", "-svg", "/tmp/_gv.pdf", ten + ".svg"], check=True)
    finally:
        os.unlink(f.name)
