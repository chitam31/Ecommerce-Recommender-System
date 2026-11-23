"""Dựng thử slide bằng Chromium (Playwright): ảnh PNG từng slide để kiểm tra tràn, hoặc một PDF vector.

  python3 dung_thu_slide.py [--vi] <id slide> ...                 # PNG /tmp/sl_<id>.png, in "TRAN" nếu tràn
  python3 dung_thu_slide.py [--vi] --pdf <tệp.pdf> <id slide> ...  # mỗi slide một trang 1920x1080

--vi dùng bản tiếng Việt (gen_vi.py, project_vi/); mặc định là bản tiếng Anh (gen.py, project/). Ảnh /_blob/<id>
được thay bằng tệp gốc trong bao_cao/hinh theo bảng IMG của tệp gen tương ứng. Đường dẫn Chromium: biến CHROME
(mặc định để Playwright tự tìm). Thứ tự slide lấy từ deck.json, ví dụ:
  python3 dung_thu_slide.py --vi --pdf "../CS411V_SLIDE Final Project_Group Chí Tâm & Quốc Anh_Viet.pdf" $(python3 -c "import json;print(' '.join(json.load(open('project_vi/deck.json'))['order']))")
"""
import os
import re
import sys

from playwright.sync_api import sync_playwright

GOC = os.path.dirname(os.path.abspath(__file__))
HINH = os.path.join(GOC, "..", "hinh")
args = sys.argv[1:]
VI = bool(args) and args[0] == "--vi"
if VI:
    args = args[1:]
GEN, DU_AN = ("gen_vi.py", "project_vi") if VI else ("gen.py", "project")
src = open(os.path.join(GOC, GEN), encoding="utf-8").read()
ids = {i: os.path.join(HINH, f) for i, f in re.findall(r'"\w+": \("/_blob/([0-9a-f]+)", \d+, \d+, "([^"]+)"\)', src)}
CSS = ('*{margin:0;box-sizing:border-box} section{width:1920px;height:1080px;overflow:hidden;position:relative;'
       'page-break-after:always;break-after:page} table{border-collapse:collapse} td,th{padding:6px 10px} aside{display:none}'
       '@page{size:1920px 1080px;margin:0} html,body{width:1920px}')
pdf = None
if args and args[0] == "--pdf":
    pdf, args = args[1], args[2:]


def doc(s):
    html = open(os.path.join(GOC, DU_AN, "slides", s + ".html"), encoding="utf-8").read()
    for i, f in ids.items():
        html = html.replace("/_blob/" + i, "file://" + f)
    return html


def trang(noi_dung):
    return '<html><head><meta charset="utf-8"><style>%s</style></head><body>%s</body></html>' % (CSS, noi_dung)


with sync_playwright() as p:
    b = p.chromium.launch(executable_path=os.environ.get("CHROME") or None)
    pg = b.new_page(viewport={"width": 1920, "height": 1080})
    if pdf:
        open("/tmp/deck_all.html", "w", encoding="utf-8").write(trang("".join(doc(s) for s in args)))
        pg.goto("file:///tmp/deck_all.html", wait_until="networkidle")
        pg.pdf(path=pdf, width="1920px", height="1080px", print_background=True,
               margin={"top": "0", "bottom": "0", "left": "0", "right": "0"})
        print("pdf", pdf)
    else:
        for s in args:
            open("/tmp/sl_%s.html" % s, "w", encoding="utf-8").write(trang(doc(s)))
            pg.goto("file:///tmp/sl_%s.html" % s, wait_until="networkidle")
            sh = pg.evaluate("()=>{const s=document.querySelector('section');let m=0;s.querySelectorAll('*').forEach("
                             "e=>{const r=e.getBoundingClientRect();if(r.bottom>m)m=r.bottom});return [s.scrollHeight,Math.round(m)]}")
            pg.screenshot(path="/tmp/sl_%s.png" % s)
            print(s, sh, "TRAN" if sh[0] > 1080 or sh[1] > 1016 else "")
    b.close()
