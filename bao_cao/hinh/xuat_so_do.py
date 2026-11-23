"""Chụp ba sơ đồ nhóm thiết kế (../slide/so_do.py) thành PNG độ phân giải cao cho báo cáo Word/PDF.

  python3 xuat_so_do.py            # sinh so_do_{tong_quan,nam_dag,lakehouse}_{vi,en}.png (bề rộng ~3900 px)

Sơ đồ trong slide là HTML + SVG vector; bản PNG này chỉ dùng cho báo cáo, chụp ở hệ số 2,5 nên in hay phóng to vẫn nét.
"""
import os
import sys

GOC = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(GOC, "..", "slide"))
import so_do  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402

HE_SO = 2.5


def main():
    viec = []
    for nn in ("vi", "en"):
        viec += [("so_do_tong_quan_%s.png" % nn, so_do.tong_quan(nn)),
                 ("so_do_nam_dag_%s.png" % nn, so_do.nam_dag(nn)),
                 ("so_do_lakehouse_%s.png" % nn, so_do.lakehouse(nn))]
    with sync_playwright() as p:
        b = p.chromium.launch()
        trang = b.new_page(device_scale_factor=HE_SO, viewport={"width": 1700, "height": 900})
        for ten, html in viec:
            trang.set_content(so_do.trang_xem(html, nen="#FFFFFF"))
            trang.wait_for_timeout(100)
            trang.locator(".sodo").first.screenshot(path=os.path.join(GOC, ten))
            print(ten)
        b.close()


if __name__ == "__main__":
    main()
