"""cap_nhat_muc_luc.py - mở báo cáo bằng LibreOffice (UNO), cập nhật mục lục và danh mục hình/bảng,
rồi xuất PDF (và tùy chọn một bản .docx đã có sẵn mục lục).

    python3 cap_nhat_muc_luc.py BD03_Recommender_System_Report.docx  [--docx]

Word tự cập nhật các trường khi mở (tài liệu bật updateFields), nhưng bản PDF xem trước và người mở
bằng trình xem khác cần mục lục đã được điền sẵn.
"""
import os
import subprocess
import sys
import time

import uno
from com.sun.star.beans import PropertyValue


def tc(ten, gia_tri):
    p = PropertyValue()
    p.Name, p.Value = ten, gia_tri
    return p


def main():
    vao = os.path.abspath(sys.argv[1])
    goc, _ = os.path.splitext(vao)
    cong = 2002
    lo = subprocess.Popen(["soffice", "-env:UserInstallation=file:///tmp/lo_bd03", "--headless", "--invisible",
                           "--norestore", "--accept=socket,host=localhost,port=%d;urp;" % cong],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        ctx_cuc_bo = uno.getComponentContext()
        resolver = ctx_cuc_bo.ServiceManager.createInstanceWithContext("com.sun.star.bridge.UnoUrlResolver", ctx_cuc_bo)
        for _ in range(60):
            try:
                ctx = resolver.resolve("uno:socket,host=localhost,port=%d;urp;StarOffice.ComponentContext" % cong)
                break
            except Exception:  # noqa: BLE001 - LibreOffice chưa sẵn sàng
                time.sleep(1)
        else:
            sys.exit("Không nối được LibreOffice")
        desktop = ctx.ServiceManager.createInstanceWithContext("com.sun.star.frame.Desktop", ctx)
        doc = desktop.loadComponentFromURL(uno.systemPathToFileUrl(vao), "_blank", 0, (tc("Hidden", True),))
        cac_muc = doc.getDocumentIndexes()
        for _ in range(2):  # lần 2 để số trang trong mục lục đúng sau khi mục lục chiếm chỗ
            for i in range(cac_muc.getCount()):
                cac_muc.getByIndex(i).update()
        # Giữ ảnh nguyên độ phân giải, nén không mất dữ liệu: hình trong PDF nét như ảnh gốc
        loc = uno.Any("[]com.sun.star.beans.PropertyValue", (tc("UseLosslessCompression", True),
                                                             tc("ReduceImageResolution", False)))
        doc.storeToURL(uno.systemPathToFileUrl(goc + ".pdf"), (tc("FilterName", "writer_pdf_Export"), tc("FilterData", loc)))
        if "--docx" in sys.argv:
            doc.storeToURL(uno.systemPathToFileUrl(goc + "_muc_luc.docx"), (tc("FilterName", "MS Word 2007 XML"),))
        so_muc = cac_muc.getCount()
        doc.close(True)
        print("đã cập nhật %d mục lục; xuất %s.pdf" % (so_muc, goc))
    finally:
        lo.terminate()
        lo.wait(timeout=30)


if __name__ == "__main__":
    main()
