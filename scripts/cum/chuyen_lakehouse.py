"""chuyen_lakehouse.py - chuyển hồ dữ liệu từ /lake sang /lakehouse/bd03_khuyen_nghi (mục A.4 đề cương).

Chạy trên máy master, SAU KHI đã dừng 3 luồng streaming và không còn job Spark nào của dự án
(scripts/cum/chuyen_lakehouse.sh lo các bước đó rồi mới gọi tệp này).

    python3 scripts/cum/chuyen_lakehouse.py --xem         # in kế hoạch, không đổi gì
    python3 scripts/cum/chuyen_lakehouse.py --thuc-hien   # chuyển thật
    python3 scripts/cum/chuyen_lakehouse.py --hoan-tac    # chuyển ngược về /lake

Vì sao chuyển được mà không phải ghi lại dữ liệu:
  - "hdfs dfs -mv" trong cùng một HDFS chỉ đổi tên trên NameNode, không chép khối dữ liệu.
  - Bảng Delta lưu đường dẫn TƯƠNG ĐỐI của tệp dữ liệu trong _delta_log, nên dời cả thư mục
    bảng (kèm _delta_log) vẫn đọc được, lịch sử phiên bản và VERSION AS OF giữ nguyên.
  - Checkpoint streaming chỉ lưu offset Kafka, mã lô và trạng thái RocksDB theo đường dẫn tương
    đối trong chính thư mục checkpoint.
  - Ngoại lệ duy nhất là bronze: file sink Parquet ghi đường dẫn TUYỆT ĐỐI của từng tệp vào
    _spark_metadata. Tệp này phải viết lại tiền tố đường dẫn, nếu không Spark đọc bronze qua
    _spark_metadata sẽ báo FileNotFound.

Chỉ chuyển những gì dự án tạo ra (bảng trong cau_hinh._BANG_GOC_CU, checkpoint, mô hình, landing).
Dữ liệu cũ khác của nhóm dưới /lake (vd. /lake/bronze/su_kien, /lake/cu_nhom) được giữ nguyên.
"""
import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import sys

GOC_DU_AN = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(GOC_DU_AN, "jobs"))

from common import cau_hinh as ch  # noqa: E402


# ------------------------------------------------------------------ kế hoạch
def ten_checkpoint_trong_ma(thu_muc_jobs=os.path.join(GOC_DU_AN, "jobs")):
    """Tên thư mục checkpoint mà các job streaming đang dùng (đọc từ mã nguồn)."""
    ten = set()
    for tep in glob.glob(os.path.join(thu_muc_jobs, "**", "*.py"), recursive=True):
        with open(tep, encoding="utf-8") as f:
            ten |= set(re.findall(r'duong_dan_checkpoint\(\s*"([^"]+)"', f.read()))
    return sorted(ten)


def ke_hoach(goc_cu=ch.LAKE_DUONG_DAN_CU, goc_moi=ch.LAKE_DUONG_DAN, checkpoint=None):
    """Danh sách cặp (nguồn, đích) theo đường dẫn HDFS không kèm host, đúng thứ tự cần chuyển."""
    cap = [("{}/{}".format(goc_cu, ch._BANG_GOC_CU[t]), "{}/{}".format(goc_moi, ch._BANG[t]))
           for t in sorted(ch._BANG_GOC_CU)]
    for t in checkpoint if checkpoint is not None else ten_checkpoint_trong_ma():
        cap.append(("{}/checkpoints/{}".format(goc_cu, t), "{}/checkpoints/{}".format(goc_moi, t)))
    cap.append(("{}/models".format(goc_cu), "{}/models".format(goc_moi)))
    cap.append(("{}/landing/retailrocket".format(goc_cu), "{}/landing/retailrocket".format(goc_moi)))
    return cap


def bang_bronze(goc_cu=ch.LAKE_DUONG_DAN_CU, goc_moi=ch.LAKE_DUONG_DAN):
    return ("{}/{}".format(goc_cu, ch._BANG_GOC_CU["bronze.su_kien"]),
            "{}/{}".format(goc_moi, ch._BANG["bronze.su_kien"]))


# ------------------------------------------------------------------ _spark_metadata
def _mau_tien_to(cu):
    """"path":"<scheme:>//<host>" rồi NGAY SAU là tiền tố cu/ (không khớp thư mục chỉ trùng chữ đầu)."""
    return re.compile(r'("path"\s*:\s*"(?:[a-zA-Z][a-zA-Z0-9+.-]*:)?(?://[^/"]*)?)' + re.escape(cu.rstrip("/")) + "/")


def doi_tien_to(noi_dung, cu, moi):
    """Viết lại "path" trong một tệp _spark_metadata của file sink: tiền tố cu/ -> moi/.

    Mỗi dòng (trừ dòng đầu "v1") là JSON có "path":"hdfs://host:port/<cu>/ngay=.../part-...".
    Giữ nguyên scheme và host (hdfs://..., file:...), chỉ đổi phần đường dẫn. Trả về
    (nội dung mới, số chỗ đã đổi).
    """
    return _mau_tien_to(cu).subn(lambda m: m.group(1) + moi.rstrip("/") + "/", noi_dung)


def kiem_tra_metadata(noi_dung, cu):
    """Số dòng có "path" và số dòng vẫn còn trỏ vào tiền tố cũ (phải bằng 0 sau khi đổi)."""
    co_path = [d for d in noi_dung.splitlines() if re.search(r'"path"\s*:\s*"', d)]
    mau = _mau_tien_to(cu)
    con_cu = [d for d in co_path if mau.search(d)]
    return len(co_path), len(con_cu)


# ------------------------------------------------------------------ hệ tệp
class HdfsQuaDocker:
    """Thao tác HDFS bằng "docker exec hadoop-master hdfs dfs" như các script khác của dự án."""

    def __init__(self, container="hadoop-master", chay=subprocess.run):
        self.container = container
        self._chay = chay

    def _lenh(self, *thamso, dau_vao=None, kiem=True):
        kq = self._chay(["docker", "exec", "-i", self.container, "hdfs", "dfs", *thamso],
                        input=dau_vao, capture_output=True)
        if kiem and kq.returncode != 0:
            raise RuntimeError("hdfs dfs {} lỗi: {}".format(" ".join(thamso), kq.stderr.decode("utf-8", "replace")))
        return kq

    def vat_ly(self, p):
        """Đường dẫn như nó xuất hiện trong _spark_metadata (sau phần hdfs://host:port)."""
        return p

    def ton_tai(self, p):
        return self._lenh("-test", "-e", p, kiem=False).returncode == 0

    def tao_thu_muc(self, p):
        self._lenh("-mkdir", "-p", p)

    def doi_ten(self, nguon, dich):
        self._lenh("-mv", nguon, dich)

    def liet_ke(self, p):
        kq = self._lenh("-ls", "-C", p)
        return sorted(d.strip().split("/")[-1] for d in kq.stdout.decode("utf-8").splitlines() if d.strip())

    def doc(self, p):
        return self._lenh("-cat", p).stdout.decode("utf-8")

    def ghi_de(self, p, noi_dung):
        """Ghi ra tệp tạm rồi thay tệp cũ: tệp cũ chỉ bị xóa khi tệp mới đã ghi xong."""
        tam = p + "._chuyen_tam"
        self._lenh("-put", "-f", "-", tam, dau_vao=noi_dung.encode("utf-8"))
        self._lenh("-rm", "-f", "-skipTrash", p)
        self._lenh("-mv", tam, p)

    def kich_thuoc(self, p):
        return self._lenh("-du", "-s", "-h", p, kiem=False).stdout.decode("utf-8").strip()


class HeTepCucBo:
    """Cùng giao diện trên đĩa cục bộ: dùng để kiểm thử với Spark local (đường dẫn file:)."""

    def __init__(self, goc):
        self.goc = goc.rstrip("/")

    def _p(self, p):
        return self.goc + p

    def vat_ly(self, p):
        return self.goc + p

    def ton_tai(self, p):
        return os.path.exists(self._p(p))

    def tao_thu_muc(self, p):
        os.makedirs(self._p(p), exist_ok=True)

    def doi_ten(self, nguon, dich):
        shutil.move(self._p(nguon), self._p(dich))

    def liet_ke(self, p):
        return sorted(t for t in os.listdir(self._p(p)) if not t.startswith("."))

    def doc(self, p):
        with open(self._p(p), encoding="utf-8") as f:
            return f.read()

    def ghi_de(self, p, noi_dung):
        with open(self._p(p), "w", encoding="utf-8") as f:
            f.write(noi_dung)
        # Hệ tệp cục bộ của Hadoop giữ tổng kiểm .<tệp>.crc: nội dung đổi thì phải bỏ tệp này.
        crc = os.path.join(os.path.dirname(self._p(p)), "." + os.path.basename(p) + ".crc")
        if os.path.exists(crc):
            os.remove(crc)

    def kich_thuoc(self, p):
        return ""


# ------------------------------------------------------------------ thực hiện
def viet_lai_metadata(fs, thu_muc_bang, cu, moi, in_ra=print):
    """Viết lại mọi tệp trong <thu_muc_bang>/_spark_metadata; trả về tổng số đường dẫn đã đổi."""
    meta = thu_muc_bang + "/_spark_metadata"
    if not fs.ton_tai(meta):
        in_ra("   (không có {}, bỏ qua)".format(meta))
        return 0
    tong = 0
    for ten in fs.liet_ke(meta):
        if ten.endswith("._chuyen_tam"):
            continue
        p = meta + "/" + ten
        cu_nd = fs.doc(p)
        moi_nd, so = doi_tien_to(cu_nd, cu, moi)
        so_path, con_cu = kiem_tra_metadata(moi_nd, cu)
        if con_cu or len(moi_nd.splitlines()) != len(cu_nd.splitlines()):
            raise RuntimeError("Viết lại {} không trọn vẹn: còn {} đường dẫn cũ".format(p, con_cu))
        if so:
            fs.ghi_de(p, moi_nd)
        tong += so
        in_ra("   {}: {} / {} đường dẫn".format(ten, so, so_path))
    return tong


def thuc_hien(fs, cap, bronze, goc_moi, in_ra=print):
    """Chuyển từng cặp (nguồn -> đích), rồi viết lại _spark_metadata của bronze."""
    co_san = [d for _, d in cap if fs.ton_tai(d)]
    if co_san:
        raise RuntimeError("Đích đã tồn tại, không chuyển để tránh ghi lồng thư mục:\n  " + "\n  ".join(co_san))
    da_chuyen = []
    for nguon, dich in cap:
        if not fs.ton_tai(nguon):
            in_ra("-- bỏ qua (chưa có): {}".format(nguon))
            continue
        fs.tao_thu_muc(dich.rsplit("/", 1)[0])
        fs.doi_ten(nguon, dich)
        da_chuyen.append((nguon, dich))
        in_ra("-- {}  ->  {}".format(nguon, dich))
    if (bronze[0], bronze[1]) in da_chuyen:
        in_ra("-- viết lại _spark_metadata của bronze")
        viet_lai_metadata(fs, bronze[1], fs.vat_ly(bronze[0]), fs.vat_ly(bronze[1]), in_ra)
    fs.tao_thu_muc(goc_moi + "/_quarantine")
    return da_chuyen


def hoan_tac(fs, cap, bronze, in_ra=print):
    """Đảo ngược thuc_hien: viết lại metadata về tiền tố cũ rồi chuyển từng đích về nguồn."""
    if fs.ton_tai(bronze[1]) and not fs.ton_tai(bronze[0]):
        in_ra("-- viết lại _spark_metadata của bronze về tiền tố cũ")
        viet_lai_metadata(fs, bronze[1], fs.vat_ly(bronze[1]), fs.vat_ly(bronze[0]), in_ra)
    da_tra = []
    for nguon, dich in reversed(cap):
        if fs.ton_tai(dich) and not fs.ton_tai(nguon):
            fs.tao_thu_muc(nguon.rsplit("/", 1)[0])
            fs.doi_ten(dich, nguon)
            da_tra.append((dich, nguon))
            in_ra("-- {}  ->  {}".format(dich, nguon))
    return da_tra


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    nhom = ap.add_mutually_exclusive_group(required=True)
    nhom.add_argument("--xem", action="store_true")
    nhom.add_argument("--thuc-hien", action="store_true")
    nhom.add_argument("--hoan-tac", action="store_true")
    ap.add_argument("--goc-cuc-bo", help="Chạy trên thư mục cục bộ thay cho HDFS (kiểm thử)")
    args = ap.parse_args()

    fs = HeTepCucBo(args.goc_cuc_bo) if args.goc_cuc_bo else HdfsQuaDocker()
    cap = ke_hoach()
    bronze = bang_bronze()
    if args.xem:
        for nguon, dich in cap:
            print("{} {}  ->  {}".format("co " if fs.ton_tai(nguon) else "--", nguon, dich))
        return
    if args.thuc_hien:
        da = thuc_hien(fs, cap, bronze, ch.LAKE_DUONG_DAN)
        print(json.dumps({"da_chuyen": len(da), "goc_moi": ch.LAKE_DUONG_DAN}, ensure_ascii=False))
        print(fs.kich_thuoc(ch.LAKE_DUONG_DAN))
    else:
        da = hoan_tac(fs, cap, bronze)
        print(json.dumps({"da_tra_ve": len(da), "goc_cu": ch.LAKE_DUONG_DAN_CU}, ensure_ascii=False))


if __name__ == "__main__":
    main()
