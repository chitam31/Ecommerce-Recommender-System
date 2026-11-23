"""Đọc bảng Delta cho web bằng một phiên Spark cục bộ chạy ngay trong tiến trình web.

Vì sao dùng Spark: Delta Lake là thư viện chạy bên trong Spark (giáo trình CS411, mục 6.1),
và truy vấn "lấy gợi ý theo user" của web chính là truy vấn được đo OPTIMIZE/ZORDER.
Spark chạy local[2] trong container web, đọc HDFS của cụm qua ZeroTier.

Mỗi truy vấn Spark mất cỡ vài trăm mili-giây, nên kết quả được giữ đệm ngắn hạn:
thịnh hành 10 giây, gợi ý theo user 30 giây, danh mục sản phẩm 1 giờ.
"""
import json
import logging
import threading
import time
from collections import OrderedDict
from datetime import datetime

from common.ab import tong_hop_ab
from common.tron import NGUON_DANH_MUC, NGUON_THINH_HANH

nhat_ky = logging.getLogger("web.kho")


class BoDem:
    """Bộ đệm có hạn sống (TTL) và giới hạn số khóa (LRU)."""

    def __init__(self, giay, toi_da=2000):
        self.giay = giay
        self.toi_da = toi_da
        self._du_lieu = OrderedDict()
        self._khoa = threading.Lock()

    def lay(self, khoa, ham_tinh):
        bay_gio = time.time()
        with self._khoa:
            if khoa in self._du_lieu and bay_gio - self._du_lieu[khoa][0] < self.giay:
                self._du_lieu.move_to_end(khoa)
                return self._du_lieu[khoa][1]
        gia_tri = ham_tinh()
        with self._khoa:
            self._du_lieu[khoa] = (bay_gio, gia_tri)
            while len(self._du_lieu) > self.toi_da:
                self._du_lieu.popitem(last=False)
        return gia_tri

    def xoa(self):
        with self._khoa:
            self._du_lieu.clear()


def _sang_json(gia_tri):
    """Đổi Row/datetime sang kiểu JSON được."""
    if isinstance(gia_tri, datetime):
        return gia_tri.isoformat(sep=" ", timespec="seconds")
    if hasattr(gia_tri, "isoformat"):
        return gia_tri.isoformat()
    return gia_tri


def goc_cua(ma, cha, toi_da=20):
    """Danh mục cấp 2 (con trực tiếp của gốc) chứa `ma`; `ma` là gốc thì trả chính nó.

    Lấy cấp 2 thay vì gốc vì RetailRocket chỉ có khoảng 25 gốc và phần lớn sản phẩm nằm dưới
    vài gốc: gán theo gốc thì gần như mọi thẻ cùng một ngành hàng minh họa. Chặn vòng lặp nếu
    dữ liệu cha-con lỗi.
    """
    duong = [ma]
    while cha.get(duong[-1]) not in (None, "") and len(duong) <= toi_da:
        tiep = cha[duong[-1]]
        if tiep in duong:
            break
        duong.append(tiep)
    return duong[-2] if len(duong) >= 2 else duong[-1]


def _dong(r):
    return {k: _sang_json(v) for k, v in r.asDict().items()}


class KhoDelta:
    """Truy cập dữ liệu thật trên HDFS qua Spark + Delta."""

    def __init__(self):
        self._spark = None
        self._khoa = threading.Lock()
        self.dem_thinh_hanh = BoDem(10, 4)
        self.dem_goi_y = BoDem(30, 5000)
        self.dem_lien_quan = BoDem(600, 5000)
        self.dem_bang_nho = BoDem(60, 50)
        # Bảng chỉ đổi khi DAG hằng đêm chạy (lịch sử huấn luyện, tinh chỉnh, số đo Z-ORDER): 10 phút.
        # Lần đọc đầu mất ~8 giây (nạp nhật ký Delta của 3 bảng), nên 60 giây là quá ngắn cho /mo-hinh.
        self.dem_bao_cao = BoDem(600, 50)
        self.dem_lon = BoDem(3600, 4)

    # ---------------------------------------------------------------- Spark
    @property
    def spark(self):
        with self._khoa:
            if self._spark is None:
                from common.spark_utils import tao_spark

                self._spark = tao_spark(
                    "bd03-web",
                    {
                        "spark.master": "local[2]",
                        "spark.driver.memory": "1g",
                        "spark.ui.enabled": "false",
                        "spark.sql.shuffle.partitions": "4",
                        "spark.hadoop.dfs.client.use.datanode.hostname": "true",
                    },
                )
            return self._spark

    def _doc(self, ten_bang, phien_ban=None):
        from common.spark_utils import doc_delta

        return doc_delta(self.spark, ten_bang, phien_ban)

    def _co_bang(self, ten_bang):
        from common.spark_utils import bang_ton_tai

        return self.dem_bang_nho.lay(("co", ten_bang), lambda: bang_ton_tai(self.spark, ten_bang))

    # ---------------------------------------------------------------- gợi ý
    def goi_y_cua(self, ma_nguoi_dung):
        """Truy vấn nóng: đúng truy vấn mà bao_tri/do_zorder.py đo trước/sau ZORDER."""
        from pyspark.sql import functions as F

        def tinh():
            if not self._co_bang("gold.goi_y_nguoi_dung"):
                return []
            dong = (
                self._doc("gold.goi_y_nguoi_dung")
                .filter(F.col("ma_nguoi_dung") == str(ma_nguoi_dung))
                .orderBy("xep_hang")
                .collect()
            )
            return [_dong(r) for r in dong]

        return self.dem_goi_y.lay(str(ma_nguoi_dung), tinh)

    def thinh_hanh(self, n=10):
        from pyspark.sql import functions as F

        def tinh():
            if not self._co_bang("gold.thinh_hanh_15p"):
                return []
            th = self._doc("gold.thinh_hanh_15p")
            moi_nhat = th.agg(F.max("cua_so_ket_thuc")).first()[0]
            if moi_nhat is None:
                return []
            dong = (
                th.filter(F.col("cua_so_ket_thuc") == F.lit(moi_nhat))
                .orderBy(F.desc("diem_thinh_hanh"), "ma_san_pham")
                .limit(50)
                .collect()
            )
            return [
                dict(_dong(r), diem_du_doan=float(r["diem_thinh_hanh"]), nguon=NGUON_THINH_HANH) for r in dong
            ]

        return self.dem_thinh_hanh.lay("top", tinh)[:n]

    def thinh_hanh_san_pham(self, ma_san_pham, bat_dau_ms=None):
        """Một dòng thịnh hành của sản phẩm cho trang sản phẩm. Không đệm.

        Không truyền bat_dau_ms: cửa sổ 15 phút đang chạy, tức cửa sổ kết thúc sớm nhất sau
        thời điểm hiện tại, chứa mọi sự kiện của 15 phút gần nhất (None nếu không có).
        Cửa sổ mới nhất theo cua_so_ket_thuc thì không dùng được: cửa sổ trượt nên cửa sổ đó
        bắt đầu ở phút của sự kiện cuối, chỉ chứa sự kiện từ phút ấy, và số đếm tụt xuống.

        bat_dau_ms (mốc phút, epoch ms): đúng cửa sổ bắt đầu ở phút đó. Trang sản phẩm dùng
        cửa sổ bắt đầu ở phút của lần bấm để đo độ trễ: cửa sổ này chỉ chứa sự kiện từ phút
        đó trở đi, nên lượt thêm giỏ cũ hơn không làm sai phép đo.
        """
        from pyspark.sql import functions as F

        if not self._co_bang("gold.thinh_hanh_15p"):
            return None
        th = self._doc("gold.thinh_hanh_15p").filter(F.col("ma_san_pham") == str(ma_san_pham))
        if bat_dau_ms is not None:
            moc = F.timestamp_seconds(F.lit(int(bat_dau_ms) // 60000 * 60))
            th = th.filter(F.col("cua_so_bat_dau") == moc)
        else:
            th = th.filter(F.col("cua_so_ket_thuc") > F.timestamp_seconds(F.lit(int(time.time()))))
        r = (
            th.orderBy("cua_so_ket_thuc")
            # Epoch ms để trình duyệt tự đổi sang giờ địa phương (chuỗi giờ của Spark ở đây là UTC).
            .withColumn("cua_so_bat_dau_ms", F.col("cua_so_bat_dau").cast("long") * 1000)
            .withColumn("cua_so_ket_thuc_ms", F.col("cua_so_ket_thuc").cast("long") * 1000)
            .first()
        )
        return _dong(r) if r else None

    # ---------------------------------------------------------------- sản phẩm
    def _danh_muc_san_pham(self):
        def tinh():
            if not self._co_bang("silver.san_pham"):
                return {}
            cot = self._doc("silver.san_pham").columns
            chon = [c for c in ("ma_san_pham", "ma_danh_muc", "gia_san_pham", "con_hang", "xep_hang_sao") if c in cot]
            return {r["ma_san_pham"]: _dong(r) for r in self._doc("silver.san_pham").select(*chon).collect()}

        return self.dem_lon.lay("san_pham", tinh)

    def san_pham(self, ma_san_pham):
        return self._danh_muc_san_pham().get(str(ma_san_pham), {"ma_san_pham": str(ma_san_pham)})

    def _cay_danh_muc(self):
        """ma_danh_muc -> danh mục cấp 2, từ category_tree.csv của RetailRocket (chỉ dùng để hiển thị)."""
        from common import cau_hinh as ch

        def tinh():
            try:
                df = self.spark.read.option("header", True).csv(ch.duong_dan_landing("category_tree.csv"))
                cha = {r["categoryid"]: r["parentid"] for r in df.collect()}
            except Exception:  # thiếu tệp gốc: lớp minh họa dùng chính mã danh mục
                logging.getLogger(__name__).warning("Không đọc được category_tree.csv", exc_info=True)
                return {}
            return {c: goc_cua(c, cha) for c in cha}

        return self.dem_lon.lay("cay_danh_muc", tinh)

    def danh_muc_goc(self, ma_danh_muc):
        if ma_danh_muc in (None, ""):
            return None
        return self._cay_danh_muc().get(str(ma_danh_muc), str(ma_danh_muc))

    def _ban_chay_tat_ca(self):
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        def tinh():
            if not self._co_bang("gold.thong_ke_san_pham"):
                return {}
            dong = (
                self._doc("gold.thong_ke_san_pham")
                .filter(F.col("ma_danh_muc").isNotNull())
                .withColumn(
                    "h",
                    F.row_number().over(Window.partitionBy("ma_danh_muc").orderBy(F.desc("diem_pho_bien"), "ma_san_pham")),
                )
                .filter(F.col("h") <= 20)
                .select("ma_danh_muc", "h", "ma_san_pham", "diem_pho_bien")
                .collect()
            )
            theo_dm = {}
            for r in sorted(dong, key=lambda x: (x["ma_danh_muc"], x["h"])):
                theo_dm.setdefault(r["ma_danh_muc"], []).append(
                    {"ma_san_pham": r["ma_san_pham"], "diem_du_doan": float(r["diem_pho_bien"] or 0), "nguon": NGUON_DANH_MUC}
                )
            return theo_dm

        return self.dem_lon.lay("ban_chay", tinh)

    def ban_chay_danh_muc(self, ma_danh_muc, n=20):
        return self._ban_chay_tat_ca().get(str(ma_danh_muc), [])[:n]

    def lien_quan(self, ma_san_pham, n=10):
        from pyspark.sql import functions as F

        def tinh():
            if not self._co_bang("gold.san_pham_lien_quan"):
                return []
            dong = (
                self._doc("gold.san_pham_lien_quan")
                .filter(F.col("ma_san_pham") == str(ma_san_pham))
                .orderBy("xep_hang")
                .collect()
            )
            return [
                {"ma_san_pham": r["ma_san_pham_lien_quan"], "diem_du_doan": float(r["do_lien_quan"]), "nguon": "dong_xuat_hien"}
                for r in dong
            ]

        ds = self.dem_lien_quan.lay(str(ma_san_pham), tinh)
        if not ds:
            # Thiếu dữ liệu đồng xuất hiện: lấy bán chạy cùng danh mục.
            dm = self.san_pham(ma_san_pham).get("ma_danh_muc")
            ds = [g for g in self.ban_chay_danh_muc(dm) if g["ma_san_pham"] != str(ma_san_pham)] if dm else []
        return ds[:n]

    def nguoi_dung_mau(self, n=12):
        """Danh sách user cho dropdown: một nửa có ALS, một nửa ít tương tác."""
        from pyspark.sql import functions as F

        def tinh():
            if not self._co_bang("gold.goi_y_nguoi_dung"):
                return []
            df = self._doc("gold.goi_y_nguoi_dung").filter(F.col("xep_hang") == 1)
            ket_qua = []
            for nguon, ten in (("als", "als"), (NGUON_DANH_MUC, "it_tuong_tac")):
                dong = (
                    df.filter(F.col("nguon") == nguon)
                    .select("ma_nguoi_dung")
                    .orderBy(F.md5("ma_nguoi_dung"))
                    .limit(n // 2)
                    .collect()
                )
                ket_qua += [{"ma_nguoi_dung": r["ma_nguoi_dung"], "chien_luoc": ten} for r in dong]
            return ket_qua

        # Quét cả bảng gợi ý (~8 giây trên Spark local): giữ 1 giờ, danh sách mẫu không cần đổi từng phút.
        return self.dem_lon.lay(("mau", n), tinh)

    # ---------------------------------------------------------------- báo cáo
    def _bang_nho(self, ten_bang, sap_xep=None, loc=None, bo_dem=None):
        def tinh():
            if not self._co_bang(ten_bang):
                return []
            df = self._doc(ten_bang)
            if loc is not None:
                df = df.filter(loc)
            if sap_xep:
                df = df.orderBy(*sap_xep)
            return [_dong(r) for r in df.collect()]

        return (bo_dem or self.dem_bang_nho).lay((ten_bang, str(sap_xep), str(loc)), tinh)

    def ket_qua_ab(self):
        return self._bang_nho("gold.ket_qua_ab", ["ngay", "nhom_ab"])

    def lich_su_mo_hinh(self):
        # _bang_nho trả về CHÍNH danh sách trong bộ đệm (60 giây): phải chép từng dòng trước khi sửa.
        # Bản cũ pop("chi_tiet_json") thẳng trên bộ đệm, nên mọi lần gọi sau trong 60 giây
        # (tải lại /mo-hinh, /api/mo-hinh/chi-so, /tai-lap) lỗi KeyError -> HTTP 500.
        ds = []
        for d in self._bang_nho("gold.lich_su_huan_luyen", ["thoi_diem"], bo_dem=self.dem_bao_cao):
            d = dict(d)
            d["chi_tiet"] = json.loads(d.pop("chi_tiet_json", None) or "{}")
            ds.append(d)
        return ds

    def tuning(self, ma_chay):
        from pyspark.sql import functions as F

        return self._bang_nho("gold.ket_qua_tuning", ["rank", "reg_param", "alpha"], F.col("ma_chay") == ma_chay,
                              bo_dem=self.dem_bao_cao)

    def ket_qua_tai_lap(self):
        return self._bang_nho("gold.ket_qua_tai_lap", ["thoi_diem"])

    def do_zorder(self):
        return self._bang_nho("gold.do_hieu_nang_zorder", ["thoi_diem"], bo_dem=self.dem_bao_cao)

    def tai_lap_ab(self, ngay):
        """Chạy lại A/B của một ngày trên đúng phiên bản Delta đã ghi, so với kết quả cũ."""
        from pyspark.sql import functions as F

        cu = [
            _dong(r)
            for r in self._doc("gold.ket_qua_ab").filter(F.col("ngay") == F.lit(ngay)).orderBy("nhom_ab").collect()
        ]
        if not cu:
            return None
        phien_ban = int(cu[0]["phien_ban_delta_du_lieu"])
        bat_dau = time.time()
        moi = tong_hop_ab(self._doc("gold.nhat_ky_hien_thi", phien_ban), [datetime.strptime(ngay, "%Y-%m-%d").date()])
        moi = [{k: _sang_json(v) for k, v in d.items()} for d in moi]
        hien_tai = [
            {k: _sang_json(v) for k, v in d.items()}
            for d in tong_hop_ab(self._doc("gold.nhat_ky_hien_thi"), [datetime.strptime(ngay, "%Y-%m-%d").date()])
        ]
        return {
            "ngay": ngay,
            "phien_ban_delta_du_lieu": phien_ban,
            "cu": cu,
            "moi": moi,
            "phien_ban_hien_tai": hien_tai,
            "so_giay": round(time.time() - bat_dau, 2),
        }

    def ghi_ket_qua_tai_lap(self, dong):
        """Ghi ngược một dòng vào Delta gold.ket_qua_tai_lap (append), trả về phiên bản mới.

        Chỉ ghi khi bảng đã tồn tại ở gốc hồ dữ liệu hiện tại: nếu web chạy mã mới trong lúc dữ liệu
        còn ở gốc cũ (chưa chạy chuyen_lakehouse) thì không được tạo bảng lẻ ở gốc mới.
        Append không xung đột với commit của DAG kn_tai_lap (Delta cho phép hai blind append song song).
        """
        from common.luoc_do import tao_df
        from common.spark_utils import bang_ton_tai, ghi_delta, phien_ban_moi_nhat

        if not bang_ton_tai(self.spark, "gold.ket_qua_tai_lap"):
            from common import cau_hinh as ch

            raise RuntimeError("Chưa có bảng gold.ket_qua_tai_lap ở " + ch.LAKE_GOC)
        ghi_delta(tao_df(self.spark, "gold.ket_qua_tai_lap", [dong]), "gold.ket_qua_tai_lap", "append")
        self.dem_bang_nho.xoa()
        return phien_ban_moi_nhat(self.spark, "gold.ket_qua_tai_lap")

    def xoa_bo_dem(self):
        for bd in (self.dem_thinh_hanh, self.dem_goi_y, self.dem_lien_quan, self.dem_bang_nho, self.dem_bao_cao,
                   self.dem_lon):
            bd.xoa()

    def lam_nong(self, in_ra=logging.getLogger("web").info):
        """Chạy trước các truy vấn chậm nhất khi web vừa khởi động (luồng nền), để người đầu tiên mở
        trang chủ, /mo-hinh, /tai-lap, /ab hay trang sản phẩm không phải chờ 8-20 giây
        (đo trên cụm 16/11: danh sách người dùng mẫu 19,4 s, /mo-hinh 8,5 s lần đầu).
        Lỗi ở bước nào chỉ ghi log rồi làm bước sau: làm nóng không bao giờ làm hỏng web."""
        cac_buoc = [
            ("nguoi_dung_mau", self.nguoi_dung_mau),
            ("lich_su_mo_hinh", self.lich_su_mo_hinh),
            ("tuning", lambda: self.tuning((self.lich_su_mo_hinh() or [{}])[-1].get("ma_chay"))),
            ("do_zorder", self.do_zorder),
            ("ket_qua_ab", self.ket_qua_ab),
            ("ket_qua_tai_lap", self.ket_qua_tai_lap),
            ("thinh_hanh", self.thinh_hanh),
            ("danh_muc_san_pham", self._danh_muc_san_pham),
            ("cay_danh_muc", self._cay_danh_muc),
        ]
        xong = []
        for ten, ham in cac_buoc:
            bat_dau = time.time()
            try:
                ham()
                xong.append(ten)
                in_ra("Làm nóng %s: %.1f s", ten, time.time() - bat_dau)
            except Exception:  # noqa: BLE001 - làm nóng chỉ là tối ưu
                logging.getLogger("web").exception("Làm nóng %s thất bại", ten)
        return xong


def lay_kho(che_do):
    """che_do = 'delta' (mặc định, đọc cụm thật) hoặc 'gia' (dữ liệu giả lập, không cần cụm)."""
    if che_do == "gia":
        from .kho_gia import KhoGia

        return KhoGia()
    return KhoDelta()


__all__ = ["KhoDelta", "BoDem", "lay_kho"]
