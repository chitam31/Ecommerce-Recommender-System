"""Bộ nhớ phiên của web: tương tác vừa xảy ra của từng user.

Sự kiện mới từ web đi vào bronze ngay, nhưng phải chờ job batch ban đêm mới vào silver
và gold. Để demo chiến lược "user < 5 tương tác" với một user vừa tạo tại chỗ, web nhớ
tạm các tương tác gần đây trong RAM. Mất khi khởi động lại, đúng tính chất của phiên.
"""
import threading
import time
from collections import OrderedDict, defaultdict, deque


class BoNhoPhien:
    def __init__(self, toi_da_nguoi=10000, toi_da_su_kien=50):
        self._du_lieu = OrderedDict()
        self._toi_da_nguoi = toi_da_nguoi
        self._toi_da_su_kien = toi_da_su_kien
        self._khoa = threading.Lock()

    def ghi(self, ma_nguoi_dung, ma_san_pham, loai, ma_danh_muc):
        with self._khoa:
            ds = self._du_lieu.pop(ma_nguoi_dung, None) or deque(maxlen=self._toi_da_su_kien)
            ds.append({"ma_san_pham": ma_san_pham, "loai": loai, "ma_danh_muc": ma_danh_muc, "luc": time.time()})
            self._du_lieu[ma_nguoi_dung] = ds
            while len(self._du_lieu) > self._toi_da_nguoi:
                self._du_lieu.popitem(last=False)

    def so_tuong_tac(self, ma_nguoi_dung):
        with self._khoa:
            return len(self._du_lieu.get(ma_nguoi_dung, ()))

    def danh_muc_da_xem(self, ma_nguoi_dung, toi_da=3):
        """Danh mục xếp theo tổng trọng số tương tác (xem 1, thêm giỏ 3, mua 5)."""
        trong_so = {"xem": 1.0, "them_gio": 3.0, "mua": 5.0}
        diem = defaultdict(float)
        with self._khoa:
            for su_kien in self._du_lieu.get(ma_nguoi_dung, ()):
                if su_kien["ma_danh_muc"]:
                    diem[su_kien["ma_danh_muc"]] += trong_so.get(su_kien["loai"], 0.0)
        return [dm for dm, _ in sorted(diem.items(), key=lambda x: (-x[1], x[0]))[:toi_da]]
