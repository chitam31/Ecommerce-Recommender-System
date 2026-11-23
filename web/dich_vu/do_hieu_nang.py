"""Đo thời gian phản hồi của web theo từng API (mục A.8 đề cương: ứng dụng web phản hồi < 3 giây).

Middleware trong app.py ghi thời gian xử lý của mọi yêu cầu (trừ tệp tĩnh) vào bộ đếm này, theo mẫu
đường dẫn của route (/api/goi-y/{user_id}, không phải từng user), và gắn header
X-Thoi-Gian-Xu-Ly-Ms vào phản hồi. GET /api/hieu-nang trả về trung vị, p95, lớn nhất và tỷ lệ yêu
cầu dưới ngưỡng của từng route. Chỉ giữ N lần đo gần nhất mỗi route để bộ nhớ không tăng mãi.
"""
import math
import threading
from collections import deque

NGUONG_MS = 3000.0


def phan_vi(ds, q):
    """Phân vị theo hạng gần nhất (nearest-rank); ds rỗng thì None."""
    if not ds:
        return None
    s = sorted(ds)
    return s[min(len(s), max(1, math.ceil(q * len(s)))) - 1]


class ThongKeThoiGian:
    def __init__(self, giu_toi_da=500, nguong_ms=NGUONG_MS):
        self.giu_toi_da = giu_toi_da
        self.nguong_ms = nguong_ms
        self._do = {}
        self._tong = {}
        self._khoa = threading.Lock()

    def ghi(self, route, phuong_thuc, ms):
        khoa = "{} {}".format(phuong_thuc, route)
        with self._khoa:
            self._do.setdefault(khoa, deque(maxlen=self.giu_toi_da)).append(float(ms))
            self._tong[khoa] = self._tong.get(khoa, 0) + 1

    def tom_tat(self):
        with self._khoa:
            ban_sao = {k: list(v) for k, v in self._do.items()}
            tong = dict(self._tong)
        theo_route = {}
        tat_ca = []
        for khoa, ds in sorted(ban_sao.items()):
            tat_ca.extend(ds)
            theo_route[khoa] = {
                "so_yeu_cau": tong[khoa],
                "so_lan_do_giu_lai": len(ds),
                "trung_vi_ms": round(phan_vi(ds, 0.5), 1),
                "p95_ms": round(phan_vi(ds, 0.95), 1),
                "lon_nhat_ms": round(max(ds), 1),
                "ty_le_duoi_nguong": round(sum(1 for x in ds if x < self.nguong_ms) / len(ds), 4),
            }
        return {
            "nguong_ms": self.nguong_ms,
            "theo_route": theo_route,
            "tat_ca": {
                "so_lan_do": len(tat_ca),
                "p95_ms": round(phan_vi(tat_ca, 0.95), 1) if tat_ca else None,
                "ty_le_duoi_nguong": round(sum(1 for x in tat_ca if x < self.nguong_ms) / len(tat_ca), 4) if tat_ca else None,
            },
        }
