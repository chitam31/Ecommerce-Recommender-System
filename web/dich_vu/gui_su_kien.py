"""Gửi sự kiện từ web vào Kafka (khép kín vòng dữ liệu).

Hai topic:
  retailrocket-events : clickstream, cùng định dạng với bộ phát lại, thêm nguon="web"
  hien-thi            : impression log và phản hồi click/mua của từng impression
"""
import json
import logging
import threading
import time

from common import cau_hinh as ch

nhat_ky = logging.getLogger("web.gui_su_kien")

# Ánh xạ ngược từ tên tiếng Việt về tên sự kiện gốc của RetailRocket.
SU_KIEN_GOC = {"xem": "view", "them_gio": "addtocart", "mua": "transaction"}


def thong_diep_click_stream(ma_nguoi_dung, ma_san_pham, loai, ma_danh_muc=None):
    """Thông điệp clickstream theo đúng lược đồ mà nap_su_kien.py đọc."""
    bay_gio_ms = int(time.time() * 1000)
    return {
        "timestamp": bay_gio_ms,
        "visitorid": str(ma_nguoi_dung),
        "event": SU_KIEN_GOC[loai],
        "itemid": str(ma_san_pham),
        "transactionid": "web-{}".format(bay_gio_ms) if loai == "mua" else None,
        "ma_danh_muc": ma_danh_muc,
        "thiet_bi": "web",
        "nguon": "web",
    }


class GuiKafka:
    """Gửi thật vào Kafka bằng kafka-python; mở kết nối khi dùng lần đầu."""

    def __init__(self, broker=None):
        self.broker = broker or ch.KAFKA_BROKER
        self._producer = None
        self._khoa = threading.Lock()

    def _lay_producer(self):
        with self._khoa:
            if self._producer is None:
                from kafka import KafkaProducer

                self._producer = KafkaProducer(
                    bootstrap_servers=self.broker,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    key_serializer=lambda k: str(k).encode("utf-8"),
                    linger_ms=20,
                    acks=1,
                )
            return self._producer

    def gui(self, topic, khoa, gia_tri):
        self._lay_producer().send(topic, key=khoa, value=gia_tri)

    def day_ngay(self):
        """Đẩy hết bộ đệm: dùng sau thao tác của người dùng để thịnh hành cập nhật nhanh."""
        if self._producer is not None:
            self._producer.flush(timeout=5)


class GuiBoNho:
    """Lưu thông điệp trong bộ nhớ: dùng cho test và chế độ chạy không có cụm."""

    def __init__(self):
        self.da_gui = []

    def gui(self, topic, khoa, gia_tri):
        self.da_gui.append((topic, khoa, gia_tri))
        nhat_ky.debug("[bo_nho] %s %s %s", topic, khoa, gia_tri)

    def day_ngay(self):
        pass
