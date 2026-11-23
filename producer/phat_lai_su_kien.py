"""producer/phat_lai_su_kien.py - bộ phát Kafka của đề tài (mục 3.2).

  * Phát lại events.csv của RetailRocket theo timestamp gốc, nén 1 ngày = 30 giây
    (4,5 tháng dữ liệu phát xong trong khoảng 68 phút).
  * Sinh thêm khoảng 5% sự kiện của NGƯỜI DÙNG MỚI HOÀN TOÀN (mã "moi_<n>") để buộc hệ
    thống xử lý cold start. Người dùng mới duyệt chính các sản phẩm đang được xem ở thời
    điểm đó nên phân phối độ phổ biến giống dữ liệu thật.

Định dạng thông điệp giữ nguyên 5 trường của bộ phát cũ (timestamp, visitorid, event,
itemid, transactionid) để job nap_su_kien_bronze.py cũ của nhóm vẫn đọc được, thêm
trường "nguon" để phân biệt phat_lai / nguoi_moi / web.

Chạy trên máy Kafka (labsit):
    python3 producer/phat_lai_su_kien.py --tep ~/du_lieu/retailrocket/events.csv
Thử không cần Kafka (ghi JSON Lines cho chế độ --nguon tep của job streaming):
    python3 producer/phat_lai_su_kien.py --tep events.csv --ra-thu-muc /tmp/su_kien --giay-moi-ngay 0
"""
import argparse
import csv
import json
import os
import random
import sys
import time

MS_MOI_NGAY = 86400000


def doc_su_kien(duong_dan, tu_ms=None, den_ms=None, gioi_han=None):
    """Đọc events.csv và sắp theo timestamp. Trả về list tuple gọn để tiết kiệm RAM."""
    ds = []
    with open(duong_dan, newline="", encoding="utf-8") as f:
        for dong in csv.DictReader(f):
            ts = int(dong["timestamp"])
            if (tu_ms and ts < tu_ms) or (den_ms and ts >= den_ms):
                continue
            ds.append((ts, dong["visitorid"], dong["event"], dong["itemid"], dong.get("transactionid") or None))
    ds.sort(key=lambda x: x[0])
    return ds[:gioi_han] if gioi_han else ds


class SinhNguoiMoi:
    """Sinh sự kiện của người dùng mới: mỗi người duyệt 1-6 lần rồi rời đi."""

    def __init__(self, ty_le, hat_giong=42):
        self.ty_le = ty_le
        self.rnd = random.Random(hat_giong)
        self.dem = 0
        self.dang_hoat_dong = []  # [ma, so_lan_con_lai]

    def co_sinh(self):
        return self.rnd.random() < self.ty_le

    def sinh(self, ts, ma_san_pham):
        if not self.dang_hoat_dong or self.rnd.random() < 0.35:
            self.dem += 1
            self.dang_hoat_dong.append(["moi_{}".format(self.dem), self.rnd.randint(1, 6)])
        nguoi = self.rnd.choice(self.dang_hoat_dong)
        nguoi[1] -= 1
        if nguoi[1] <= 0:
            self.dang_hoat_dong.remove(nguoi)
        x = self.rnd.random()
        su_kien = "view" if x < 0.90 else ("addtocart" if x < 0.98 else "transaction")
        return {
            "timestamp": ts,
            "visitorid": nguoi[0],
            "event": su_kien,
            "itemid": ma_san_pham,
            "transactionid": "moi-{}".format(ts) if su_kien == "transaction" else None,
            "nguon": "nguoi_moi",
        }


def thong_diep(ts, visitor, event, item, txn):
    return {"timestamp": ts, "visitorid": visitor, "event": event, "itemid": item, "transactionid": txn, "nguon": "phat_lai"}


def thoi_diem_phat(ts, ts_dau, luc_bat_dau, giay_moi_ngay):
    """Thời điểm đồng hồ thật phải gửi sự kiện có timestamp ts."""
    return luc_bat_dau + (ts - ts_dau) / float(MS_MOI_NGAY) * giay_moi_ngay


class GhiTep:
    """Ghi JSON Lines, mỗi tệp 5.000 dòng, để job streaming đọc ở chế độ --nguon tep."""

    def __init__(self, thu_muc):
        os.makedirs(thu_muc, exist_ok=True)
        self.thu_muc, self.so_tep, self.dem, self.f = thu_muc, 0, 0, None

    def send(self, topic, key=None, value=None):
        if self.f is None or self.dem >= 5000:
            self.flush()
            self.f = open(os.path.join(self.thu_muc, ".dang_ghi.json"), "w", encoding="utf-8")
            self.dem = 0
        self.f.write(json.dumps(value, ensure_ascii=False) + "\n")
        self.dem += 1

    def flush(self, timeout=None):
        if self.f is not None:
            self.f.close()
            self.so_tep += 1
            # Đổi tên sau khi ghi xong: file source của Spark không đọc tệp đang ghi dở.
            os.replace(os.path.join(self.thu_muc, ".dang_ghi.json"),
                       os.path.join(self.thu_muc, "su_kien_{:05d}.json".format(self.so_tep)))
            self.f = None


def tao_dich(args):
    if args.ra_thu_muc:
        return GhiTep(args.ra_thu_muc)
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=args.broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=50,
        batch_size=64 * 1024,
        acks=1,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tep", required=True, help="Đường dẫn events.csv")
    ap.add_argument("--broker", default=os.environ.get("KAFKA_BROKER", "10.68.71.204:9092"))
    ap.add_argument("--topic", default=os.environ.get("TOPIC_SU_KIEN", "retailrocket-events"))
    ap.add_argument("--giay-moi-ngay", type=float, default=30.0, help="0 = phát nhanh hết mức")
    ap.add_argument("--ty-le-nguoi-moi", type=float, default=0.05)
    ap.add_argument("--tu-ngay-ms", type=int, default=None)
    ap.add_argument("--den-ngay-ms", type=int, default=None)
    ap.add_argument("--gioi-han", type=int, default=None)
    ap.add_argument("--ra-thu-muc", default=None, help="Ghi JSON Lines thay vì gửi Kafka")
    args = ap.parse_args()

    su_kien = doc_su_kien(args.tep, args.tu_ngay_ms, args.den_ngay_ms, args.gioi_han)
    if not su_kien:
        sys.exit("Không có sự kiện nào để phát")
    dich = tao_dich(args)
    nguoi_moi = SinhNguoiMoi(args.ty_le_nguoi_moi)
    ts_dau, luc_bat_dau = su_kien[0][0], time.time()
    tong_ngay = (su_kien[-1][0] - ts_dau) / float(MS_MOI_NGAY)
    print("Phát {} sự kiện, {:.1f} ngày dữ liệu, dự kiến {:.1f} phút".format(
        len(su_kien), tong_ngay, tong_ngay * args.giay_moi_ngay / 60.0), flush=True)

    da_gui = da_sinh = 0
    for ts, visitor, event, item, txn in su_kien:
        if args.giay_moi_ngay > 0:
            cho = thoi_diem_phat(ts, ts_dau, luc_bat_dau, args.giay_moi_ngay) - time.time()
            if cho > 0:
                time.sleep(cho)
        dich.send(args.topic, key=visitor, value=thong_diep(ts, visitor, event, item, txn))
        da_gui += 1
        if nguoi_moi.co_sinh():
            tm = nguoi_moi.sinh(ts, item)
            dich.send(args.topic, key=tm["visitorid"], value=tm)
            da_sinh += 1
        if da_gui % 50000 == 0:
            print("  đã gửi {} (+{} của người dùng mới), ngày dữ liệu {:.1f}".format(
                da_gui, da_sinh, (ts - ts_dau) / float(MS_MOI_NGAY)), flush=True)
    dich.flush()
    print("Xong: {} sự kiện gốc, {} sự kiện người dùng mới ({:.1%}), {} người dùng mới".format(
        da_gui, da_sinh, da_sinh / float(da_gui), nguoi_moi.dem), flush=True)


if __name__ == "__main__":
    main()
