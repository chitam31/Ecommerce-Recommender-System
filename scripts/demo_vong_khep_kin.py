"""demo_vong_khep_kin.py - đo 2 mục nghiệm thu bằng chính API của web:

  * Vòng khép kín: bấm "Thêm giỏ" -> Kafka -> bronze -> ảnh hưởng bảng thịnh hành.
  * Streaming thịnh hành cập nhật trong <= 90 giây sau khi bấm nút.

Cách đo (giống trang sản phẩm): gửi N lần "Thêm giỏ", rồi thăm dò mỗi 3 giây cửa sổ trượt
bắt đầu ở phút của lần gửi (GET /api/thinh-hanh/<mã>?bat_dau_ms=...). Cửa sổ này chỉ chứa sự
kiện từ phút đó trở đi, nên khi nó đếm đủ N lượt là bảng đã ghi nhận các lần bấm, bất kể sản
phẩm đã có bao nhiêu lượt thêm giỏ trước đó. In số giây tính từ lần gửi đầu tiên.

    python3 scripts/demo_vong_khep_kin.py --web http://10.68.71.149:8000 --san-pham 461686
Nên chọn một sản phẩm ÍT người xem (không nằm trong khối thịnh hành) để con số chỉ đổi vì lần bấm này.
Sau đó xác nhận sự kiện đã nằm trong bronze:
    bash ha_tang/spark/submit.sh nhanh kiem_tra/tim_su_kien_bronze.py --ma-nguoi-dung demo_vong_kin
"""
import argparse
import json
import time
import urllib.request


def goi(phuong_thuc, url, du_lieu=None):
    yc = urllib.request.Request(
        url, method=phuong_thuc, headers={"Content-Type": "application/json"},
        data=json.dumps(du_lieu).encode("utf-8") if du_lieu is not None else None,
    )
    with urllib.request.urlopen(yc, timeout=30) as ph:
        return json.loads(ph.read().decode("utf-8"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--web", default="http://10.68.71.149:8000")
    ap.add_argument("--san-pham", required=True)
    ap.add_argument("--nguoi-dung", default="demo_vong_kin")
    ap.add_argument("--so-lan", type=int, default=3)
    ap.add_argument("--toi-da-giay", type=int, default=180)
    args = ap.parse_args()

    phut = 60000
    dong_truoc = goi("GET", "{}/api/thinh-hanh/{}".format(args.web, args.san_pham))
    print("Trước khi bấm: cửa sổ đang chạy {}, thêm giỏ {}".format(
        dong_truoc.get("cua_so_ket_thuc") or "(chưa có trong 15 phút gần nhất)", dong_truoc.get("so_luot_them_gio") or 0))
    bat_dau = time.time()
    luc = []
    for _ in range(args.so_lan):
        luc.append(goi("POST", args.web + "/api/su-kien",
                       {"ma_nguoi_dung": args.nguoi_dung, "ma_san_pham": args.san_pham, "loai": "them_gio"})["luc_ms"])
    print("Đã bấm 'Thêm giỏ' {} lần lúc {}".format(args.so_lan, time.strftime("%H:%M:%S")))
    # Lùi 10 giây: Kafka đóng dấu giờ lúc gửi, sớm hơn luc_ms một chút. Chỉ đòi các lần bấm chắc
    # chắn nằm trong cửa sổ (luôn gồm lần cuối).
    moc = (luc[-1] - 10000) // phut * phut
    can_co = sum(t >= moc + 10000 for t in luc)

    while time.time() - bat_dau < args.toi_da_giay:
        time.sleep(3)
        dong = goi("GET", "{}/api/thinh-hanh/{}?bat_dau_ms={}".format(args.web, args.san_pham, moc))
        if (dong.get("so_luot_them_gio") or 0) >= can_co:
            giay = time.time() - bat_dau
            hien = goi("GET", "{}/api/thinh-hanh/{}".format(args.web, args.san_pham))
            print("Bảng thịnh hành đã cập nhật sau {:.0f} giây (cửa sổ đang chạy {}, thêm giỏ {}) -> {}".format(
                giay, hien.get("cua_so_ket_thuc"), hien.get("so_luot_them_gio"), "ĐẠT (<= 90s)" if giay <= 90 else "CHƯA ĐẠT"))
            return
    print("Hết {} giây mà bảng chưa cập nhật: kiểm tra scripts/luong.sh trang_thai".format(args.toi_da_giay))


if __name__ == "__main__":
    main()
