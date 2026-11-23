"""producer/nguoi_dung_ao.py - người dùng ảo truy cập web thật (traffic "mo_phong_web"). 

Khác với transform/mo_phong_hien_thi.py (sinh impression hàng loạt bằng Spark), script này
đi qua toàn bộ vòng thật: web -> Kafka topic hien-thi -> streaming/ghi_hien_thi.py -> CDF ->
transform/tinh_ket_qua_ab.py. Dùng để chứng minh đường ống chạy đúng dưới tải.

Mô hình click CHỈ phụ thuộc vị trí, P(click) = p0 / vi_tri^eta, không phụ thuộc nhóm. Vì vậy
kết quả A/B kỳ vọng là "không khác biệt": một phép thử A/A giúp kiểm tra kiểm định không
báo khác biệt giả.

    python3 producer/nguoi_dung_ao.py --web http://10.68.71.149:8000 --so-luot 500
"""
import argparse
import json
import random
import time
import urllib.parse
import urllib.request


def goi(phuong_thuc, url, du_lieu=None):
    yc = urllib.request.Request(
        url, method=phuong_thuc,
        data=json.dumps(du_lieu).encode("utf-8") if du_lieu is not None else None,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(yc, timeout=30) as ph:
        return json.loads(ph.read().decode("utf-8"))


def mo_phong_mot_luot(web, ma_nguoi_dung, rnd, p0, eta, p_mua):
    kq = goi("GET", "{}/api/goi-y/{}?ghi_hien_thi=true&nguon_traffic=mo_phong_web".format(
        web, urllib.parse.quote(ma_nguoi_dung)))
    so_click = 0
    for vi_tri, g in enumerate(kq["goi_y"], start=1):
        if rnd.random() < p0 / (vi_tri ** eta):
            so_click += 1
            # Mở trang sản phẩm có mã impression = ghi phản hồi click.
            urllib.request.urlopen("{}/san-pham/{}?u={}&mh={}".format(
                web, g["ma_san_pham"], urllib.parse.quote(ma_nguoi_dung), g["ma_hien_thi"]), timeout=30).read()
            if rnd.random() < p_mua:
                goi("POST", web + "/api/su-kien", {"ma_nguoi_dung": ma_nguoi_dung, "ma_san_pham": g["ma_san_pham"],
                                                   "loai": "mua", "ma_hien_thi": g["ma_hien_thi"]})
    return kq["nhom_ab"], so_click


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--web", default="http://10.68.71.149:8000")
    ap.add_argument("--so-luot", type=int, default=200)
    ap.add_argument("--p0", type=float, default=0.15)
    ap.add_argument("--eta", type=float, default=0.7)
    ap.add_argument("--p-mua", type=float, default=0.1)
    ap.add_argument("--nghi", type=float, default=0.2, help="Giây nghỉ giữa các lượt")
    args = ap.parse_args()

    rnd = random.Random(42)
    nguoi = [m["ma_nguoi_dung"] for m in goi("GET", args.web + "/api/nguoi-dung-mau?n=40")]
    if not nguoi:
        raise SystemExit("Web chưa có người dùng mẫu (gold.goi_y_nguoi_dung trống?)")

    dem = {"A": [0, 0], "B": [0, 0]}
    for i in range(args.so_luot):
        nhom, so_click = mo_phong_mot_luot(args.web, rnd.choice(nguoi), rnd, args.p0, args.eta, args.p_mua)
        dem[nhom][0] += 10
        dem[nhom][1] += so_click
        if (i + 1) % 50 == 0:
            print("Lượt {}: {}".format(i + 1, dem), flush=True)
        time.sleep(args.nghi)
    print("Xong:", dem)


if __name__ == "__main__":
    main()
