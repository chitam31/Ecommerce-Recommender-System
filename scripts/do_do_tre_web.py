"""do_do_tre_web.py - đo thời gian phản hồi của web từ phía máy khách (mục A.8 đề cương: < 3 giây).

Chỉ gọi các yêu cầu ĐỌC (GET, không ghi impression), để không làm bẩn số liệu thí nghiệm A/B:
trang chủ không chọn user, gợi ý qua API với ghi_hien_thi=false, trang sản phẩm không kèm user.
Mỗi URL gọi N lần liên tiếp; lần đầu có thể chậm hơn vì bộ đệm của web hết hạn (đúng thực tế).
Cuối cùng in thêm số đo phía máy chủ từ GET /api/hieu-nang.

    python3 scripts/do_do_tre_web.py --web http://10.68.71.149:8000 --so-lan 20 > do_tre_web.md
"""
import argparse
import json
import math
import time
import urllib.request

NGUONG_MS = 3000.0


def phan_vi(ds, q):
    s = sorted(ds)
    return s[min(len(s), max(1, math.ceil(q * len(s)))) - 1] if s else None


def goi(url, cho=30):
    bat_dau = time.perf_counter()
    with urllib.request.urlopen(url, timeout=cho) as ph:
        noi_dung = ph.read()
        ma = ph.status
    return (time.perf_counter() - bat_dau) * 1000, ma, noi_dung


def danh_sach_url(web):
    """URL cần đo, lấy user và sản phẩm thật từ chính web."""
    _, _, nd = goi(web + "/api/nguoi-dung-mau?n=12")
    nguoi = [u if isinstance(u, str) else u.get("ma_nguoi_dung") for u in json.loads(nd)][:6] or ["moi_do_hieu_nang"]
    _, _, nd = goi(web + "/api/goi-y/" + nguoi[0])
    san_pham = [g["ma_san_pham"] for g in json.loads(nd).get("goi_y", [])][:4] or ["1"]
    return [
        ("Trang chủ (không chọn user)", ["/"]),
        ("GET /api/goi-y/{user_id}", ["/api/goi-y/" + u for u in nguoi]),
        ("GET /api/goi-y/{user_id} (user mới, cold start)", ["/api/goi-y/moi_do_{}".format(i) for i in range(4)]),
        ("GET /api/thinh-hanh", ["/api/thinh-hanh"]),
        ("Trang sản phẩm /san-pham/{id}", ["/san-pham/" + s for s in san_pham]),
        ("GET /api/thinh-hanh/{id} (không đệm)", ["/api/thinh-hanh/" + s for s in san_pham]),
        ("Bảng điều khiển A/B /ab", ["/ab"]),
        ("GET /api/ab/ket-qua", ["/api/ab/ket-qua"]),
        ("Chất lượng mô hình /mo-hinh", ["/mo-hinh"]),
        ("GET /api/mo-hinh/chi-so", ["/api/mo-hinh/chi-so"]),
        ("Tái lập thí nghiệm /tai-lap", ["/tai-lap"]),
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--web", default="http://10.68.71.149:8000")
    ap.add_argument("--so-lan", type=int, default=20)
    args = ap.parse_args()
    web = args.web.rstrip("/")

    print("# Thời gian phản hồi của web (đo từ máy khách)\n")
    print("Thời điểm đo: {} UTC. Web: {}. Mỗi nhóm {} yêu cầu GET liên tiếp, xoay vòng các URL trong nhóm.\n".format(
        time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), web, args.so_lan))
    print("| Nhóm yêu cầu | Số lần | Lần đầu (ms) | Trung vị (ms) | p95 (ms) | Lớn nhất (ms) | Dưới 3 s |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    tat_ca = []
    for ten, urls in danh_sach_url(web):
        do, loi = [], 0
        for i in range(args.so_lan):
            try:
                ms, ma, _ = goi(web + urls[i % len(urls)])
                do.append(ms)
                loi += ma >= 400
            except Exception:
                loi += 1
        tat_ca.extend(do)
        if not do:
            print("| {} | 0 | - | - | - | - | lỗi |".format(ten))
            continue
        duoi = sum(1 for x in do if x < NGUONG_MS)
        print("| {} | {} | {:.0f} | {:.0f} | {:.0f} | {:.0f} | {}/{}{} |".format(
            ten, len(do), do[0], phan_vi(do, 0.5), phan_vi(do, 0.95), max(do), duoi, len(do),
            " ({} lỗi)".format(loi) if loi else ""))
    if tat_ca:
        print("\nTổng cộng {} yêu cầu: p95 = {:.0f} ms, lớn nhất = {:.0f} ms, {:.1f}% dưới 3 giây.".format(
            len(tat_ca), phan_vi(tat_ca, 0.95), max(tat_ca), 100.0 * sum(1 for x in tat_ca if x < NGUONG_MS) / len(tat_ca)))
    try:
        _, _, nd = goi(web + "/api/hieu-nang")
        hn = json.loads(nd)
        print("\n## Số đo phía máy chủ (GET /api/hieu-nang, gồm cả lưu lượng thật từ lúc web khởi động)\n")
        print("| API | Số yêu cầu | Trung vị (ms) | p95 (ms) | Lớn nhất (ms) | Tỷ lệ dưới 3 s |")
        print("|---|---:|---:|---:|---:|---:|")
        for k, v in hn["theo_route"].items():
            print("| `{}` | {} | {} | {} | {} | {:.1%} |".format(k, v["so_yeu_cau"], v["trung_vi_ms"], v["p95_ms"],
                                                           v["lon_nhat_ms"], v["ty_le_duoi_nguong"]))
    except Exception as e:
        print("\n(Không đọc được /api/hieu-nang: {})".format(e))


if __name__ == "__main__":
    main()
