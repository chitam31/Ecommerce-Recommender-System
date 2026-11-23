"""tao_du_lieu_mau.py - sinh dữ liệu NHỎ đúng định dạng RetailRocket để chạy thử và kiểm thử.

Đây KHÔNG phải dữ liệu của đề tài. Nó chỉ có cùng cấu trúc tệp (events.csv,
item_properties_part1/2.csv, category_tree.csv) và vài đặc điểm cố ý đưa vào để kiểm tra
từng bước của pipeline:
  * người dùng có sở thích theo danh mục -> ALS có tín hiệu để học
  * độ phổ biến item theo luật Zipf -> có popularity bias để đo coverage/novelty
  * một "bot" > 500 sự kiện trong một giờ -> kiểm tra lọc bot
  * sự kiện lặp trong vòng 5 giây -> kiểm tra khử trùng lặp
  * vài item chỉ xuất hiện ở 20 ngày cuối -> kiểm tra chiến lược "item mới"

    python3 scripts/tao_du_lieu_mau.py --thu-muc /tmp/retailrocket_mau
"""
import argparse
import csv
import os
import random

MS_NGAY = 86400000
BAT_DAU_MS = 1430611200000  # 2015-05-03 00:00 UTC, giống mốc đầu của RetailRocket


def sinh(thu_muc, so_nguoi=3000, so_item=800, so_danh_muc=40, so_ngay=135, hat_giong=42):
    rnd = random.Random(hat_giong)
    os.makedirs(thu_muc, exist_ok=True)
    danh_muc_cua_item = {i: i % so_danh_muc for i in range(so_item)}
    # Item mới: 30 item cuối chỉ xuất hiện trong 20 ngày cuối.
    item_moi = set(range(so_item - 30, so_item))
    item_theo_dm = {}
    for i, dm in danh_muc_cua_item.items():
        item_theo_dm.setdefault(dm, []).append(i)

    def chon_item(dm, ngay):
        ds = [i for i in item_theo_dm[dm] if i not in item_moi or ngay >= so_ngay - 20]
        trong_so = [1.0 / (h + 1) ** 1.1 for h in range(len(ds))]  # Zipf trong danh mục
        return rnd.choices(ds, weights=trong_so)[0]

    su_kien = []
    for u in range(so_nguoi):
        so_kien = min(int(rnd.paretovariate(1.3)) * 2, 120)
        so_thich = rnd.sample(range(so_danh_muc), 2)
        ngay_dau = rnd.randrange(so_ngay)
        da_xem = []
        for _ in range(so_kien):
            ngay = min(so_ngay - 1, ngay_dau + int(rnd.expovariate(1 / 15.0)))
            ts = BAT_DAU_MS + ngay * MS_NGAY + rnd.randrange(MS_NGAY)
            if da_xem and rnd.random() < 0.3:
                item = rnd.choice(da_xem)
            else:
                dm = rnd.choice(so_thich) if rnd.random() < 0.85 else rnd.randrange(so_danh_muc)
                item = chon_item(dm, ngay)
            da_xem.append(item)
            su_kien.append((ts, u, "view", item, ""))
            if rnd.random() < 0.08:
                su_kien.append((ts + 20000, u, "addtocart", item, ""))
                if rnd.random() < 0.3:
                    su_kien.append((ts + 60000, u, "transaction", item, str(rnd.randrange(10 ** 6))))
            if rnd.random() < 0.02:
                su_kien.append((ts + 2000, u, "view", item, ""))  # lặp trong 5 giây
    # Bot: 600 lượt xem trong cùng một giờ.
    ts_bot = BAT_DAU_MS + 50 * MS_NGAY
    for k in range(600):
        su_kien.append((ts_bot + k * 5000, 999999, "view", rnd.randrange(so_item - 30), ""))
    su_kien.sort()

    with open(os.path.join(thu_muc, "events.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "visitorid", "event", "itemid", "transactionid"])
        w.writerows(su_kien)

    thuoc_tinh = []
    for i, dm in danh_muc_cua_item.items():
        ts = BAT_DAU_MS + rnd.randrange(10) * MS_NGAY
        thuoc_tinh.append((ts, i, "categoryid", str(dm)))
        thuoc_tinh.append((ts, i, "790", "n{}.000".format(rnd.randint(5, 900) * 1000)))
        thuoc_tinh.append((ts, i, "available", "1" if rnd.random() < 0.9 else "0"))
        thuoc_tinh.append((ts, i, "888", "{} {}".format(rnd.randrange(10 ** 5), rnd.randrange(10 ** 5))))
    nua = len(thuoc_tinh) // 2
    for ten, phan in (("item_properties_part1.csv", thuoc_tinh[:nua]), ("item_properties_part2.csv", thuoc_tinh[nua:])):
        with open(os.path.join(thu_muc, ten), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["timestamp", "itemid", "property", "value"])
            w.writerows(phan)

    with open(os.path.join(thu_muc, "category_tree.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["categoryid", "parentid"])
        for dm in range(so_danh_muc):
            w.writerow([dm, 1000 + dm % 8])
    return len(su_kien)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--thu-muc", default="/tmp/retailrocket_mau")
    ap.add_argument("--so-nguoi", type=int, default=3000)
    ap.add_argument("--so-item", type=int, default=800)
    args = ap.parse_args()
    n = sinh(args.thu_muc, args.so_nguoi, args.so_item)
    print("Đã sinh {} sự kiện vào {}".format(n, args.thu_muc))


if __name__ == "__main__":
    main()
