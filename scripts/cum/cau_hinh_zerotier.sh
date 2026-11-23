#!/usr/bin/env bash
# cau_hinh_zerotier.sh - không cho ZeroTier dùng card mạng ảo (Docker, Tailscale) làm đường đi
# giữa các máy, rồi khởi động lại ZeroTier. Chạy trên từng máy, cần sudo:
#
#   bash scripts/cum/ssh_tat_ca.sh --tty "cd ~/bd03_khuyen_nghi && sudo bash scripts/cum/cau_hinh_zerotier.sh"
#
# Vì sao: ZeroTier báo cho máy khác mọi địa chỉ IP của mình để thử nối thẳng, kể cả địa chỉ của
# các mạng Docker (docker0 = 172.17.0.1 trên MỌI máy, các mạng compose 172.18–172.31.x) và của
# Tailscale. Hai máy cùng có mạng Docker trùng dải địa chỉ (ví dụ labsit-may25 và labsit-may26,
# mỗi máy chạy nhiều container của nhóm khác) có thể thử nhầm sang chính mình, nên mãi đi relay
# dù cùng mạng Wi-Fi. Thêm các tiền tố tên card ảo vào interfacePrefixBlacklist của
# /var/lib/zerotier-one/local.conf, ZeroTier chỉ dùng card thật (Wi-Fi, cáp).
#
# Giữ nguyên các thiết lập khác nếu máy đã có local.conf (bản gốc lưu ở local.conf.truoc-bd03).
# Chạy lại nhiều lần không sao. IP ZeroTier của máy không đổi.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "Cần chạy bằng sudo: sudo bash $0" >&2; exit 1; }
THU_MUC_ZT="${THU_MUC_ZT:-/var/lib/zerotier-one}"
TEP="$THU_MUC_ZT/local.conf"
[ -d "$THU_MUC_ZT" ] || { echo "Máy chưa cài ZeroTier (không có $THU_MUC_ZT)" >&2; exit 1; }
if [ -f "$TEP" ] && [ ! -f "$TEP.truoc-bd03" ]; then
  cp -p "$TEP" "$TEP.truoc-bd03"
fi

python3 - "$TEP" <<'PY'
import json
import os
import sys

tep = sys.argv[1]
cau_hinh = {}
if os.path.exists(tep):
    with open(tep) as f:
        noi_dung = f.read().strip()
    if noi_dung:
        cau_hinh = json.loads(noi_dung)  # tệp hỏng thì dừng ở đây, không khởi động lại ZeroTier
ds = cau_hinh.setdefault("settings", {}).setdefault("interfacePrefixBlacklist", [])
for tien_to in ["docker", "br-", "veth", "tailscale"]:
    if tien_to not in ds:
        ds.append(tien_to)
with open(tep + ".moi", "w") as f:
    json.dump(cau_hinh, f, indent=2)
    f.write("\n")
os.replace(tep + ".moi", tep)
print("local.conf:", json.dumps(cau_hinh["settings"]))
PY

[ "${KHONG_KHOI_DONG_LAI:-0}" = "1" ] && exit 0
echo "Khởi động lại ZeroTier (phiên SSH có thể đứt vài giây, không sao)..."
systemctl restart zerotier-one
for _ in $(seq 1 30); do
  zerotier-cli info 2> /dev/null | grep -q ONLINE && break
  sleep 1
done
zerotier-cli info
# 200 listnetworks <nwid> <tên> <mac> <trạng thái> <kiểu> <card> <IP>
zerotier-cli listnetworks | awk 'NR > 1 {print "mang " $3 " " $6 " " $NF}'
