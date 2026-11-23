#!/usr/bin/env bash
# do_mang.sh - đo chất lượng mạng ZeroTier giữa các máy của cụm, chạy từ máy điều khiển (Git Bash).
# Chỉ đọc, không cần sudo, không đụng tới container nào.
#
#   bash scripts/cum/do_mang.sh              # phần 1 (Wi-Fi, IP LAN, ping chéo) + phần 2 (tốc độ tải)
#   bash scripts/cum/do_mang.sh --chi-ping   # chỉ phần 1, xong trong vài giây
#   bash scripts/cum/do_mang.sh --nguon labsit12        # đo tốc độ tải từ labsit12 thay vì từ master
#   KICH_THUOC_MB=64 bash scripts/cum/do_mang.sh        # tệp thử lớn hơn (mặc định 16 MB)
#
# Phần 1, trên từng máy: mạng Wi-Fi đang nối, cường độ sóng, IP LAN, rồi ping mọi máy khác qua
#   IP ZeroTier và qua IP LAN. Qua Wi-Fi, đường nối thẳng (DIRECT) thường dưới 60 ms; đi qua máy
#   chuyển tiếp của ZeroTier (RELAY) thường 150 ms trở lên. Nếu ZeroTier chậm hơn LAN nhiều, mạng
#   cho phép nối thẳng nhưng ZeroTier chưa tìm ra đường: chạy scripts/cum/cau_hinh_zerotier.sh
#   trên các máy đó. Xác nhận chắc chắn: bash scripts/cum/ssh_tat_ca.sh --tty "sudo zerotier-cli peers".
# Phần 2: máy nguồn (mặc định master) phát một tệp ngẫu nhiên qua HTTP trên IP ZeroTier của nó;
#   từng máy còn lại lần lượt tải về và báo MB/s. Đây là con số quyết định tốc độ HDFS và shuffle.
set -uo pipefail

# ------------------------------------------------ phần chạy TRÊN máy đích (gửi qua stdin)
ip_zt() { ip -4 -o addr show 2>/dev/null | grep -o '10\.68\.71\.[0-9]*' | head -1; }
ip_lan() {  # IP của card mạng đang ra Internet (Wi-Fi hoặc cáp), không phải ZeroTier/Tailscale/Docker
  ip route get 1.1.1.1 2>/dev/null | awk '{for (i = 1; i < NF; i++) if ($i == "src") {print $(i + 1); exit}}'
}
do_ping() {  # $1 = IP; in độ trễ trung bình (ms), rỗng nếu không trả lời
  ping -c 4 -i 0.3 -W 2 -q "$1" 2>/dev/null | awk -F/ '/^(rtt|round-trip)/{print $5}'
}
nhanh() { awk -v t="$1" 'BEGIN{exit !(t != "" && t < 60)}'; }

tren_may() {  # $@ = "ipZeroTier,ipLAN" của các máy cần ping (ipLAN có thể rỗng)
  local zt wifi ssid song toc cap dich lan t_zt t_lan nhan_xet
  zt="$(ip_zt)"
  echo "IP ZeroTier: ${zt:-KHONG THAY (may chua vao mang ttunet?)}"
  if command -v nmcli > /dev/null; then
    # --rescan no: đọc kết quả quét sẵn có, không bắt card Wi-Fi quét lại (gây trễ chốc lát)
    wifi="$(nmcli -t -f IN-USE,SSID,SIGNAL,RATE dev wifi list --rescan no 2>/dev/null | grep '^\*' | head -1)"
    IFS=: read -r _ ssid song toc <<< "$wifi"
    echo "Wi-Fi: ${ssid:-(khong noi Wi-Fi)} | song ${song:-?}% | toc do lien ket ${toc:-?}"
    nmcli -t -f DEVICE,TYPE,STATE dev 2>/dev/null \
      | awk -F: '$2 == "ethernet" && $1 !~ /^veth/ {print "Cong mang day " $1 ": " $3}'
  fi
  echo "IP LAN: $(ip_lan) | Gateway: $(ip route 2>/dev/null | awk '/^default/{print $3; exit}')"
  # Ping cả IP ZeroTier lẫn IP LAN của máy kia để tách các nguyên nhân:
  #   ZeroTier chậm hơn LAN nhiều  -> mạng cho phép nối thẳng nhưng ZeroTier đang đi relay
  #   ZeroTier và LAN cùng chậm    -> vẫn nối thẳng, chỉ là Wi-Fi đang nghẽn hoặc sóng yếu
  #   LAN không trả lời            -> Wi-Fi chặn các máy nói chuyện với nhau
  for cap in "$@"; do
    dich="${cap%%,*}"; lan="${cap#*,}"
    [ "$dich" = "$zt" ] && continue
    t_zt="$(do_ping "$dich")"
    t_lan=""; [ -n "$lan" ] && t_lan="$(do_ping "$lan")"
    if [ -z "$t_zt" ]; then nhan_xet="KHONG PING DUOC qua ZeroTier"
    elif nhanh "$t_zt"; then nhan_xet="noi thang"
    elif [ -z "$lan" ]; then nhan_xet="CHAM"
    elif [ -z "$t_lan" ]; then nhan_xet="RELAY, LAN khong tra loi (Wi-Fi chan may-may)"
    elif awk -v z="$t_zt" -v l="$t_lan" 'BEGIN{exit !(z - l < 50)}'; then nhan_xet="noi thang, Wi-Fi dang cham"
    else nhan_xet="RELAY du LAN thong (ZeroTier chua tim ra duong)"
    fi
    printf 'ping %-13s ZT %8s ms | LAN %-15s %8s ms | %s\n' "$dich" "${t_zt:--}" "${lan:--}" "${t_lan:--}" "$nhan_xet"
  done
}

tai_ve() {  # $1 = URL; in số MB, thời gian và tốc độ
  python3 - "$1" <<'PY'
import sys, time, urllib.request
bat_dau = time.time()
n = 0
try:
    with urllib.request.urlopen(sys.argv[1], timeout=180) as r:
        while True:
            khoi = r.read(1 << 20)
            if not khoi:
                break
            n += len(khoi)
except Exception as loi:
    print("LOI tai ve: %s" % loi)
    sys.exit(1)
giay = max(time.time() - bat_dau, 1e-3)
mb = n / 1048576
print("%.1f MB trong %.1f s = %.2f MB/s" % (mb, giay, mb / giay))
PY
}

case "${1:-}" in
  --dia-chi) echo "DIA_CHI $(ip_zt) $(ip_lan)"; exit 0 ;;
  --tren-may) shift; tren_may "$@"; exit 0 ;;
  --tai-ve) tai_ve "${2:?Thiếu URL}"; exit $? ;;
esac

# ------------------------------------------------ phần chạy trên máy điều khiển
TEP_NAY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
THU_MUC="$(cd "$(dirname "$TEP_NAY")/../.." && pwd)"
SSH_TAT_CA=(bash "$THU_MUC/scripts/cum/ssh_tat_ca.sh")
TUY_CHON_NOI=(-o "ConnectTimeout=${SSH_CHO:-20}" -o ConnectionAttempts=3 -o BatchMode=yes)
MB="${KICH_THUOC_MB:-16}"
CONG=18099
TAM_XA=/tmp/bd03_do_mang
CHI_PING=0; NGUON=""
while [ $# -gt 0 ]; do
  case "$1" in
    --chi-ping) CHI_PING=1 ;;
    --nguon) NGUON="${2:?Thiếu tên máy nguồn}"; shift ;;
    *) echo "Tùy chọn không hợp lệ: $1" >&2; exit 2 ;;
  esac
  shift
done

mapfile -t CAC_MAY < <("${SSH_TAT_CA[@]}" --liet-ke)
[ ${#CAC_MAY[@]} -gt 0 ] || { echo "Danh bạ cau_hinh/cum.env không có máy nào" >&2; exit 2; }
CAC_IP=(); for dong in "${CAC_MAY[@]}"; do CAC_IP+=("${dong#* }"); done

echo "################ Phần 1: Wi-Fi, IP LAN và ping chéo giữa ${#CAC_MAY[@]} máy (qua ZeroTier và qua LAN)"
# Hỏi trước IP LAN hiện tại của từng máy (DHCP cấp, đổi theo mạng Wi-Fi), ghép với IP ZeroTier
declare -A LAN=()
while read -r _ _ zt lan; do [ -n "$zt" ] && LAN[$zt]="$lan"; done \
  < <("${SSH_TAT_CA[@]}" --song-song --chay-tep "$TEP_NAY" -- --dia-chi | grep ' DIA_CHI ')
CAP=(); for ip in "${CAC_IP[@]}"; do CAP+=("$ip,${LAN[$ip]:-}"); done
KQ_PING="$(mktemp)"
"${SSH_TAT_CA[@]}" --song-song --chay-tep "$TEP_NAY" -- --tren-may "${CAP[@]}" | tee "$KQ_PING"
echo
echo "Tóm tắt: $(grep -c '| noi thang' "$KQ_PING") cặp nối thẳng, $(grep -c '| RELAY' "$KQ_PING") cặp đi relay" \
     "($(grep -c 'RELAY du LAN thong' "$KQ_PING") cặp LAN vẫn thông), $(grep -c 'KHONG PING DUOC' "$KQ_PING") cặp không ping được."
grep -q 'RELAY du LAN thong' "$KQ_PING" && echo "Cặp RELAY mà LAN vẫn thông: chạy scripts/cum/cau_hinh_zerotier.sh trên các máy đó (docs/TRIEN_KHAI_CUM.md mục 10b)."
rm -f "$KQ_PING"
[ "$CHI_PING" = "1" ] && exit 0

[ -n "$NGUON" ] || NGUON="${CAC_MAY[0]%% *}"
IP_NGUON=""
for dong in "${CAC_MAY[@]}"; do [ "${dong%% *}" = "$NGUON" ] && IP_NGUON="${dong#* }"; done
[ -n "$IP_NGUON" ] || { echo "Không có máy $NGUON trong danh bạ" >&2; exit 2; }
URL="http://$IP_NGUON:$CONG/f"

echo
echo "################ Phần 2: tải tệp $MB MB từ $NGUON ($IP_NGUON) về từng máy, lần lượt từng máy"
don_dep() { ssh -n "${TUY_CHON_NOI[@]}" "$NGUON" "pkill -f 'http[.]server $CONG' ; rm -rf $TAM_XA" > /dev/null 2>&1; }
trap don_dep EXIT
# setsid + chuyển hết vào/ra: phiên SSH kết thúc ngay, máy chủ HTTP vẫn chạy. timeout 900: dù
# script bị ngắt giữa chừng, máy chủ tạm cũng tự tắt sau 15 phút.
if ! ssh -n "${TUY_CHON_NOI[@]}" "$NGUON" "mkdir -p $TAM_XA && head -c ${MB}M /dev/urandom > $TAM_XA/f && cd $TAM_XA \
     && (setsid timeout 900 python3 -m http.server $CONG --bind $IP_NGUON > /dev/null 2>&1 < /dev/null &) && sleep 2"; then
  echo "!! Không dựng được máy chủ tải thử trên $NGUON"; exit 1
fi
for dong in "${CAC_MAY[@]}"; do
  ten="${dong%% *}"
  [ "$ten" = "$NGUON" ] && continue
  printf '%-14s ' "$ten"
  ssh "${TUY_CHON_NOI[@]}" "$ten" "bash -s -- --tai-ve $URL" < "$TEP_NAY" 2>&1 | tail -1
done
if command -v curl > /dev/null; then
  printf '%-14s ' "may-dieu-khien"
  curl -s -o /dev/null --max-time 180 -w '%{size_download} %{time_total}\n' "$URL" \
    | awk '$1 > 0 && $2 > 0 {m = $1/1048576; printf "%.1f MB trong %.1f s = %.2f MB/s\n", m, $2, m/$2; next} {print "LOI tai ve"}'
fi
echo
echo "Mốc so sánh: ngày 10/11/2026, khi phòng labsit dùng Wi-Fi \"TTU Open\" (đường RELAY), ghi 100 MB"
echo "vào HDFS mất 248 s, tức khoảng 0,4 MB/s. Cụm chạy được dữ liệu thật khi các máy đạt vài MB/s trở lên."
