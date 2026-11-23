#!/usr/bin/env bash
# chep_du_lieu.sh - tải bộ dữ liệu RetailRocket về MÁY ĐIỀU KHIỂN (laptop, Git Bash), rồi chép sang:
#   - máy master (vai trò namenode): đủ 4 tệp CSV, để scripts/tai_du_lieu.sh đưa lên HDFS
#   - máy Kafka (vai trò kafka): chỉ events.csv, cho bộ phát sự kiện producer/phat_lai_su_kien.py
# Cả hai nơi giải nén vào ~/du_lieu/retailrocket (không cần sudo).
#
# Khóa Kaggle chỉ nằm trên máy điều khiển: máy lab dùng chung tài khoản, ai đăng nhập cũng đọc
# được ~/.kaggle/kaggle.json nếu để ở đó. Khóa cũng không xuất hiện trên dòng lệnh (curl đọc qua
# stdin), nên không lộ trong danh sách tiến trình hay lịch sử lệnh.
#
#   bash scripts/cum/chep_du_lieu.sh                         # khóa ở ../kaggle.json (cạnh thư mục repo)
#   bash scripts/cum/chep_du_lieu.sh --khoa "/d/BIG DATA/kaggle.json"
#   bash scripts/cum/chep_du_lieu.sh --zip "/d/BIG DATA/archive.zip"   # đã có sẵn tệp zip, khỏi tải
#
# Tệp zip tải về được giữ ở ../retailrocket.zip (ngoài repo) để lần sau khỏi tải lại.
set -uo pipefail

CAC_TEP=(events.csv item_properties_part1.csv item_properties_part2.csv category_tree.csv)

# ------------------------------------------------ phần chạy TRÊN máy đích (gửi qua stdin)
if [ "${1:-}" = "--giai-nen" ]; then
  python3 - "${2:-day_du}" "${CAC_TEP[@]}" <<'PY'
import os
import shutil
import sys
import zipfile

che_do, tat_ca = sys.argv[1], sys.argv[2:]
can = ["events.csv"] if che_do == "chi_events" else tat_ca
nha = os.path.expanduser("~")
tep_zip = os.path.join(nha, "retailrocket.zip")
dich = os.path.join(nha, "du_lieu", "retailrocket")
os.makedirs(dich, exist_ok=True)
with zipfile.ZipFile(tep_zip) as z:
    # tìm theo tên tệp, không phụ thuộc thư mục con bên trong zip
    trong_zip = {os.path.basename(n): n for n in z.namelist() if not n.endswith("/")}
    for t in can:
        if t not in trong_zip:
            sys.exit("Tệp zip không có %s: có phải bộ RetailRocket không?" % t)
        tam = os.path.join(dich, t + ".tam")
        with z.open(trong_zip[t]) as nguon, open(tam, "wb") as ra:
            shutil.copyfileobj(nguon, ra, 1 << 20)
        os.replace(tam, os.path.join(dich, t))  # tệp chỉ xuất hiện khi đã giải nén xong
os.remove(tep_zip)
for t in can:
    print("  %-28s %8.1f MB" % (t, os.path.getsize(os.path.join(dich, t)) / 1048576))
print("  -> " + dich)
PY
  exit $?
fi

# ------------------------------------------------ phần chạy trên máy điều khiển
TEP_NAY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
THU_MUC="$(cd "$(dirname "$TEP_NAY")/../.." && pwd)"
CHA="$(dirname "$THU_MUC")"
SSH_TAT_CA=(bash "$THU_MUC/scripts/cum/ssh_tat_ca.sh")
TUY_CHON_NOI=(-o "ConnectTimeout=${SSH_CHO:-20}" -o ConnectionAttempts=3)
URL="https://www.kaggle.com/api/v1/datasets/download/retailrocket/ecommerce-dataset"
KHOA="$CHA/kaggle.json"
ZIP=""
while [ $# -gt 0 ]; do
  case "$1" in
    --khoa) KHOA="${2:?Thiếu đường dẫn kaggle.json}"; shift ;;
    --zip) ZIP="${2:?Thiếu đường dẫn tệp zip}"; shift ;;
    *) echo "Tùy chọn không hợp lệ: $1" >&2; exit 2 ;;
  esac
  shift
done

kich_thuoc() { wc -c < "$1" | tr -d ' '; }

if [ -z "$ZIP" ]; then
  ZIP="$CHA/retailrocket.zip"
  if [ -f "$ZIP" ] && [ "$(kich_thuoc "$ZIP")" -gt 100000000 ]; then
    echo "Đã có $ZIP từ lần trước, khỏi tải lại."
  else
    [ -f "$KHOA" ] || { echo "Không thấy $KHOA. Chỉ đường dẫn bằng --khoa <tệp kaggle.json>." >&2; exit 2; }
    ten="$(sed -n 's/.*"username" *: *"\([^"]*\)".*/\1/p' "$KHOA")"
    khoa="$(sed -n 's/.*"key" *: *"\([^"]*\)".*/\1/p' "$KHOA")"
    [ -n "$ten" ] && [ -n "$khoa" ] || { echo "$KHOA không có username/key" >&2; exit 2; }
    echo "Tải RetailRocket từ Kaggle về $ZIP (khoảng 300 MB)..."
    # -K -: đọc tài khoản từ stdin thay vì đối số dòng lệnh
    if ! curl -L --fail --progress-bar -K - -o "$ZIP.tam" "$URL" <<< "user = \"$ten:$khoa\""; then
      rm -f "$ZIP.tam"
      echo "!! Tải thất bại. Kiểm tra khóa Kaggle, hoặc tải tay từ kaggle.com/datasets/retailrocket/ecommerce-dataset rồi chạy lại với --zip <tệp>." >&2
      exit 1
    fi
    mv -f "$ZIP.tam" "$ZIP"
  fi
fi
[ -f "$ZIP" ] || { echo "Không thấy $ZIP" >&2; exit 2; }
[ "$(kich_thuoc "$ZIP")" -gt 1000000 ] || { echo "$ZIP nhỏ bất thường (có thể là trang báo lỗi), xóa đi rồi tải lại." >&2; exit 1; }
echo "Tệp zip: $ZIP ($(( $(kich_thuoc "$ZIP") / 1048576 )) MB)"

LOI=0
chep() {  # $1 = tên máy, $2 = day_du | chi_events
  echo "===== $1: chép tệp zip rồi giải nén ($2)"
  if ! scp "${TUY_CHON_NOI[@]}" "$ZIP" "$1:retailrocket.zip"; then
    echo "!! $1: chép thất bại"; LOI=1; return
  fi
  ssh "${TUY_CHON_NOI[@]}" "$1" "bash -s -- --giai-nen $2" < "$TEP_NAY" || { echo "!! $1: giải nén thất bại"; LOI=1; }
}
mapfile -t MASTER < <("${SSH_TAT_CA[@]}" --liet-ke --vai-tro namenode)
mapfile -t KAFKA < <("${SSH_TAT_CA[@]}" --liet-ke --vai-tro kafka)
for dong in "${MASTER[@]}"; do chep "${dong%% *}" day_du; done
for dong in "${KAFKA[@]}"; do chep "${dong%% *}" chi_events; done

if [ "$LOI" = 0 ]; then
  echo
  echo "Xong. Đưa lên HDFS (trên master, sau khi đã dọn dữ liệu mẫu): bash scripts/tai_du_lieu.sh"
fi
exit $LOI
