#!/usr/bin/env bash
# luong.sh - quản lý 3 luồng streaming chạy liên tục trên cụm. Chạy trên máy master.
#
#   scripts/luong.sh khoi_dong tat_ca      # nap_su_kien + tinh_thinh_hanh + ghi_hien_thi
#   scripts/luong.sh khoi_dong thinh_hanh
#   scripts/luong.sh dung thinh_hanh
#   scripts/luong.sh trang_thai
#   scripts/luong.sh log thinh_hanh
set -euo pipefail
THU_MUC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[ -f "$THU_MUC/cau_hinh/cum.env" ] && . "$THU_MUC/cau_hinh/cum.env"
MASTER_IP="${SPARK_MASTER_IP:-10.68.71.113}"

declare -A JOB=(
  [nap]="streaming/nap_su_kien.py"
  [thinh_hanh]="streaming/tinh_thinh_hanh.py"
  [hien_thi]="streaming/ghi_hien_thi.py"
)
declare -A TEN_APP=(
  [nap]="bd03-nap-su-kien"
  [thinh_hanh]="bd03-tinh-thinh-hanh"
  [hien_thi]="bd03-ghi-hien-thi"
)

dang_chay() {  # in id ứng dụng đang chạy có tên $1 (rỗng nếu không có)
  curl -s "http://$MASTER_IP:8080/json/" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(' '.join(a['id'] for a in d.get('activeapps', []) if a['name'] == '$1'))"
}

# Driver chạy client mode bên trong container spark-master. Hủy ứng dụng qua Master UI làm mất
# executor nhưng KHÔNG dừng tiến trình driver: truy vấn streaming của driver "ma" vẫn sống, im lặng
# khi không có dữ liệu, rồi tranh checkpoint với luồng mới ở lô đầu tiên và làm cả hai cùng chết
# (NHAT_KY lỗi 27). Vì vậy dừng luồng = hủy ứng dụng + dừng hẳn tiến trình driver.
driver_con() {  # PID các tiến trình driver (spark-submit java + python) của luồng $1, rỗng nếu không có
  docker exec spark-master pgrep -f "${JOB[$1]}" 2>/dev/null | tr '\n' ' ' || true
}

dung_driver() {
  local ten="$1" i
  [ -z "$(driver_con "$ten")" ] && return 0
  docker exec spark-master pkill -TERM -f "${JOB[$ten]}" || true
  for i in $(seq 1 "${CHO_DUNG_GIAY:-30}"); do
    [ -z "$(driver_con "$ten")" ] && { echo "[$ten] tiến trình driver đã dừng"; return 0; }
    sleep 1
  done
  docker exec spark-master pkill -KILL -f "${JOB[$ten]}" || true
  echo "[$ten] buộc dừng tiến trình driver (KILL)"
}

khoi_dong() {
  local ten="$1"
  if [ -n "$(dang_chay "${TEN_APP[$ten]}")" ]; then
    echo "[$ten] đang chạy, bỏ qua"; return 0
  fi
  if [ -n "$(driver_con "$ten")" ]; then
    echo "[$ten] còn tiến trình driver cũ dù Master không còn ứng dụng: dừng trước khi bật lại"
    dung_driver "$ten"
  fi
  CHAY_NEN=1 bash "$THU_MUC/ha_tang/spark/submit.sh" "$ten" "${JOB[$ten]}"
  echo "[$ten] đã nộp ${JOB[$ten]} (log: docker exec spark-master tail -f /tmp/bd03_logs/$ten.log)"
}

dung() {
  local ten="$1" id
  for id in $(dang_chay "${TEN_APP[$ten]}"); do
    # Dừng ứng dụng qua Master UI (standalone): checkpoint bảo đảm khởi động lại không mất/không trùng.
    curl -s -X POST "http://$MASTER_IP:8080/app/kill/" -d "id=$id&terminate=true" > /dev/null
    echo "[$ten] đã gửi lệnh dừng $id"
  done
  dung_driver "$ten"
}

LENH="${1:-trang_thai}"; MUC="${2:-tat_ca}"
if [ "$MUC" = "tat_ca" ]; then DS=(nap thinh_hanh hien_thi); else DS=("$MUC"); fi

case "$LENH" in
  khoi_dong) for t in "${DS[@]}"; do khoi_dong "$t"; done ;;
  dung)      for t in "${DS[@]}"; do dung "$t"; done ;;
  log)       docker exec spark-master tail -n 80 "/tmp/bd03_logs/${DS[0]}.log" ;;
  trang_thai)
    for t in nap thinh_hanh hien_thi; do
      id="$(dang_chay "${TEN_APP[$t]}")"
      pid="$(driver_con "$t")"
      ghi_chu=""
      [ -z "$id" ] && [ -n "$pid" ] && ghi_chu="  !! còn tiến trình driver $pid: chạy 'luong.sh dung $t' trước khi bật lại"
      printf "%-12s %-24s %s%s\n" "$t" "${TEN_APP[$t]}" "${id:-KHONG CHAY}" "$ghi_chu"
    done ;;
  *) echo "Lệnh: khoi_dong|dung|trang_thai|log [nap|thinh_hanh|hien_thi|tat_ca]"; exit 2 ;;
esac
