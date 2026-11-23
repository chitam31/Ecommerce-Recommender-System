#!/usr/bin/env bash
# trien_khai_lai.sh - đánh giá lại và sinh lại danh sách phục vụ cho MỘT lần huấn luyện đã có,
# không tuning lại (dùng mô hình đã lưu ở /lakehouse/bd03_khuyen_nghi/models/als/<ma_chay>). Dùng khi đổi cách phục
# vụ, ví dụ bật xếp lại theo độ mới (BETA_XEP_LAI, SO_UNG_VIEN_XEP_LAI trong cau_hinh.py).
#   danh_gia (ghi thêm 1 dòng lịch sử, có số đo cấu hình phục vụ) -> sinh_goi_y_toan_bo ->
#   tron_goi_y -> ghi_gold (MERGE, tạo phiên bản gold mới) -> xuất số liệu
#
# Khởi động từ laptop, chạy ngầm trong tmux:
#   ssh labsoe-11 "tmux new -d -s tkl 'bash ~/bd03_khuyen_nghi/scripts/trien_khai_lai.sh r20261110T075240'"
# Theo dõi:    ssh labsoe-11 "grep -E '^==|^[{]|lỗi|^Xong' ~/trien_khai_lai/nhat_ky.log"
# Lấy kết quả: ssh labsoe-11 "cat ~/trien_khai_lai/ket_qua.md" > ket_qua_phuc_vu.md
set -uo pipefail
MA_CHAY="${1:?Cần mã lần huấn luyện, ví dụ r20261110T075240}"
THU_MUC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
S="bash $THU_MUC/ha_tang/spark/submit.sh"
RA="${RA:-$HOME/trien_khai_lai}"
mkdir -p "$RA"
LOG="$RA/nhat_ky.log"
ghi() { echo "$*" | tee -a "$LOG"; }

: > "$LOG"
ghi "Bắt đầu $(date '+%F %T'), ma_chay=$MA_CHAY"
for buoc in "als ml/danh_gia.py" "als ml/sinh_goi_y_toan_bo.py" "lo ml/tron_goi_y.py" "lo ml/ghi_gold.py"; do
  set -- $buoc
  ghi "== $2"
  if ! $S "$1" "$2" --ma-chay "$MA_CHAY" >> "$LOG" 2>&1; then
    ghi "LỖI: $2 thất bại (lỗi), xem $LOG"
    exit 1
  fi
  grep '^{' "$LOG" | tail -1
done
ghi "== xuất số liệu (stdout -> ket_qua.md)"
$S nhanh kiem_tra/xuat_ket_qua.py > "$RA/ket_qua.md" 2>> "$LOG" || ghi "LỖI: xuat_ket_qua.py thất bại (lỗi)"
ghi "Xong $(date '+%F %T'). Kết quả: $RA/ket_qua.md"
