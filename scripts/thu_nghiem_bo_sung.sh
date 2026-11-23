#!/usr/bin/env bash
# thu_nghiem_bo_sung.sh - hai thí nghiệm bổ sung sau lần chạy chính (MO_HINH.md mục 12.2, 12.3).
# Chạy trên máy master, không huấn luyện lại, dùng mô hình và gold của lần huấn luyện mới nhất:
#   1. Quét re-ranking novelty (β x số ứng viên): đường đánh đổi NDCG@10 - Coverage.
#   2. A/B đủ mẫu: mô phỏng traffic cho mọi user có gợi ý ALS (--chi-als), tính lại kết quả A/B.
#   3. Xuất lại toàn bộ số liệu (có phần gộp A/B và cỡ mẫu cần thiết).
#
# Khởi động từ laptop, chạy ngầm trong tmux (laptop tắt được):
#   ssh labsoe-11 "tmux new -d -s thu 'bash ~/bd03_khuyen_nghi/scripts/thu_nghiem_bo_sung.sh'"
# Theo dõi:   ssh labsoe-11 "tail -5 ~/thu_nghiem/nhat_ky.log"
# Lấy kết quả: ssh labsoe-11 "cat ~/thu_nghiem/xep_lai.md ~/thu_nghiem/ket_qua.md" > ket_qua_bo_sung.md
set -uo pipefail
THU_MUC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
S="bash $THU_MUC/ha_tang/spark/submit.sh"
RA="${RA:-$HOME/thu_nghiem}"
mkdir -p "$RA"
LOG="$RA/nhat_ky.log"
ghi() { echo "$*" | tee -a "$LOG"; }

: > "$LOG"
ghi "Bắt đầu $(date '+%F %T')"
ghi "== 1. Quét re-ranking novelty (stdout -> xep_lai.md)"
$S als kiem_tra/thu_xep_lai.py > "$RA/xep_lai.md" 2>> "$LOG" || ghi "!! thu_xep_lai.py lỗi, xem $LOG"
grep '^\[xep_lai\]' "$LOG" | tail -20

ghi "== 2. A/B: mô phỏng cho user có gợi ý ALS (hạt giống 7, khác lần chạy chính), rồi tính lại"
if $S lo transform/mo_phong_hien_thi.py --chi-als --so-nguoi 10000 --hat-giong 7 >> "$LOG" 2>&1; then
  $S lo transform/tinh_ket_qua_ab.py >> "$LOG" 2>&1 || ghi "!! tinh_ket_qua_ab.py lỗi, xem $LOG"
else
  ghi "!! mo_phong_hien_thi.py lỗi, xem $LOG"
fi
grep '^{' "$LOG" | tail -2 | tee /dev/null

ghi "== 3. Xuất số liệu (stdout -> ket_qua.md)"
$S nhanh kiem_tra/xuat_ket_qua.py > "$RA/ket_qua.md" 2>> "$LOG" || ghi "!! xuat_ket_qua.py lỗi, xem $LOG"
ghi "Xong $(date '+%F %T'). Kết quả: $RA/xep_lai.md, $RA/ket_qua.md"
