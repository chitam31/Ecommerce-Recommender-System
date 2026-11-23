#!/usr/bin/env bash
# don_du_lieu.sh - dọn dữ liệu mẫu (hoặc dữ liệu cũ của dự án) trước khi chạy lại từ đầu,
# chạy từ máy điều khiển (Git Bash). Mặc định CHỈ IN những gì sẽ xóa; thêm --xoa mới xóa thật.
#
#   bash scripts/cum/don_du_lieu.sh          # xem trước: đường dẫn nào, dung lượng bao nhiêu
#   bash scripts/cum/don_du_lieu.sh --xoa    # xóa thật (hỏi xác nhận: gõ XOA)
#
# Chỉ xóa những gì dự án này tạo ra, lấy từ chính mã nguồn trên master:
#   - mọi bảng trong jobs/common/cau_hinh.py (_BANG), CSV ở <gốc>/landing/retailrocket
#     (<gốc> = /lakehouse/bd03_khuyen_nghi, mục A.4 đề cương)
#   - checkpoint của các luồng streaming và thư mục mô hình mà các job có ghi
#   - hai topic Kafka retailrocket-events, hien-thi (xóa rồi tạo lại rỗng, 3 phân vùng)
# Thư mục khác dưới gốc (và toàn bộ /lake cũ, ví dụ dữ liệu cũ của nhóm ở /lake/bronze/su_kien) được GIỮ NGUYÊN
# và in ra để biết. Không xóa gì khi còn ứng dụng Spark đang chạy.
set -uo pipefail

# ------------------------------------------------ phần chạy TRÊN máy master (gửi qua stdin)
if [ "${1:-}" = "--tren-master" ]; then
  XOA="${2:-0}"
  cd ~/bd03_khuyen_nghi || { echo "Không có ~/bd03_khuyen_nghi trên master: chạy ssh_tat_ca.sh --dong-bo-ma trước"; exit 1; }
  dang_chay="$(python3 -c "import json,sys,urllib.request as u; print(len(json.load(u.urlopen(sys.argv[1] + '/json/', timeout=10))['activeapps']))" \
               "${SPARK_UI:-http://localhost:8080}" 2> /dev/null)"
  if [ -z "$dang_chay" ]; then echo "!! Không hỏi được Spark Master (cổng 8080)"; exit 1; fi
  if [ "$dang_chay" != "0" ]; then
    echo "!! Đang có $dang_chay ứng dụng Spark chạy (xem http://10.68.71.113:8080): đợi xong hoặc dừng rồi mới dọn."
    exit 1
  fi
  # Danh sách đường dẫn của dự án, đọc từ mã nguồn để không sót bảng/checkpoint mới thêm
  mapfile -t CUA_DU_AN < <(python3 - <<'PY'
import glob, re, sys
sys.path.insert(0, "jobs")
from common import cau_hinh as ch
goc = ch.LAKE_GOC
ds = {"{}/{}".format(goc, p) for p in ch._BANG.values()}
ds.add("{}/landing/retailrocket".format(goc))
for tep in glob.glob("jobs/**/*.py", recursive=True):
    s = open(tep, encoding="utf-8").read()
    ds |= {ch.duong_dan_checkpoint(t) for t in re.findall(r'duong_dan_checkpoint\(\s*"([^"]+)"', s)}
    ds |= {ch.duong_dan_mo_hinh(t.split("/")[0].split("{")[0])
           for t in re.findall(r'duong_dan_mo_hinh\(\s*"([^"]+)"', s)}
print("\n".join(sorted(ds)))
PY
)
  GOC_LAKE="$(python3 -c 'import sys; sys.path.insert(0, "jobs"); from common import cau_hinh as ch; print(ch.LAKE_DUONG_DAN)')"
  [ ${#CUA_DU_AN[@]} -gt 10 ] || { echo "!! Không đọc được danh sách bảng từ jobs/common/cau_hinh.py"; exit 1; }
  HD="docker exec hadoop-master hdfs dfs"
  docker exec hadoop-master hdfs dfsadmin -safemode wait > /dev/null
  echo "-- Thuộc dự án (sẽ xóa):"
  # Một lệnh du cho mọi đường dẫn (mỗi lệnh hdfs là một JVM, gọi từng cái rất chậm);
  # đường dẫn chưa có chỉ sinh lỗi ở stderr, bỏ qua.
  DU="$($HD -du -s -h "${CUA_DU_AN[@]}" 2> /dev/null)"
  mapfile -t CO < <(awk 'NF {print $NF}' <<< "$DU")
  if [ ${#CO[@]} -gt 0 ]; then sed 's#hdfs://[^/]*##; s#^#   #' <<< "$DU"; else echo "   (không còn gì)"; fi
  echo "-- Thư mục khác dưới $GOC_LAKE, 2 cấp đầu (GIỮ NGUYÊN):"
  $HD -ls -d "$GOC_LAKE/*" "$GOC_LAKE/*/*" 2> /dev/null | awk '$1 ~ /^d/ {print $NF}' | sed 's#^hdfs://[^/]*##' \
    | while read -r d; do
        giu=1
        for p in "${CUA_DU_AN[@]}"; do
          q="/${p#hdfs://*/}"
          # bỏ qua thư mục của dự án, thư mục con của chúng và các thư mục cha chứa chúng
          case "$d/" in "$q/"*) giu=0; break ;; esac
          case "$q/" in "$d/"*) giu=0; break ;; esac
        done
        [ "$giu" = 1 ] && echo "   $d"
      done
  if [ "$XOA" = "1" ] && [ ${#CO[@]} -gt 0 ]; then
    $HD -rm -r -f -skipTrash "${CO[@]}" > /dev/null && echo "-- Đã xóa ${#CO[@]} đường dẫn trên HDFS."
  fi
  if [ "$XOA" = "1" ]; then
    MAU="${THU_MUC_MAU:-/tmp/mau}"  # nơi tao_du_lieu_mau.py đã sinh CSV mẫu
    [ -d "$MAU" ] && rm -rf "$MAU" && echo "-- Đã xóa dữ liệu mẫu cục bộ $MAU trên master."
  fi
  exit 0
fi

if [ "${1:-}" = "--tren-kafka" ]; then
  XOA="${2:-0}"
  KT="docker exec kafka-server /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"
  for t in retailrocket-events hien-thi; do
    if $KT --describe --topic "$t" > /dev/null 2>&1; then
      tin="$(docker exec kafka-server /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$t" 2> /dev/null \
            | awk -F: '{s += $3} END {print s + 0}')"
      echo "   topic $t: ${tin:-?} tin nhắn"
      [ "$XOA" = "1" ] && $KT --delete --topic "$t"
    else
      echo "   topic $t: chưa có"
    fi
  done
  if [ "$XOA" = "1" ]; then
    for _ in $(seq 1 30); do  # Kafka xóa topic không đồng bộ: chờ hẳn rồi mới tạo lại
      $KT --list | grep -qxE 'retailrocket-events|hien-thi' || break
      sleep 2
    done
    cd ~/bd03_khuyen_nghi && bash scripts/tao_topic.sh > /dev/null && echo "-- Đã tạo lại 2 topic rỗng (3 phân vùng)."
  fi
  exit 0
fi

# ------------------------------------------------ phần chạy trên máy điều khiển
TEP_NAY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
THU_MUC="$(cd "$(dirname "$TEP_NAY")/../.." && pwd)"
SSH_TAT_CA=(bash "$THU_MUC/scripts/cum/ssh_tat_ca.sh")
TUY_CHON_NOI=(-o "ConnectTimeout=${SSH_CHO:-20}" -o ConnectionAttempts=3)
XOA=0
case "${1:-}" in
  "") ;;
  --xoa) XOA=1 ;;
  *) echo "Tùy chọn không hợp lệ: $1 (chỉ có --xoa)" >&2; exit 2 ;;
esac
MASTER="$("${SSH_TAT_CA[@]}" --liet-ke --vai-tro namenode | head -1 | cut -d' ' -f1)"
KAFKA="$("${SSH_TAT_CA[@]}" --liet-ke --vai-tro kafka | head -1 | cut -d' ' -f1)"

echo "########## HDFS ($MASTER)"
ssh "${TUY_CHON_NOI[@]}" "$MASTER" "bash -s -- --tren-master 0" < "$TEP_NAY" || exit 1
echo "########## Kafka ($KAFKA)"
ssh "${TUY_CHON_NOI[@]}" "$KAFKA" "bash -s -- --tren-kafka 0" < "$TEP_NAY" || exit 1

if [ "$XOA" != "1" ]; then
  echo
  echo "Mới xem trước, chưa xóa gì. Xóa thật: bash scripts/cum/don_du_lieu.sh --xoa"
  exit 0
fi
echo
read -r -p "Xóa toàn bộ phần 'thuộc dự án' ở trên và làm rỗng 2 topic Kafka? Gõ XOA để xác nhận: " xac_nhan
[ "$xac_nhan" = "XOA" ] || { echo "Không xóa gì."; exit 1; }
echo "########## Xóa trên HDFS ($MASTER)"
ssh "${TUY_CHON_NOI[@]}" "$MASTER" "bash -s -- --tren-master 1" < "$TEP_NAY" || exit 1
echo "########## Làm rỗng topic Kafka ($KAFKA)"
ssh "${TUY_CHON_NOI[@]}" "$KAFKA" "bash -s -- --tren-kafka 1" < "$TEP_NAY" || exit 1
echo
echo "Xong. Tiếp theo trên master: bash scripts/tai_du_lieu.sh"
