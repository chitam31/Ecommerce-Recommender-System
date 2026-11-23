#!/usr/bin/env bash
# tinh_trang.sh - một màn hình tình trạng của máy master: RAM, ứng dụng Spark, Airflow, tmux.
# Chạy trên master. Từ máy điều khiển:
#
#   ssh labsoe-11 "bash ~/bd03_khuyen_nghi/scripts/cum/tinh_trang.sh"
#
# Chỉ đọc, không thay đổi gì. Gom các lệnh hay dùng khi theo dõi cụm vào một chỗ, để không phải
# gõ những lệnh có nhiều lớp ngoặc kép qua SSH (dễ sai trong Git Bash).
set -uo pipefail
THU_MUC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ -f "$THU_MUC/cau_hinh/cum.env" ] && . "$THU_MUC/cau_hinh/cum.env"
MASTER_IP="${SPARK_MASTER_IP:-10.68.71.113}"
PG="${PG_CONTAINER:-bd03-airflow-postgres-1}"

echo "== RAM ($(date '+%H:%M:%S'))"
free -h | sed -n '1,3p'

echo
echo "== RAM theo container"
docker stats --no-stream --format '{{.Name}}\t{{.MemUsage}}' 2>/dev/null | sed 's/^/  /'

echo
echo "== Spark (http://$MASTER_IP:8080)"
curl -s --max-time 10 "http://$MASTER_IP:8080/json/" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except ValueError:
    sys.exit("  Không đọc được JSON của Spark Master UI")
print("  worker sống: {}  | lõi đang dùng: {}/{}  | RAM executor đang dùng: {}/{} MB".format(
    d.get("aliveworkers"), d.get("coresused"), d.get("cores"), d.get("memoryused"), d.get("memory")))
ds = d.get("activeapps", [])
if not ds:
    print("  (không có ứng dụng nào đang chạy)")
for a in ds:
    print("  {:<26} {:>3} lõi  {:<8} {}".format(a.get("name", "?"), a.get("cores", "?"), a.get("state", "?"), a.get("id", "")))
'

if docker inspect "$PG" > /dev/null 2>&1; then
  sql() { docker exec "$PG" psql -U airflow -d airflow -P pager=off -c "$1" 2>&1 | sed 's/^/  /'; }
  echo
  echo "== Airflow: DAG đang bật lịch"
  sql "SELECT dag_id, is_paused FROM dag WHERE dag_id LIKE 'kn_%' ORDER BY dag_id"
  echo "== Airflow: 8 lần chạy gần nhất (giờ Việt Nam)"
  sql "SELECT dag_id, run_type, state,
              to_char(start_date AT TIME ZONE 'Asia/Ho_Chi_Minh', 'DD/MM HH24:MI') AS bat_dau,
              to_char(end_date AT TIME ZONE 'Asia/Ho_Chi_Minh', 'DD/MM HH24:MI') AS ket_thuc
       FROM dag_run ORDER BY queued_at DESC NULLS LAST LIMIT 8"
  echo "== Airflow: task chưa xong hoặc lỗi"
  sql "SELECT dag_id, task_id, state, try_number,
              to_char(start_date AT TIME ZONE 'Asia/Ho_Chi_Minh', 'DD/MM HH24:MI') AS bat_dau
       FROM task_instance
       WHERE state IN ('running', 'queued', 'scheduled', 'up_for_retry', 'up_for_reschedule', 'failed')
       ORDER BY start_date DESC NULLS FIRST LIMIT 10"
else
  echo
  echo "== Airflow: không thấy container $PG"
fi

echo "== tmux"
tmux ls 2>/dev/null | sed 's/^/  /' || echo "  (không có phiên tmux nào)"
