#!/usr/bin/env bash
# thu_bang_chung.sh - thu mọi bằng chứng chạy trên cụm cho báo cáo và hồ sơ nộp (mục A.6, A.8, C.3 đề cương).
# Chạy trên máy master SAU KHI đã chuyển gốc lakehouse (scripts/cum/chuyen_lakehouse.sh), trong tmux:
#
#   bash scripts/thu_bang_chung.sh            # khoảng 20-25 phút
#
# Kết quả nằm NGOÀI repo (để lần đồng bộ mã sau không bị chặn vì tệp lạ trong thư mục làm việc):
#   ~/bd03_bang_chung/<thời điểm>/  và  ~/bd03_bang_chung/<thời điểm>.tar.gz
# Trên laptop:  scp labsoe-11:~/bd03_bang_chung/<thời điểm>.tar.gz .  rồi giải nén vào docs/bang_chung/cum/
#
#   01_hdfs_du_h.txt        hdfs dfs -du -h /lakehouse/bd03_khuyen_nghi   (output phải nộp theo A.6.3)
#   02_hdfs_cay.txt         du -h, -count, cây thư mục (hdfs dfs -ls -R đã lọc)
#   03_quarantine.json      chỉ khi _quarantine chưa có: dựng lại từ bronze (không ghi silver)
#   04_delta.txt            DESCRIBE HISTORY, thao tác theo bảng (MERGE/OPTIMIZE...), VERSION AS OF, schema
#                           evolution, số liệu OPTIMIZE/ZORDER, thống kê làm sạch theo lý do
#   05_airflow.txt          5 DAG, lỗi import, Deadline Alert từng DAG, lượt chạy gần nhất, canh_bao.log
#   06_streaming.txt        ứng dụng Spark đang chạy và thời gian đã chạy (luồng >= 30 phút khi demo)
#   07_do_tre_web.md        thời gian phản hồi web (máy khách + /api/hieu-nang)
#   eda.json                khảo sát dữ liệu gốc (kiem_tra/eda.py) cho notebooks/eda_retailrocket.ipynb
#   phan_tich_loi.json      NDCG từng người gom theo nhóm: ALS thua baseline ở đâu (kiem_tra/phan_tich_loi.py)
# Bước nào lỗi thì ghi lỗi vào tệp của bước đó rồi chạy tiếp.
set -uo pipefail
THU_MUC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$THU_MUC"
[ -f cau_hinh/cum.env ] && . cau_hinh/cum.env
MASTER_IP="${SPARK_MASTER_IP:-10.68.71.113}"
GOC="${LAKE_DUONG_DAN:-/lakehouse/bd03_khuyen_nghi}"
WEB_URL="${WEB_URL:-http://$(for b in $(compgen -v MAY_); do IFS='|' read -r _ ip v _ _ <<< "${!b}"; [[ ",$v," == *",web,"* ]] && echo "$ip"; done | head -1):8000}"
LUC="$(date +%Y%m%d_%H%M)"
RA="$HOME/bd03_bang_chung/$LUC"
mkdir -p "$RA"
HD="docker exec hadoop-master hdfs dfs"
SQL=(bash scripts/demo/delta_sql.sh cau)
buoc() { echo "[$(date +%H:%M:%S)] $*"; }

{ echo "# Thu lúc $(date '+%Y-%m-%d %H:%M:%S %Z') trên $(hostname), mã: $(git log -1 --format='%h %s' 2> /dev/null)"; } > "$RA/00_thong_tin.txt"

# _quarantine dựng trước, rồi mới chụp du -h và cây thư mục: lần thu 16/11 chụp du trước nên _quarantine = 0
# trong 01/02 dù bảng đã có ngay sau đó (NHAT_KY, 16/11).
buoc "1/9 _quarantine"
if $HD -test -e "$GOC/_quarantine/tuong_tac/_delta_log"; then
  echo '{"ghi_chu": "_quarantine.tuong_tac đã có, không dựng lại"}' > "$RA/03_quarantine.json"
else
  bash ha_tang/spark/submit.sh lo transform/bronze_to_silver.py --toan-bo --chi-cach-ly > "$RA/03_quarantine.json" 2>&1
fi

buoc "2/9 hdfs dfs -du -h $GOC"
{ echo ">> hdfs dfs -du -h $GOC"; $HD -du -h "$GOC"; echo; echo ">> hdfs dfs -du -s -h $GOC"; $HD -du -s -h "$GOC"; } \
  > "$RA/01_hdfs_du_h.txt" 2>&1

buoc "3/9 cây thư mục HDFS"
bash scripts/demo/cay_hdfs.sh --tep > "$RA/02_hdfs_cay.txt" 2>&1

buoc "4/9 Delta: HISTORY, VERSION AS OF, MERGE, OPTIMIZE, làm sạch (vài phút)"
"${SQL[@]}" \
  "# A. DESCRIBE HISTORY các bảng chính (A.6.2)" \
  "DESCRIBE HISTORY {silver.tuong_tac} LIMIT 8" \
  "DESCRIBE HISTORY {features.ma_tran_tuong_tac} LIMIT 5" \
  "DESCRIBE HISTORY {gold.goi_y_nguoi_dung} LIMIT 6" \
  "DESCRIBE HISTORY {gold.nhat_ky_hien_thi} LIMIT 5" \
  "DESCRIBE HISTORY {gold.ket_qua_ab} LIMIT 5" \
  "DESCRIBE HISTORY {gold.thinh_hanh_15p} LIMIT 3" \
  "DESCRIBE HISTORY {gold.ket_qua_tai_lap} LIMIT 5" \
  "DESCRIBE HISTORY {_quarantine.tuong_tac} LIMIT 3" \
  "# B. Số commit theo thao tác (MERGE INTO, OPTIMIZE, WRITE, STREAMING UPDATE...)" \
  "THAO_TAC {silver.tuong_tac}" "THAO_TAC {gold.goi_y_nguoi_dung}" "THAO_TAC {gold.thinh_hanh_15p}" \
  "THAO_TAC {gold.nhat_ky_hien_thi}" "THAO_TAC {gold.ket_qua_ab}" \
  "# C. Time travel: VERSION AS OF so với bản hiện tại" \
  "SELECT 'VERSION AS OF 0' AS ban, count(*) AS so_dong FROM {silver.tuong_tac} VERSION AS OF 0 UNION ALL SELECT 'hien_tai', count(*) FROM {silver.tuong_tac}" \
  "SELECT 'VERSION AS OF 1' AS ban, count(*) AS so_dong, max(phien_ban_delta_du_lieu) AS pb_du_lieu FROM {gold.ket_qua_ab} VERSION AS OF 1 UNION ALL SELECT 'hien_tai', count(*), max(phien_ban_delta_du_lieu) FROM {gold.ket_qua_ab}" \
  "# D. Schema evolution: lược đồ silver hiện tại (có gia_san_pham, xep_hang_sao) và ở phiên bản 0" \
  "DESCRIBE {silver.tuong_tac}" \
  "SELECT * FROM {silver.tuong_tac} VERSION AS OF 0 LIMIT 2" \
  "# E. OPTIMIZE / ZORDER: số liệu trước/sau trên truy vấn lấy gợi ý theo user" \
  "SELECT giai_doan, so_tep, so_truy_van, round(trung_vi_ms) AS trung_vi_ms, round(p95_ms) AS p95_ms, tep_doc_trung_binh, thoi_diem FROM {gold.do_hieu_nang_zorder} ORDER BY thoi_diem" \
  "# F. Thống kê làm sạch bronze -> silver (C.3): mỗi dòng bronze ở đúng một nơi" \
  "SELECT 'bronze.su_kien' AS bang, count(*) AS so_dong FROM {bronze.su_kien} UNION ALL SELECT 'silver.tuong_tac', count(*) FROM {silver.tuong_tac} UNION ALL SELECT '_quarantine.tuong_tac', count(*) FROM {_quarantine.tuong_tac}" \
  "SELECT ly_do, count(*) AS so_dong, count(DISTINCT ma_nguoi_dung) AS so_nguoi_dung FROM {_quarantine.tuong_tac} GROUP BY ly_do ORDER BY so_dong DESC" \
  "SELECT loai_su_kien, count(*) AS so_dong FROM {silver.tuong_tac} GROUP BY loai_su_kien ORDER BY so_dong DESC" \
  > "$RA/04_delta.txt" 2>&1

buoc "5/9 Airflow"
AF="$(docker ps -q --filter label=com.docker.compose.project=bd03-airflow --filter label=com.docker.compose.service=airflow-scheduler | head -1)"
{
  if [ -z "$AF" ]; then echo "!! không thấy container airflow-scheduler"; else
    echo ">> airflow dags list"; docker exec "$AF" airflow dags list 2> /dev/null
    echo; echo ">> airflow dags list-import-errors"; docker exec "$AF" airflow dags list-import-errors 2> /dev/null
    echo; echo ">> Deadline Alert (thay sla) và retries của từng DAG"
    docker exec -i "$AF" python - 2> /dev/null <<'PY'
from airflow.dag_processing.dagbag import DagBag
db = DagBag("/opt/bd03/airflow/dags")
for dag_id, dag in sorted(db.dags.items()):
    dl = dag.deadline[0] if isinstance(dag.deadline, list) else dag.deadline
    print("{:24s} lich={!s:14s} deadline={} phut ({}) callback={} retries={}".format(
        dag_id, dag.schedule, int(dl.interval.total_seconds() // 60), type(dl.reference).__name__,
        dl.callback.path, sorted({t.retries for t in dag.tasks})))
PY
    for d in kn_pipeline_hang_ngay kn_giam_sat_streaming kn_tinh_ab kn_bao_tri kn_tai_lap; do
      echo; echo ">> airflow dags list-runs $d (5 lượt gần nhất)"
      docker exec "$AF" airflow dags list-runs "$d" 2> /dev/null | head -7
    done
  fi
  echo; echo ">> tail -20 logs/canh_bao.log"; tail -20 logs/canh_bao.log 2> /dev/null || echo "(chưa có cảnh báo nào)"
} > "$RA/05_airflow.txt" 2>&1

buoc "6/9 streaming"
curl -s -m 15 "http://$MASTER_IP:8080/json/" | python3 -c '
import json, sys, time
d = json.load(sys.stdin)
print("Thời điểm: {} | worker sống: {} | lõi đang dùng: {}/{}".format(
    time.strftime("%Y-%m-%d %H:%M:%S"), d.get("aliveworkers"), d.get("coresused"), d.get("cores")))
for a in d.get("activeapps", []):
    phut = (time.time() * 1000 - a["starttime"]) / 60000
    print("{:28s} {:26s} lõi={:<3} đã chạy {:7.1f} phút".format(a["name"], a["id"], a.get("cores"), phut))
' > "$RA/06_streaming.txt" 2>&1
bash scripts/luong.sh trang_thai >> "$RA/06_streaming.txt" 2>&1

buoc "7/9 độ trễ web ($WEB_URL)"
python3 scripts/do_do_tre_web.py --web "$WEB_URL" --so-lan 20 > "$RA/07_do_tre_web.md" 2>&1

buoc "8/9 EDA trên CSV gốc (vài phút)"
bash ha_tang/spark/submit.sh lo kiem_tra/eda.py > "$RA/08_eda.log" 2>&1
tail -n 1 "$RA/08_eda.log" > "$RA/eda.json"

buoc "9/9 phân tích lỗi ALS so với baseline (vài phút)"
bash ha_tang/spark/submit.sh lo kiem_tra/phan_tich_loi.py > "$RA/09_phan_tich_loi.log" 2>&1
tail -n 1 "$RA/09_phan_tich_loi.log" > "$RA/phan_tich_loi.json"

tar -C "$HOME/bd03_bang_chung" -czf "$HOME/bd03_bang_chung/$LUC.tar.gz" "$LUC"
echo
echo "Xong: $RA"
echo "Trên laptop: scp $(hostname):~/bd03_bang_chung/$LUC.tar.gz .   rồi giải nén vào docs/bang_chung/cum/"
grep -l "!!\|Lỗi\|Error" "$RA"/* 2> /dev/null | sed 's/^/Có lỗi cần xem: /'
