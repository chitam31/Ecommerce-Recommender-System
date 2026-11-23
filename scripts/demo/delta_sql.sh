#!/usr/bin/env bash
# delta_sql.sh - chạy câu SQL Delta Lake trên cụm để demo (mục A.6.2 và C.4 đề cương). Chạy trên máy master.
#
#   bash scripts/demo/delta_sql.sh lich_su gold.nhat_ky_hien_thi [5]   # DESCRIBE HISTORY (5 phiên bản gần nhất)
#   bash scripts/demo/delta_sql.sh phien_ban gold.ket_qua_ab 3         # SELECT ... VERSION AS OF 3 so với bản hiện tại
#   bash scripts/demo/delta_sql.sh xac_nhan_hien_thi <ma_nguoi_dung>   # impression mới nhất của user (web -> Kafka -> MERGE)
#   bash scripts/demo/delta_sql.sh xac_nhan_tai_lap                    # dòng web vừa ghi trực tiếp vào gold.ket_qua_tai_lap
#   bash scripts/demo/delta_sql.sh cach_ly                             # _quarantine theo lý do loại
#   bash scripts/demo/delta_sql.sh demo [ma_nguoi_dung]                # gộp HISTORY + VERSION AS OF + xác nhận, MỘT lần khởi động
#   bash scripts/demo/delta_sql.sh sql "SELECT count(*) FROM {gold.ket_qua_ab}"   # {tên bảng} -> delta.`đường dẫn`
#   bash scripts/demo/delta_sql.sh cau "DESCRIBE HISTORY {silver.tuong_tac}" "THAO_TAC {gold.ket_qua_ab}"  # nhiều câu
#
# Chạy jobs/kiem_tra/chay_sql.py bằng spark-submit --master local[2] TRONG container spark-master: không
# lấy lõi của cụm, không tranh với 3 luồng streaming, đọc HDFS thật. Mỗi lần gọi mất khoảng 15-20 giây
# khởi động JVM, nên lúc demo dùng "demo" (mọi câu trong một lần). Log Spark ghi ra /tmp/bd03_delta_sql.log.
set -uo pipefail
THU_MUC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ -f "$THU_MUC/cau_hinh/cum.env" ] && . "$THU_MUC/cau_hinh/cum.env"

thay_bang() {  # thay mọi {ten.bang} trong câu SQL bằng delta.`đường dẫn` (bronze là Parquet: parquet.`...`)
  python3 - "$THU_MUC" "$1" <<'PY'
import re, sys
sys.path.insert(0, sys.argv[1] + "/jobs")
from common import cau_hinh as ch
dang = lambda t: ("parquet" if t == "bronze.su_kien" else "delta") + ".`{}`".format(ch.duong_dan(t))
print(re.sub(r"\{([A-Za-z_]+\.[A-Za-z_0-9]+)\}", lambda m: dang(m.group(1)), sys.argv[2]))
PY
}

bang() { thay_bang "{$1}"; }  # tên bảng logic -> delta.`đường dẫn đầy đủ`, đúng gốc lakehouse hiện tại

chay() {  # mỗi đối số là một câu SQL (hoặc "# tiêu đề")
  docker exec -i -e BD03_SQL_DAY_DU="${BD03_SQL_DAY_DU:-0}" spark-master /opt/spark/bin/spark-submit \
    --master "local[2]" --driver-memory 1g \
    --conf spark.ui.enabled=false \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
    /opt/spark/jobs/kiem_tra/chay_sql.py "$@" 2> /tmp/bd03_delta_sql.log
  local ma=$?
  [ $ma -ne 0 ] && echo "(có câu lỗi; log Spark: /tmp/bd03_delta_sql.log)" >&2
  return $ma
}

LENH="${1:-}"; shift || true
case "$LENH" in
  lich_su)
    B="$(bang "${1:?Thiếu tên bảng, ví dụ gold.nhat_ky_hien_thi}")"
    chay "DESCRIBE HISTORY $B LIMIT ${2:-5}" ;;
  phien_ban)
    B="$(bang "${1:?Thiếu tên bảng}")"; V="${2:?Thiếu số phiên bản}"
    chay "SELECT 'VERSION AS OF $V' AS ban, count(*) AS so_dong FROM $B VERSION AS OF $V
          UNION ALL SELECT 'hien_tai', count(*) FROM $B" ;;
  xac_nhan_hien_thi)
    U="${1:?Thiếu ma_nguoi_dung}"
    chay "SELECT ma_hien_thi, nhom_ab, vi_tri, ma_san_pham, thoi_diem_hien_thi, da_click, da_mua, nguon_traffic
          FROM $(bang gold.nhat_ky_hien_thi) WHERE ma_nguoi_dung = '$U'
          AND thoi_diem_hien_thi >= current_timestamp() - INTERVAL 2 HOURS ORDER BY thoi_diem_hien_thi DESC, vi_tri LIMIT 12" ;;
  xac_nhan_tai_lap)
    chay "SELECT thoi_diem, loai, khoa, phien_ban_delta, lech_lon_nhat, trung_khop
          FROM $(bang gold.ket_qua_tai_lap) ORDER BY thoi_diem DESC LIMIT 5" ;;
  cach_ly)
    chay "SELECT ly_do, count(*) AS so_dong, count(DISTINCT ma_nguoi_dung) AS so_nguoi_dung
          FROM $(bang _quarantine.tuong_tac) GROUP BY ly_do ORDER BY so_dong DESC" ;;
  demo)
    U="${1:-}"; HT="$(bang gold.nhat_ky_hien_thi)"; KQ="$(bang gold.ket_qua_ab)"; TL="$(bang gold.ket_qua_tai_lap)"
    CAU=(
      "# 1. DESCRIBE HISTORY gold.nhat_ky_hien_thi (luồng ghi_hien_thi MERGE mỗi 30 giây)"
      "DESCRIBE HISTORY $HT LIMIT 4"
      "# 2. VERSION AS OF: gold.ket_qua_ab ở phiên bản 1 so với hiện tại"
      "SELECT 'VERSION AS OF 1' AS ban, count(*) AS so_dong, max(phien_ban_delta_du_lieu) AS pb_du_lieu FROM $KQ VERSION AS OF 1
       UNION ALL SELECT 'hien_tai', count(*), max(phien_ban_delta_du_lieu) FROM $KQ"
      "# 3. Ghi ngược trực tiếp từ web (nút Chạy lại): gold.ket_qua_tai_lap"
      "SELECT thoi_diem, loai, khoa, phien_ban_delta, trung_khop FROM $TL ORDER BY thoi_diem DESC LIMIT 3"
    )
    if [ -n "$U" ]; then
      CAU+=("# 4. Ghi ngược qua Kafka: impression và click mới nhất của $U trong gold.nhat_ky_hien_thi (dòng có click lên đầu)"
            "SELECT ma_hien_thi, vi_tri, ma_san_pham, thoi_diem_hien_thi, da_click, da_mua FROM $HT
             WHERE ma_nguoi_dung = '$U' AND thoi_diem_hien_thi >= current_timestamp() - INTERVAL 2 HOURS
             ORDER BY da_click DESC, thoi_diem_hien_thi DESC, vi_tri LIMIT 10")
    fi
    chay "${CAU[@]}" ;;
  sql)
    chay "$(thay_bang "${1:?Thiếu câu SQL}")" ;;
  cau)  # nhiều câu, mỗi đối số một câu, dùng {tên bảng}; dùng trong scripts/thu_bang_chung.sh
    CAU=(); for c in "$@"; do CAU+=("$(thay_bang "$c")"); done
    chay "${CAU[@]}" ;;
  *)
    sed -n '2,10p' "${BASH_SOURCE[0]}"; exit 2 ;;
esac
