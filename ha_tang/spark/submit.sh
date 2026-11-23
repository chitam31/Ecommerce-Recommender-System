#!/usr/bin/env bash
# submit.sh - nộp job PySpark vào cụm. Chạy trên máy master (10.68.71.113).
#
# Cách dùng:  ha_tang/spark/submit.sh <ho_so> <job.py> [tham số của job...]
#   Ví dụ:    ha_tang/spark/submit.sh lo transform/bronze_to_silver.py
#             ha_tang/spark/submit.sh als ml/huan_luyen_als.py --luoi day_du
#
# Mỗi hồ sơ có cổng driver riêng để nhiều job chạy song song, và giới hạn số lõi
# riêng: ở chế độ standalone, job đến trước sẽ chiếm hết lõi nếu không đặt cores.max.
set -euo pipefail

THU_MUC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ -f "$THU_MUC/cau_hinh/cum.env" ] && . "$THU_MUC/cau_hinh/cum.env"

MASTER_IP="${SPARK_MASTER_IP:-10.68.71.113}"
HO_SO="${1:?Thieu ho so: nap|thinh_hanh|hien_thi|lo|als|bao_tri|tai_lap|nhanh}"
JOB="${2:?Thieu duong dan job, vi du transform/bronze_to_silver.py}"
shift 2

# hồ sơ -> cổng driver, cổng UI, số lõi tối đa, lõi mỗi executor, RAM executor, RAM driver, shuffle.
# Cỡ theo máy thật 7 GB RAM: driver chạy NGAY TRONG container spark-master trên máy master
# (PySpark không hỗ trợ cluster deploy mode ở standalone), nên RAM driver phải nhỏ để máy
# master đủ chỗ cho NameNode, Airflow và các luồng streaming cùng lúc.
case "$HO_SO" in
  nap)        CONG=4041; UI=4140; LOI=1;  NHAN=1; RAM_E=768m; RAM_D=512m; SHUFFLE=4  ;;
  thinh_hanh) CONG=4043; UI=4141; LOI=2;  NHAN=2; RAM_E=1g;   RAM_D=768m; SHUFFLE=8  ;;
  hien_thi)   CONG=4045; UI=4142; LOI=1;  NHAN=1; RAM_E=768m; RAM_D=640m; SHUFFLE=4  ;;
  lo)         CONG=4047; UI=4143; LOI=${LOI_LO:-8};   NHAN=2; RAM_E=${RAM_LO:-2g};  RAM_D=1g;    SHUFFLE=64 ;;
  als)        CONG=4049; UI=4144; LOI=${LOI_ALS:-10}; NHAN=2; RAM_E=${RAM_ALS:-2g}; RAM_D=1536m; SHUFFLE=64 ;;
  bao_tri)    CONG=4051; UI=4145; LOI=4;  NHAN=2; RAM_E=1536m; RAM_D=1g;  SHUFFLE=32 ;;
  tai_lap)    CONG=4053; UI=4146; LOI=${LOI_ALS:-10}; NHAN=2; RAM_E=${RAM_ALS:-2g}; RAM_D=1536m; SHUFFLE=64 ;;
  nhanh)      CONG=4055; UI=4147; LOI=2;  NHAN=1; RAM_E=768m; RAM_D=640m; SHUFFLE=8  ;;
  *) echo "Ho so khong hop le: $HO_SO" >&2; exit 2 ;;
esac

# Luồng thịnh hành giữ trạng thái của hàng chục nghìn (cửa sổ, sản phẩm): để trong RocksDB thay
# vì heap JVM, executor 1 GB vẫn đủ. Đổi provider làm checkpoint cũ không dùng lại được,
# nên chỉ áp dụng cho hồ sơ này ngay từ đầu.
THEM_CONF=()
if [ "$HO_SO" = "thinh_hanh" ]; then
  THEM_CONF=(--conf spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider)
fi

# Chốt an toàn khi chuyển gốc hồ dữ liệu (15/11/2026): mã từ bản này đọc/ghi /lakehouse/bd03_khuyen_nghi.
# Nếu dữ liệu vẫn còn ở /lake (chưa chạy scripts/cum/chuyen_lakehouse.sh) thì KHÔNG nộp job: job chạy
# với gốc mới sẽ tạo bảng/checkpoint rỗng ở đó và làm bước chuyển sau này bị chặn. Hỏi qua WebHDFS
# (một lệnh curl, không khởi động JVM). Không hỏi được NameNode thì cứ nộp, job tự báo lỗi.
LAKE_DUONG_DAN="${LAKE_DUONG_DAN:-/lakehouse/bd03_khuyen_nghi}"
if [ -z "${LAKE_GOC:-}" ] && [ "${BO_QUA_KIEM_TRA_GOC:-0}" != "1" ]; then
  _trang_thai() { curl -s -o /dev/null -w '%{http_code}' -m 10 "http://$MASTER_IP:9870/webhdfs/v1$1?op=GETFILESTATUS" || true; }
  if [ "$(_trang_thai "$LAKE_DUONG_DAN/silver")" = "404" ] && [ "$(_trang_thai /lake/silver/tuong_tac)" = "200" ]; then
    echo "!! Dữ liệu còn ở /lake nhưng mã đã dùng gốc $LAKE_DUONG_DAN: chạy scripts/cum/chuyen_lakehouse.sh --chay" >&2
    echo "   từ laptop trước (không nộp $JOB để tránh tạo bảng rỗng ở gốc mới)." >&2
    exit 3
  fi
fi

# Đóng gói jobs/common để executor import được. Ghi ra tệp tạm rồi đổi tên một lần,
# để hai job nộp cùng lúc (DAG song song) không đọc phải tệp zip đang ghi dở.
(cd "$THU_MUC/jobs" && python3 - <<'PY'
import os, zipfile
tam = "common.zip.%d" % os.getpid()
with zipfile.ZipFile(tam, "w", zipfile.ZIP_DEFLATED) as z:
    for goc, _, tep in os.walk("common"):
        for t in sorted(tep):
            if t.endswith(".py"):
                z.write(os.path.join(goc, t))
os.replace(tam, "common.zip")
PY
)

# CHAY_NEN=1: chạy tách rời bên trong container spark-master (dùng cho luồng streaming,
# xem scripts/luong.sh); log ghi vào /opt/spark/jobs/../logs/<ho_so>.log trong container.
# Mặc định: chạy tiền cảnh, không dùng -t vì Airflow không có TTY và stdout phải sạch
# để lấy dòng JSON cuối cùng.
if [ "${CHAY_NEN:-0}" = "1" ]; then
  CHE_DO_EXEC=(-d)
  BOC=(sh -c 'mkdir -p /tmp/bd03_logs && exec "$@" > /tmp/bd03_logs/'"$HO_SO"'.log 2>&1' _)
else
  CHE_DO_EXEC=(-i)
  BOC=()
fi

exec docker exec "${CHE_DO_EXEC[@]}" \
  -e HDFS_GOC="${HDFS_GOC:-hdfs://$MASTER_IP:8020}" \
  -e LAKE_DUONG_DAN="$LAKE_DUONG_DAN" \
  -e KAFKA_BROKER="${KAFKA_BROKER:-10.68.71.204:9092}" \
  -e PYTHONPATH=/opt/spark/jobs \
  spark-master "${BOC[@]}" /opt/spark/bin/spark-submit \
  --master "spark://$MASTER_IP:7077" \
  --driver-memory "$RAM_D" \
  --conf "spark.cores.max=$LOI" \
  --conf "spark.executor.memory=$RAM_E" \
  --conf "spark.executor.cores=$NHAN" \
  --conf "spark.sql.shuffle.partitions=$SHUFFLE" \
  --conf "spark.driver.host=$MASTER_IP" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf "spark.driver.port=$CONG" \
  --conf "spark.driver.blockManager.port=$((CONG + 1))" \
  --conf "spark.ui.port=$UI" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
  --conf "spark.hadoop.dfs.replication=${HE_SO_SAO_CHEP:-2}" \
  --conf spark.hadoop.dfs.client.socket-timeout=180000 \
  --conf spark.task.maxFailures=8 \
  --conf spark.network.timeout=300s \
  --conf spark.executor.heartbeatInterval=30s \
  --conf spark.shuffle.io.maxRetries=10 \
  --conf spark.shuffle.io.retryWait=10s \
  --conf spark.cleaner.periodicGC.interval=5min \
  "${THEM_CONF[@]}" \
  --py-files /opt/spark/jobs/common.zip \
  "/opt/spark/jobs/$JOB" "$@"
