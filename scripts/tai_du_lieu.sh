#!/usr/bin/env bash
# tai_du_lieu.sh - đưa 4 tệp CSV của RetailRocket từ đĩa máy master lên HDFS
# /lakehouse/bd03_khuyen_nghi/landing (gốc hồ dữ liệu theo mục A.4 đề cương).
#
# Cách đưa CSV về máy master, nên dùng: chạy TRÊN LAPTOP  bash scripts/cum/chep_du_lieu.sh
# (tải bằng khóa Kaggle trên laptop rồi chép sang ~/du_lieu/retailrocket của master và máy Kafka;
# khóa không nằm trên máy lab dùng chung). Nếu master có sẵn lệnh kaggle và ~/.kaggle/kaggle.json
# thì script tự tải khi chưa có CSV.
#
#   bash scripts/tai_du_lieu.sh                       # mặc định ~/du_lieu/retailrocket
#   bash scripts/tai_du_lieu.sh /duong/dan/thu_muc_csv
set -euo pipefail
LAKE="${LAKE_DUONG_DAN:-/lakehouse/bd03_khuyen_nghi}"
DICH="${1:-$HOME/du_lieu/retailrocket}"
mkdir -p "$DICH"
if [ ! -f "$DICH/events.csv" ]; then
  if command -v kaggle > /dev/null && [ -f "$HOME/.kaggle/kaggle.json" ]; then
    kaggle datasets download -d retailrocket/ecommerce-dataset -p "$DICH" --unzip
  else
    echo "Chưa có $DICH/events.csv. Trên laptop chạy: bash scripts/cum/chep_du_lieu.sh" >&2
    exit 1
  fi
fi
ls -lh "$DICH"

# Đưa lên HDFS qua container hadoop-master (máy master). NameNode vừa khởi động nằm ở safe mode
# (từ chối ghi) cho tới khi các DataNode báo cáo xong: chờ thay vì để lệnh ghi thất bại.
echo "Chờ NameNode ra khỏi safe mode..."
docker exec hadoop-master hdfs dfsadmin -safemode wait
docker exec hadoop-master hdfs dfs -mkdir -p $LAKE/landing/retailrocket
for t in events.csv item_properties_part1.csv item_properties_part2.csv category_tree.csv; do
  [ -f "$DICH/$t" ] || { echo "Thiếu tệp $DICH/$t" >&2; exit 1; }
  echo "Đưa $t lên HDFS ($(du -h "$DICH/$t" | cut -f1))..."
  docker cp "$DICH/$t" "hadoop-master:/tmp/$t"
  docker exec hadoop-master hdfs dfs -put -f "/tmp/$t" "$LAKE/landing/retailrocket/$t"
  docker exec hadoop-master rm -f "/tmp/$t"
done
docker exec hadoop-master hdfs dfs -ls -h $LAKE/landing/retailrocket
echo "Bộ phát trên máy Kafka (labsit) đọc ~/du_lieu/retailrocket/events.csv: chep_du_lieu.sh đã chép sẵn."
