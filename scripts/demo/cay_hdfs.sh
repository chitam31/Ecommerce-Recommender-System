#!/usr/bin/env bash
# cay_hdfs.sh - xem hồ dữ liệu trên HDFS cho demo và báo cáo (mục A.4, A.6.3, C.4 đề cương). Chạy trên máy master.
#
#   bash scripts/demo/cay_hdfs.sh          # du -h từng thư mục + cây thư mục (hdfs dfs -ls -R, chỉ thư mục, 3 cấp)
#   bash scripts/demo/cay_hdfs.sh --tep    # thêm vài tệp mẫu của một bảng (để thấy tệp parquet và _delta_log)
#   bash scripts/demo/cay_hdfs.sh --tho    # hdfs dfs -ls -R nguyên bản (rất dài: mọi tệp parquet, log Delta, checkpoint)
#
# "hdfs dfs -ls -R" trên cả hồ dữ liệu in hàng chục nghìn dòng (mỗi lô streaming, mỗi commit Delta là
# vài tệp), nên mặc định chỉ giữ dòng thư mục và bỏ phân vùng ngay=..., _delta_log, nội dung checkpoint.
set -uo pipefail
GOC="${LAKE_DUONG_DAN:-/lakehouse/bd03_khuyen_nghi}"
HD="docker exec hadoop-master hdfs dfs"

echo ">> hdfs dfs -du -h $GOC"
$HD -du -h "$GOC"
echo; echo ">> hdfs dfs -du -s -h $GOC"
$HD -du -s -h "$GOC"
echo; echo ">> hdfs dfs -count $GOC/*   (số thư mục, số tệp, số byte, đường dẫn)"
$HD -count "$GOC/*"

if [ "${1:-}" = "--tho" ]; then
  echo; echo ">> hdfs dfs -ls -R $GOC"
  $HD -ls -R "$GOC"
  exit 0
fi

echo; echo ">> hdfs dfs -ls -R $GOC   (chỉ thư mục, tối đa 3 cấp dưới gốc; bỏ ngay=..., _delta_log, nội dung checkpoint)"
$HD -ls -R "$GOC" | awk -v goc="$GOC" '
  $1 ~ /^d/ {
    p = $NF; sub(/^hdfs:\/\/[^\/]*/, "", p)
    r = substr(p, length(goc) + 2); n = split(r, phan, "/")
    if (n > 3 || p ~ /\/ngay=|_delta_log|_change_data|_spark_metadata|\/checkpoints\/[^\/]+\// ) next
    printf "%-11s %s %s  %s\n", $1, $6, $7, p
  }'

if [ "${1:-}" = "--tep" ]; then
  echo; echo ">> hdfs dfs -ls $GOC/silver/tuong_tac | head   (bảng Delta phân vùng theo ngay)"
  $HD -ls "$GOC/silver/tuong_tac" | head -8
  echo; echo ">> hdfs dfs -ls $GOC/silver/tuong_tac/_delta_log | tail -5   (nhật ký giao dịch Delta)"
  $HD -ls "$GOC/silver/tuong_tac/_delta_log" | tail -5
fi
