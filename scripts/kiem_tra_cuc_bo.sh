#!/usr/bin/env bash
# kiem_tra_cuc_bo.sh - chạy TOÀN BỘ pipeline trên một máy (Spark local + Delta thật) với dữ liệu
# mẫu nhỏ, không cần cụm, không cần Kafka (luồng đọc từ thư mục JSON Lines). Mất khoảng 10-15 phút.
#
# Cần: Java 17, pip install -r requirements.txt, và một trong hai:
#   - Internet tới Maven Central (mặc định dùng --packages), hoặc
#   - DELTA_JARS=/duong/delta-spark_2.12-3.2.0.jar,/duong/delta-storage-3.2.0.jar
#
#   bash scripts/kiem_tra_cuc_bo.sh
#   BO_NHO=2g bash scripts/kiem_tra_cuc_bo.sh     # máy ít RAM (WSL2): giảm bộ nhớ driver, mặc định 3g
set -euo pipefail
GOC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TAM="${TAM:-/tmp/bd03_cuc_bo}"
PY="${PY:-python3}"
export LAKE_GOC="file://$TAM/lake"
export PYTHONPATH="$GOC/jobs"
if [ -n "${DELTA_JARS:-}" ]; then GOI="--jars $DELTA_JARS"; else GOI="--packages io.delta:delta-spark_2.12:3.2.0"; fi
export PYSPARK_SUBMIT_ARGS="--master local[4] --driver-memory ${BO_NHO:-3g} $GOI --conf spark.sql.shuffle.partitions=8 --conf spark.ui.enabled=false pyspark-shell"

chay() { echo; echo ">>> $*"; "$PY" "$GOC/jobs/$1" "${@:2}" 2>&1 | grep -v -E "WARN|^\s*$" | tail -n 5; }

rm -rf "$TAM"; mkdir -p "$TAM/lake/landing/retailrocket"
"$PY" "$GOC/scripts/tao_du_lieu_mau.py" --thu-muc "$TAM/mau"
cp "$TAM"/mau/*.csv "$TAM/lake/landing/retailrocket/"
"$PY" "$GOC/producer/phat_lai_su_kien.py" --tep "$TAM/mau/events.csv" --ra-thu-muc "$TAM/su_kien" --giay-moi-ngay 0

chay kiem_tra/khoi_tao_bang.py
chay transform/nhap_danh_muc.py
chay streaming/nap_su_kien.py --nguon tep --thu-muc-tep "$TAM/su_kien" --mot-lan
chay transform/bronze_to_silver.py
chay transform/thong_ke_san_pham.py
chay ml/san_pham_lien_quan.py
chay streaming/tinh_thinh_hanh.py --nguon tep --thu-muc-tep "$TAM/su_kien" --mot-lan --bat-dau earliest
chay ml/chuan_bi_ma_tran.py
chay ml/huan_luyen_als.py --ma-chay rcucbo --luoi toi_thieu --max-iter 5
chay ml/danh_gia.py --ma-chay rcucbo
chay ml/sinh_goi_y_toan_bo.py --ma-chay rcucbo
chay ml/tron_goi_y.py --ma-chay rcucbo
chay ml/ghi_gold.py --ma-chay rcucbo
chay transform/mo_phong_hien_thi.py --so-nguoi 500
chay transform/tinh_ket_qua_ab.py
chay kiem_tra/tai_lap_ab.py
chay kiem_tra/tai_lap_huan_luyen.py --ma-chay rcucbo
chay transform/tien_hoa_luoc_do.py
chay bao_tri/do_zorder.py --phan-manh 50 --so-truy-van 10
chay bao_tri/toi_uu_delta.py
echo; echo "Xong. Dữ liệu ở $TAM/lake. Xem web với dữ liệu này (rồi mở http://localhost:8000):"
echo "  cd \"$GOC/web\" && WEB_CHE_DO=delta WEB_KAFKA=tat LAKE_GOC=$LAKE_GOC PYTHONPATH=\"$GOC/jobs\" PYSPARK_SUBMIT_ARGS=\"$GOI pyspark-shell\" uvicorn app:app --port 8000"
