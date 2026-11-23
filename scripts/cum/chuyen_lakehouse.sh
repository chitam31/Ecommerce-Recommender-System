#!/usr/bin/env bash
# chuyen_lakehouse.sh - chuyển hồ dữ liệu từ /lake sang /lakehouse/bd03_khuyen_nghi (mục A.4, A.6.3
# đề cương) cho cả cụm, chạy MỘT LẦN từ máy điều khiển (Git Bash trên laptop), sau khi git pull.
#
#   bash scripts/cum/chuyen_lakehouse.sh            # xem trước: kế hoạch chuyển, không đổi gì
#   bash scripts/cum/chuyen_lakehouse.sh --chay     # chuyển thật (hỏi xác nhận: gõ CHUYEN)
#   bash scripts/cum/chuyen_lakehouse.sh --hoan-tac # đưa dữ liệu về /lake (xem cuối tệp)
#
# Thứ tự khi --chay (mất khoảng 10-15 phút, phần lớn là build lại ảnh web):
#   1. master: tạm dừng DAG 2, 3, 4 (DAG 1 vẫn để tạm dừng như hiện tại) để không job nào chạy giữa chừng
#   2. master: dừng 3 luồng streaming, chờ đến khi không còn ứng dụng Spark nào của dự án
#   3. laptop: đồng bộ mã mới sang mọi máy (ssh_tat_ca.sh --dong-bo-ma)
#   4. master: chuyen_lakehouse.py --thuc-hien (hdfs dfs -mv + viết lại _spark_metadata của bronze)
#   5. master: job kiểm tra đọc bronze/silver/gold ở gốc mới (kiem_tra_lakehouse.py)
#   6. master: bật lại 3 luồng; labsoe-18: build lại và bật web; master: bật lại DAG 2, 3, 4
#   7. in "hdfs dfs -du -h /lakehouse/bd03_khuyen_nghi" (output phải nộp theo A.6.3)
# Bước 5 lỗi thì DỪNG, không bật lại gì: chạy --hoan-tac rồi báo lại.
#
# Không đụng container của nhóm khác; không xóa dữ liệu nào (mv trong cùng HDFS chỉ đổi tên).
set -uo pipefail

GOC_MOI="/lakehouse/bd03_khuyen_nghi"
DAG_TAM_DUNG=(kn_giam_sat_streaming kn_tinh_ab kn_bao_tri)

# ------------------------------------------------ các bước chạy TRÊN máy master (gửi qua stdin)
if [ "${1:-}" = "--tren-master" ]; then
  BUOC="${2:?thiếu bước}"
  cd ~/bd03_khuyen_nghi || { echo "Không có ~/bd03_khuyen_nghi trên master"; exit 1; }
  [ -f cau_hinh/cum.env ] && . cau_hinh/cum.env
  MASTER_IP="${SPARK_MASTER_IP:-10.68.71.113}"
  AF="$(docker ps -q --filter label=com.docker.compose.project=bd03-airflow \
                     --filter label=com.docker.compose.service=airflow-scheduler | head -1)"
  so_app_du_an() {
    curl -s -m 10 "http://$MASTER_IP:8080/json/" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(' '.join(a['name'] for a in d.get('activeapps', []) if a['name'].startswith('bd03-')))"
  }
  case "$BUOC" in
    xem)
      python3 scripts/cum/chuyen_lakehouse.py --xem ;;
    dung)
      [ -n "$AF" ] || { echo "!! Không thấy container airflow-scheduler"; exit 1; }
      for d in "${DAG_TAM_DUNG[@]}"; do docker exec "$AF" airflow dags pause "$d" > /dev/null && echo "-- tạm dừng $d"; done
      bash scripts/luong.sh dung tat_ca
      for _ in $(seq 1 60); do
        con="$(so_app_du_an)"
        [ -z "$con" ] && { echo "-- không còn ứng dụng Spark nào của dự án"; exit 0; }
        echo "   còn chạy: $con (chờ job đang chạy xong, tối đa 10 phút)"; sleep 10
      done
      echo "!! Sau 10 phút vẫn còn ứng dụng Spark của dự án: dừng tại đây, chưa chuyển gì."; exit 1 ;;
    chuyen)
      [ -z "$(so_app_du_an)" ] || { echo "!! Còn ứng dụng Spark của dự án đang chạy"; exit 1; }
      docker exec hadoop-master hdfs dfsadmin -safemode wait > /dev/null
      python3 scripts/cum/chuyen_lakehouse.py --thuc-hien ;;
    kiem_tra)
      bash ha_tang/spark/submit.sh nhanh kiem_tra/kiem_tra_lakehouse.py ;;
    bat_lai)
      bash scripts/luong.sh khoi_dong tat_ca
      [ -n "$AF" ] && for d in "${DAG_TAM_DUNG[@]}"; do docker exec "$AF" airflow dags unpause "$d" > /dev/null && echo "-- bật lại $d"; done
      sleep 20; bash scripts/luong.sh trang_thai ;;
    du)
      docker exec hadoop-master hdfs dfs -du -h "$GOC_MOI" ;;
    hoan_tac)
      bash scripts/luong.sh dung tat_ca
      [ -z "$(so_app_du_an)" ] || { echo "!! Còn ứng dụng Spark của dự án đang chạy"; exit 1; }
      python3 scripts/cum/chuyen_lakehouse.py --hoan-tac ;;
    *) echo "Bước không hợp lệ: $BUOC"; exit 2 ;;
  esac
  exit $?
fi

# ------------------------------------------------ phần chạy trên máy điều khiển
TEP_NAY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
THU_MUC="$(cd "$(dirname "$TEP_NAY")/../.." && pwd)"
SSH_TAT_CA=(bash "$THU_MUC/scripts/cum/ssh_tat_ca.sh")
TUY_CHON_NOI=(-o "ConnectTimeout=${SSH_CHO:-20}" -o ConnectionAttempts=3)
MASTER="$("${SSH_TAT_CA[@]}" --liet-ke --vai-tro namenode | head -1 | cut -d' ' -f1)"
WEB="$("${SSH_TAT_CA[@]}" --tat-ca --liet-ke --vai-tro web | head -1 | cut -d' ' -f1)"
tren_master() { ssh "${TUY_CHON_NOI[@]}" "$MASTER" "bash -s -- --tren-master $1" < "$TEP_NAY"; }

case "${1:-}" in
  "")
    echo "########## Kế hoạch chuyển (mã trên master có thể còn là bản cũ: xem bản trong repo này)"
    (cd "$THU_MUC" && python3 scripts/cum/chuyen_lakehouse.py --xem 2> /dev/null) \
      || echo "(máy này không có python3: kế hoạch nằm trong scripts/cum/chuyen_lakehouse.py, hàm ke_hoach)"
    echo; echo "Chưa đổi gì. Chuyển thật: bash scripts/cum/chuyen_lakehouse.sh --chay"
    exit 0 ;;
  --chay)
    read -r -p "Chuyển /lake -> $GOC_MOI trên cụm (dừng luồng ~10 phút)? Gõ CHUYEN để xác nhận: " xn
    [ "$xn" = "CHUYEN" ] || { echo "Không làm gì."; exit 1; }
    echo "########## 1-2. Tạm dừng DAG 2-4 và dừng luồng ($MASTER)"
    tren_master dung || exit 1
    echo "########## 3. Đồng bộ mã sang các máy"
    "${SSH_TAT_CA[@]}" --tat-ca --dong-bo-ma || { echo "!! Đồng bộ mã lỗi: dữ liệu chưa chuyển, chạy lại lệnh này."; exit 1; }
    echo "########## 4. Chuyển trên HDFS ($MASTER)"
    tren_master chuyen || { echo "!! Chuyển lỗi giữa chừng: bash scripts/cum/chuyen_lakehouse.sh --hoan-tac"; exit 1; }
    echo "########## 5. Kiểm tra đọc dữ liệu ở gốc mới"
    tren_master kiem_tra || { echo "!! Kiểm tra lỗi: KHÔNG bật lại gì. Chạy --hoan-tac và gửi lại output này."; exit 1; }
    echo "########## 6. Bật lại luồng, web, DAG"
    tren_master bat_lai
    if [ -n "$WEB" ]; then
      ssh "${TUY_CHON_NOI[@]}" "$WEB" "cd ~/bd03_khuyen_nghi && bash scripts/cum/khoi_dong_may.sh --chi=web" \
        || echo "!! Bật web lỗi: ssh $WEB rồi chạy bash scripts/cum/khoi_dong_may.sh --chi=web"
    fi
    echo "########## 7. hdfs dfs -du -h $GOC_MOI"
    tren_master du
    echo; echo "Xong. Kiểm tra toàn cụm: bash scripts/cum/kiem_tra_cum.sh" ;;
  --hoan-tac)
    read -r -p "Đưa dữ liệu về /lake? Gõ HOANTAC để xác nhận: " xn
    [ "$xn" = "HOANTAC" ] || { echo "Không làm gì."; exit 1; }
    tren_master hoan_tac || exit 1
    echo
    echo "Đã đưa dữ liệu về /lake. Mã trên các máy vẫn là bản mới (đọc $GOC_MOI): trên laptop"
    echo "checkout lại commit trước khi chuyển rồi chạy ssh_tat_ca.sh --tat-ca --dong-bo-ma, sau đó"
    echo "bật luồng (scripts/luong.sh khoi_dong tat_ca) và bật lại DAG 2-4 trên giao diện Airflow." ;;
  *) echo "Tùy chọn: (trống) | --chay | --hoan-tac" >&2; exit 2 ;;
esac
