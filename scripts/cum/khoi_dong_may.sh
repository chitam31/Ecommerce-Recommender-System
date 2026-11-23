#!/usr/bin/env bash
# khoi_dong_may.sh - chạy trên TỪNG máy của cụm. Tự nhận ra máy mình qua IP ZeroTier,
# sinh tệp .env cho các docker compose rồi bật đúng các dịch vụ theo vai trò trong cau_hinh/cum.env.
#
#   bash scripts/cum/khoi_dong_may.sh              # tự nhận máy, bật dịch vụ
#   bash scripts/cum/khoi_dong_may.sh labsit12     # chỉ định tên máy
#   bash scripts/cum/khoi_dong_may.sh --in-thu     # chỉ in lệnh, không chạy
#   bash scripts/cum/khoi_dong_may.sh --dung       # dừng các dịch vụ của máy này
#   bash scripts/cum/khoi_dong_may.sh --build      # buộc build lại ảnh bd03/spark (sau khi sửa Dockerfile)
#   ZT_MTU=1280 bash scripts/cum/khoi_dong_may.sh   # máy ở ngoài trường: hạ MTU card ZeroTier (cần sudo)
#   bash scripts/cum/khoi_dong_may.sh --chi=web     # chỉ bật/dừng MỘT vai trò, không đụng dịch vụ khác của máy
set -euo pipefail
THU_MUC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
. "$THU_MUC/cau_hinh/cum.env"

IN_THU=0; DUNG=0; BUILD_LAI=0; TEN_CHI_DINH=""; CHI_VAI=""
for a in "$@"; do
  case "$a" in
    --in-thu) IN_THU=1 ;;
    --dung) DUNG=1 ;;
    --build) BUILD_LAI=1 ;;
    --chi=*) CHI_VAI="${a#--chi=}" ;;
    *) TEN_CHI_DINH="$a" ;;
  esac
done

chay() { echo "+ $*"; [ "$IN_THU" = "1" ] || "$@"; }

IP_CUA_MAY="$( (ip -4 -o addr show 2>/dev/null || true) | awk '{print $4}' | cut -d/ -f1 | tr '\n' ' ')"
DONG=""
for bien in $(compgen -v MAY_); do
  IFS='|' read -r ten ip vai_tro so_loi bo_nho <<< "${!bien}"
  if [ -n "$TEN_CHI_DINH" ] && [ "$ten" = "$TEN_CHI_DINH" ]; then DONG="${!bien}"; break; fi
  if [ -z "$TEN_CHI_DINH" ] && [[ " $IP_CUA_MAY " == *" $ip "* ]]; then DONG="${!bien}"; break; fi
done
if [ -z "$DONG" ]; then
  echo "Không tìm thấy máy này trong cau_hinh/cum.env (IP hiện có: $IP_CUA_MAY)."
  echo "Kiểm tra: sudo zerotier-cli listnetworks  (phải thấy mạng 633e31d8a29b038c và IP 10.68.71.x)"
  exit 1
fi
IFS='|' read -r TEN NODE_IP VAI_TRO SO_LOI BO_NHO <<< "$DONG"
if [ -n "$CHI_VAI" ]; then
  # Chỉ một vai trò: dùng khi máy đang chạy dịch vụ khác của cụm (vd. thêm web vào một worker
  # mà không để compose tạo lại DataNode/Spark worker đang phục vụ job).
  if [[ ",$VAI_TRO," != *",$CHI_VAI,"* ]]; then
    echo "Máy $TEN không có vai trò '$CHI_VAI' (vai trò: $VAI_TRO)."; exit 1
  fi
  VAI_TRO="$CHI_VAI"
fi

du_an_hdfs() {  # project compose của HDFS trên máy này (để dùng lại đúng volume dữ liệu cũ)
  local cap
  [ -n "${COMPOSE_PROJECT_NAME:-}" ] && { echo "$COMPOSE_PROJECT_NAME"; return; }
  for cap in ${DU_AN_HADOOP:-}; do
    [ "${cap%%=*}" = "$TEN" ] && { echo "${cap#*=}"; return; }
  done
  echo "hadoop-cluster"
}
DU_AN_HDFS="$(du_an_hdfs)"
echo "== Máy $TEN ($NODE_IP), vai trò: $VAI_TRO, project HDFS: $DU_AN_HDFS"

# Đường mạng ngoài trường (vd. Wi-Fi ở nhà) làm rớt gói ZeroTier lớn: ping 1300 byte qua được,
# 1400 byte thì mất hết, nên TCP treo khi đọc khối HDFS. Hạ MTU của card ZeroTier (mặc định 2800)
# để mọi gói đủ nhỏ. Thiết lập mất khi khởi động lại máy/WSL hoặc dịch vụ ZeroTier.
if [ -n "${ZT_MTU:-}" ]; then
  CARD_ZT="$( (ip -4 -o addr show 2>/dev/null || true) | awk -v ip="$NODE_IP" '$4 ~ "^"ip"/" {print $2; exit}')"
  if [ -n "$CARD_ZT" ]; then
    chay sudo ip link set "$CARD_ZT" mtu "$ZT_MTU"
  else
    echo "!! Không thấy card mạng mang IP $NODE_IP, bỏ qua ZT_MTU"
  fi
fi

# Tệp .env cho từng docker compose (không đưa lên Git). Tệp được sinh lại mỗi lần chạy;
# cấu hình riêng của máy (mật khẩu, WEB_CHE_DO...) đặt trong .env.rieng cùng thư mục,
# nội dung tệp đó được nối vào cuối .env nên không bị mất.
ghi_env() {
  [ "$IN_THU" = "1" ] && { echo "+ ghi $1"; return; }
  printf '%s\n' "${@:2}" > "$1"
  [ -f "$1.rieng" ] && cat "$1.rieng" >> "$1"
  return 0
}
# Mã băm cấu hình HDFS: compose không tự nhận ra nội dung tệp XML được mount đã đổi, nên đưa
# mã băm vào biến môi trường của container để XML đổi thì container được tạo lại.
CAU_HINH_HDFS="$(cat "$THU_MUC"/ha_tang/hadoop/config/*.xml | sha1sum | cut -c1-12)"
ghi_env "$THU_MUC/ha_tang/hadoop/.env" "NODE_IP=$NODE_IP" "COMPOSE_PROJECT_NAME=$DU_AN_HDFS" "CAU_HINH_HDFS=$CAU_HINH_HDFS" \
  "HE_SO_SAO_CHEP=${HE_SO_SAO_CHEP:-2}"
ghi_env "$THU_MUC/ha_tang/spark/.env" "NODE_IP=$NODE_IP" "SPARK_MASTER_IP=$SPARK_MASTER_IP" "SO_LOI=$SO_LOI" "BO_NHO=$BO_NHO" \
  "HDFS_GOC=$HDFS_GOC" "KAFKA_BROKER=$KAFKA_BROKER"
ghi_env "$THU_MUC/ha_tang/kafka/.env" "NODE_IP=$NODE_IP"
ghi_env "$THU_MUC/ha_tang/web/.env" "NODE_IP=$NODE_IP" "HDFS_GOC=$HDFS_GOC" "KAFKA_BROKER=$KAFKA_BROKER"

HADOOP="docker compose -f $THU_MUC/ha_tang/hadoop/docker-compose.yml --env-file $THU_MUC/ha_tang/hadoop/.env"
SPARK_M="docker compose -f $THU_MUC/ha_tang/spark/docker-compose-master.yml --env-file $THU_MUC/ha_tang/spark/.env"
SPARK_W="docker compose -f $THU_MUC/ha_tang/spark/docker-compose-worker.yml --env-file $THU_MUC/ha_tang/spark/.env"
KAFKA="docker compose -f $THU_MUC/ha_tang/kafka/docker-compose.yml --env-file $THU_MUC/ha_tang/kafka/.env"
WEB="docker compose -f $THU_MUC/ha_tang/web/docker-compose.yml --env-file $THU_MUC/ha_tang/web/.env"
AIRFLOW="docker compose -f $THU_MUC/ha_tang/airflow/docker-compose.yml"

co_vai() { [[ ",$VAI_TRO," == *",$1,"* ]]; }

if [ "$DUNG" = "1" ]; then
  co_vai web && chay $WEB down
  co_vai airflow && chay $AIRFLOW down
  co_vai spark-worker && chay $SPARK_W down
  co_vai spark-master && chay $SPARK_M down
  co_vai kafka && chay $KAFKA down
  co_vai datanode && chay $HADOOP stop worker
  co_vai namenode && chay $HADOOP stop master
  exit 0
fi

# Container cũ trùng tên nhưng thuộc project compose khác thì compose sẽ báo lỗi trùng tên.
# Kiểm tra trước để dừng sớm với hướng dẫn rõ ràng, chưa bật dịch vụ nào.
XUNG_DOT=0
kiem_tra_container() {  # $1 = tên container, $2 = project dự án sẽ dùng, $3 = du_lieu nếu có volume dữ liệu
  # Chỉ đọc (docker inspect), nên chạy cả ở chế độ --in-thu để biết trước máy nào sẽ vướng.
  local du_an
  du_an="$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "$1" 2>/dev/null)" || return 0
  [ "$du_an" = "$2" ] && return 0
  XUNG_DOT=1
  echo "!! Container $1 thuộc project '${du_an:-không rõ (chạy bằng docker run)}', dự án cần project '$2'."
  if [ "${3:-}" != "du_lieu" ]; then
    echo "   Container này không giữ dữ liệu, xóa an toàn: docker rm -f $1"
  elif [[ "$1" == hadoop-* ]] && [ -n "$du_an" ]; then
    echo "   Có dữ liệu HDFS: thêm $TEN=$du_an vào DU_AN_HADOOP trong cau_hinh/cum.env để dùng lại volume cũ."
  else
    echo "   Có dữ liệu: đừng xóa vội, báo lại để chỉnh project cho khớp volume cũ."
  fi
}
co_vai namenode && kiem_tra_container hadoop-master "$DU_AN_HDFS" du_lieu
co_vai datanode && kiem_tra_container hadoop-worker "$DU_AN_HDFS" du_lieu
co_vai spark-master && kiem_tra_container spark-master spark-cluster
co_vai spark-worker && kiem_tra_container spark-worker spark-cluster
co_vai kafka && kiem_tra_container kafka-server kafka-docker du_lieu
co_vai kafka && kiem_tra_container kafka-ui kafka-docker
co_vai web && kiem_tra_container bd03-web bd03-web
if [ "$XUNG_DOT" = "1" ] && [ "$IN_THU" = "1" ]; then
  echo "(Chạy thử) Khi chạy thật, script sẽ dừng ở đây cho tới khi xử lý xong các dòng !! ở trên."
elif [ "$XUNG_DOT" = "1" ]; then
  echo "Dừng lại, chưa bật dịch vụ nào. Xử lý các dòng !! ở trên rồi chạy lại."
  exit 1
fi

co_vai namenode && chay $HADOOP up -d master
co_vai datanode && chay $HADOOP up -d worker
co_vai kafka && chay $KAFKA up -d
# Ảnh bd03/spark chỉ build khi máy chưa có (hoặc khi gọi kèm --build). Mã job được gắn qua
# volume nên git pull xong không cần build lại; máy không ra Internet vẫn bật được nếu đã
# nạp sẵn ảnh bằng docker load (xem docs/TRIEN_KHAI_CUM.md, mục 2.1).
CO_BUILD=""
if [ "$BUILD_LAI" = "1" ] || ! docker image inspect bd03/spark:3.5.3 > /dev/null 2>&1; then CO_BUILD="--build"; fi
co_vai spark-master && chay $SPARK_M up -d $CO_BUILD
co_vai spark-worker && chay $SPARK_W up -d $CO_BUILD
if co_vai airflow; then
  if [ -f "$THU_MUC/ha_tang/airflow/.env" ] && [ -f "$THU_MUC/ha_tang/airflow/mat_khau.json" ]; then
    chay $AIRFLOW up -d --build
  else
    echo "!! Chưa có ha_tang/airflow/.env và mat_khau.json: sao chép từ .env.mau, mat_khau.json.mau rồi chạy lại."
  fi
fi
if co_vai web; then
  # Ảnh web kế thừa bd03/spark: build ảnh Spark trước nếu máy này chưa có.
  docker image inspect bd03/spark:3.5.3 > /dev/null 2>&1 || chay docker build -t bd03/spark:3.5.3 "$THU_MUC/ha_tang/spark"
  chay $WEB up -d --build
fi
if co_vai may-demo; then
  IP_WEB="$(for b in $(compgen -v MAY_); do IFS='|' read -r _ ip v _ _ <<< "${!b}"; [[ ",$v," == *",web,"* ]] && echo "$ip"; done | head -1)"
  echo "Máy demo: mở trình duyệt tới http://${IP_WEB:-?}:8000 (web), http://$SPARK_MASTER_IP:8080 (Spark), :9870 (HDFS), :8088 (Airflow)"
fi
echo "== Xong. Kiểm tra toàn cụm: bash scripts/cum/kiem_tra_cum.sh"
