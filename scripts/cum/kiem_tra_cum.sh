#!/usr/bin/env bash
# kiem_tra_cum.sh - chạy từ BẤT KỲ máy nào trong ZeroTier: kiểm tra cổng của từng máy theo
# vai trò, số DataNode sống, số Spark worker sống và tổng số lõi.
set -uo pipefail
THU_MUC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
. "$THU_MUC/cau_hinh/cum.env"

declare -A CONG=(
  [namenode]="8020 9870" [datanode]="9866 9864" [spark-master]="7077 8080"
  [spark-worker]="8081" [kafka]="9092" [airflow]="8088" [web]="8000" [may-demo]=""
)
mo_cong() { timeout 3 bash -c "</dev/tcp/$1/$2" 2>/dev/null; }

LOI=0
printf "%-18s %-15s %-40s %s\n" "MÁY" "IP" "VAI TRÒ" "CỔNG"
for bien in $(compgen -v MAY_); do
  IFS='|' read -r ten ip vai_tro _ _ <<< "${!bien}"
  ket_qua=""
  IFS=',' read -ra cac_vai <<< "$vai_tro"
  for v in "${cac_vai[@]}"; do
    for c in ${CONG[$v]:-}; do
      if mo_cong "$ip" "$c"; then ket_qua+="$c:OK "; else ket_qua+="$c:DONG "; LOI=1; fi
    done
  done
  printf "%-18s %-15s %-40s %s\n" "$ten" "$ip" "$vai_tro" "${ket_qua:--}"
done

echo
echo "== HDFS"
curl -s -m 10 "http://$SPARK_MASTER_IP:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" \
  | python3 -c "import json,sys; b=json.load(sys.stdin)['beans'][0]; print('DataNode sống:', b['NumLiveDataNodes'], '| chết:', b['NumDeadDataNodes'], '| dung lượng còn (GB):', round(b['CapacityRemaining']/1e9,1))" \
  || { echo "Không đọc được JMX của NameNode"; LOI=1; }

echo "== Spark"
curl -s -m 10 "http://$SPARK_MASTER_IP:8080/json/" \
  | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('Worker sống:', d['aliveworkers'], '| lõi:', d['cores'], '| lõi đang dùng:', d['coresused'], '| RAM (MB):', d['memory'])
for w in d['workers']:
    print('   ', w['host'], w['state'], w['cores'], 'lõi', w['memory'], 'MB')
print('Ứng dụng đang chạy:', [a['name'] for a in d.get('activeapps', [])])" \
  || { echo "Không đọc được Spark Master"; LOI=1; }

[ "$LOI" = "0" ] && echo "== Cụm ổn" || echo "== Có thành phần chưa sẵn sàng (xem các dòng DONG ở trên)"
exit $LOI
