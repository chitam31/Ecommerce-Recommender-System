#!/usr/bin/env bash
# chay_lan_dau.sh - chạy tay toàn bộ pipeline lần đầu trên cụm (máy master), theo đúng thứ tự
# mà DAG kn_pipeline_hang_ngay sẽ chạy hằng đêm. Dùng để có dữ liệu demo trước buổi bảo vệ.
#
#   bash scripts/chay_lan_dau.sh            # đủ 27 tổ hợp tuning
#   LUOI=toi_thieu bash scripts/chay_lan_dau.sh
#   TU_BUOC=4 bash scripts/chay_lan_dau.sh  # chạy tiếp từ bước 4 sau khi bước đó bị lỗi
#   TU_BUOC=6 MA_CHAY=r20261110T120000 bash scripts/chay_lan_dau.sh   # từ bước 6 cần mã lần tuning ở bước 5
#
# Mọi job đều chạy lại được an toàn (MERGE theo khóa, ghi đè đúng phân vùng, checkpoint của
# streaming), nên chạy lại một bước đã xong chỉ tốn thời gian, không làm sai dữ liệu.
set -euo pipefail
THU_MUC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
S="bash $THU_MUC/ha_tang/spark/submit.sh"
LAKE="${LAKE_DUONG_DAN:-/lakehouse/bd03_khuyen_nghi}"
LUOI="${LUOI:-day_du}"
TU_BUOC="${TU_BUOC:-1}"
if [ "$TU_BUOC" -ge 6 ] && [ -z "${MA_CHAY:-}" ]; then
  echo "Bắt đầu từ bước $TU_BUOC cần MA_CHAY của lần tuning ở bước 5 (dòng 'ma_chay=...' trong log lần trước)." >&2
  exit 2
fi
MA_CHAY="${MA_CHAY:-r$(date -u +%Y%m%dT%H%M%S)}"
tieu_de() { echo; echo "=================== $* ==================="; }

buoc_1() {
  tieu_de "1. Tạo thư mục HDFS và bảng Delta"
  docker exec hadoop-master hdfs dfs -mkdir -p "$LAKE/landing/retailrocket" "$LAKE/models" "$LAKE/checkpoints" "$LAKE/_quarantine"
  # Kiểm tra đầu vào trước, để không chạy nửa chừng mới báo thiếu tệp.
  for t in events.csv item_properties_part1.csv item_properties_part2.csv category_tree.csv; do
    docker exec hadoop-master hdfs dfs -test -e "$LAKE/landing/retailrocket/$t" || {
      echo "Thiếu $LAKE/landing/retailrocket/$t trên HDFS: chạy bash scripts/tai_du_lieu.sh <thư mục CSV> trước." >&2
      exit 1
    }
  done
  $S nhanh kiem_tra/khoi_tao_bang.py
}

buoc_2() {
  tieu_de "2. Danh mục sản phẩm -> silver.san_pham"
  $S lo transform/nhap_danh_muc.py
}

buoc_3() {
  tieu_de "3. Kafka -> bronze (xử lý hết dữ liệu đang có trong topic rồi dừng)"
  $S nap streaming/nap_su_kien.py --mot-lan
}

buoc_4() {
  tieu_de "4. bronze -> silver -> thống kê, sản phẩm liên quan"
  $S lo transform/bronze_to_silver.py
  $S lo transform/thong_ke_san_pham.py
  $S lo ml/san_pham_lien_quan.py
}

buoc_5() {
  tieu_de "5. Ma trận, tuning ($LUOI), đánh giá  [ma_chay=$MA_CHAY]"
  $S lo ml/chuan_bi_ma_tran.py
  $S als ml/huan_luyen_als.py --ma-chay "$MA_CHAY" --luoi "$LUOI"
  $S als ml/danh_gia.py --ma-chay "$MA_CHAY"
}

buoc_6() {
  tieu_de "6. Sinh gợi ý, trộn, ghi gold  [ma_chay=$MA_CHAY]"
  $S als ml/sinh_goi_y_toan_bo.py --ma-chay "$MA_CHAY"
  $S lo ml/tron_goi_y.py --ma-chay "$MA_CHAY"
  $S lo ml/ghi_gold.py --ma-chay "$MA_CHAY"
}

buoc_7() {
  tieu_de "7. Traffic mô phỏng cho A/B rồi tính kết quả A/B"
  $S lo transform/mo_phong_hien_thi.py --so-nguoi 5000
  $S lo transform/tinh_ket_qua_ab.py
}

buoc_8() {
  tieu_de "8. Số liệu OPTIMIZE/ZORDER (phân mảnh 200 tệp để mô phỏng nhiều ngày MERGE)"
  $S bao_tri bao_tri/do_zorder.py --phan-manh 200 --so-truy-van 30
}

echo "Bắt đầu $(date '+%F %T'), từ bước $TU_BUOC, lưới tuning $LUOI, ma_chay=$MA_CHAY"
docker exec hadoop-master hdfs dfsadmin -safemode wait
for n in $(seq "$TU_BUOC" 8); do
  "buoc_$n"
done

echo; echo "Xong $(date '+%F %T'). ma_chay=$MA_CHAY. Bật 3 luồng: bash scripts/luong.sh khoi_dong tat_ca"
