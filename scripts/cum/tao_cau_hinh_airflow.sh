#!/usr/bin/env bash
# tao_cau_hinh_airflow.sh - tạo ha_tang/airflow/.env và mat_khau.json trên máy master, không
# cần sửa tay (thay cho các bước nano ở docs/TRIEN_KHAI_CUM.md mục 2.2). Chạy trên máy master:
#
#   bash scripts/cum/tao_cau_hinh_airflow.sh                 # mật khẩu admin sinh ngẫu nhiên, in ra một lần
#   MAT_KHAU_AIRFLOW='...' bash scripts/cum/tao_cau_hinh_airflow.sh   # tự đặt mật khẩu
#   GHI_DE=1 bash scripts/cum/tao_cau_hinh_airflow.sh        # tạo lại khi đã có
#
# Cả hai tệp nằm trong .gitignore. Ghi lại mật khẩu: web trên máy labsoe-18 cần nó (AIRFLOW_PASS).
set -euo pipefail
THU_MUC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
A="$THU_MUC/ha_tang/airflow"
UID_MAY="$(id -u)"
if [ -f "$A/.env" ] && [ -f "$A/mat_khau.json" ] && [ "${GHI_DE:-0}" != "1" ]; then
  # .env tạo trước khi có AIRFLOW_UID: chỉ bổ sung dòng này, giữ nguyên khóa và mật khẩu cũ.
  if ! grep -q '^AIRFLOW_UID=' "$A/.env"; then
    echo "AIRFLOW_UID=$UID_MAY" >> "$A/.env"
    echo "Đã thêm AIRFLOW_UID=$UID_MAY vào $A/.env: container Airflow sẽ chạy bằng uid của tài khoản này."
  fi
  chmod 600 "$A/mat_khau.json"
  echo "Đã có $A/.env và mat_khau.json (đặt GHI_DE=1 để tạo lại). Mật khẩu admin nằm trong mat_khau.json."
  exit 0
fi
gid="$(getent group docker | cut -d: -f3)"
[ -n "$gid" ] || { echo "Không thấy nhóm docker trên máy này" >&2; exit 1; }
ngau_nhien() { python3 -c "import secrets, sys; print(secrets.token_urlsafe(int(sys.argv[1])))" "$1"; }
jwt="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
fernet="$(python3 -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')"
mk="${MAT_KHAU_AIRFLOW:-$(ngau_nhien 12)}"

# Ký tự trong các giá trị chỉ gồm chữ, số, - _ =, nên dùng '#' làm dấu phân cách của sed là an toàn.
sed -e "s#^BD03_HOST_DIR=.*#BD03_HOST_DIR=$THU_MUC#" \
    -e "s#^AIRFLOW_UID=.*#AIRFLOW_UID=$UID_MAY#" \
    -e "s#^DOCKER_GID=.*#DOCKER_GID=$gid#" \
    -e "s#^AIRFLOW__API_AUTH__JWT_SECRET=.*#AIRFLOW__API_AUTH__JWT_SECRET=$jwt#" \
    -e "s#^AIRFLOW__CORE__FERNET_KEY=.*#AIRFLOW__CORE__FERNET_KEY=$fernet#" \
    "$A/.env.mau" > "$A/.env"
chmod 600 "$A/.env"
# mat_khau.json được gắn vào container; Airflow chạy bằng AIRFLOW_UID (chính tài khoản này) nên
# đọc và ghi được tệp dù chỉ chủ tệp có quyền (600): tài khoản khác trên máy không đọc được mật khẩu.
python3 -c "import json, sys; json.dump({'admin': sys.argv[1]}, open(sys.argv[2], 'w'))" "$mk" "$A/mat_khau.json"
chmod 600 "$A/mat_khau.json"
echo "Đã tạo $A/.env (BD03_HOST_DIR=$THU_MUC, AIRFLOW_UID=$UID_MAY, DOCKER_GID=$gid) và mat_khau.json."
echo "Tài khoản Airflow: admin / $mk"
echo "Ghi lại mật khẩu này: web trên máy labsoe-18 cần nó (AIRFLOW_PASS trong ha_tang/web/.env.rieng)."
