#!/usr/bin/env bash
# khao_sat_may.sh - in nhanh cấu hình và trạng thái của MỘT máy. Không cần sudo, không thay đổi gì.
#
# Chạy trên cả cụm từ máy điều khiển (máy lab chưa cần có repo):
#   bash scripts/cum/ssh_tat_ca.sh --song-song --chay-tep scripts/cum/khao_sat_may.sh
# Kết quả dùng để điền so_loi / bo_nho trong cau_hinh/cum.env và biết máy nào còn thiếu gì.

ten_he_dieu_hanh() {
  lsb_release -ds 2>/dev/null || grep PRETTY_NAME /etc/os-release 2>/dev/null | cut -d'"' -f2
}

echo "may: $(hostname) | $(ten_he_dieu_hanh)"
echo "ip_zerotier: $(ip -4 -o addr show 2>/dev/null | awk '{print $4}' | grep '^10\.68\.71\.' | cut -d/ -f1 | tr '\n' ' ')"
echo "loi: $(nproc) | ram_GB: $(free -g | awk '/^Mem:/{print $2}') | ram_con_trong_GB: $(free -g | awk '/^Mem:/{print $7}')"
echo "dia_trong_home: $(df -h "$HOME" | awk 'NR==2{print $4}') | dia_trong_docker: $(df -h /var/lib/docker 2>/dev/null | awk 'NR==2{print $4}')"

if command -v docker > /dev/null 2>&1; then
  echo "docker: $(docker --version | cut -d, -f1 | awk '{print $3}') | compose_v2: $(docker compose version --short 2>/dev/null || echo KHONG_CO)"
  if docker ps > /dev/null 2>&1; then
    echo "quyen_docker: co"
    echo "container:"
    docker ps -a --format '  {{.Names}} | {{.Image}} | {{.Status}} | project={{.Label "com.docker.compose.project"}}'
    echo "volume_cua_cum_cu:"
    docker volume ls --format '  {{.Name}}' | grep -E 'hadoop|kafka|spark' || echo "  (khong co)"
  else
    echo "quyen_docker: KHONG -> sudo usermod -aG docker $(whoami), roi dang xuat va dang nhap lai"
  fi
else
  echo "docker: CHUA CAI"
fi

phien_ban_git="$(git --version 2>/dev/null | awk '{print $3}')"
echo "swap_GB: $(free -g | awk '/^Swap:/{print $2}') | git: ${phien_ban_git:-CHUA CAI}"
echo "dong_ho_dong_bo: $(timedatectl show -p NTPSynchronized --value 2>/dev/null || echo khong_ro)"
if command -v java > /dev/null 2>&1; then
  echo "java: $(java -version 2>&1 | grep -m1 -i version)"
else
  echo "java: khong co (chi can neu chay scripts/kiem_tra_cuc_bo.sh tren may nay)"
fi
if [ -d "$HOME/bd03_khuyen_nghi/.git" ]; then
  echo "repo: co - $(git -C "$HOME/bd03_khuyen_nghi" log --oneline -1 2>/dev/null)"
else
  echo "repo: CHUA CLONE vao ~/bd03_khuyen_nghi"
fi
