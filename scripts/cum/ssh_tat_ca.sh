#!/usr/bin/env bash
# ssh_tat_ca.sh - điều khiển nhiều máy của cụm từ MỘT máy qua SSH.
# Windows: chạy trong cửa sổ Git Bash (có sẵn khi cài Git for Windows). KHÔNG gõ "bash ..." trong
# PowerShell: lệnh đó gọi WSL chứ không phải Git Bash, và PowerShell 5.1 ghi ">" theo UTF-16
# làm hỏng ~/.ssh/config. Linux: chạy bằng bash.
#
# Danh sách máy và IP lấy từ cau_hinh/cum.env. Tên SSH của mỗi máy chính là tên trong danh bạ
# (labsoe-11, labsit12, ...), khai báo trong ~/.ssh/config bằng --tao-config.
#
#   bash scripts/cum/ssh_tat_ca.sh --tao-config > ~/.ssh/config    # một lần (tài khoản lấy từ TAI_KHOAN_SSH)
#   bash scripts/cum/ssh_tat_ca.sh --chep-khoa                       # một lần: chép khóa công khai sang từng máy
#   bash scripts/cum/ssh_tat_ca.sh hostname                          # chạy LẦN LƯỢT trên 6 máy lab, master trước
#   bash scripts/cum/ssh_tat_ca.sh --song-song "nproc; free -g"      # chạy ĐỒNG THỜI, gom kết quả theo máy
#   bash scripts/cum/ssh_tat_ca.sh --vai-tro spark-worker "docker ps"
#   bash scripts/cum/ssh_tat_ca.sh --may labsit12,labsit "cd ~/bd03_khuyen_nghi && git pull"
#   bash scripts/cum/ssh_tat_ca.sh --tty "sudo apt-get install -y chrony"   # lệnh cần gõ mật khẩu sudo
#   bash scripts/cum/ssh_tat_ca.sh --song-song --chay-tep scripts/cum/khao_sat_may.sh   # chạy một script cục bộ trên từng máy
#   bash scripts/cum/ssh_tat_ca.sh --dong-bo-ma        # đẩy mã (bản đã commit) sang ~/bd03_khuyen_nghi của từng máy
#   bash scripts/cum/ssh_tat_ca.sh --liet-ke           # chỉ in "tên IP" của các máy được chọn (dùng trong script khác)
#
# Mặc định chọn các máy hạ tầng của cụm (namenode, datanode, spark-master, spark-worker, kafka),
# tức 6 máy lab; --tat-ca chọn thêm máy web. Máy demo (Windows) không bao giờ được chọn vì đó
# chính là máy đang điều khiển.
set -uo pipefail
THU_MUC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
. "$THU_MUC/cau_hinh/cum.env"

SONG_SONG=0; TTY=0; TAT_CA=0; VAI_TRO=""; DS_MAY=""; TEP=""; CHE_DO="lenh"
SSH_USER_MAC_DINH="${SSH_USER:-hduser}"
KHOA="${KHOA:-$HOME/.ssh/id_ed25519.pub}"
# ZeroTier qua máy chuyển tiếp (relay) có thể mất hơn 10 giây để dựng lại đường đi sau khi
# để yên một lúc: chờ lâu hơn và tự thử lại vài lần thay vì báo "Connection timed out" ngay.
SSH_CHO="${SSH_CHO:-20}"
TUY_CHON_NOI=(-o "ConnectTimeout=$SSH_CHO" -o ConnectionAttempts=3)

while [ $# -gt 0 ]; do
  case "$1" in
    --song-song) SONG_SONG=1 ;;
    --tty) TTY=1 ;;
    --tat-ca) TAT_CA=1 ;;
    --vai-tro) VAI_TRO="${2:?Thiếu tên vai trò}"; shift ;;
    --may) DS_MAY="${2:?Thiếu danh sách máy}"; shift ;;
    --chay-tep) TEP="${2:?Thiếu đường dẫn tệp script}"; shift ;;
    --tao-config) CHE_DO="tao_config" ;;
    --chep-khoa) CHE_DO="chep_khoa" ;;
    --dong-bo-ma) CHE_DO="dong_bo_ma" ;;
    --liet-ke) CHE_DO="liet_ke" ;;
    --) shift; break ;;
    -*) echo "Tùy chọn không hợp lệ: $1" >&2; exit 2 ;;
    *) break ;;
  esac
  shift
done
LENH="$*"

chon_may() {  # in "ten ip" của các máy được chọn, đúng thứ tự trong danh bạ
  local bien ten ip vai
  for bien in $(compgen -v MAY_ | sort -t_ -k2 -n); do
    IFS='|' read -r ten ip vai _ _ <<< "${!bien}"
    [[ ",$vai," == *",may-demo,"* ]] && continue
    if [ -n "$DS_MAY" ]; then
      [[ ",$DS_MAY," == *",$ten,"* ]] || continue
    elif [ -n "$VAI_TRO" ]; then
      [[ ",$vai," == *",$VAI_TRO,"* ]] || continue
    elif [ "$TAT_CA" != "1" ]; then
      [[ "$vai" =~ (namenode|datanode|spark-master|spark-worker|kafka) ]] || continue
    fi
    echo "$ten $ip"
  done
}

# ---------------------------------------------------------------- sinh ~/.ssh/config
tai_khoan() {  # tài khoản SSH của máy $1 theo TAI_KHOAN_SSH trong cum.env
  local cap
  for cap in ${TAI_KHOAN_SSH:-}; do
    [ "${cap%%=*}" = "$1" ] && { echo "${cap#*=}"; return; }
  done
  echo "$SSH_USER_MAC_DINH"
}

if [ "$CHE_DO" = "tao_config" ]; then
  TAT_CA=1
  mapfile -t CAC_MAY < <(chon_may)
  echo "# --- Cụm bd03, sinh từ cau_hinh/cum.env (tài khoản theo TAI_KHOAN_SSH) ---"
  for dong in "${CAC_MAY[@]}"; do
    ten="${dong%% *}"
    [[ " ${TAI_KHOAN_SSH:-} " == *" $ten="* ]] || echo "# $ten: chưa khai báo tài khoản trong TAI_KHOAN_SSH, đang dùng mặc định"
    # accept-new: tự nhận khóa của máy lần đầu kết nối (mạng riêng ZeroTier), nhưng vẫn
    # từ chối nếu khóa của máy đã biết bị thay đổi.
    printf 'Host %s\n    HostName %s\n    User %s\n    IdentityFile ~/.ssh/id_ed25519\n    StrictHostKeyChecking accept-new\n    ConnectTimeout 20\n    ConnectionAttempts 3\n    ServerAliveInterval 30\n\n' \
      "$ten" "${dong#* }" "$(tai_khoan "$ten")"
  done
  exit 0
fi

mapfile -t CAC_MAY < <(chon_may)
[ ${#CAC_MAY[@]} -gt 0 ] || { echo "Không có máy nào khớp điều kiện chọn" >&2; exit 2; }
[ "$CHE_DO" = "liet_ke" ] && { printf '%s\n' "${CAC_MAY[@]}"; exit 0; }

# ---------------------------------------------------------------- chép khóa công khai
if [ "$CHE_DO" = "chep_khoa" ]; then
  [ -f "$KHOA" ] || { echo "Chưa có khóa $KHOA. Tạo bằng: ssh-keygen -t ed25519 -C bd03" >&2; exit 1; }
  LOI=0
  for dong in "${CAC_MAY[@]}"; do
    ten="${dong%% *}"
    echo "===== $ten (${dong#* }): nhập mật khẩu tài khoản trên máy này, chỉ cần một lần"
    # Chỉ thêm khóa nếu chưa có, nên chạy lại nhiều lần cũng không bị trùng dòng.
    ssh "${TUY_CHON_NOI[@]}" "$ten" \
      'k=$(cat); mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys && (grep -qxF "$k" ~/.ssh/authorized_keys || echo "$k" >> ~/.ssh/authorized_keys)' \
      < "$KHOA" || { echo "!! $ten: chép khóa thất bại"; LOI=1; }
  done
  exit $LOI
fi

# ---------------------------------------------------------------- đồng bộ mã nguồn
if [ "$CHE_DO" = "dong_bo_ma" ]; then
  # Đẩy mã từ repo trên máy điều khiển sang ~/bd03_khuyen_nghi của từng máy bằng git push qua SSH.
  # Máy lab không cần token GitHub (tài khoản lab dùng chung): chỉ máy điều khiển kéo từ GitHub.
  # receive.denyCurrentBranch=updateInstead: push xong thì thư mục làm việc trên máy đích được
  # cập nhật luôn; git từ chối nếu ở đó có tệp bị sửa tay, nên không ghi đè mất thay đổi của ai.
  git -C "$THU_MUC" rev-parse --git-dir > /dev/null 2>&1 || { echo "$THU_MUC không phải repo git" >&2; exit 2; }
  [ -z "$(git -C "$THU_MUC" status --porcelain --untracked-files=no)" ] \
    || echo "Lưu ý: repo trên máy này có thay đổi chưa commit; chỉ phần đã commit được đẩy đi."
  if [ "$(git -C "$THU_MUC" rev-parse --is-shallow-repository)" = "true" ]; then
    echo "Repo trên máy này là bản clone nông (shallow), máy đích sẽ từ chối. Chạy trước: git fetch --unshallow" >&2
    exit 2
  fi
  echo "Đẩy bản $(git -C "$THU_MUC" log --oneline -1)"
  LOI=0
  for dong in "${CAC_MAY[@]}"; do
    ten="${dong%% *}"
    echo "===== $ten"
    if ! ssh -n "${TUY_CHON_NOI[@]}" "$ten" 'command -v git > /dev/null || { echo "Máy chưa có git: sudo apt-get install -y git"; exit 3; }
      [ -d ~/bd03_khuyen_nghi/.git ] || git init -q -b main ~/bd03_khuyen_nghi
      git -C ~/bd03_khuyen_nghi config receive.denyCurrentBranch updateInstead'; then
      echo "!! $ten: chưa chuẩn bị được repo trên máy này"; LOI=1; continue
    fi
    # Điều khiển từ mạng ngoài (ở nhà), SSH qua ZeroTier có lúc rớt ở bước bắt tay
    # ("banner exchange"): lỗi mạng thì thử lại tối đa 3 lần; lỗi khác (máy đích có tệp sửa
    # tay, git từ chối) thì dừng ngay, không thử lại vô ích.
    TEP_LOI="$(mktemp)"; DAY=0
    for lan in 1 2 3; do
      if git -C "$THU_MUC" push -q "$ten:bd03_khuyen_nghi" HEAD:main 2> "$TEP_LOI"; then DAY=1; break; fi
      grep -qiE "timed out|connection (reset|closed|refused)|broken pipe|could not resolve" "$TEP_LOI" || break
      [ "$lan" -lt 3 ] && { echo "   mạng chập chờn, thử lại lần $((lan + 1))..."; sleep 3; }
    done
    if [ "$DAY" = "1" ]; then
      ssh -n "${TUY_CHON_NOI[@]}" "$ten" 'git -C ~/bd03_khuyen_nghi log --oneline -1'
    elif grep -qiE "timed out|connection (reset|closed|refused)|broken pipe|could not resolve" "$TEP_LOI"; then
      sed 's/^/   /' "$TEP_LOI"
      echo "!! $ten: mất kết nối SSH sau 3 lần thử; chạy lại lệnh sau ít phút"
      LOI=1
    else
      sed 's/^/   /' "$TEP_LOI"
      echo "!! $ten: git push thất bại; thường do có tệp bị sửa tay ở đó (ssh $ten 'cd ~/bd03_khuyen_nghi && git status')"
      LOI=1
    fi
    rm -f "$TEP_LOI"
  done
  exit $LOI
fi

# ---------------------------------------------------------------- chạy lệnh
if [ -n "$TEP" ]; then
  # Gửi nội dung script cục bộ qua stdin cho bash trên máy đích: máy đích không cần có repo.
  [ -f "$TEP" ] || { echo "Không thấy tệp $TEP" >&2; exit 2; }
  [ "$TTY" = "1" ] && { echo "--tty không dùng cùng --chay-tep được" >&2; exit 2; }
  LENH="bash -s -- $LENH"
fi
[ -n "$LENH" ] || { echo "Thiếu lệnh. Ví dụ: bash scripts/cum/ssh_tat_ca.sh hostname" >&2; exit 2; }

goi_ssh() {  # $1 = tên máy, phần còn lại = tùy chọn ssh bổ sung
  local ten="$1"; shift
  if [ -n "$TEP" ]; then
    ssh "$@" "${TUY_CHON_NOI[@]}" "$ten" "$LENH" < "$TEP"
  elif [ "$TTY" = "1" ]; then
    ssh -t "$@" "${TUY_CHON_NOI[@]}" "$ten" "$LENH"
  else
    ssh -n "$@" "${TUY_CHON_NOI[@]}" "$ten" "$LENH"
  fi
}

LOI=0
if [ "$SONG_SONG" = "1" ]; then
  [ "$TTY" = "1" ] && { echo "--tty không dùng cùng --song-song được" >&2; exit 2; }
  TAM="$(mktemp -d)"
  for dong in "${CAC_MAY[@]}"; do
    ten="${dong%% *}"
    # BatchMode: chế độ song song không hỏi mật khẩu (cần đã chép khóa), tránh treo cả lô.
    ( goi_ssh "$ten" -o BatchMode=yes > "$TAM/$ten.log" 2>&1; echo $? > "$TAM/$ten.ma" ) &
  done
  wait
  for dong in "${CAC_MAY[@]}"; do
    ten="${dong%% *}"; ma="$(cat "$TAM/$ten.ma")"
    echo "===== $ten (mã thoát $ma)"
    sed "s/^/[$ten] /" "$TAM/$ten.log"
    [ "$ma" = "0" ] || LOI=1
  done
  rm -rf "$TAM"
else
  for dong in "${CAC_MAY[@]}"; do
    ten="${dong%% *}"
    echo "===== $ten (${dong#* })"
    goi_ssh "$ten"
    ma=$?
    [ "$ma" = "0" ] || { echo "!! $ten: mã thoát $ma"; LOI=1; }
  done
fi
exit $LOI
