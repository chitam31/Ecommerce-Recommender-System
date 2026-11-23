# Triển khai trên cụm 8 máy

Tài liệu đi theo đúng thứ tự bật cụm. Mỗi lệnh ghi rõ chạy **trên máy nào**. Mọi máy dùng cùng một bản mã (clone từ repo của nhóm) và cùng danh bạ `cau_hinh/cum.env`; script `scripts/cum/khoi_dong_may.sh` tự nhận ra máy qua IP ZeroTier và chỉ bật dịch vụ đúng vai trò.

| # | Máy | IP | Dịch vụ sẽ chạy |
|---|---|---|---|
| 1 | labsoe-11 (master) | 10.68.71.113 | hadoop-master, spark-master, Airflow |
| 2 | labsit12 | 10.68.71.198 | hadoop-worker, spark-worker |
| 3 | labsit | 10.68.71.204 | hadoop-worker, spark-worker, kafka-server, kafka-ui, bộ phát |
| 4 | labsit-may25 | 10.68.71.42 | hadoop-worker, spark-worker |
| 5 | labsit-may26 | 10.68.71.90 | hadoop-worker, spark-worker |
| 6 | labsoe-18 | 10.68.71.149 | hadoop-worker, spark-worker, **bd03-web** |
| 7 | Ctam (WSL2 trong laptop) | 10.68.71.205 | dự phòng (từng chạy web đến 11/11) |
| 8 | Windows cá nhân | 10.68.71.59 | trình duyệt, script demo |
| 9 | Laptop Windows `Whitehathacker` | 10.68.71.182 | điều khiển cụm qua SSH (mục 1.5), trình chiếu |

---

## 0. Nếu máy đang chạy cụm cũ của nhóm

Dự án mới giữ nguyên **tên project compose** (`hadoop-cluster`, `spark-cluster`, `kafka-docker`) nên dùng lại đúng các volume cũ (`hadoop-cluster_namenode_data`, `hadoop-cluster_worker_data`, `kafka-docker_kafka_data`). Dữ liệu HDFS đã có không mất.

```bash
# Trên mỗi máy, trong thư mục repo CŨ tương ứng:
docker compose down                    # KHÔNG thêm -v (sẽ xóa volume dữ liệu)
docker compose -f docker-compose-worker.yml down   # với spark-cluster trên máy worker
```

Nếu máy từng chạy từ thư mục `hadoop-custom` (giống hệt `hadoop-cluster`), volume mang tên `hadoop-custom_...`; khi đó đặt `export COMPOSE_PROJECT_NAME=hadoop-custom` trước khi chạy `khoi_dong_may.sh`. Kiểm tra bằng `docker volume ls | grep -E "hadoop|kafka"`.

---

## 1. Chuẩn bị chung trên MỖI máy Linux (máy 1–7)

### 1.1. ZeroTier

```bash
curl -s https://install.zerotier.com | sudo bash          # nếu chưa cài
sudo zerotier-cli join 633e31d8a29b038c
sudo zerotier-cli listnetworks       # phải thấy trạng thái OK và IP 10.68.71.x
ip -4 addr | grep 10.68.71           # IP này phải trùng với cau_hinh/cum.env
```

Máy mới phải được quản trị mạng bấm **Authorize** trên ZeroTier Central. IP ZeroTier cố định theo máy; nếu khác danh bạ, sửa `cau_hinh/cum.env` rồi commit để cả nhóm dùng chung.

### 1.2. Docker, Git, Python

```bash
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 git python3 python3-pip chrony
sudo usermod -aG docker $USER && newgrp docker
docker compose version            # cần Compose v2
timedatectl status | grep synchronized   # đồng hồ phải đồng bộ (Kafka, watermark, Airflow)
```

### 1.2b. Không cho máy tự ngủ

Ubuntu có giao diện mặc định có thể tự ngủ khi không ai dùng, kể cả ở màn hình đăng nhập; máy ngủ thì cụm mất DataNode, worker hoặc cả master. Sau khi xin phép phòng máy, tắt chế độ ngủ cho máy trong cụm (gỡ lại bằng `unmask` khi trả máy):

```bash
sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target
```

Từ máy điều khiển có thể làm cho cả 6 máy một lần: `bash scripts/cum/ssh_tat_ca.sh --tty "sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target"`.

### 1.3. Tường lửa

Spark executor mở cổng ngẫu nhiên cho block manager; cách đơn giản và an toàn trong mạng riêng là **cho phép mọi kết nối đi vào từ giao diện ZeroTier**:

```bash
sudo ufw status
sudo ufw allow in on zt+ comment "ZeroTier ttunet - cum bd03"
```

(Nếu không dùng ufw thì bỏ qua.)

### 1.4. Đưa mã lên máy và tài nguyên của từng máy

**Cách khuyên dùng — đẩy mã từ máy điều khiển qua SSH** (cần làm mục 1.5 trước). Repo để private, còn các tài khoản lab dùng chung, nên không đặt token GitHub trên máy lab. Chỉ máy điều khiển kéo mã từ GitHub, rồi đẩy sang `~/bd03_khuyen_nghi` của 6 máy:

```bash
# Trong Git Bash trên máy điều khiển, thư mục repo
git pull
bash scripts/cum/ssh_tat_ca.sh --tty "sudo apt-get install -y git"   # nếu máy lab chưa có git
bash scripts/cum/ssh_tat_ca.sh --dong-bo-ma
```

Mỗi lần có bản mới trên GitHub: `git pull` rồi `--dong-bo-ma` lại. Máy đích nào có tệp bị sửa tay thì git từ chối cập nhật máy đó thay vì ghi đè; mọi chỉnh sửa (kể cả `cau_hinh/cum.env`) làm trong repo rồi đẩy đi. Các tệp sinh ra khi chạy (`ha_tang/*/.env`, `jobs/common.zip`, `logs/`) nằm trong `.gitignore` nên không cản việc cập nhật.

Cách thay thế: `git clone` trực tiếp trên từng máy, gõ token chỉ đọc khi git hỏi mật khẩu (không lưu token vào URL).

**Tài nguyên** đã đo ngày 10/11/2026 bằng `scripts/cum/khao_sat_may.sh`: cả 6 máy lab đều **12 lõi, 7 GB RAM**, nên RAM là giới hạn chính. `cau_hinh/cum.env` giao cho Spark 3 GB RAM ở labsit12, labsit-may26, labsoe-18; 2 GB ở labsit (có Kafka) và labsit-may25 (đang chạy ứng dụng của nhóm khác). Mỗi executor dùng 2 GB nên phải nhỏ hơn hoặc bằng `bo_nho` nhỏ nhất, nếu không máy đó sẽ không nhận được executor nào. Khi đổi máy hoặc thêm máy, chạy lại khảo sát và sửa theo cùng quy tắc: chừa khoảng 2 GB cho Ubuntu có giao diện, 1 GB cho DataNode, cộng RAM của các dịch vụ khác đang chạy trên máy.

**Máy dùng chung với nhóm khác:** labsit-may25 đang chạy `coffeehouse-face-*` (cùng PostgreSQL, MySQL, RabbitMQ), labsit-may26 đang chạy `airflow-worker-kafka`. Không dừng, không xóa các container này. Các container đã dừng từ lâu cũng không phải của dự án, cứ để nguyên.

**Máy master:** ngoài NameNode, Spark Master và Airflow, mọi driver Spark cũng chạy trên máy này (PySpark ở standalone không hỗ trợ cluster deploy mode). Nên dừng DataNode cũ đang chạy trên master để nhường RAM; volume vẫn giữ nguyên:

```bash
ssh labsoe-11 docker stop hadoop-worker
```

Máy master còn chạy một **Cassandra** không thuộc dự án (tài khoản `cassandra`, khoảng 2,5 GB RAM). Còn Cassandra thì master chỉ đủ cho Airflow và một job batch, không đủ thêm driver của 3 luồng streaming. Nhóm được phép dừng tạm thời trong thời gian chạy đồ án; chỉ `stop`, không `disable`, và báo người phụ trách phòng Lab:

```bash
ssh -t labsoe-11 "sudo systemctl stop cassandra; systemctl is-active cassandra"     # in ra inactive
ssh -t labsoe-11 "sudo systemctl start cassandra"                                    # bật lại khi xong đồ án
```

Cassandra tự bật lại nếu máy khởi động lại: sau mỗi lần master khởi động lại phải dừng lại lần nữa.

Xem trước những gì script sẽ làm, không chạy thật:

```bash
bash scripts/cum/khoi_dong_may.sh --in-thu
```

### 1.5. Điều khiển cả cụm từ máy cá nhân qua SSH (tùy chọn, nên làm)

Mọi máy đã chung mạng ZeroTier nên máy cá nhân (Windows 10.68.71.59) SSH thẳng tới IP `10.68.71.x` được. Cần xin phép phòng máy trước khi bật SSH server trên máy của trường.

**Trên mỗi máy lab, làm một lần tại chỗ:**

```bash
sudo apt-get install -y openssh-server tmux
sudo systemctl enable --now ssh
sudo ufw allow in on zt+ to any port 22 proto tcp     # nếu dùng ufw: chỉ mở SSH trên giao diện ZeroTier
whoami                                                # tên tài khoản: khai báo vào TAI_KHOAN_SSH trong cau_hinh/cum.env
```

**Trên máy cá nhân, mở cửa sổ Git Bash** (menu Start → *Git Bash*). Không gõ `bash ...` trong PowerShell: lệnh đó gọi WSL chứ không phải Git Bash (lỗi `Bash/Service/RPC_S_CALL_FAILED`), và PowerShell 5.1 ghi `>` / `>>` theo mã UTF-16 làm hỏng `~/.ssh/config`.

```bash
cd "/d/BIG DATA/bd03_khuyen_nghi" && git pull                     # thư mục chứa repo trên máy Windows
ssh-keygen -t ed25519 -C "bd03"                                   # nếu chưa có khóa; Enter để giữ mặc định
bash scripts/cum/ssh_tat_ca.sh --tao-config > ~/.ssh/config       # ghi đè: Host, IP, tài khoản lấy từ cum.env
bash scripts/cum/ssh_tat_ca.sh --chep-khoa                        # nhập mật khẩu mỗi máy một lần
bash scripts/cum/ssh_tat_ca.sh hostname                           # không còn hỏi mật khẩu là xong
```

Nếu `~/.ssh/config` đang có cấu hình của việc khác, sao lưu trước (`cp ~/.ssh/config ~/.ssh/config.bak`) rồi chép phần cần giữ vào sau. Cấu hình sinh ra có `StrictHostKeyChecking accept-new`: lần đầu nối tới máy nào thì tự ghi nhận khóa của máy đó, nhưng vẫn từ chối nếu khóa đã biết bị thay đổi.

Khi đăng nhập bằng khóa đã chạy trên mọi máy, nên tắt đăng nhập bằng mật khẩu: sửa `PasswordAuthentication no` trong `/etc/ssh/sshd_config`, rồi `sudo systemctl restart ssh`. Chỉ làm khi phiên SSH bằng khóa đang mở, để không tự khóa mình ở ngoài.

**Dùng hằng ngày:**

```bash
bash scripts/cum/ssh_tat_ca.sh "cd ~/bd03_khuyen_nghi && git pull -q && git log --oneline -1"
bash scripts/cum/ssh_tat_ca.sh "cd ~/bd03_khuyen_nghi && bash scripts/cum/khoi_dong_may.sh"   # lần lượt, master trước
bash scripts/cum/ssh_tat_ca.sh --song-song "nproc; free -g | head -2"
bash scripts/cum/ssh_tat_ca.sh --tty "sudo apt-get install -y chrony"     # lệnh cần mật khẩu sudo
bash scripts/cum/ssh_tat_ca.sh --song-song --chay-tep scripts/cum/khao_sat_may.sh   # khảo sát lõi, RAM, đĩa, Docker, repo của cả 6 máy
bash scripts/cum/do_mang.sh                                                # đo mạng: Wi-Fi, ping chéo, tốc độ tải (mục 10b)
ssh -t labsoe-11 tmux new -A -s bd03                                      # vào master trong tmux
```

Việc chạy lâu (bộ phát dữ liệu, `chay_lan_dau.sh`) phải chạy **trong tmux** trên máy đích: máy cá nhân ngủ hoặc rớt mạng thì phiên SSH đứt, nhưng tmux vẫn giữ tiến trình chạy tiếp. Gõ `Ctrl+B` rồi `D` để thoát mà không dừng; vào lại bằng đúng lệnh `tmux new -A -s bd03`.

---

## 2. Máy 1 — labsoe-11 (10.68.71.113): NameNode, Spark Master, Airflow

### 2.1. HDFS và Spark Master

```bash
cd ~/bd03_khuyen_nghi
bash scripts/cum/khoi_dong_may.sh
```

Script sinh `ha_tang/hadoop/.env`, `ha_tang/spark/.env`, rồi chạy `hadoop-master` và build ảnh `bd03/spark:3.5.3` (thêm numpy, jar Delta 3.2.0 và Kafka connector vào `apache/spark:3.5.3`). Lần build đầu cần Internet tới PyPI và Maven Central.

Script chỉ build ảnh khi máy chưa có (thêm `--build` để buộc build lại sau khi sửa `ha_tang/spark/Dockerfile`). Nếu các máy worker không ra được Internet, build một lần trên master rồi chép ảnh sang:

```bash
# Trên master
docker save bd03/spark:3.5.3 | gzip > /tmp/bd03_spark.tar.gz
scp /tmp/bd03_spark.tar.gz <user>@10.68.71.198:/tmp/      # lặp lại cho từng worker
# Trên mỗi worker, trước khi chạy khoi_dong_may.sh
gunzip -c /tmp/bd03_spark.tar.gz | docker load
```

Kiểm tra:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
curl -s http://localhost:9870 > /dev/null && echo "HDFS UI OK"
curl -s http://localhost:8080/json/ | python3 -m json.tool | head
docker exec spark-master ls /opt/spark/jars | grep -E "delta|kafka"     # phải thấy 2 jar delta, 4 jar kafka
```

### 2.2. Airflow

Cách nhanh, không sửa tay tệp nào (tạo `.env` với khóa JWT, khóa Fernet, GID nhóm docker, đường dẫn dự án, và `mat_khau.json` với mật khẩu admin ngẫu nhiên in ra một lần):

```bash
cd ~/bd03_khuyen_nghi && bash scripts/cum/tao_cau_hinh_airflow.sh && bash scripts/cum/khoi_dong_may.sh
```

Hoặc làm tay từng bước:

```bash
cd ~/bd03_khuyen_nghi/ha_tang/airflow
cp .env.mau .env
cp mat_khau.json.mau mat_khau.json
id -u                                      # điền vào AIRFLOW_UID (container chạy bằng uid này)
getent group docker | cut -d: -f3          # điền vào DOCKER_GID
openssl rand -hex 32                       # điền vào AIRFLOW__API_AUTH__JWT_SECRET
python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())"   # FERNET_KEY
nano .env              # BD03_HOST_DIR=/home/<user>/bd03_khuyen_nghi (đường dẫn tuyệt đối)
nano mat_khau.json     # {"admin": "<mật khẩu mạnh>"}
chmod 600 mat_khau.json
cd ~/bd03_khuyen_nghi && bash scripts/cum/khoi_dong_may.sh      # chạy lại để bật Airflow
```

Compose đặt Airflow ở cấu hình gọn, khoảng 1 GB (api-server 1 tiến trình, tối đa 6 task đồng thời, không bật triggerer vì không DAG nào dùng toán tử deferrable), vì master 7,4 GB còn phải chứa NameNode, Spark Master và driver của mọi job Spark. Đo trên master tối 10/11 trước khi bật Airflow: còn 3,2 GB khả dụng. Kiểm tra sau khi bật: `free -h; docker stats --no-stream`.

Mở http://10.68.71.113:8088 (cổng 8080 đã dành cho Spark Master UI), đăng nhập `admin`. Năm DAG `kn_*` sẽ xuất hiện ở trạng thái tạm dừng. Chưa bật DAG cho tới bước 9.

Kiểm tra Airflow gọi được Spark:

```bash
docker exec bd03-airflow-airflow-scheduler-1 docker ps --format "{{.Names}}" | grep spark-master
```

---

## 3. Máy 2, 4, 5, 6 — labsit12, labsit-may25, labsit-may26, labsoe-18: DataNode + Spark Worker

```bash
cd ~/bd03_khuyen_nghi
bash scripts/cum/khoi_dong_may.sh
```

Script sinh `.env` với `NODE_IP` của chính máy, bật `hadoop-worker` (DataNode khai báo `dfs.datanode.hostname = NODE_IP`) và `spark-worker` (`--host NODE_IP --cores SO_LOI --memory BO_NHO`, `network_mode: host`).

Kiểm tra từ máy master:

```bash
docker exec hadoop-master hdfs dfsadmin -report | grep -E "Name:|Live datanodes"
curl -s http://10.68.71.113:8080/json/ | python3 -c "import json,sys;[print(w['host'],w['state'],w['cores']) for w in json.load(sys.stdin)['workers']]"
```

Mỗi worker phải hiện đúng IP ZeroTier của nó, trạng thái `ALIVE`.

---

## 4. Máy 3 — labsit (10.68.71.204): DataNode, Spark Worker, Kafka, bộ phát

### 4.1. Dịch vụ

```bash
cd ~/bd03_khuyen_nghi
bash scripts/cum/khoi_dong_may.sh        # DataNode, Spark Worker, Kafka (+ kafka-ui :8080)
bash scripts/tao_topic.sh                # retailrocket-events (giữ nếu đã có) và hien-thi, 3 phân vùng
```

Kafka quảng bá `PLAINTEXT://10.68.71.204:9092`. Bản compose mới thêm `KAFKA_LOG_DIRS=/var/lib/kafka/data` để volume thật sự giữ dữ liệu khi tạo lại container (bản cũ ghi vào `/tmp/kraft-combined-logs` bên trong container).

### 4.2. Dữ liệu cho bộ phát

`events.csv` được chép sẵn vào `~/du_lieu/retailrocket/` của máy này bởi `scripts/cum/chep_du_lieu.sh` chạy trên laptop (mục 7). Bộ phát cần thêm thư viện Kafka:

```bash
pip3 install --user kafka-python   # Ubuntu 23.04 trở lên báo "externally-managed-environment": thêm --break-system-packages
```

Bộ phát được bật ở bước 8.

---

## 5. Web: labsoe-18 (10.68.71.149)

Web chạy cùng máy với DataNode và Spark worker của labsoe-18, nằm trong LAN của cụm nên đọc HDFS
nhanh và không vướng giới hạn MTU của đường mạng ngoài trường. RAM đo lúc chọn (11/11, 3 luồng
đang chạy): labsoe-18 còn 5,7 GB khả dụng, labsit12 5,2 GB; lúc job lớn dùng hết executor 3 GB
vẫn còn khoảng 2,7 GB, web cần khoảng 1,5 GB (driver Spark cục bộ 1 GB).

```bash
# Trên laptop (Git Bash): đưa mã lên máy rồi vào máy
bash scripts/cum/ssh_tat_ca.sh --may labsoe-18 --dong-bo-ma
ssh labsoe-18
cd ~/bd03_khuyen_nghi
# Cấu hình riêng của máy: khoi_dong_may.sh nối tệp này vào ha_tang/web/.env mỗi lần chạy.
# Để nút "Chạy lại mô hình" gọi được DAG 5. Dán RIÊNG dòng này rồi gõ mật khẩu (read -s: không
# hiện, không vào lịch sử lệnh); dán chung với lệnh khác thì read sẽ lấy dòng kế tiếp làm mật khẩu.
read -rs -p "Mật khẩu admin Airflow: " MK && printf 'AIRFLOW_PASS=%s\n' "$MK" > ha_tang/web/.env.rieng && chmod 600 ha_tang/web/.env.rieng; unset MK; echo
bash scripts/cum/khoi_dong_may.sh --chi=web      # chỉ bật web, không tạo lại DataNode/Spark worker của máy
curl -s http://localhost:8000/api/suc-khoe        # {"che_do":"delta","kafka":"GuiKafka"}
```

Mở web: ở trường `http://10.68.71.149:8000`; từ ngoài trường mở đường hầm trên laptop
`ssh -N -L 8001:localhost:8000 labsoe-18` (để nguyên cửa sổ) rồi vào `http://localhost:8001`.

**Vì sao không dùng Ctam.** Ctam là Ubuntu 24.04 trong WSL2 của laptop (ZeroTier riêng 10.68.71.205).
Ngày 11/11, WSL (3,8 GB RAM, Windows chỉ còn khoảng 4 GB trống khi Chrome và Meet đang mở) sập hai lần lúc
build và bật web; lần sập đầu còn làm hỏng tệp của ảnh gốc trong bộ nhớ đệm build (`pip._vendor.six`),
phải `docker builder prune -af` rồi tải lại. Từ nhà, WSL còn phải hạ MTU ZeroTier (`ZT_MTU=1280`, NHAT_KY
lỗi 25). Ctam giữ trong `cum.env` với vai trò `du-phong`.

---

## 6. Máy 8 — Windows cá nhân (10.68.71.59): máy trình chiếu

1. Cài ZeroTier One cho Windows, join `633e31d8a29b038c`, chờ Authorize.
2. Mở các trang:

| Trang | Địa chỉ |
|---|---|
| Web giả lập sàn TMĐT | http://10.68.71.149:8000 (ngoài trường: http://localhost:8001 qua đường hầm) |
| Spark Master | http://10.68.71.113:8080 |
| HDFS NameNode | http://10.68.71.113:9870 |
| Airflow | http://10.68.71.113:8088 |
| Kafka UI | http://10.68.71.204:8080 |

3. Các script demo chỉ dùng thư viện chuẩn của Python, chạy được trên Windows (cài Python 3.10+ từ python.org):

```powershell
git clone https://github.com/chitam31/CS411V_Final-Project_Ecommerce-Recommender-System.git bd03_khuyen_nghi
cd bd03_khuyen_nghi
python scripts\demo_vong_khep_kin.py --web http://10.68.71.149:8000 --san-pham <ma_san_pham_it_nguoi_xem>
python producer\nguoi_dung_ao.py --web http://10.68.71.149:8000 --so-luot 300
```

---

## 7. Kiểm tra toàn cụm và nạp dữ liệu (máy master)

```bash
cd ~/bd03_khuyen_nghi
bash scripts/cum/kiem_tra_cum.sh
```

Kết quả mong đợi: mọi cổng `OK`, `DataNode sống: 5`, `Worker sống: 5` (labsit12, labsit, labsit-may25, labsit-may26, labsoe-18), không có dòng `DONG`.

Tải RetailRocket và đưa lên HDFS. Khóa Kaggle (`kaggle.json`) **để trên laptop, không chép lên máy lab**: tài khoản lab dùng chung, ai đăng nhập cũng đọc được. Laptop tải tệp zip (khoảng 300 MB, giữ lại ở thư mục cha của repo để lần sau khỏi tải) rồi chép sang hai máy:

```bash
# Trên laptop (Git Bash). Mặc định đọc khóa ở ../kaggle.json, cạnh thư mục repo
bash scripts/cum/chep_du_lieu.sh                     # hoặc --khoa <tệp kaggle.json>, --zip <tệp zip có sẵn>
# -> labsoe-11: ~/du_lieu/retailrocket/ đủ 4 tệp CSV; labsit: ~/du_lieu/retailrocket/events.csv

# Trên máy master
bash scripts/tai_du_lieu.sh                          # ~/du_lieu/retailrocket -> HDFS /lakehouse/bd03_khuyen_nghi/landing/retailrocket
docker exec hadoop-master hdfs dfs -ls -h /lakehouse/bd03_khuyen_nghi/landing/retailrocket
```

Đã chạy thử pipeline bằng dữ liệu mẫu (`scripts/tao_du_lieu_mau.py`) thì phải dọn trước khi đưa dữ liệu thật lên, nếu không bảng Delta và checkpoint sẽ trộn hai bộ dữ liệu. Chạy trên laptop, khi không còn job Spark nào chạy:

```bash
bash scripts/cum/don_du_lieu.sh          # xem trước: đường dẫn nào sẽ xóa, dung lượng, số tin nhắn Kafka
bash scripts/cum/don_du_lieu.sh --xoa    # xóa thật, gõ XOA để xác nhận
```

Script chỉ xóa những gì dự án tạo ra (danh sách bảng đọc từ `jobs/common/cau_hinh.py`, checkpoint và thư mục mô hình đọc từ mã các job, CSV ở `/lakehouse/bd03_khuyen_nghi/landing/retailrocket`), làm rỗng hai topic Kafka (xóa rồi tạo lại 3 phân vùng) và xóa `/tmp/mau` trên master. Thư mục khác dưới gốc lakehouse và toàn bộ `/lake` cũ (dữ liệu cũ của nhóm) được giữ nguyên và in ra để kiểm tra.

---

## 8. Chạy lần đầu

**Máy labsit** — bật bộ phát (khoảng 70 phút cho 4,5 tháng dữ liệu; nên chạy trong `tmux`):

```bash
tmux new -s phat_lai
python3 ~/bd03_khuyen_nghi/producer/phat_lai_su_kien.py --tep ~/du_lieu/retailrocket/events.csv
```

Muốn có dữ liệu nhanh để thử pipeline, phát hết mức bằng `--giay-moi-ngay 0`.

**Máy master** — tạo bảng, nạp danh mục, bật luồng nạp bronze và thịnh hành trong lúc bộ phát chạy:

```bash
cd ~/bd03_khuyen_nghi
docker exec hadoop-master hdfs dfs -mkdir -p /lakehouse/bd03_khuyen_nghi/landing/retailrocket /lakehouse/bd03_khuyen_nghi/models /lakehouse/bd03_khuyen_nghi/checkpoints /lakehouse/bd03_khuyen_nghi/_quarantine
bash ha_tang/spark/submit.sh nhanh kiem_tra/khoi_tao_bang.py
bash ha_tang/spark/submit.sh lo transform/nhap_danh_muc.py
bash scripts/luong.sh khoi_dong tat_ca
bash scripts/luong.sh trang_thai
```

Khi bộ phát chạy xong, chạy toàn bộ pipeline lô (đúng thứ tự của DAG 1, cộng mô phỏng A/B và đo ZORDER):

```bash
bash scripts/chay_lan_dau.sh                    # đủ 27 tổ hợp tuning cho báo cáo
# hoặc LUOI=toi_thieu bash scripts/chay_lan_dau.sh   # 9 tổ hợp, nhanh hơn
# Dừng giữa chừng vì lỗi: sửa xong chạy tiếp từ bước đó, ví dụ bước 4 (log ghi nối vào tệp cũ)
# TU_BUOC=4 LUOI=toi_thieu bash scripts/chay_lan_dau.sh 2>&1 | tee -a ~/lan_dau.log
# Từ bước 6 trở đi cần thêm MA_CHAY=<mã lần tuning ở bước 5, in trong log>
```

Bước 3 trong `chay_lan_dau.sh` chạy `nap_su_kien.py --mot-lan`; nếu luồng nạp đang chạy liên tục thì bước này chỉ xử lý phần còn lại (cùng checkpoint, không trùng dữ liệu).

Theo dõi job: Spark UI của từng job ở `http://10.68.71.113:4140`–`4150` (mỗi hồ sơ một cổng, xem `ha_tang/spark/submit.sh`).

---

### 8b. Chuyển gốc hồ dữ liệu sang `/lakehouse/bd03_khuyen_nghi` (một lần, 15/11/2026)

Đề cương (mục A.4, A.6.3) quy định gốc `hdfs:///lakehouse/<ma_de_tai>/`. Cụm đã chạy từ 10/11 với gốc `/lake`.
Mã từ commit ngày 15/11 dùng gốc mới. `submit.sh` sẽ **từ chối nộp job** cho tới khi dữ liệu được chuyển,
để không job nào tạo bảng rỗng ở gốc mới.

Trên laptop (Git Bash), sau khi `git pull`:

```bash
bash scripts/cum/chuyen_lakehouse.sh          # xem trước kế hoạch (cặp nguồn -> đích), không đổi gì
bash scripts/cum/chuyen_lakehouse.sh --chay   # chuyển thật, gõ CHUYEN để xác nhận (~10-15 phút)
```

Script làm lần lượt:

1. Tạm dừng DAG 2–4 (DAG 1 vẫn để tạm dừng).
2. Dừng 3 luồng và chờ tới khi không còn ứng dụng Spark nào của dự án.
3. Đồng bộ mã sang mọi máy.
4. `hdfs dfs -mv` từng bảng, checkpoint, mô hình, landing sang gốc mới, rồi viết lại `_spark_metadata` của bronze.
5. Chạy job `kiem_tra/kiem_tra_lakehouse.py`: đọc bronze qua metadata mới, in phiên bản của mọi bảng Delta.
6. Bật lại 3 luồng, build lại web trên labsoe-18, bật lại DAG 2–4.
7. In `hdfs dfs -du -h /lakehouse/bd03_khuyen_nghi`.

Nếu bước 5 lỗi, script dừng và không bật lại gì. Khi đó chạy `bash scripts/cum/chuyen_lakehouse.sh --hoan-tac`:
dữ liệu về lại `/lake`, metadata được viết lại về tiền tố cũ. Sau đó checkout commit trước 15/11 trên laptop và
đồng bộ mã. Dữ liệu cũ khác của nhóm dưới `/lake` (`/lake/bronze/su_kien`, `/lake/cu_nhom`...) không bị đụng tới.

Làm tay từng bước (khi không chạy được từ laptop), trên master:

```bash
bash scripts/luong.sh dung tat_ca                       # và tạm dừng DAG 2-4 trên giao diện Airflow
python3 scripts/cum/chuyen_lakehouse.py --xem
python3 scripts/cum/chuyen_lakehouse.py --thuc-hien
bash ha_tang/spark/submit.sh nhanh kiem_tra/kiem_tra_lakehouse.py
bash scripts/luong.sh khoi_dong tat_ca                  # rồi trên labsoe-18: bash scripts/cum/khoi_dong_may.sh --chi=web
```

### 8c. Dựng `_quarantine` và thu bằng chứng cho báo cáo

Trên master, trong `tmux`, sau bước 8b:

```bash
bash scripts/thu_bang_chung.sh        # 15-20 phút; kết quả ở ~/bd03_bang_chung/<thời điểm>/ và .tar.gz
```

Script làm các việc sau:

- Dựng `_quarantine.tuong_tac` từ bronze nếu chưa có (`bronze_to_silver.py --toan-bo --chi-cach-ly`, không ghi silver).
- Thu output `hdfs dfs -du -h` (phải nộp theo A.6.3) và cây thư mục (`hdfs dfs -ls -R` đã lọc).
- Thu DESCRIBE HISTORY, số commit theo thao tác (MERGE, OPTIMIZE...), VERSION AS OF, schema evolution, số đo Z-ORDER.
- Thu thống kê làm sạch, danh sách DAG kèm Deadline Alert, thời gian chạy của các luồng, độ trễ web và EDA.

Trên laptop: `scp labsoe-11:~/bd03_bang_chung/<thời điểm>.tar.gz .`, giải nén vào `docs/bang_chung/cum/` rồi commit.
Kết quả để ngoài repo trên master vì tệp lạ trong thư mục làm việc sẽ chặn lần `--dong-bo-ma` sau.

---

## 9. Bật lịch Airflow

Trong giao diện Airflow, bật (unpause) lần lượt: `kn_giam_sat_streaming`, `kn_tinh_ab`, `kn_bao_tri`, `kn_pipeline_hang_ngay`. `kn_tai_lap` không có lịch (chỉ chạy khi được kích hoạt), nhưng vẫn phải unpause: DAG mới tạo ở trạng thái tạm dừng, và lần chạy kích hoạt tay (kể cả từ nút "Chạy lại mô hình" trên web) của DAG đang tạm dừng chỉ nằm chờ, không chạy. Hoặc dùng dòng lệnh:

```bash
for d in kn_tai_lap kn_giam_sat_streaming kn_tinh_ab kn_bao_tri kn_pipeline_hang_ngay; do
  docker exec bd03-airflow-airflow-scheduler-1 airflow dags unpause $d
done
```

Trước khi bật lại `kn_pipeline_hang_ngay` sau thời gian tạm dừng (sau khi nộp bài): mở DAG trên giao diện, xem tab **Runs**. Lượt nào còn `running` từ trước khi tạm dừng (ngày 16/11 có lượt `scheduled__2026-11-14T19:00:00+00:00` treo từ trưa 15/11) thì chọn lượt đó → **Mark as failed**, nếu không nó sẽ chạy tiếp ngay khi bật DAG.

Lưu ý: Airflow 3 tạo ngay một lần chạy cho mốc lịch gần nhất đã qua khi DAG vừa được bật (đo ngày 10/11: bật lúc 23:21, cả `kn_pipeline_hang_ngay` lẫn `kn_bao_tri`, `kn_tinh_ab` chạy luôn, không chờ 02:00/04:00). Muốn DAG 1 chạy đúng giờ thì bật sau mốc 02:00 của ngày hôm đó, hoặc chấp nhận lần chạy ngay này.

Giới hạn số driver Spark chạy đồng thời trên master bằng pool mặc định của Airflow. Ban đêm, khi DAG 1 huấn luyện và 3 luồng vẫn chạy, đặt 1 chỗ; lúc demo cần DAG giám sát chạy song song với DAG khác thì nâng lên 2 (sau khi `free -h` cho thấy còn đủ RAM):

```bash
docker exec bd03-airflow-airflow-scheduler-1 airflow pools set default_pool 1 "mot driver Spark moi luc tren master"
```

`kn_giam_sat_streaming` chạy một job Spark mỗi 15 phút: với 1 chỗ, nó phải chờ các task huấn luyện dài và lần chạy bị quá hạn 14 phút. Vì vậy ban đêm để DAG này tạm dừng, bật lại trước buổi demo.

Chạy thử DAG 5 với phiên bản features của lần chạy đầu:

```bash
docker exec bd03-airflow-airflow-scheduler-1 airflow dags trigger kn_tai_lap -c '{"phien_ban_delta": 0, "che_do": "mo_hinh_chon"}'
```

---

## 10. Xử lý sự cố

Xem nhanh tình trạng master (RAM, ứng dụng Spark đang chạy, DAG đang bật, các lần chạy và task gần nhất của Airflow, phiên tmux), chỉ đọc:

```bash
ssh labsoe-11 "bash ~/bd03_khuyen_nghi/scripts/cum/tinh_trang.sh"
```

| Triệu chứng | Nguyên nhân thường gặp | Cách xử lý |
|---|---|---|
| Worker không hiện trong Spark UI | Máy chưa được Authorize trên ZeroTier; `NODE_IP` sai; tường lửa | `zerotier-cli listnetworks`; xem `ha_tang/spark/.env`; mục 1.3 |
| Job treo ở "Initial job has not accepted any resources" | Các ứng dụng đang chạy đã chiếm hết lõi hoặc RAM của worker, hoặc RAM executor lớn hơn `bo_nho` của worker | `bash scripts/luong.sh trang_thai`; tạm dừng luồng không cần (`luong.sh dung hien_thi`); xem cột Memory trên Spark UI `:8080` |
| `khoi_dong_may.sh` báo `!! Container ... thuộc project ...` | Container cũ cùng tên được tạo từ thư mục khác | Làm theo hướng dẫn in ra: xóa container không giữ dữ liệu; với HDFS thì khai báo project cũ trong `DU_AN_HADOOP` |
| Máy lab chậm hẳn, `free -h` gần hết RAM | 7 GB RAM dùng chung với giao diện và dịch vụ khác | Đóng trình duyệt trên máy lab; giảm `bo_nho` của máy đó trong `cum.env`; chạy batch khi không chạy luồng `hien_thi` |
| `Could not obtain block` / ghi HDFS chậm rồi lỗi | Client không nối được cổng 9866 của DataNode, hoặc DataNode khai sai hostname | `hdfs dfsadmin -report` phải thấy IP ZeroTier của từng DataNode; mở tường lửa |
| Executor lỗi "Failed to connect to 10.68.71.113:404x" | Cổng driver của container spark-master chưa mở hoặc bị trùng | Compose mới mở 4041–4060; mỗi hồ sơ một cổng; không chạy hai job cùng hồ sơ |
| Log DataNode: `Retrying connect to server: 0.0.0.0/0.0.0.0:8020` | `dfs.namenode.rpc-address` trong `hdfs-site.xml` (tệp dùng chung với DataNode) để `0.0.0.0` | Để `10.68.71.113:8020`, còn `rpc-bind-host = 0.0.0.0` (đã sửa trong repo); chạy lại `khoi_dong_may.sh` trên mọi máy |
| Log Spark worker: `Connection refused: /0.0.0.0:7077` | Spark Master chạy mạng bridge với `--host 0.0.0.0`, tự quảng bá địa chỉ 0.0.0.0 | Master chạy `network_mode: host`, `--host 10.68.71.113` (đã sửa trong repo) |
| Cụm chạy rất chậm, ping giữa các máy lab hàng trăm ms | ZeroTier không nối thẳng được giữa hai phòng lab, phải đi qua máy chuyển tiếp | Mục 10b |
| Kafka: `Connection to node -1 could not be established` | Sai `KAFKA_ADVERTISED_LISTENERS` hoặc cổng 9092 bị chặn | `ha_tang/kafka/.env` phải có `NODE_IP=10.68.71.204` |
| Luồng streaming lỗi sau khi sửa mã | Checkpoint cũ không tương thích với truy vấn mới | Đổi tên checkpoint trong job (hậu tố `_v2`...) hoặc xóa `/lakehouse/bd03_khuyen_nghi/checkpoints/<ten>` |
| `ConcurrentAppendException` khi hai job ghi cùng bảng | Chạy tay một job trong lúc DAG đang chạy job đó | Chờ DAG xong; các DAG đặt `max_active_runs=1` |
| ALS báo `OutOfMemoryError` | Ma trận quá lớn so với RAM executor | `huan_luyen_als.py --so-khoi 50`; tăng `RAM_ALS`; tăng `--min-tuong-tac` ở `chuan_bi_ma_tran.py` |
| Airflow task: `permission denied ... docker.sock` | `DOCKER_GID` sai | `getent group docker`, sửa `ha_tang/airflow/.env`, `docker compose up -d` lại |
| Thịnh hành không cập nhật dù bấm nút | Luồng `tinh_thinh_hanh` không chạy, hoặc đồng hồ các máy lệch | `scripts/luong.sh log thinh_hanh`; `timedatectl` trên máy Kafka và master |
| Web báo lỗi Delta / HDFS | Web không nối được DataNode hoặc bảng chưa tạo | `kiem_tra/khoi_tao_bang.py`; web phải chạy `network_mode: host` |

### 10b. Đường nối ZeroTier giữa các máy: DIRECT hay RELAY

Đo nhanh từ máy điều khiển, không cần sudo:

```bash
bash scripts/cum/do_mang.sh              # Wi-Fi + IP LAN + ping chéo 6 máy, rồi tốc độ tải từ master về từng máy
bash scripts/cum/do_mang.sh --chi-ping   # chỉ phần ping, vài giây
```

Script ping mỗi cặp máy hai lần: qua IP ZeroTier và qua IP LAN hiện tại của máy kia. Qua Wi-Fi, nối thẳng thường dưới 60 ms, đi relay thường 150 ms trở lên. Cột nhận xét cho biết nguyên nhân:

- `noi thang, Wi-Fi dang cham`: ZeroTier và LAN cùng chậm, vẫn nối thẳng, chỉ là Wi-Fi đang nghẽn.
- `RELAY du LAN thong`: mạng cho phép hai máy thấy nhau nhưng ZeroTier chưa tìm ra đường thẳng. Thường do ZeroTier thử nhầm địa chỉ của các mạng Docker (trùng dải 172.17–172.31.x giữa các máy) hoặc Tailscale. Chặn các card ảo đó rồi khởi động lại ZeroTier, trên cả 6 máy (mỗi máy hỏi mật khẩu sudo; phiên SSH có thể đứt vài giây ngay sau khi khởi động lại):

  ```bash
  bash scripts/cum/ssh_tat_ca.sh --tty "cd ~/bd03_khuyen_nghi && sudo bash scripts/cum/cau_hinh_zerotier.sh"
  ```

  Script thêm `interfacePrefixBlacklist` vào `/var/lib/zerotier-one/local.conf` (giữ thiết lập cũ, bản gốc lưu ở `local.conf.truoc-bd03`). Khởi động lại ZeroTier làm mọi kết nối HDFS/Spark của máy đó khựng vài chục giây: **chỉ làm khi không có job nào đang chạy**.
- `RELAY, LAN khong tra loi`: Wi-Fi chặn các máy nói chuyện với nhau, khởi động lại không giúp gì. Tốc độ tải ở phần 2 là con số quyết định HDFS và shuffle chạy nhanh hay chậm: cần vài MB/s trở lên (mốc ngày 10/11/2026 khi còn RELAY: khoảng 0,4 MB/s). Xác nhận chắc chắn bằng bảng đường nối của ZeroTier (mỗi máy hỏi mật khẩu sudo):

```bash
bash scripts/cum/ssh_tat_ca.sh --tty "sudo zerotier-cli peers"
```

Cột `link` cho biết hai máy nối thẳng (`DIRECT`, kèm địa chỉ LAN hoặc địa chỉ công cộng) hay phải đi qua máy chuyển tiếp của ZeroTier trên Internet (`RELAY`, độ trễ hàng trăm đến hơn một nghìn ms, băng thông thấp). HDFS và shuffle của Spark cần các máy trong cụm nối thẳng với nhau. Khi hai phòng lab ở hai mạng con khác nhau và mạng trường chặn UDP giữa hai phòng, ZeroTier chỉ còn đường RELAY. Kiểm tra nguyên nhân (mỗi máy hỏi mật khẩu sudo một lần):

```bash
bash scripts/cum/ssh_tat_ca.sh --tty 'sudo ufw status | head -1; ip -br link | grep -vE "^(lo|docker|br-|veth|zt)"; nmcli -t -f active,ssid dev wifi | grep "^yes"'
```

Máy nối Wi-Fi (`wlo1`, `wlp…`) mà không ping được IP mạng nội bộ của máy cùng phòng nghĩa là mạng Wi-Fi đó chặn các máy nói chuyện với nhau (client isolation): tình trạng của phòng labsit ngày 10/11/2026. Cách xử lý, không cần sửa cấu hình cụm vì IP ZeroTier giữ nguyên:

1. Cắm cáp mạng vào cổng `enp44s0` (nếu phòng có ổ mạng dây), ZeroTier tự chuyển sang đường thẳng qua LAN.
2. Đưa các máy vào cùng một mạng Wi-Fi cho phép các máy thấy nhau (ví dụ mạng `LabSoe` mà phòng labsoe đang dùng). Đổi mạng Wi-Fi ngay tại máy, không đổi qua SSH: nếu mạng mới cần đăng nhập trang web thì máy sẽ mất kết nối từ xa. Đổi xong chờ khoảng một phút cho ZeroTier tìm lại đường, rồi chạy `do_mang.sh`. Nếu vẫn chậm, khởi động lại ZeroTier trên máy đó (`sudo systemctl restart zerotier-one`; IP ZeroTier giữ nguyên, cụm tự nối lại sau vài giây). Để máy tự chọn lại mạng này sau khi khởi động lại mà không tắt mạng cũ của người dùng khác: `sudo nmcli connection modify LabSoe connection.autoconnect-priority 100` (tên kết nối xem bằng `nmcli connection show`).
3. Dùng một router hoặc điểm phát Wi-Fi riêng cho các máy trong cụm.
4. Nếu tường lửa đang bật, mở cổng ZeroTier: `sudo ufw allow 9993/udp`.

Trong lúc còn đi đường chuyển tiếp (khoảng 0,4 MB/s), giữ `HE_SO_SAO_CHEP=1` trong `cau_hinh/cum.env` để HDFS và Spark không phải chép bản sao qua mạng chậm. Khi tốc độ giữa các máy đạt vài MB/s, đặt 2 (giá trị hiện tại từ 10/11/2026), rồi `--dong-bo-ma` và chạy lại `khoi_dong_may.sh` trên mọi máy. Với 1 bản sao, một DataNode khựng là job đọc tệp của nó hỏng hẳn; với 2 bản sao, HDFS client tự đọc bản còn lại. Tệp cũ giữ số bản sao cũ; muốn nâng lên thì chạy `docker exec hadoop-master hdfs dfs -setrep 2 /lakehouse/bd03_khuyen_nghi` (NameNode chép thêm ở chế độ nền; thêm `-w` nếu muốn chờ chép xong). Cũng vì mạng Wi-Fi, HDFS client chờ DataNode 3 phút (`dfs.client.socket-timeout` trong `hdfs-site.xml` và `submit.sh`) thay vì 60 giây, và mỗi task Spark được thử 8 lần thay vì 4.

### 10c. SSH từ nhà vào master treo ở `expecting SSH2_MSG_KEX_ECDH_REPLY`

Hiện tượng (10/11/2026, laptop ở nhà): `ssh labsoe-11` lúc báo `Connection reset by 10.68.71.113 port 22`, lúc `timed out`; ping gói nhỏ vẫn trả lời, SSH sang máy khác (labsoe-18) vẫn được, trang Spark của master vẫn chạy. `ssh -v` dừng ở `debug1: expecting SSH2_MSG_KEX_ECDH_REPLY`.

Nguyên nhân: gói trả lời trao khóa của OpenSSH bản mới (`sntrup761x25519`, khoảng 1,3 KB) là gói lớn nhất của bước bắt tay, và bị rớt trên đường ZeroTier từ master về laptop khi đi qua Internet (lỗi MTU). Gói nhỏ thì qua bình thường. Ở lab, laptop nối thẳng với master qua LAN nên không gặp.

Cách làm việc từ nhà: đi vòng qua một máy mà SSH vẫn được, khóa vẫn chỉ ở laptop (ProxyJump chỉ chuyển tiếp kết nối TCP):

```bash
ssh -J labsoe-18 labsoe-11 "cat ~/trien_khai_lai/ket_qua.md" > ket_qua.md
# hoặc tạo tên tắt một lần rồi dùng ssh m11 ... thay cho ssh labsoe-11 ...
printf '\nHost m11\n  HostName 10.68.71.113\n  User hduser\n  ProxyJump labsoe-18\n' >> ~/.ssh/config
```

Cách xem tiến độ job mà không cần SSH vào master: hỏi trang Spark của master qua một máy trong cụm, ví dụ `ssh labsoe-18 "python3 -c '...urlopen(\"http://10.68.71.113:8080/json/\")...'"` (lệnh đầy đủ ở NHAT_KY.md ngày 10/11/2026).

---

## 11. Khi nào cần dùng server của trường

Chỉ cần khi tuning **27 tổ hợp trên toàn bộ dữ liệu** không chạy xong trong thời gian cho phép, hoặc ALS rank 50 vẫn OOM sau khi đã tăng `numBlocks` và lọc user. Không cần chuyển cả cụm: thêm server như **một Spark Worker nữa**.

1. Server join ZeroTier `633e31d8a29b038c`, ghi lại IP.
2. Thêm vào `cau_hinh/cum.env`, ví dụ: `MAY_10="server-truong|10.68.71.X|spark-worker|16|48g"`, rồi tăng `LOI_ALS`/`RAM_ALS`.
3. Trên server: clone repo, `bash scripts/cum/khoi_dong_may.sh server-truong`.

Không giao vai trò DataNode cho server nếu nó không chạy thường trực, để HDFS không mất bản sao khi server tắt.

---

## 12. Dừng cụm an toàn

```bash
# Máy master: dừng luồng trước (checkpoint bảo đảm khởi động lại không mất, không trùng)
bash scripts/luong.sh dung tat_ca
# Từng máy:
bash scripts/cum/khoi_dong_may.sh --dung
```

Thứ tự bật lại: master → các worker → labsit → web trên labsoe-18 (`khoi_dong_may.sh --chi=web`), rồi `scripts/luong.sh khoi_dong tat_ca`.
