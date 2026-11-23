"""Cấu hình, lược đồ, bộ phát Kafka, DAG Airflow và tiện ích đo ZORDER."""
import os
import subprocess

import pytest

from common import cau_hinh as ch
from common.luoc_do import BANG_DELTA, ten_cot

GOC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_luoc_do_dung_dac_ta():
    assert ten_cot("gold.goi_y_nguoi_dung")[:5] == ["ma_nguoi_dung", "xep_hang", "ma_san_pham", "diem_du_doan", "nguon"]
    assert {"phien_ban_mo_hinh", "sinh_luc"} <= set(ten_cot("gold.goi_y_nguoi_dung"))
    assert ten_cot("gold.nhat_ky_hien_thi")[:8] == ["ma_hien_thi", "ma_nguoi_dung", "nhom_ab", "vi_tri", "ma_san_pham",
                                                     "thoi_diem_hien_thi", "da_click", "da_mua"]
    assert "phien_ban_delta_du_lieu" in ten_cot("gold.ket_qua_ab")
    assert BANG_DELTA["gold.nhat_ky_hien_thi"]["thuoc_tinh"]["delta.enableChangeDataFeed"] == "true"
    assert ch.duong_dan("silver.tuong_tac").endswith("/lakehouse/bd03_khuyen_nghi/silver/tuong_tac")
    assert sum(len(v) for v in ch.LUOI_DAY_DU.values()) == 9 and len(ch.LUOI_TOI_THIEU["rank"]) * len(ch.LUOI_TOI_THIEU["regParam"]) == 9


def test_tao_df_theo_ddl(spark):
    from common.luoc_do import tao_df

    df = tao_df(spark, "gold.ket_qua_tuning", [{"ma_chay": "r1", "rank": 10, "reg_param": 0.1, "alpha": 1.0, "ndcg_at_10": 0.2}])
    assert df.columns == ten_cot("gold.ket_qua_tuning") and df.first()["coverage"] is None


def test_tao_df_ep_int_vao_cot_double(spark):
    """Lỗi bước 8 ngày 10/11: statistics.mean([1, 1]) trả về int 1, Spark từ chối int ở cột DOUBLE."""
    import statistics
    from datetime import datetime

    from common.luoc_do import tao_df

    dong = {"thoi_diem": datetime(2026, 11, 10), "giai_doan": "sau_zorder", "so_tep": 2, "so_truy_van": 30,
            "trung_binh_ms": 120, "trung_vi_ms": 100.5, "p95_ms": 300, "tep_doc_trung_binh": statistics.mean([1, 1])}
    ket_qua = tao_df(spark, "gold.do_hieu_nang_zorder", [dong]).first()
    assert ket_qua["tep_doc_trung_binh"] == 1.0 and isinstance(ket_qua["tep_doc_trung_binh"], float)
    assert ket_qua["so_tep"] == 2 and ket_qua["trung_binh_ms"] == 120.0


def test_bo_phat(tmp_path):
    from phat_lai_su_kien import MS_MOI_NGAY, SinhNguoiMoi, doc_su_kien, thoi_diem_phat

    tep = tmp_path / "events.csv"
    tep.write_text("timestamp,visitorid,event,itemid,transactionid\n300,2,view,9,\n100,1,view,8,\n200,1,transaction,8,77\n")
    ds = doc_su_kien(str(tep))
    assert [d[0] for d in ds] == [100, 200, 300] and ds[1][4] == "77"
    assert thoi_diem_phat(1000 + MS_MOI_NGAY, 1000, 50.0, 30.0) == pytest.approx(80.0)  # 1 ngày = 30 giây
    sinh = SinhNguoiMoi(0.05, hat_giong=1)
    so = sum(sinh.co_sinh() for _ in range(20000))
    assert 0.04 < so / 20000 < 0.06
    tm = sinh.sinh(123, "9")
    assert tm["visitorid"].startswith("moi_") and tm["nguon"] == "nguoi_moi"


def test_du_lieu_mau(tmp_path):
    from tao_du_lieu_mau import sinh

    n = sinh(str(tmp_path), so_nguoi=200, so_item=100)
    assert n > 600 and (tmp_path / "item_properties_part2.csv").exists()


def test_script_shell_dung_cu_phap():
    for goc, _, tep in os.walk(os.path.join(GOC, "scripts")):
        for t in tep:
            if t.endswith(".sh"):
                subprocess.run(["bash", "-n", os.path.join(goc, t)], check=True)
    subprocess.run(["bash", "-n", os.path.join(GOC, "ha_tang", "spark", "submit.sh")], check=True)


def test_khoi_dong_may_nhan_dung_vai_tro():
    kq = subprocess.run(["bash", os.path.join(GOC, "scripts", "cum", "khoi_dong_may.sh"), "labsit", "--in-thu"],
                        capture_output=True, text=True, check=True).stdout
    assert "10.68.71.204" in kq and "kafka/docker-compose.yml" in kq and "up -d worker" in kq
    assert "spark-master" not in kq.split("vai trò:")[1].split("\n")[0]


def test_khoi_dong_may_chi_build_khi_chua_co_anh(tmp_path):
    # Giả lập lệnh docker: "image inspect" thành công, tức máy đã có ảnh bd03/spark.
    gia = tmp_path / "docker"
    gia.write_text("#!/bin/sh\nexit 0\n")
    gia.chmod(0o755)
    moi_truong = dict(os.environ, PATH="{}:{}".format(tmp_path, os.environ["PATH"]))
    lenh = ["bash", os.path.join(GOC, "scripts", "cum", "khoi_dong_may.sh"), "labsit12", "--in-thu"]
    dong_spark = lambda kq: [d for d in kq.splitlines() if "docker-compose-worker.yml" in d][0]
    kq = subprocess.run(lenh, capture_output=True, text=True, check=True, env=moi_truong).stdout
    assert dong_spark(kq).endswith("up -d")  # đã có ảnh: không build lại
    kq = subprocess.run(lenh + ["--build"], capture_output=True, text=True, check=True, env=moi_truong).stdout
    assert dong_spark(kq).endswith("up -d --build")


def _ssh_gia(tmp_path):
    """Lệnh ssh giả: in tên máy và lệnh nhận được; máy labsit giả vờ lỗi (mã 3)."""
    gia = tmp_path / "ssh"
    gia.write_text(
        "#!/bin/bash\n"
        "while [ $# -gt 0 ]; do case \"$1\" in -o) shift 2;; -n|-t) shift;; *) break;; esac; done\n"
        "echo \"may=$1 lenh=$2\"\n"
        "[ \"$1\" = labsit ] && exit 3\n"
        "exit 0\n"
    )
    gia.chmod(0o755)
    return dict(os.environ, PATH="{}:{}".format(tmp_path, os.environ["PATH"]))


def test_ssh_tat_ca_chon_may_va_thu_tu(tmp_path):
    mt = _ssh_gia(tmp_path)
    tep = os.path.join(GOC, "scripts", "cum", "ssh_tat_ca.sh")
    chay = lambda *a: subprocess.run(["bash", tep, *a], capture_output=True, text=True, env=mt)
    kq = chay("hostname")
    may = [d.split()[0][4:] for d in kq.stdout.splitlines() if d.startswith("may=")]
    # 6 máy lab, master đứng đầu; không có máy web và máy Windows điều khiển
    assert may == ["labsoe-11", "labsit12", "labsit", "labsit-may25", "labsit-may26", "labsoe-18"]
    assert kq.returncode == 1 and "!! labsit: mã thoát 3" in kq.stdout
    assert len([d for d in chay("--vai-tro", "spark-worker", "x").stdout.splitlines() if d.startswith("may=")]) == 5
    assert "may=ctam lenh=uptime" in chay("--may", "ctam,labsit12", "uptime").stdout
    ss = chay("--song-song", "nproc").stdout
    assert ss.count("=====") == 6 and "[labsoe-18] may=labsoe-18 lenh=nproc" in ss
    cfg = chay("--tao-config").stdout
    assert cfg.count("Host ") == 7 and "HostName 10.68.71.205" in cfg and "10.68.71.59" not in cfg
    khoi = {b.split()[0]: b for b in cfg.split("Host ")[1:]}  # "HostName" không có dấu cách sau "Host"
    assert "User labsit12" in khoi["labsit12"] and "User hduser" in khoi["labsoe-11"] and "User labsit" in khoi["labsit-may26"]
    assert "chưa khai báo tài khoản" in cfg and "StrictHostKeyChecking accept-new" in cfg and "ConnectTimeout 20" in cfg
    kich_ban = tmp_path / "s.sh"  # tên khác "tep": lambda chay đang dùng biến tep
    kich_ban.write_text("hostname\n")
    kq = chay("--song-song", "--chay-tep", str(kich_ban))
    assert kq.stdout.count("lenh=bash -s --") == 6 and "10.68.71.182" not in kq.stdout


def test_ssh_tat_ca_dong_bo_ma(tmp_path):
    """--dong-bo-ma: git push qua ssh giả (chạy lệnh ngay trên máy này, mỗi máy một HOME riêng).

    Dùng một repo tạm chứa bản sao script và danh bạ, không đụng tới repo thật."""
    import shutil
    repo = tmp_path / "repo"
    (repo / "scripts" / "cum").mkdir(parents=True)
    (repo / "cau_hinh").mkdir()
    shutil.copy(os.path.join(GOC, "scripts", "cum", "ssh_tat_ca.sh"), repo / "scripts" / "cum")
    shutil.copy(os.path.join(GOC, "cau_hinh", "cum.env"), repo / "cau_hinh")
    danh_tinh = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t", GIT_COMMITTER_NAME="t",
                     GIT_COMMITTER_EMAIL="t@t")
    git = lambda *a: subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True, env=danh_tinh)
    git("init", "-q", "-b", "main")
    git("add", "-A")
    git("commit", "-q", "-m", "ban dau")

    goc_may = tmp_path / "may"
    gia = tmp_path / "bin" / "ssh"
    gia.parent.mkdir()
    gia.write_text(
        "#!/bin/bash\n"
        "while [ $# -gt 0 ]; do case \"$1\" in -o|-p) shift 2;; -G) exit 0;; -*) shift;; *) break;; esac; done\n"
        "h=\"$1\"; shift; mkdir -p \"$GOC_MAY/$h\"; cd \"$GOC_MAY/$h\"\n"
        "HOME=\"$GOC_MAY/$h\" exec bash -c \"$*\"\n"
    )
    gia.chmod(0o755)
    mt = dict(danh_tinh, PATH="{}:{}".format(gia.parent, os.environ["PATH"]), GOC_MAY=str(goc_may),
              GIT_SSH_VARIANT="ssh")
    lenh = ["bash", str(repo / "scripts" / "cum" / "ssh_tat_ca.sh"), "--may", "labsit12,labsoe-18", "--dong-bo-ma"]
    chay = lambda: subprocess.run(lenh, capture_output=True, text=True, env=mt)

    kq = chay()
    assert kq.returncode == 0, kq.stdout + kq.stderr
    (repo / "GHI_CHU.md").write_text("ban moi\n")
    git("add", "-A")
    git("commit", "-q", "-m", "ban moi")
    kq = chay()  # máy đích đã có repo: push cập nhật, thư mục làm việc phải có tệp mới
    assert kq.returncode == 0, kq.stdout + kq.stderr
    for may in ("labsit12", "labsoe-18"):
        assert (goc_may / may / "bd03_khuyen_nghi" / "GHI_CHU.md").read_text() == "ban moi\n"

    # Tệp bị sửa tay trên máy đích: push phải bị từ chối thay vì ghi đè
    (goc_may / "labsit12" / "bd03_khuyen_nghi" / "GHI_CHU.md").write_text("sua tay\n")
    git("commit", "-q", "--allow-empty", "-m", "ban thu ba")
    kq = chay()
    assert kq.returncode == 1 and "!! labsit12: git push thất bại" in kq.stdout
    assert (goc_may / "labsit12" / "bd03_khuyen_nghi" / "GHI_CHU.md").read_text() == "sua tay\n"


def test_do_mang_chi_ping(tmp_path):
    """do_mang.sh --chi-ping: hỏi IP LAN từng máy, ping chéo qua ZeroTier và LAN, phân loại đúng nguyên nhân."""
    tep = os.path.join(GOC, "scripts", "cum", "ssh_tat_ca.sh")
    ds = subprocess.run(["bash", tep, "--liet-ke"], capture_output=True, text=True, check=True).stdout.split("\n")
    assert ds[0] == "labsoe-11 10.68.71.113" and len([d for d in ds if d]) == 6
    bin_gia = tmp_path / "bin"
    bin_gia.mkdir()
    lenh_gia = {
        # ssh giả: chạy lệnh ngay trên máy này, báo tên máy cho các lệnh giả qua biến MAY
        "ssh": "while [ $# -gt 0 ]; do case \"$1\" in -o) shift 2;; -*) shift;; *) break;; esac; done\n"
               "h=\"$1\"; shift; MAY=\"$h\" exec bash -c \"$*\"\n",
        # mỗi máy: đuôi IP ZeroTier và đuôi IP LAN (192.168.80.x) như kết quả đo ngày 10/11
        "ip": "case \"$MAY\" in labsoe-11) z=113 l=103;; labsit12) z=198 l=156;; labsit) z=204 l=127;;\n"
              "  labsit-may25) z=42 l=135;; labsit-may26) z=90 l=155;; *) z=149 l=234;; esac\n"
              "case \"$*\" in 'route get 1.1.1.1') echo \"1.1.1.1 via 192.168.80.254 dev wlo1 src 192.168.80.$l uid 1\";;\n"
              "  route) echo 'default via 192.168.80.254 dev wlo1';; *) echo \"5: zt inet 10.68.71.$z/24 scope global zt\";; esac\n",
        # .204 đi relay nhưng LAN của nó thông; .42 đi relay và LAN của nó không trả lời;
        # .90 chậm cả qua ZeroTier lẫn LAN (Wi-Fi nghẽn, vẫn nối thẳng)
        "ping": "d=\"${@: -1}\"; echo '4 packets transmitted'\n"
                "case \"$d\" in 10.68.71.204) echo 'rtt min/avg/max/mdev = 200/312.5/700/90 ms';;\n"
                "  10.68.71.42) echo 'rtt min/avg/max/mdev = 150/201.8/260/30 ms';; 192.168.80.135) exit 1;;\n"
                "  10.68.71.90) echo 'rtt min/avg/max/mdev = 60/80.2/99/9 ms';;\n"
                "  192.168.80.155) echo 'rtt min/avg/max/mdev = 50/75.4/90/9 ms';;\n"
                "  *) echo 'rtt min/avg/max/mdev = 3/4.5/7/1 ms';; esac\n",
    }
    for ten, noi_dung in lenh_gia.items():
        (bin_gia / ten).write_text("#!/bin/bash\n" + noi_dung)
        (bin_gia / ten).chmod(0o755)
    mt = dict(os.environ, PATH="{}:{}".format(bin_gia, os.environ["PATH"]))
    kq = subprocess.run(["bash", os.path.join(GOC, "scripts", "cum", "do_mang.sh"), "--chi-ping"],
                        capture_output=True, text=True, env=mt)
    assert kq.returncode == 0, kq.stdout + kq.stderr
    assert kq.stdout.count("=====") == 6 and "Phần 2" not in kq.stdout
    dong = [d for d in kq.stdout.splitlines() if d.startswith("[labsit12] ping")]
    assert len(dong) == 5  # bỏ qua chính IP ZeroTier của máy
    tim = lambda ip: [d for d in dong if "ping " + ip + " " in d][0]
    assert "192.168.80.127" in tim("10.68.71.204") and "RELAY du LAN thong" in tim("10.68.71.204")
    assert "Wi-Fi chan" in tim("10.68.71.42")
    assert tim("10.68.71.90").endswith("| noi thang, Wi-Fi dang cham")
    assert tim("10.68.71.113").endswith("| noi thang")
    assert "20 cặp nối thẳng, 10 cặp đi relay (5 cặp LAN vẫn thông), 0 cặp không ping được" in kq.stdout
    assert "cau_hinh_zerotier.sh" in kq.stdout


def test_chay_lan_dau_tu_buoc(tmp_path):
    """TU_BUOC: chạy tiếp từ bước bị lỗi; từ bước 6 bắt buộc có MA_CHAY của lần tuning trước."""
    nhat_ky = tmp_path / "docker.log"
    gia = tmp_path / "docker"
    gia.write_text("#!/bin/bash\necho \"$*\" >> {}\nexit 0\n".format(nhat_ky))
    gia.chmod(0o755)
    mt = dict(os.environ, PATH="{}:{}".format(tmp_path, os.environ["PATH"]), LUOI="toi_thieu")
    tep = os.path.join(GOC, "scripts", "chay_lan_dau.sh")

    def cac_job(**bien):
        if nhat_ky.exists():
            nhat_ky.unlink()
        kq = subprocess.run(["bash", tep], capture_output=True, text=True, env=dict(mt, **bien))
        dong = nhat_ky.read_text().splitlines() if nhat_ky.exists() else []
        return kq, [d.split("/opt/spark/jobs/")[-1] for d in dong if "spark-submit" in d]

    kq, job = cac_job(TU_BUOC="4")
    assert kq.returncode == 0, kq.stdout + kq.stderr
    assert job[0] == "transform/bronze_to_silver.py" and len(job) == 12  # bước 4-8
    assert not any("khoi_tao_bang" in j or "nhap_danh_muc" in j or "nap_su_kien" in j for j in job)
    assert any(j.startswith("ml/huan_luyen_als.py") and "--luoi toi_thieu" in j for j in job)

    kq, job = cac_job(TU_BUOC="6")
    assert kq.returncode == 2 and "MA_CHAY" in kq.stderr and job == []

    kq, job = cac_job(TU_BUOC="6", MA_CHAY="r20261110T120000")
    assert kq.returncode == 0 and job[0] == "ml/sinh_goi_y_toan_bo.py --ma-chay r20261110T120000"


def test_chep_du_lieu(tmp_path):
    """chep_du_lieu.sh: tải zip trên laptop (khóa không nằm trên dòng lệnh curl), chép sang master
    đủ 4 CSV và sang máy Kafka chỉ events.csv; zip có thư mục con vẫn tìm được tệp."""
    import json
    import shutil
    repo = tmp_path / "BIG DATA" / "repo"  # có dấu cách như đường dẫn thật D:\BIG DATA
    (repo / "scripts" / "cum").mkdir(parents=True)
    (repo / "cau_hinh").mkdir()
    for t in ("chep_du_lieu.sh", "ssh_tat_ca.sh"):
        shutil.copy(os.path.join(GOC, "scripts", "cum", t), repo / "scripts" / "cum")
    shutil.copy(os.path.join(GOC, "cau_hinh", "cum.env"), repo / "cau_hinh")
    (tmp_path / "BIG DATA" / "kaggle.json").write_text(json.dumps({"username": "sv", "key": "KHOA_BI_MAT_123"}))

    bin_gia, goc_may = tmp_path / "bin", tmp_path / "may"
    bin_gia.mkdir()
    lenh_gia = {
        # curl giả: ghi lại đối số, đọc stdin (cấu hình -K -), tạo zip ~2 MB có thư mục con
        "curl": "echo \"$*\" >> \"$NHAT_KY\"; cat > \"$NHAT_KY.stdin\"\n"
                "while [ $# -gt 0 ]; do [ \"$1\" = -o ] && ra=\"$2\"; shift; done\n"
                "python3 -c \"import sys,zipfile,os\nz=zipfile.ZipFile(sys.argv[1],'w')\n"
                "[z.writestr('archive/'+t,'a,b\\n'+'x'*700000) for t in "
                "('events.csv','item_properties_part1.csv','item_properties_part2.csv','category_tree.csv')]\" \"$ra\"\n",
        "scp": "while [ $# -gt 0 ]; do case \"$1\" in -o) shift 2;; *) break;; esac; done\n"
               "h=\"${2%%:*}\"; mkdir -p \"$GOC_MAY/$h\"; cp \"$1\" \"$GOC_MAY/$h/${2#*:}\"\n",
        "ssh": "while [ $# -gt 0 ]; do case \"$1\" in -o) shift 2;; -*) shift;; *) break;; esac; done\n"
               "h=\"$1\"; shift; mkdir -p \"$GOC_MAY/$h\"; HOME=\"$GOC_MAY/$h\" exec bash -c \"$*\"\n",
    }
    for ten, noi_dung in lenh_gia.items():
        (bin_gia / ten).write_text("#!/bin/bash\n" + noi_dung)
        (bin_gia / ten).chmod(0o755)
    nhat_ky = tmp_path / "curl.log"
    mt = dict(os.environ, PATH="{}:{}".format(bin_gia, os.environ["PATH"]), GOC_MAY=str(goc_may), NHAT_KY=str(nhat_ky))
    kq = subprocess.run(["bash", str(repo / "scripts" / "cum" / "chep_du_lieu.sh")], capture_output=True, text=True, env=mt)
    assert kq.returncode == 0, kq.stdout + kq.stderr

    assert "KHOA_BI_MAT_123" not in nhat_ky.read_text()          # không lộ trên dòng lệnh
    assert "sv:KHOA_BI_MAT_123" in (tmp_path / "curl.log.stdin").read_text()
    assert (tmp_path / "BIG DATA" / "retailrocket.zip").exists()  # giữ lại ngoài repo
    master = goc_may / "labsoe-11" / "du_lieu" / "retailrocket"
    assert sorted(os.listdir(master)) == ["category_tree.csv", "events.csv", "item_properties_part1.csv",
                                          "item_properties_part2.csv"]
    assert os.listdir(goc_may / "labsit" / "du_lieu" / "retailrocket") == ["events.csv"]
    assert not (goc_may / "labsoe-11" / "retailrocket.zip").exists()  # zip trên máy đích đã xóa

    # Lần hai: zip đã có (>100 MB thì dùng lại; ở đây nhỏ nên tải lại), --zip dùng tệp có sẵn không gọi curl
    nhat_ky.unlink()
    kq = subprocess.run(["bash", str(repo / "scripts" / "cum" / "chep_du_lieu.sh"), "--zip",
                         str(tmp_path / "BIG DATA" / "retailrocket.zip")], capture_output=True, text=True, env=mt)
    assert kq.returncode == 0 and not nhat_ky.exists(), kq.stdout + kq.stderr


def test_don_du_lieu(tmp_path):
    """don_du_lieu.sh: mặc định chỉ xem trước; --xoa cần gõ XOA; chỉ xóa đường dẫn của dự án,
    giữ thư mục khác (dữ liệu cũ của nhóm); từ chối khi còn ứng dụng Spark đang chạy."""
    R = "/lakehouse/bd03_khuyen_nghi"
    import http.server
    import threading
    web = tmp_path / "web" / "json"
    web.mkdir(parents=True)
    (web / "index.html").write_text('{"activeapps": []}')
    xu_ly = lambda *a, **k: http.server.SimpleHTTPRequestHandler(*a, directory=str(tmp_path / "web"), **k)
    may_chu = http.server.ThreadingHTTPServer(("127.0.0.1", 0), xu_ly)
    threading.Thread(target=may_chu.serve_forever, daemon=True).start()

    goc_may, bin_gia, nhat_ky = tmp_path / "may", tmp_path / "bin", tmp_path / "docker.log"
    for h in ("labsoe-11", "labsit"):
        (goc_may / h).mkdir(parents=True)
        os.symlink(GOC, goc_may / h / "bd03_khuyen_nghi")
    bin_gia.mkdir()
    ls_lake = "\\n".join("drwxr-xr-x   - u g 0 2026-11-10 12:00 " + d for d in (
        "/lakehouse/bd03_khuyen_nghi/bronze", "/lakehouse/bd03_khuyen_nghi/bronze/su_kien", "/lakehouse/bd03_khuyen_nghi/bronze/cu", "/lakehouse/bd03_khuyen_nghi/silver",
        "/lakehouse/bd03_khuyen_nghi/silver/tuong_tac", "/lakehouse/bd03_khuyen_nghi/cu_nhom", "/lakehouse/bd03_khuyen_nghi/checkpoints", "/lakehouse/bd03_khuyen_nghi/checkpoints/nap_su_kien_v2"))
    lenh_gia = {
        "ssh": "while [ $# -gt 0 ]; do case \"$1\" in -o) shift 2;; -*) shift;; *) break;; esac; done\n"
               "h=\"$1\"; shift; HOME=\"$GOC_MAY/$h\" exec bash -c \"$*\"\n",
        "docker": "echo \"$*\" >> \"$NHAT_KY\"\n"
                  "case \"$*\" in\n"
                  "  *' -du '*) for p in \"$@\"; do case \"$p\" in *silver/tuong_tac|*landing/retailrocket) echo \"1.5 M  3.0 M  $p\";; esac; done;;\n"
                  "  *' -ls -d '*) printf '" + ls_lake + "\\n';;\n"
                  "  *kafka-get-offsets*) printf 'x:0:100\\nx:1:50\\n';;\n"
                  "  *' --list'*) ;;\n"
                  "esac\nexit 0\n",
    }
    for ten, noi_dung in lenh_gia.items():
        (bin_gia / ten).write_text("#!/bin/bash\n" + noi_dung)
        (bin_gia / ten).chmod(0o755)
    mau = tmp_path / "mau"
    mau.mkdir()
    mt = dict(os.environ, PATH="{}:{}".format(bin_gia, os.environ["PATH"]), GOC_MAY=str(goc_may),
              NHAT_KY=str(nhat_ky), SPARK_UI="http://127.0.0.1:{}".format(may_chu.server_port), THU_MUC_MAU=str(mau))
    tep = os.path.join(GOC, "scripts", "cum", "don_du_lieu.sh")
    chay = lambda *a, nhap="": subprocess.run(["bash", tep, *a], input=nhap, capture_output=True, text=True, env=mt)
    try:
        kq = chay()  # xem trước
        assert kq.returncode == 0, kq.stdout + kq.stderr
        assert R + "/silver/tuong_tac" in kq.stdout and "1.5 M" in kq.stdout and "150 tin" in kq.stdout
        giu = kq.stdout.split("GIỮ NGUYÊN")[1].split("#####")[0]
        assert R + "/bronze/cu\n" in giu and R + "/cu_nhom" in giu
        assert "/bronze/su_kien\n" not in giu and R + "/silver" not in giu and R + "/checkpoints" not in giu
        assert " -rm " not in nhat_ky.read_text() and "--delete" not in nhat_ky.read_text() and mau.exists()

        kq = chay("--xoa", nhap="khong\n")  # không gõ đúng XOA: không xóa
        assert kq.returncode == 1 and " -rm " not in nhat_ky.read_text()

        kq = chay("--xoa", nhap="XOA\n")
        assert kq.returncode == 0, kq.stdout + kq.stderr
        dong_xoa = [d for d in nhat_ky.read_text().splitlines() if " -rm " in d]
        assert len(dong_xoa) == 1 and "silver/tuong_tac" in dong_xoa[0] and "landing/retailrocket" in dong_xoa[0]
        assert "bronze/su_kien" not in dong_xoa[0]  # du giả không báo có, nên không đưa vào lệnh xóa
        assert "bronze/cu" not in dong_xoa[0] and "cu_nhom" not in dong_xoa[0]
        assert nhat_ky.read_text().count("--delete --topic") == 2 and "--create" in nhat_ky.read_text()
        assert not mau.exists()

        (web / "index.html").write_text('{"activeapps": [{"name": "bd03-lo"}]}')
        kq = chay()
        assert kq.returncode == 1 and "đang" in kq.stdout.lower() and "ứng dụng Spark" in kq.stdout
    finally:
        may_chu.shutdown()


def test_dags_airflow():
    pytest.importorskip("airflow.sdk")  # thư mục airflow/ của dự án không phải gói Airflow
    from airflow.dag_processing.dagbag import DagBag

    db = DagBag(dag_folder=os.path.join(GOC, "airflow", "dags"))
    assert db.import_errors == {}
    assert set(db.dags) == {"kn_pipeline_hang_ngay", "kn_giam_sat_streaming", "kn_tinh_ab", "kn_bao_tri", "kn_tai_lap"}
    lich = {k: d.schedule for k, d in db.dags.items()}
    assert lich["kn_pipeline_hang_ngay"] == "0 2 * * *" and lich["kn_giam_sat_streaming"] == "*/15 * * * *"
    assert lich["kn_tinh_ab"] == "0 * * * *" and lich["kn_bao_tri"] == "0 4 * * *" and lich["kn_tai_lap"] is None
    re_nhanh = db.dags["kn_pipeline_hang_ngay"].get_task("ndcg_co_cai_thien")
    assert re_nhanh.downstream_task_ids == {"sinh_goi_y_toan_bo", "giu_mo_hinh_cu"}


def test_do_zorder_tien_ich(spark, tmp_path):
    from bao_tri.do_zorder import phan_vi, so_tep_da_doc

    assert phan_vi([1, 2, 3, 4, 5], 0.5) == 3 and phan_vi([], 0.9) is None
    spark.range(100).repartition(4).write.parquet(str(tmp_path / "p"))
    df = spark.read.parquet(str(tmp_path / "p")).filter("id = 5")
    df.collect()
    assert so_tep_da_doc(df) == 4


def test_tao_cau_hinh_airflow_uid_va_quyen(tmp_path):
    """Airflow chạy bằng uid của chủ thư mục dự án (AIRFLOW_UID); .env cũ chỉ được bổ sung dòng
    AIRFLOW_UID, mật khẩu cũ giữ nguyên; mat_khau.json chỉ chủ tệp đọc/ghi."""
    import json
    import shutil
    import stat

    goc = tmp_path / "du_an"
    (goc / "scripts" / "cum").mkdir(parents=True)
    (goc / "ha_tang" / "airflow").mkdir(parents=True)
    shutil.copy(os.path.join(GOC, "scripts", "cum", "tao_cau_hinh_airflow.sh"), goc / "scripts" / "cum")
    shutil.copy(os.path.join(GOC, "ha_tang", "airflow", ".env.mau"), goc / "ha_tang" / "airflow")
    bin_gia = tmp_path / "bin"
    bin_gia.mkdir()
    (bin_gia / "getent").write_text("#!/bin/sh\necho docker:x:998:\n")
    (bin_gia / "getent").chmod(0o755)
    mt = dict(os.environ, PATH="{}:{}".format(bin_gia, os.environ["PATH"]), MAT_KHAU_AIRFLOW="mk-thu")
    tep = str(goc / "scripts" / "cum" / "tao_cau_hinh_airflow.sh")
    a = goc / "ha_tang" / "airflow"

    subprocess.run(["bash", tep], check=True, capture_output=True, text=True, env=mt)
    env = (a / ".env").read_text()
    assert "AIRFLOW_UID={}\n".format(os.getuid()) in env and "DOCKER_GID=998\n" in env
    assert "BD03_HOST_DIR={}\n".format(goc) in env
    assert json.loads((a / "mat_khau.json").read_text()) == {"admin": "mk-thu"}
    assert stat.S_IMODE((a / "mat_khau.json").stat().st_mode) == 0o600

    # .env tạo bằng bản cũ (chưa có AIRFLOW_UID), mat_khau.json đang mở quyền đọc cho mọi người
    (a / ".env").write_text("".join(d + "\n" for d in env.splitlines() if not d.startswith("AIRFLOW_UID=")))
    (a / "mat_khau.json").chmod(0o664)
    kq = subprocess.run(["bash", tep], check=True, capture_output=True, text=True, env=dict(mt, MAT_KHAU_AIRFLOW="khac"))
    assert "Đã thêm AIRFLOW_UID" in kq.stdout
    assert (a / ".env").read_text().count("AIRFLOW_UID=") == 1
    assert json.loads((a / "mat_khau.json").read_text()) == {"admin": "mk-thu"}  # không đổi mật khẩu
    assert stat.S_IMODE((a / "mat_khau.json").stat().st_mode) == 0o600
    kq = subprocess.run(["bash", tep], check=True, capture_output=True, text=True, env=mt)
    assert "Đã thêm" not in kq.stdout and (a / ".env").read_text().count("AIRFLOW_UID=") == 1


def test_ssh_tat_ca_dong_bo_ma_thu_lai_khi_mang_rot(tmp_path):
    """Lỗi mạng khi git push (rớt ở bước bắt tay SSH) được thử lại; rớt quá 3 lần thì báo mất kết nối,
    không đổ lỗi cho tệp sửa tay."""
    import shutil
    repo = tmp_path / "repo"
    (repo / "scripts" / "cum").mkdir(parents=True)
    (repo / "cau_hinh").mkdir()
    shutil.copy(os.path.join(GOC, "scripts", "cum", "ssh_tat_ca.sh"), repo / "scripts" / "cum")
    shutil.copy(os.path.join(GOC, "cau_hinh", "cum.env"), repo / "cau_hinh")
    danh_tinh = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t", GIT_COMMITTER_NAME="t",
                     GIT_COMMITTER_EMAIL="t@t")
    for lenh_git in (["init", "-q", "-b", "main"], ["add", "-A"], ["commit", "-q", "-m", "ban dau"]):
        subprocess.run(["git", "-C", str(repo), *lenh_git], check=True, capture_output=True, env=danh_tinh)

    goc_may, dem = tmp_path / "may", tmp_path / "dem"
    gia = tmp_path / "bin" / "ssh"
    gia.parent.mkdir()
    # Lần gọi git-receive-pack thứ 1..SO_LAN_ROT giả vờ rớt mạng.
    gia.write_text(
        "#!/bin/bash\n"
        "while [ $# -gt 0 ]; do case \"$1\" in -o|-p) shift 2;; -G) exit 0;; -*) shift;; *) break;; esac; done\n"
        "h=\"$1\"; shift\n"
        "if [[ \"$*\" == *receive-pack* ]]; then n=$(( $(cat \"$DEM\" 2>/dev/null || echo 0) + 1 )); echo $n > \"$DEM\";\n"
        "  [ $n -le $SO_LAN_ROT ] && { echo 'Connection timed out during banner exchange' >&2; exit 255; }; fi\n"
        "mkdir -p \"$GOC_MAY/$h\"; cd \"$GOC_MAY/$h\"\n"
        "HOME=\"$GOC_MAY/$h\" exec bash -c \"$*\"\n"
    )
    gia.chmod(0o755)
    mt = dict(danh_tinh, PATH="{}:{}".format(gia.parent, os.environ["PATH"]), GOC_MAY=str(goc_may),
              GIT_SSH_VARIANT="ssh", DEM=str(dem))
    lenh = ["bash", str(repo / "scripts" / "cum" / "ssh_tat_ca.sh"), "--may", "labsit12", "--dong-bo-ma"]

    kq = subprocess.run(lenh, capture_output=True, text=True, env=dict(mt, SO_LAN_ROT="2"))
    assert kq.returncode == 0, kq.stdout + kq.stderr
    assert kq.stdout.count("thử lại lần") == 2 and (goc_may / "labsit12" / "bd03_khuyen_nghi" / "cau_hinh").is_dir()

    dem.unlink()
    kq = subprocess.run(lenh, capture_output=True, text=True, env=dict(mt, SO_LAN_ROT="5"))
    assert kq.returncode == 1 and "mất kết nối SSH sau 3 lần thử" in kq.stdout
    assert "tệp bị sửa tay" not in kq.stdout and dem.read_text().strip() == "3"


def test_tinh_trang_doc_json_spark(tmp_path):
    """tinh_trang.sh: in số worker, lõi và từng ứng dụng đang chạy từ JSON của Spark Master UI;
    không có container Airflow thì báo, không lỗi."""
    import json

    bin_gia = tmp_path / "bin"
    bin_gia.mkdir()
    json_spark = {"aliveworkers": 5, "cores": 34, "coresused": 14, "memory": 13312, "memoryused": 7424,
                  "activeapps": [{"name": "bd03-tinh-thinh-hanh", "cores": 2, "state": "RUNNING", "id": "app-1"},
                                 {"name": "bd03-tai-lap", "cores": 10, "state": "RUNNING", "id": "app-2"}]}
    (tmp_path / "spark.json").write_text(json.dumps(json_spark))
    gia = {
        "curl": "#!/bin/sh\ncat {}\n".format(tmp_path / "spark.json"),
        "docker": "#!/bin/sh\n[ \"$1\" = inspect ] && exit 1\necho 'spark-master\t2GiB / 7GiB'\n",
        "free": "#!/bin/sh\necho 'Mem: 7,4Gi'\n",
        "tmux": "#!/bin/sh\necho 'tien_hoa: 1 windows'\n",
    }
    for ten, noi_dung in gia.items():
        (bin_gia / ten).write_text(noi_dung)
        (bin_gia / ten).chmod(0o755)
    mt = dict(os.environ, PATH="{}:{}".format(bin_gia, os.environ["PATH"]))
    kq = subprocess.run(["bash", os.path.join(GOC, "scripts", "cum", "tinh_trang.sh")],
                        capture_output=True, text=True, env=mt)
    assert kq.returncode == 0, kq.stderr
    assert "worker sống: 5" in kq.stdout and "14/34" in kq.stdout
    assert "bd03-tinh-thinh-hanh" in kq.stdout and "bd03-tai-lap" in kq.stdout
    assert "không thấy container" in kq.stdout and "tien_hoa" in kq.stdout


def test_sensor_dag1_bo_qua_cho_khi_chay_tay(monkeypatch):
    """Sensor DAG 1: lần chạy tay với bo_qua_cho_du_lieu = true chạy ngay, không gọi WebHDFS;
    lần chạy theo lịch vẫn so mốc bronze/silver như cũ."""
    import importlib
    import sys
    import types

    gia_pendulum = types.SimpleNamespace(timezone=lambda *a, **k: None, datetime=lambda *a, **k: None)
    monkeypatch.setitem(sys.modules, "pendulum", gia_pendulum)
    monkeypatch.syspath_prepend(os.path.join(GOC, "airflow", "dags"))
    sys.modules.pop("bd03_chung", None)
    chung = importlib.import_module("bd03_chung")

    goi = []
    moc = {"bronze": 2, "silver": 1}
    monkeypatch.setattr(chung, "_webhdfs_mtime",
                        lambda p: goi.append(p) or moc["bronze" if "bronze" in p else "silver"])
    assert chung.co_du_lieu_moi(params={"bo_qua_cho_du_lieu": True}) is True and goi == []
    assert chung.co_du_lieu_moi(params={"bo_qua_cho_du_lieu": False}) is True and len(goi) == 2
    moc["bronze"] = 1
    assert chung.co_du_lieu_moi(params={}) is False
    assert chung.co_du_lieu_moi() is False
    sys.modules.pop("bd03_chung", None)


def test_khoi_dong_may_ha_mtu_zerotier(tmp_path):
    """ZT_MTU: tìm đúng card mang IP ZeroTier của máy và hạ MTU (chỉ in lệnh ở --in-thu);
    không đặt ZT_MTU thì không đụng tới card mạng."""
    gia = tmp_path / "ip"
    gia.write_text("#!/bin/bash\necho '2: eth0    inet 172.19.209.111/20 brd 172.19.223.255 scope global eth0'\n"
                   "echo '3: ztabc123    inet 10.68.71.205/24 brd 10.68.71.255 scope global ztabc123'\n")
    gia.chmod(0o755)
    mt = dict(os.environ, PATH="{}:{}".format(tmp_path, os.environ["PATH"]))
    lenh = ["bash", os.path.join(GOC, "scripts", "cum", "khoi_dong_may.sh"), "--in-thu"]
    kq = subprocess.run(lenh, capture_output=True, text=True, env=dict(mt, ZT_MTU="1280"))
    assert kq.returncode == 0, kq.stdout + kq.stderr
    assert "== Máy ctam (10.68.71.205)" in kq.stdout
    assert "+ sudo ip link set ztabc123 mtu 1280" in kq.stdout
    kq = subprocess.run(lenh, capture_output=True, text=True, env=mt)
    assert kq.returncode == 0 and "mtu" not in kq.stdout


def test_khoi_dong_may_chi_mot_vai_tro():
    """--chi=<vai_tro>: chỉ bật đúng vai trò đó (không chạm DataNode/Spark worker đang chạy);
    vai trò máy không có thì dừng với mã lỗi."""
    tep = os.path.join(GOC, "scripts", "cum", "khoi_dong_may.sh")
    kq = subprocess.run(["bash", tep, "labsit", "--chi=kafka", "--in-thu"], capture_output=True, text=True)
    assert kq.returncode == 0, kq.stdout + kq.stderr
    lenh = [d for d in kq.stdout.splitlines() if d.startswith("+ docker")]
    assert lenh and all("kafka/docker-compose.yml" in d for d in lenh)
    kq = subprocess.run(["bash", tep, "labsit12", "--chi=namenode", "--in-thu"], capture_output=True, text=True)
    assert kq.returncode == 1 and "không có vai trò 'namenode'" in kq.stdout


def test_luong_dung_va_khoi_dong_xu_ly_driver_ma(tmp_path):
    """luong.sh: hủy ứng dụng qua Master chưa đủ — driver client mode vẫn sống và tranh checkpoint
    với luồng mới (NHAT_KY lỗi 27). khoi_dong phải dừng driver "ma" trước khi nộp job;
    trang_thai phải cảnh báo; dung phải gửi TERM cho tiến trình driver."""
    nhat_ky, trang_thai_ma = tmp_path / "docker.log", tmp_path / "con_driver"
    (tmp_path / "docker").write_text(
        "#!/bin/bash\necho \"$*\" >> {log}\ncase \"$*\" in\n"
        "  *pgrep*) [ -f {ma} ] && echo 4242; exit 0 ;;\n"
        "  *pkill*) rm -f {ma}; exit 0 ;;\n"
        "esac\nexit 0\n".format(log=nhat_ky, ma=trang_thai_ma))
    (tmp_path / "curl").write_text("#!/bin/bash\necho '{\"activeapps\": []}'\n")
    for t in ("docker", "curl"):
        (tmp_path / t).chmod(0o755)
    mt = dict(os.environ, PATH="{}:{}".format(tmp_path, os.environ["PATH"]), CHO_DUNG_GIAY="2")
    tep = os.path.join(GOC, "scripts", "luong.sh")

    trang_thai_ma.write_text("x")
    kq = subprocess.run(["bash", tep, "trang_thai"], capture_output=True, text=True, env=mt)
    assert kq.returncode == 0 and "còn tiến trình driver 4242" in kq.stdout

    kq = subprocess.run(["bash", tep, "khoi_dong", "thinh_hanh"], capture_output=True, text=True, env=mt)
    assert kq.returncode == 0, kq.stdout + kq.stderr
    dong = nhat_ky.read_text().splitlines()
    vi_tri_kill = next(i for i, d in enumerate(dong) if "pkill -TERM -f streaming/tinh_thinh_hanh.py" in d)
    vi_tri_nop = next(i for i, d in enumerate(dong) if "spark-submit" in d and "tinh_thinh_hanh.py" in d)
    assert vi_tri_kill < vi_tri_nop and "driver đã dừng" in kq.stdout

    nhat_ky.unlink()
    trang_thai_ma.write_text("x")
    kq = subprocess.run(["bash", tep, "dung", "hien_thi"], capture_output=True, text=True, env=mt)
    assert kq.returncode == 0 and "pkill -TERM -f streaming/ghi_hien_thi.py" in nhat_ky.read_text()


def test_submit_chan_khi_du_lieu_con_o_goc_cu(tmp_path):
    """submit.sh không nộp job khi mã dùng gốc mới mà dữ liệu vẫn còn ở /lake (chưa chuyển gốc)."""
    bin_gia, nhat_ky = tmp_path / "bin", tmp_path / "docker.log"
    bin_gia.mkdir()
    (bin_gia / "docker").write_text('#!/bin/bash\necho "$*" >> "$NHAT_KY"\n')
    # curl giả: in mã HTTP theo đường dẫn được hỏi, đọc từ biến môi trường MA_MOI / MA_CU
    (bin_gia / "curl").write_text('#!/bin/bash\ncase "$*" in *lakehouse*) printf "$MA_MOI";; *) printf "$MA_CU";; esac\n')
    for t in ("docker", "curl"):
        (bin_gia / t).chmod(0o755)
    tep = os.path.join(GOC, "ha_tang", "spark", "submit.sh")
    mt = {k: v for k, v in os.environ.items() if k not in ("LAKE_GOC", "LAKE_DUONG_DAN")}
    mt.update(PATH="{}:{}".format(bin_gia, os.environ["PATH"]), NHAT_KY=str(nhat_ky))

    kq = subprocess.run(["bash", tep, "nhanh", "kiem_tra/kiem_tra_lakehouse.py"], capture_output=True, text=True,
                        env=dict(mt, MA_MOI="404", MA_CU="200"))
    assert kq.returncode == 3 and "chuyen_lakehouse.sh" in kq.stderr and not nhat_ky.exists()

    for moi, cu in (("200", "404"), ("404", "404"), ("000", "000")):   # đã chuyển / cụm mới / không hỏi được
        kq = subprocess.run(["bash", tep, "nhanh", "kiem_tra/kiem_tra_lakehouse.py"], capture_output=True, text=True,
                            env=dict(mt, MA_MOI=moi, MA_CU=cu))
        assert kq.returncode == 0, kq.stderr
    dong = nhat_ky.read_text().splitlines()
    assert len(dong) == 3 and all("-e LAKE_DUONG_DAN=/lakehouse/bd03_khuyen_nghi" in d for d in dong)


def test_chon_user_mau_do_zorder_khong_collect_toan_bo(spark):
    """do_zorder chọn user mẫu trên cụm: cố định theo hạt giống, đủ n, không trùng."""
    from bao_tri.do_zorder import chon_user_mau

    df = spark.createDataFrame([("u{}".format(i % 500), i) for i in range(5000)], "ma_nguoi_dung string, xep_hang int")
    a, b = chon_user_mau(df, 30), chon_user_mau(df, 30)
    assert a == b and len(a) == 30 and len(set(a)) == 30
    assert chon_user_mau(df, 30, hat_giong=7) != a
    assert len(chon_user_mau(df, 10_000)) == 500


def _bin_gia(tmp_path, docker):
    bin_gia = tmp_path / "bin"
    bin_gia.mkdir(exist_ok=True)
    (bin_gia / "docker").write_text("#!/bin/bash\n" + docker)
    (bin_gia / "docker").chmod(0o755)
    return dict(os.environ, PATH="{}:{}".format(bin_gia, os.environ["PATH"]), NHAT_KY=str(tmp_path / "docker.log"))


def test_delta_sql_demo_gui_dung_cau_lenh(tmp_path):
    """delta_sql.sh: mỗi câu là một đối số của chay_sql.py, tên bảng thành delta.`<gốc lakehouse>/...`."""
    mt = _bin_gia(tmp_path, 'for a in "$@"; do echo "$a"; done >> "$NHAT_KY"\n')
    tep = os.path.join(GOC, "scripts", "demo", "delta_sql.sh")
    kq = subprocess.run(["bash", tep, "demo", "u_123"], capture_output=True, text=True, env=mt)
    assert kq.returncode == 0, kq.stderr
    log = (tmp_path / "docker.log").read_text()
    assert "--master\nlocal[2]" in log and "/opt/spark/jobs/kiem_tra/chay_sql.py" in log
    assert "DESCRIBE HISTORY delta.`hdfs://10.68.71.113:8020/lakehouse/bd03_khuyen_nghi/gold/nhat_ky_hien_thi` LIMIT 4" in log
    assert "VERSION AS OF 1" in log and "ma_nguoi_dung = 'u_123'" in log and "gold/ket_qua_tai_lap" in log
    (tmp_path / "docker.log").unlink()
    kq = subprocess.run(["bash", tep, "cau", "SELECT count(*) FROM {bronze.su_kien}", "THAO_TAC {gold.ket_qua_ab}"],
                        capture_output=True, text=True, env=mt)
    log = (tmp_path / "docker.log").read_text()
    assert "parquet.`hdfs://10.68.71.113:8020/lakehouse/bd03_khuyen_nghi/bronze/su_kien`" in log
    assert "THAO_TAC delta.`hdfs://10.68.71.113:8020/lakehouse/bd03_khuyen_nghi/gold/ket_qua_ab`" in log


def test_cay_hdfs_loc_thu_muc(tmp_path):
    R = "/lakehouse/bd03_khuyen_nghi"
    dong = ["drwxr-xr-x   - root supergroup 0 2026-11-15 10:00 hdfs://nn:8020" + R + p for p in (
        "/bronze", "/bronze/su_kien", "/bronze/su_kien/ngay=2015-05-03", "/bronze/su_kien/_spark_metadata",
        "/silver/tuong_tac", "/silver/tuong_tac/_delta_log", "/checkpoints/nap_su_kien_v2",
        "/checkpoints/nap_su_kien_v2/offsets", "/_quarantine/tuong_tac", "/models/als/r1/metadata")]
    dong.append("-rw-r--r--   2 root supergroup 10 2026-11-15 10:00 hdfs://nn:8020" + R + "/silver/tuong_tac/x.parquet")
    (tmp_path / "ls.txt").write_text("\n".join(dong) + "\n")
    mt = _bin_gia(tmp_path, 'case "$*" in *"-ls -R"*) cat "' + str(tmp_path / "ls.txt") + '";; *) echo "$*";; esac\n')
    kq = subprocess.run(["bash", os.path.join(GOC, "scripts", "demo", "cay_hdfs.sh")], capture_output=True, text=True, env=mt)
    assert kq.returncode == 0, kq.stderr
    cay = kq.stdout.split("chỉ thư mục")[1].split("\n", 1)[1]
    for p in ("/bronze/su_kien", "/silver/tuong_tac", "/checkpoints/nap_su_kien_v2", "/_quarantine/tuong_tac"):
        assert R + p + "\n" in cay, p
    for p in ("ngay=", "_spark_metadata", "_delta_log", "/offsets", "/models/als/r1/metadata", "x.parquet"):
        assert p not in cay, p
    assert "hdfs dfs -du -h " + R in kq.stdout


def test_chay_sql_rut_gon_describe_history(spark):
    from datetime import datetime

    from kiem_tra.chay_sql import rut_gon_lich_su

    df = spark.createDataFrame(
        [(3, datetime(2026, 11, 15, 10), "u", "MERGE", {"predicate": "x" * 200}, {"numTargetRowsUpdated": "12"},
          '{"ma_chay": "r1"}', "Apache-Spark/3.5.3 Delta-Lake/3.2.0")],
        "version long, timestamp timestamp, userId string, operation string, operationParameters map<string,string>, "
        "operationMetrics map<string,string>, userMetadata string, engineInfo string")
    kq = rut_gon_lich_su(df).first().asDict()
    assert list(kq) == ["version", "timestamp", "operation", "operationParameters", "operationMetrics", "userMetadata"]
    assert kq["operationParameters"].endswith("...") and len(kq["operationParameters"]) == 73
    assert kq["operationMetrics"] == '{"numTargetRowsUpdated":"12"}'


def test_eda_tren_du_lieu_mau(spark, tmp_path):
    """kiem_tra/eda.py chạy được trên dữ liệu mẫu đúng định dạng RetailRocket, số liệu nhất quán."""
    import json

    from kiem_tra.eda import chuan_hoa_su_kien, tinh_eda
    from tao_du_lieu_mau import sinh

    sinh(str(tmp_path), so_nguoi=300, so_item=120, so_ngay=20)
    doc = lambda t: spark.read.option("header", True).csv(str(tmp_path / t))
    ev = chuan_hoa_su_kien(doc("events.csv"))
    kq = tinh_eda(ev, doc("item_properties_part1.csv").unionByName(doc("item_properties_part2.csv")),
                  doc("category_tree.csv"))
    json.dumps(kq)  # phải tuần tự hóa được
    tq = kq["tong_quan"]
    assert tq["so_su_kien"] == sum(d["so_su_kien"] for d in kq["theo_loai"]) == sum(d["so_su_kien"] for d in kq["theo_ngay"])
    assert sum(d["so_luong"] for d in kq["phan_bo_nguoi_dung"]) == tq["so_nguoi_dung"]
    assert sum(d["count"] for d in kq["theo_gio_utc"]) == tq["so_su_kien"]
    assert kq["chat_luong"]["so_nguoi_dung_bot"] >= 1 and kq["chat_luong"]["nhap_lap_trong_5_giay"] >= 1
    assert 0 < kq["tap_trung_san_pham"]["top_1pct"] <= kq["tap_trung_san_pham"]["top_10pct"] <= 1
    assert kq["pheu_nguoi_dung"]["co_xem"] >= kq["pheu_nguoi_dung"]["co_mua"]
    assert kq["thuoc_tinh"]["san_pham_co_danh_muc"] > 0 and kq["cay_danh_muc"]["so_danh_muc"] > 0


def test_conf_spark_defaults_khop_submit():
    """conf/spark-defaults.conf (mục A.5) phải khớp các --conf cố định mà submit.sh truyền cho mọi job."""
    import re

    mac_dinh = {}
    for dong in open(os.path.join(GOC, "conf", "spark-defaults.conf"), encoding="utf-8"):
        if dong.strip() and not dong.startswith("#"):
            khoa, gia_tri = dong.split(None, 1)
            mac_dinh[khoa] = gia_tri.strip()
    submit = open(os.path.join(GOC, "ha_tang", "spark", "submit.sh"), encoding="utf-8").read()
    co_dinh = dict(re.findall(r"--conf \"?([a-z][\w.\-]+)=([^\"$\s]+)\"? \\", submit))
    assert co_dinh, "không đọc được --conf trong submit.sh"
    for khoa, gia_tri in co_dinh.items():
        if khoa in ("spark.hadoop.dfs.replication",):
            continue
        assert mac_dinh.get(khoa) == gia_tri, khoa
    assert mac_dinh["spark.master"] == "spark://10.68.71.113:7077"
    from common import cau_hinh as ch
    assert mac_dinh["spark.databricks.delta.properties.defaults.deletedFileRetentionDuration"] == ch.GIU_FILE_DA_XOA
    assert mac_dinh["spark.databricks.delta.properties.defaults.logRetentionDuration"] == ch.GIU_NHAT_KY_DELTA
    assert os.path.realpath(os.path.join(GOC, "conf", "hdfs-site.xml")) == os.path.realpath(
        os.path.join(GOC, "ha_tang", "hadoop", "config", "hdfs-site.xml"))
