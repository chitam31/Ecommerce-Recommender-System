"""Tiện ích dùng chung cho 5 DAG của đề tài 3 (Airflow 3.3.1, airflow.sdk)

Airflow chỉ ĐIỀU PHỐI: mọi bước nặng là một spark-submit vào cụm qua ha_tang/spark/submit.sh
(docker exec vào container spark-master). Mỗi job in một dòng JSON ở cuối stdout; BashOperator
đẩy dòng cuối vào XCom để task sau đọc (ví dụ nhánh "NDCG có cải thiện?").

SLA (mục A.6.5 đề cương): Airflow 3.0 đã gỡ tham số sla/sla_miss_callback của Airflow 2 và từ 3.1
thay bằng Deadline Alerts. han_chot() tạo một DeadlineAlert cho cả lượt chạy DAG: quá hạn mà lượt
chạy chưa xong thì scheduler gọi canh_bao_tre_han() NGAY (SLA cũ chỉ kiểm tra khi lượt chạy kết
thúc). Dùng SyncCallback (chạy trong executor) vì triggerer mặc định không bật trên master.
"""
import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta

import pendulum

THU_MUC_DU_AN = os.environ.get("BD03_HOME", "/opt/bd03")
SPARK_MASTER_IP = os.environ.get("SPARK_MASTER_IP", "10.68.71.113")
WEBHDFS = os.environ.get("WEBHDFS", "http://{}:9870/webhdfs/v1".format(SPARK_MASTER_IP))
# Gốc hồ dữ liệu trên HDFS theo mục A.4 đề cương (khớp jobs/common/cau_hinh.py LAKE_DUONG_DAN).
LAKE_DUONG_DAN = os.environ.get("LAKE_DUONG_DAN", "/lakehouse/bd03_khuyen_nghi")
MUI_GIO = pendulum.timezone("Asia/Ho_Chi_Minh")
NGAY_BAT_DAU = pendulum.datetime(2026, 11, 1, tz=MUI_GIO)

nhat_ky = logging.getLogger("bd03")


def lenh_spark(ho_so, job, tham_so=""):
    """Lệnh bash nộp một job vào cụm Spark."""
    return "bash {}/ha_tang/spark/submit.sh {} {} {}".format(THU_MUC_DU_AN, ho_so, job, tham_so).strip()


def canh_bao(context):
    """on_failure_callback: ghi cảnh báo ra log của task và tệp logs/canh_bao.log."""
    ti = context.get("ti")
    noi_dung = "[CANH BAO] {} | dag={} task={} run={}".format(
        datetime.utcnow().isoformat(timespec="seconds"),
        getattr(ti, "dag_id", "?"),
        getattr(ti, "task_id", "?"),
        getattr(ti, "run_id", "?"),
    )
    _ghi_canh_bao(noi_dung)


def _ghi_canh_bao(noi_dung):
    nhat_ky.error(noi_dung)
    try:
        os.makedirs(os.path.join(THU_MUC_DU_AN, "logs"), exist_ok=True)
        with open(os.path.join(THU_MUC_DU_AN, "logs", "canh_bao.log"), "a", encoding="utf-8") as f:
            f.write(noi_dung + "\n")
    except OSError:
        pass


def canh_bao_tre_han(context=None, han_phut=None, **_):
    """Callback của Deadline Alert (thay sla_miss_callback): lượt chạy DAG chưa xong khi tới hạn.

    Airflow truyền context gồm dag_run (JSON) và deadline (mã, thời điểm hạn chót).
    """
    dag_run = (context or {}).get("dag_run") or {}
    han = (context or {}).get("deadline") or {}
    _ghi_canh_bao("[CANH BAO TRE HAN] {} | dag={} run={} han={} phut, han_chot={} trang_thai={}".format(
        datetime.utcnow().isoformat(timespec="seconds"), dag_run.get("dag_id", "?"), dag_run.get("dag_run_id", "?"),
        han_phut, han.get("deadline_time", "?"), dag_run.get("state", "?")))


def han_chot(phut):
    """Deadline Alert: lượt chạy chưa xong sau `phut` phút kể từ lúc vào hàng đợi -> canh_bao_tre_han.

    Mốc DAGRUN_QUEUED_AT có ở mọi kiểu lượt chạy (theo lịch, chạy tay, backfill); mốc logical_date
    thì không (lượt chạy tay ở Airflow 3 có thể không có logical_date).
    Nhập airflow.sdk ngay trong hàm để các tiện ích khác của tệp này dùng được cả khi không cài Airflow
    (kiểm thử sensor, script).
    """
    from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback

    return DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=phut),
        callback=SyncCallback(canh_bao_tre_han, kwargs={"han_phut": phut}),
    )


THAM_SO_MAC_DINH = {
    "owner": "nhom-bd03",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": canh_bao,
}


def doc_json_xcom(ti, task_id):
    """Đọc dòng JSON cuối mà job Spark in ra (đã được BashOperator đẩy vào XCom)."""
    gia_tri = ti.xcom_pull(task_ids=task_id)
    if not gia_tri:
        return {}
    if isinstance(gia_tri, dict):
        return gia_tri
    return json.loads(str(gia_tri).strip().splitlines()[-1])


def _webhdfs_mtime(duong_dan):
    """Thời điểm sửa đổi (ms) của một thư mục HDFS qua WebHDFS; None nếu không tồn tại."""
    url = "{}{}?op=GETFILESTATUS".format(WEBHDFS, duong_dan)
    try:
        with urllib.request.urlopen(url, timeout=15) as ph:
            return json.loads(ph.read().decode("utf-8"))["FileStatus"]["modificationTime"]
    except Exception:
        return None


def co_du_lieu_moi(params=None, **_):
    """Sensor của DAG 1: bronze có lô mới hơn lần ghi silver gần nhất chưa?

    Lần chạy tay có thể đặt params.bo_qua_cho_du_lieu = true để chạy ngay trên dữ liệu hiện có
    (vd. sau khi đổi lưới tuning hay mã huấn luyện); lần chạy theo lịch luôn chờ dữ liệu mới.
    Lượt backfill (params.tu_ngay) tính lại dữ liệu đã có nên cũng không chờ.

    File sink ghi một tệp vào bronze/su_kien/_spark_metadata sau mỗi lô thành công;
    Delta ghi một tệp vào silver/tuong_tac/_delta_log sau mỗi commit. So hai mốc là đủ,
    không cần khởi động Spark chỉ để kiểm tra.
    """
    if params and (params.get("bo_qua_cho_du_lieu") or params.get("tu_ngay")):
        nhat_ky.info("bo_qua_cho_du_lieu hoặc backfill: chạy trên dữ liệu hiện có")
        return True
    bronze = _webhdfs_mtime(LAKE_DUONG_DAN + "/bronze/su_kien/_spark_metadata")
    silver = _webhdfs_mtime(LAKE_DUONG_DAN + "/silver/tuong_tac/_delta_log")
    nhat_ky.info("mtime bronze=%s silver=%s", bronze, silver)
    if bronze is None:
        return False
    return silver is None or bronze > silver


def ngay_backfill(dag_run=None, logical_date=None, **_):
    """ShortCircuit của DAG 3 khi chạy bằng "airflow backfill create".

    Lượt thường (theo lịch, chạy tay): trả True, tính tăng dần theo CDF như cũ.
    Lượt backfill: DAG chạy mỗi giờ nên một ngày có 24 lượt; chỉ lượt ở 00:00 UTC tính lại cả ngày
    đó (trả chuỗi YYYY-MM-DD cho --tu-ngay), 23 lượt còn lại trả False để bỏ qua job Spark. Ngày
    tính theo UTC vì Spark của dự án đặt spark.sql.session.timeZone=UTC.
    """
    loai = getattr(dag_run, "run_type", None)
    loai = getattr(loai, "value", loai)
    if loai != "backfill" or logical_date is None:
        return True
    gio_utc = logical_date.astimezone(pendulum.timezone("UTC"))
    if gio_utc.hour != 0:
        nhat_ky.info("Backfill: bỏ qua lượt %s (chỉ tính lại ở 00:00 UTC)", gio_utc.isoformat())
        return False
    return gio_utc.strftime("%Y-%m-%d")
