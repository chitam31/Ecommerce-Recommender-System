"""DAG 2 - kn_giam_sat_streaming (*/15 * * * *).

Kiểm tra job thịnh hành còn sống (qua JSON của Spark Master UI) và gold.thinh_hanh_15p có
dữ liệu trong 15 phút qua. Một trong hai thất bại -> tự khởi động lại luồng + cảnh báo.
"""
import json
import logging
import urllib.request
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from bd03_chung import NGAY_BAT_DAU, SPARK_MASTER_IP, THAM_SO_MAC_DINH, THU_MUC_DU_AN, han_chot, lenh_spark

UNG_DUNG_LUONG = {"bd03-tinh-thinh-hanh": "bat_buoc", "bd03-nap-su-kien": "nen_co", "bd03-ghi-hien-thi": "nen_co"}


def kiem_tra_job_song():
    with urllib.request.urlopen("http://{}:8080/json/".format(SPARK_MASTER_IP), timeout=15) as ph:
        trang_thai = json.loads(ph.read().decode("utf-8"))
    dang_chay = {a["name"] for a in trang_thai.get("activeapps", [])}
    logging.info("Ứng dụng đang chạy: %s; worker sống: %s", sorted(dang_chay), trang_thai.get("aliveworkers"))
    thieu_bat_buoc = [t for t, muc in UNG_DUNG_LUONG.items() if muc == "bat_buoc" and t not in dang_chay]
    for ten, muc in UNG_DUNG_LUONG.items():
        if muc == "nen_co" and ten not in dang_chay:
            logging.warning("Luồng %s không chạy", ten)
    if thieu_bat_buoc:
        raise RuntimeError("Job thịnh hành không chạy: {}".format(thieu_bat_buoc))


with DAG(
    dag_id="kn_giam_sat_streaming",
    schedule="*/15 * * * *",
    start_date=NGAY_BAT_DAU,
    catchup=False,
    max_active_runs=1,
    default_args=dict(THAM_SO_MAC_DINH, retries=0),
    dagrun_timeout=timedelta(minutes=14),
    deadline=han_chot(10),   # SLA: phải xong trước lượt giám sát kế tiếp
    tags=["bd03", "giam_sat"],
) as dag:
    job_song = PythonOperator(task_id="kiem_tra_job_song", python_callable=kiem_tra_job_song)
    co_du_lieu = BashOperator(
        task_id="kiem_tra_du_lieu_15_phut",
        bash_command=lenh_spark("nhanh", "kiem_tra/kiem_tra_thinh_hanh.py", "--nguong-phut 15"),
    )
    khoi_dong_lai = BashOperator(
        task_id="khoi_dong_lai_luong",
        bash_command="bash {}/scripts/luong.sh khoi_dong thinh_hanh".format(THU_MUC_DU_AN),
        trigger_rule="one_failed",
    )
    bao_dong = BashOperator(
        task_id="canh_bao",
        # mkdir trước: lần cảnh báo đầu tiên thư mục logs có thể chưa tồn tại.
        bash_command="mkdir -p {0}/logs && echo '[CANH BAO] Luồng thịnh hành gặp sự cố lúc {{{{ ts }}}}, "
        "đã thử khởi động lại' | tee -a {0}/logs/canh_bao.log".format(THU_MUC_DU_AN),
        trigger_rule="one_failed",
    )
    [job_song, co_du_lieu] >> khoi_dong_lai
    [job_song, co_du_lieu] >> bao_dong
