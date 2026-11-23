"""DAG 5 - kn_tai_lap (thủ công, BẮT BUỘC DEMO).

Nhận phien_ban_delta (phiên bản của features.ma_tran_tuong_tac) qua dag_run.conf, chạy lại
toàn bộ pipeline huấn luyện + đánh giá trên VERSION AS OF đó, so metric với bản ghi lịch sử.

Kích hoạt:
  Giao diện Airflow: Trigger DAG w/ config -> {"phien_ban_delta": 12, "che_do": "mo_hinh_chon"}
  Dòng lệnh:         airflow dags trigger kn_tai_lap -c '{"phien_ban_delta": 12}'
  Web:               màn hình "Tái lập thí nghiệm" -> nút "Kích hoạt kn_tai_lap"
"""
import logging
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

from bd03_chung import NGAY_BAT_DAU, THAM_SO_MAC_DINH, doc_json_xcom, han_chot, lenh_spark


def bao_cao(ti):
    kq = doc_json_xcom(ti, "chay_lai_huan_luyen")
    logging.info("=== KẾT QUẢ TÁI LẬP ===")
    for k, v in kq.items():
        logging.info("%s: %s", k, v)
    if not kq.get("trung_khop"):
        raise RuntimeError(
            "Metric chạy lại lệch {} so với lịch sử (dung sai {})".format(kq.get("lech_lon_nhat"), kq.get("dung_sai"))
        )
    return kq


with DAG(
    dag_id="kn_tai_lap",
    schedule=None,
    start_date=NGAY_BAT_DAU,
    catchup=False,
    default_args=dict(THAM_SO_MAC_DINH, retries=0),
    dagrun_timeout=timedelta(hours=4),
    deadline=han_chot(180),   # SLA: tái lập phải xong trong 3 giờ
    params={
        "phien_ban_delta": Param(0, type="integer", minimum=0, description="Phiên bản Delta của features.ma_tran_tuong_tac"),
        "che_do": Param("mo_hinh_chon", enum=["mo_hinh_chon", "toan_bo_luoi"]),
    },
    tags=["bd03", "tai_lap"],
) as dag:
    chay_lai = BashOperator(
        task_id="chay_lai_huan_luyen",
        bash_command=lenh_spark(
            "tai_lap",
            "kiem_tra/tai_lap_huan_luyen.py",
            "--phien-ban {{ params.phien_ban_delta }} --che-do {{ params.che_do }}",
        ),
    )
    chay_lai >> PythonOperator(task_id="so_sanh_voi_lich_su", python_callable=bao_cao)
