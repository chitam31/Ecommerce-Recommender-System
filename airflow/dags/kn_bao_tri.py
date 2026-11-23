"""DAG 4 - kn_bao_tri (0 4 * * *): OPTIMIZE + ZORDER + VACUUM + xóa gợi ý cũ hơn 7 ngày."""
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from bd03_chung import NGAY_BAT_DAU, THAM_SO_MAC_DINH, han_chot, lenh_spark

with DAG(
    dag_id="kn_bao_tri",
    schedule="0 4 * * *",
    start_date=NGAY_BAT_DAU,
    catchup=False,
    max_active_runs=1,
    default_args=THAM_SO_MAC_DINH,
    dagrun_timeout=timedelta(hours=2),
    deadline=han_chot(90),   # SLA: bảo trì 04:00 phải xong trước 05:30
    tags=["bd03", "bao_tri"],
) as dag:
    BashOperator(
        task_id="toi_uu_delta",
        bash_command=lenh_spark("bao_tri", "bao_tri/toi_uu_delta.py", "--so-ngay-giu-goi-y 7"),
    )
