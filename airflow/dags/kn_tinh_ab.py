"""DAG 3 - kn_tinh_ab (0 * * * *).

Đọc CDF của gold.nhat_ky_hien_thi -> tổng hợp các ngày có thay đổi -> z-test -> MERGE vào
gold.ket_qua_ab kèm phien_ban_delta_du_lieu.

SLA: Deadline Alert 30 phút. Backfill bằng lệnh chuẩn của Airflow 3, ví dụ tính lại 3 ngày:
  airflow backfill create --dag-id kn_tinh_ab --from-date 2026-11-10 --to-date 2026-11-12 \
      --reprocess-behavior completed
Mỗi ngày chỉ lượt 00:00 UTC chạy job (--tu-ngay = ngày đó), 23 lượt còn lại bị cong_backfill bỏ qua
(skipped). MERGE theo (ngày, nhóm) nên chạy lại không nhân đôi dòng.
"""
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG

from bd03_chung import NGAY_BAT_DAU, THAM_SO_MAC_DINH, han_chot, lenh_spark, ngay_backfill

with DAG(
    dag_id="kn_tinh_ab",
    schedule="0 * * * *",
    start_date=NGAY_BAT_DAU,
    catchup=False,
    max_active_runs=1,
    default_args=THAM_SO_MAC_DINH,
    dagrun_timeout=timedelta(minutes=50),
    deadline=han_chot(30),
    tags=["bd03", "ab"],
) as dag:
    cong = ShortCircuitOperator(task_id="cong_backfill", python_callable=ngay_backfill)
    tinh = BashOperator(
        task_id="tinh_ket_qua_ab",
        # Lượt thường: XCom là True -> không thêm tham số. Lượt backfill: XCom là ngày cần tính lại.
        bash_command=lenh_spark(
            "lo", "transform/tinh_ket_qua_ab.py",
            "{% set n = ti.xcom_pull(task_ids='cong_backfill') %}"
            "{% if n is string %}--tu-ngay {{ n }} --den-ngay {{ n }}{% endif %}",
        ),
    )
    cong >> tinh
