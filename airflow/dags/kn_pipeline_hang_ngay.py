"""DAG 1 - kn_pipeline_hang_ngay (0 2 * * *), đúng sơ đồ mục 3.9:

cho_du_lieu_ngay_moi (Sensor)
        -> bronze_to_silver -> thong_ke_san_pham
        -> chuan_bi_ma_tran -> huan_luyen_als -> danh_gia
                [BranchPythonOperator: NDCG có cải thiện?]
                 |- có    -> sinh_goi_y_toan_bo -> tron_goi_y -> ghi_gold
                 |- không -> giu_mo_hinh_cu -> canh_bao

Thêm so với sơ đồ: tao_ma_chay (mã định danh lần chạy dùng chung cho mọi bước) và
san_pham_lien_quan (bảng cho khối "Sản phẩm liên quan" của web).

SLA: Deadline Alert 7 giờ (lượt 02:00 phải xong trước 09:00, đầu giờ làm việc), xem bd03_chung.han_chot.
Backfill: Trigger DAG w/ config {"tu_ngay": "2015-09-10", "den_ngay": "2015-09-12"} (hoặc
airflow backfill create ... --dag-run-conf '<cùng JSON>'): bronze_to_silver tính lại đúng các ngày đó
bằng replaceWhere, không chờ sensor, không đổi mốc tăng dần; các bước sau chạy như thường lệ.
"""
import logging
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG, Param

from bd03_chung import NGAY_BAT_DAU, THAM_SO_MAC_DINH, co_du_lieu_moi, doc_json_xcom, han_chot, lenh_spark

MA_CHAY = "{{ ti.xcom_pull(task_ids='tao_ma_chay') }}"
# Backfill: chỉ thêm tham số khi params.tu_ngay có giá trị; để trống là chạy tăng dần theo mốc.
THAM_SO_BACKFILL = ("{% if params.tu_ngay %}--tu-ngay {{ params.tu_ngay }} "
                    "--den-ngay {{ params.den_ngay or params.tu_ngay }}{% endif %}")


def tao_ma_chay():
    return datetime.utcnow().strftime("r%Y%m%dT%H%M%S")


def chon_nhanh(ti):
    kq = doc_json_xcom(ti, "danh_gia")
    logging.info("Kết quả đánh giá: %s", kq)
    return "sinh_goi_y_toan_bo" if kq.get("cai_thien") else "giu_mo_hinh_cu"


def giu_mo_hinh_cu(ti):
    kq = doc_json_xcom(ti, "danh_gia")
    logging.warning(
        "Giữ mô hình cũ: NDCG mới %.5f, mô hình đang phục vụ %.5f. gold.goi_y_nguoi_dung không đổi.",
        kq.get("ndcg_at_10") or 0.0,
        kq.get("ndcg_mo_hinh_cu") or 0.0,
    )


def canh_bao_mo_hinh(ti):
    kq = doc_json_xcom(ti, "danh_gia")
    thong_diep = "[CANH BAO] Mô hình {} không cải thiện NDCG; đạt ngưỡng nghiệm thu: {}".format(
        kq.get("ma_chay"), kq.get("dat_nguong")
    )
    logging.error(thong_diep)
    return thong_diep


with DAG(
    dag_id="kn_pipeline_hang_ngay",
    schedule="0 2 * * *",
    start_date=NGAY_BAT_DAU,
    catchup=False,
    max_active_runs=1,
    default_args=THAM_SO_MAC_DINH,
    dagrun_timeout=timedelta(hours=8),
    deadline=han_chot(7 * 60),
    params={
        "luoi": Param("toi_thieu", enum=["toi_thieu", "day_du"], description="9 hoặc 27 tổ hợp tuning"),
        "min_tuong_tac": Param(3, type="integer", minimum=1),
        "bo_qua_cho_du_lieu": Param(False, type="boolean",
                                    description="Chạy tay: bỏ qua sensor, chạy trên dữ liệu hiện có"),
        "tu_ngay": Param("", type="string", description="Backfill: ngày dữ liệu đầu (YYYY-MM-DD), để trống = tăng dần"),
        "den_ngay": Param("", type="string", description="Backfill: ngày dữ liệu cuối (mặc định bằng tu_ngay)"),
    },
    tags=["bd03", "cs411"],
) as dag:
    cho_du_lieu = PythonSensor(
        task_id="cho_du_lieu_ngay_moi",
        python_callable=co_du_lieu_moi,
        mode="reschedule",
        poke_interval=300,
        timeout=6 * 3600,
        soft_fail=True,
    )
    ma_chay = PythonOperator(task_id="tao_ma_chay", python_callable=tao_ma_chay)
    bronze_to_silver = BashOperator(task_id="bronze_to_silver", bash_command=lenh_spark("lo", "transform/bronze_to_silver.py", THAM_SO_BACKFILL))
    thong_ke = BashOperator(task_id="thong_ke_san_pham", bash_command=lenh_spark("lo", "transform/thong_ke_san_pham.py"))
    lien_quan = BashOperator(task_id="san_pham_lien_quan", bash_command=lenh_spark("lo", "ml/san_pham_lien_quan.py"))
    chuan_bi = BashOperator(
        task_id="chuan_bi_ma_tran",
        bash_command=lenh_spark("lo", "ml/chuan_bi_ma_tran.py", "--min-tuong-tac {{ params.min_tuong_tac }}"),
    )
    huan_luyen = BashOperator(
        task_id="huan_luyen_als",
        bash_command=lenh_spark("als", "ml/huan_luyen_als.py", "--ma-chay " + MA_CHAY + " --luoi {{ params.luoi }}"),
        execution_timeout=timedelta(hours=4),
    )
    danh_gia = BashOperator(task_id="danh_gia", bash_command=lenh_spark("als", "ml/danh_gia.py", "--ma-chay " + MA_CHAY))
    re_nhanh = BranchPythonOperator(task_id="ndcg_co_cai_thien", python_callable=chon_nhanh)
    sinh_goi_y = BashOperator(
        task_id="sinh_goi_y_toan_bo", bash_command=lenh_spark("als", "ml/sinh_goi_y_toan_bo.py", "--ma-chay " + MA_CHAY)
    )
    tron = BashOperator(task_id="tron_goi_y", bash_command=lenh_spark("lo", "ml/tron_goi_y.py", "--ma-chay " + MA_CHAY))
    ghi_gold = BashOperator(task_id="ghi_gold", bash_command=lenh_spark("lo", "ml/ghi_gold.py", "--ma-chay " + MA_CHAY))
    giu_cu = PythonOperator(task_id="giu_mo_hinh_cu", python_callable=giu_mo_hinh_cu)
    canh_bao = PythonOperator(task_id="canh_bao", python_callable=canh_bao_mo_hinh)

    cho_du_lieu >> ma_chay >> bronze_to_silver
    bronze_to_silver >> thong_ke >> lien_quan
    bronze_to_silver >> chuan_bi >> huan_luyen >> danh_gia >> re_nhanh
    re_nhanh >> sinh_goi_y >> tron >> ghi_gold
    thong_ke >> tron
    re_nhanh >> giu_cu >> canh_bao
