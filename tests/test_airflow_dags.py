"""5 DAG: Deadline Alerts thay cho sla (Airflow 3), retries, backfill (mục A.6.5 và A.8 đề cương).

Cần cài apache-airflow==3.3.1 (requirements-airflow.txt); không có thì bỏ qua cả tệp.
"""
import os
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

pytest.importorskip("airflow.sdk")  # thư mục airflow/ của dự án không phải gói Airflow

GOC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CAC_DAG = {"kn_pipeline_hang_ngay": 420, "kn_giam_sat_streaming": 10, "kn_tinh_ab": 30, "kn_bao_tri": 90,
           "kn_tai_lap": 180}


@pytest.fixture(scope="module")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag

    db = DagBag(dag_folder=os.path.join(GOC, "airflow", "dags"))
    assert db.import_errors == {}
    return db


def test_moi_dag_co_deadline_alert_thay_sla(dagbag):
    from airflow.sdk import DeadlineAlert, SyncCallback

    assert set(dagbag.dags) == set(CAC_DAG)
    for dag_id, phut in CAC_DAG.items():
        dl = dagbag.dags[dag_id].deadline
        dl = dl[0] if isinstance(dl, list) else dl
        assert isinstance(dl, DeadlineAlert), dag_id
        assert dl.interval.total_seconds() == phut * 60, dag_id
        assert isinstance(dl.callback, SyncCallback) and dl.callback.path == "bd03_chung.canh_bao_tre_han"
        assert dl.callback.kwargs == {"han_phut": phut}


def test_dag_co_deadline_van_tuan_tu_hoa_duoc(dagbag):
    """Scheduler lưu DAG ở dạng tuần tự hóa: deadline phải đi qua được bước này."""
    from airflow.serialization.serialized_objects import DagSerialization

    for dag_id, phut in CAC_DAG.items():
        du_lieu = DagSerialization.to_dict(dagbag.dags[dag_id])
        assert "deadline" in str(du_lieu["dag"]), dag_id
        dag = DagSerialization.from_dict(du_lieu)
        assert dag.dag_id == dag_id


def test_retries_va_xu_ly_loi(dagbag):
    for dag_id in ("kn_pipeline_hang_ngay", "kn_tinh_ab", "kn_bao_tri"):
        for t in dagbag.dags[dag_id].tasks:
            assert t.retries >= 1 and t.on_failure_callback, (dag_id, t.task_id)
    # DAG giám sát không retry (lượt sau 15 phút nữa), xử lý lỗi bằng trigger_rule one_failed
    dag2 = dagbag.dags["kn_giam_sat_streaming"]
    assert dag2.get_task("khoi_dong_lai_luong").trigger_rule == "one_failed"


def test_canh_bao_tre_han_ghi_log(tmp_path, monkeypatch):
    import bd03_chung

    monkeypatch.setattr(bd03_chung, "THU_MUC_DU_AN", str(tmp_path))
    bd03_chung.canh_bao_tre_han(context={"dag_run": {"dag_id": "kn_tinh_ab", "dag_run_id": "r1", "state": "running"},
                                         "deadline": {"deadline_time": "2026-11-15T10:30:00+00:00"}}, han_phut=30)
    noi_dung = (tmp_path / "logs" / "canh_bao.log").read_text(encoding="utf-8")
    assert "TRE HAN" in noi_dung and "dag=kn_tinh_ab" in noi_dung and "han=30" in noi_dung
    bd03_chung.canh_bao_tre_han()  # context trống vẫn không lỗi


def test_cong_backfill_dag3():
    import pendulum

    from bd03_chung import ngay_backfill

    luot = lambda loai: SimpleNamespace(run_type=SimpleNamespace(value=loai))
    gio = lambda h: pendulum.datetime(2026, 11, 12, h, 0, tz="UTC")
    assert ngay_backfill(dag_run=luot("scheduled"), logical_date=gio(0)) is True
    assert ngay_backfill(dag_run=luot("manual"), logical_date=None) is True
    assert ngay_backfill(dag_run=luot("backfill"), logical_date=gio(0)) == "2026-11-12"
    assert ngay_backfill(dag_run=luot("backfill"), logical_date=gio(5)) is False
    # 07:00 giờ Việt Nam là 00:00 UTC cùng ngày
    assert ngay_backfill(dag_run=luot("backfill"),
                         logical_date=pendulum.datetime(2026, 11, 12, 7, tz="Asia/Ho_Chi_Minh")) == "2026-11-12"
    assert ngay_backfill(dag_run=luot("backfill"), logical_date=datetime(2026, 11, 13, 0, tzinfo=timezone.utc)) \
        == "2026-11-13"


def _lenh(dagbag, dag_id, task_id, ngu_canh):
    task = dagbag.dags[dag_id].get_task(task_id)
    return task.render_template(task.bash_command, ngu_canh, jinja_env=dagbag.dags[dag_id].get_template_env())


def test_tham_so_backfill_dag1(dagbag):
    thuong = _lenh(dagbag, "kn_pipeline_hang_ngay", "bronze_to_silver", {"params": {"tu_ngay": "", "den_ngay": ""}})
    assert thuong.endswith("transform/bronze_to_silver.py ") or thuong.endswith("transform/bronze_to_silver.py")
    bf = _lenh(dagbag, "kn_pipeline_hang_ngay", "bronze_to_silver", {"params": {"tu_ngay": "2015-09-10", "den_ngay": ""}})
    assert bf.endswith("--tu-ngay 2015-09-10 --den-ngay 2015-09-10")
    from bd03_chung import co_du_lieu_moi
    assert co_du_lieu_moi(params={"tu_ngay": "2015-09-10"}) is True   # backfill không chờ sensor


def test_tham_so_backfill_dag3(dagbag):
    class TI:
        def __init__(self, gia_tri):
            self.gia_tri = gia_tri

        def xcom_pull(self, task_ids=None, **_):
            return self.gia_tri

    assert "--tu-ngay" not in _lenh(dagbag, "kn_tinh_ab", "tinh_ket_qua_ab", {"ti": TI(True)})
    assert _lenh(dagbag, "kn_tinh_ab", "tinh_ket_qua_ab", {"ti": TI("2026-11-12")}).endswith(
        "--tu-ngay 2026-11-12 --den-ngay 2026-11-12")
    assert dagbag.dags["kn_tinh_ab"].get_task("cong_backfill").downstream_task_ids == {"tinh_ket_qua_ab"}
