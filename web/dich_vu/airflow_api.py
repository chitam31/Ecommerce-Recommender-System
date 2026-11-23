"""Gọi REST API của Airflow 3 để kích hoạt DAG (dùng cho nút tái lập mô hình trên web).

Airflow 3 dùng mã JWT: lấy bằng POST /auth/token rồi gửi kèm "Authorization: Bearer".
Cấu hình qua biến môi trường AIRFLOW_URL, AIRFLOW_USER, AIRFLOW_PASS.
"""
import json
import os
import urllib.request


def _goi(phuong_thuc, url, du_lieu=None, token=None):
    yeu_cau = urllib.request.Request(
        url,
        data=json.dumps(du_lieu).encode("utf-8") if du_lieu is not None else None,
        method=phuong_thuc,
        headers={"Content-Type": "application/json"},
    )
    if token:
        yeu_cau.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(yeu_cau, timeout=20) as phan_hoi:
        return json.loads(phan_hoi.read().decode("utf-8") or "{}")


def kich_hoat_dag(dag_id, conf):
    goc = os.environ.get("AIRFLOW_URL")
    if not goc:
        raise RuntimeError("Chưa đặt AIRFLOW_URL, ví dụ http://10.68.71.113:8088")
    token = _goi(
        "POST",
        goc.rstrip("/") + "/auth/token",
        {"username": os.environ.get("AIRFLOW_USER", "admin"), "password": os.environ.get("AIRFLOW_PASS", "")},
    )["access_token"]
    ket_qua = _goi(
        "POST",
        "{}/api/v2/dags/{}/dagRuns".format(goc.rstrip("/"), dag_id),
        {"logical_date": None, "conf": conf},
        token,
    )
    return {"dag_id": dag_id, "dag_run_id": ket_qua.get("dag_run_id"), "trang_thai": ket_qua.get("state")}
