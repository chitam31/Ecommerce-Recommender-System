"""Cấu hình chung cho pytest: đường dẫn import và một SparkSession cục bộ dùng chung.

Các test Spark chạy ở chế độ local, KHÔNG cần cụm và KHÔNG cần jar Delta: chúng kiểm tra
phần logic DataFrame của từng job. Phần đọc/ghi Delta được kiểm chứng bằng
scripts/kiem_tra_cuc_bo.sh (cần jar Delta) và trên cụm thật.
"""
import os
import sys

import pytest

GOC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CAC_THU_MUC = [os.path.join(GOC, t) for t in ("jobs", "web", "airflow/dags", "producer", "scripts")]
for thu_muc in CAC_THU_MUC:
    sys.path.insert(0, thu_muc)
# Tiến trình Python worker của Spark không thừa hưởng sys.path của driver; trên cụm việc này
# do --py-files common.zip đảm nhận, còn khi test cục bộ thì truyền qua PYTHONPATH.
os.environ["PYTHONPATH"] = os.pathsep.join(CAC_THU_MUC + [os.environ.get("PYTHONPATH", "")])
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    phien = (
        SparkSession.builder.master("local[2]")
        .appName("bd03-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )
    phien.sparkContext.setLogLevel("ERROR")
    assert pyspark.__version__.startswith("3.5")
    yield phien
    phien.stop()
