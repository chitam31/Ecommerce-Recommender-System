# Web giả lập sàn TMĐT. Kế thừa ảnh Spark của dự án vì web đọc Delta bằng một phiên
# Spark cục bộ (local[2]) nằm ngay trong tiến trình FastAPI.
# Build từ thư mục gốc dự án:  docker build -f ha_tang/web/Dockerfile -t bd03/web .
# (cần build ảnh bd03/spark:3.5.3 trước: cd ha_tang/spark && docker compose -f docker-compose-master.yml build)
FROM bd03/spark:3.5.3

USER root
RUN pip3 install --no-cache-dir fastapi==0.115.14 uvicorn==0.33.0 jinja2==3.1.6 kafka-python==2.0.2
COPY jobs/common /opt/bd03/jobs/common
COPY web /opt/bd03/web
RUN chown -R spark:spark /opt/bd03
USER spark

ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/bd03/jobs:/opt/bd03/web \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3
WORKDIR /opt/bd03/web
EXPOSE 8000
CMD ["python3", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
