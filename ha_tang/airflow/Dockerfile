# Airflow 3.3.1 cho đề tài 3. Thêm docker CLI để BashOperator gọi được
# ha_tang/spark/submit.sh (docker exec vào container spark-master cùng máy).
FROM docker:27-cli AS docker_cli

FROM apache/airflow:3.3.1-python3.11
COPY --from=docker_cli /usr/local/bin/docker /usr/local/bin/docker
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
USER airflow
