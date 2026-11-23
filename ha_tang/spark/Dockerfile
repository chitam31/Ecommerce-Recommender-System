# Ảnh Spark của dự án, kế thừa đúng phiên bản nhóm đang chạy (apache/spark:3.5.3).
#
# Vì sao cần ảnh riêng:
#   1. pyspark.ml import numpy ngay khi nạp module, ảnh gốc không có numpy.
#   2. Đưa sẵn jar Delta và Kafka vào ảnh: không phải tải lại bằng --packages mỗi
#      lần submit, tránh tranh khóa ivy khi nhiều job chạy đồng thời lúc demo.
FROM apache/spark:3.5.3

USER root

# numpy 1.24.4 hỗ trợ Python 3.8 đến 3.11, an toàn với mọi biến thể Ubuntu của ảnh gốc.
RUN pip3 install --no-cache-dir numpy==1.24.4

ADD --chown=spark:spark https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar /opt/spark/jars/
ADD --chown=spark:spark https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar /opt/spark/jars/
ADD --chown=spark:spark https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chown=spark:spark https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chown=spark:spark https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chown=spark:spark https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

RUN chmod 644 /opt/spark/jars/*.jar

USER spark
