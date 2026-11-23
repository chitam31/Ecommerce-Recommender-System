#!/usr/bin/env bash
# tao_topic.sh - chạy trên máy Kafka (labsit, 10.68.71.204). Tạo 2 topic nếu chưa có.
# retailrocket-events đã có từ trước thì giữ nguyên; hien-thi là topic mới của web.
set -euo pipefail
KT="docker exec kafka-server /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"
$KT --create --if-not-exists --topic retailrocket-events --partitions 3 --replication-factor 1
$KT --create --if-not-exists --topic hien-thi --partitions 3 --replication-factor 1
$KT --list
$KT --describe --topic hien-thi
