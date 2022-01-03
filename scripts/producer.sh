#!/bin/bash

echo "Producer Number: $1"

timestamp=$(date +%s)

echo "Producer Started At: $timestamp"

sh /adpushup/kafka/bin/kafka-producer-perf-test.sh \
--topic 100B-1M-3R-lz4 \
--num-records 1000000 \
--throughput -1 \
--producer-props \
bootstrap.servers=kafka1:9092,kafka2:9092,kafka3:9092 \
batch.size=10 \
acks=1 \
linger.ms=1000  \
buffer.memory=4294967296 \
request.timeout.ms=300000 \
--record-size 1000000000

timestamp=$(date +%s)

echo "Producer Finished At: $timestamp"