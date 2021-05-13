#!/bin/bash

sleep 7

# Create topic
/opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --topic tp1
