#!/bin/bash
KAFKA_HOME=$1
PARTITIONS=$2

$KAFKA_HOME/bin/kafka-topics.sh --create --topic PSM_CLICKS --zookeeper localhost:2181 --partitions $PARTITIONS --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --create --topic PSM_IMPRESSIONS --zookeeper localhost:2181 --partitions $PARTITIONS --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --create --topic PSM_SEARCH --zookeeper localhost:2181 --partitions $PARTITIONS --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --create --topic psm_results --zookeeper localhost:2181 --partitions $PARTITIONS --replication-factor 1




