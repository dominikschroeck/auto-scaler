#!/bin/bash
KAFKA_HOME=$1
PARTITIONS=$2

$KAFKA_HOME/bin/kafka-topics.sh --create --topic personEvents --zookeeper localhost:2181 --partitions $PARTITIONS --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --create --topic itemEvents --zookeeper localhost:2181 --partitions $PARTITIONS --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --create --topic auctionEvents --zookeeper localhost:2181 --partitions $PARTITIONS --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --create --topic bidEvents --zookeeper localhost:2181 --partitions $PARTITIONS --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --create --topic nexmark_results --zookeeper localhost:2181 --partitions $PARTITIONS --replication-factor 1






