#!/bin/bash
KAFKA_HOME=$1

$KAFKA_HOME/bin/kafka-topics.sh --delete --topic personEvents --zookeeper localhost:2181
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic itemEvents --zookeeper localhost:2181
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic auctionEvents --zookeeper localhost:2181
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic bidEvents --zookeeper localhost:2181
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic nexmark_results --zookeeper localhost:2181







