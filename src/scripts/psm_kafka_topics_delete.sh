#!/bin/bash
KAFKA_HOME=$1

$KAFKA_HOME/bin/kafka-topics.sh --delete --topic PSM_CLICKS --zookeeper localhost:2181
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic PSM_IMPRESSIONS --zookeeper localhost:2181 
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic PSM_SEARCH --zookeeper localhost:2181 
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic psm_results --zookeeper localhost:2181 