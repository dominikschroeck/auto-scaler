#!/bin/bash
FLINK_HOME=$1
JOB_ID=$2
SAVEPOINT_PATH=$3
JAR=$4
YAML=$5

echo *************************
echo ** RESCALER FOR FLINK  **
echo *************************

echo "This script automatically generates a savepoint and restarts the job with the same savepoint"
echo "Taskmanagers have to be added before starting this script"

echo "Triggering Savepoint and cancelling the job"
$FLINK_HOME/bin/flink cancel -s $SAVEPOINT_PATH $JOB_ID

echo "Restarting with Savepoint"
$FLINK_HOME/bin/flink run -s $SAVEPOINT_PATH -n $JAR $YAML
