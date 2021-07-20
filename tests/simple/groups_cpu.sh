#!/usr/bin/env bash

$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --num-executors=8 --executor-cores=2 --executor-memory=4G \
    --deploy-mode cluster \
    groups.py
