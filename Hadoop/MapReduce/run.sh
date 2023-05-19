#!/usr/bin/env bash
set -x

HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar
hdfs dfs -rm -r -skipTrash $2

yarn jar $HADOOP_STREAMING_JAR \
        -files mapper.py,reducer.py \
        -mapper 'python3 mapper.py' \
        -reducer 'python3 reducer.py' \
        -numReduceTasks 3 \
        -input $1 \
        -output $2 \

hdfs dfs -cat $2/part-00000 | head -n 50
