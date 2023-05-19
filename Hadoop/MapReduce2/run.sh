#!/usr/bin/env bash
set -x

HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar
hdfs dfs -rm -r -skipTrash $2

( yarn jar $HADOOP_STREAMING_JAR \
    -D mapreduce.job.name="HW_03_1" \
    -files mapper.py,reducer.py \
    -mapper 'python3 mapper.py' \
    -combiner 'python3 reducer.py' \
    -reducer 'python3 reducer.py' \
    -numReduceTasks 2 \
    -input $1 \
    -output $2_tmp &&

# Global sorting as we use only 1 reducer
yarn jar $HADOOP_STREAMING_JAR \
    -D stream.num.map.output.key.fields=3 \
    -D mapreduce.job.name="HW_03_2" \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options="-k1,1n -k3,3nr" \
    -files reducer2.py \
    -mapper cat \
    -reducer 'python3 reducer2.py' \
    -numReduceTasks 1 \
    -input $2_tmp \
    -output $2
) || echo "Error happens"

hdfs dfs -rm -r -skipTrash $2_tmp

hdfs dfs -cat $2/* | head -20
