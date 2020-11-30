#!/bin/bash
echo "Running Parking Violations Log Analysis"
PROJECT_NAME="Lab2_Q3"
IN_HADOOP_INPUT_PATH="/${PROJECT_NAME}/input/"
OUT_HADOOP_OUTPUT_PATH="/${PROJECT_NAME}/output/"
DATA_FILE_PATH="../../test-data/tix_data.csv"
P1="./when.py"
P2="./YearType.py"
P3="./where.py"
P4="./Color.py"

source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r $IN_HADOOP_INPUT_PATH
/usr/local/hadoop/bin/hdfs dfs -rm -r $OUT_HADOOP_OUTPUT_PATH
/usr/local/hadoop/bin/hdfs dfs -mkdir -p $IN_HADOOP_INPUT_PATH
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal $DATA_FILE_PATH $IN_HADOOP_INPUT_PATH

#when 
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 $P1 hdfs://$SPARK_MASTER:9000$IN_HADOOP_INPUT_PATH
/usr/local/hadoop/bin/hdfs dfs -rm -r $OUT_HADOOP_OUTPUT_PATH

#YearType
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 $P2 hdfs://$SPARK_MASTER:9000$IN_HADOOP_INPUT_PATH
/usr/local/hadoop/bin/hdfs dfs -rm -r $OUT_HADOOP_OUTPUT_PATH

#where
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 $P3 hdfs://$SPARK_MASTER:9000$IN_HADOOP_INPUT_PATH
/usr/local/hadoop/bin/hdfs dfs -rm -r $OUT_HADOOP_OUTPUT_PATH

#Color
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 $P4 hdfs://$SPARK_MASTER:9000$IN_HADOOP_INPUT_PATH
/usr/local/hadoop/bin/hdfs dfs -rm -r $OUT_HADOOP_OUTPUT_PATH