#!/bin/bash
echo "Running Parking Violations Log Analysis - KMeans on filtered data"
PROJECT_NAME="Lab2_Q2"
INPUT_PATH="/input/"
OUTPUT_PATH="/output/"
DATA_PATH="FiltData.csv"
P1="kmeans_2.py"

source ../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r $INPUT_PATH
/usr/local/hadoop/bin/hdfs dfs -mkdir -p $INPUT_PATH
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal $DATA_PATH $INPUT_PATH
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 $P1 hdfs://$SPARK_MASTER:9000$INPUT_PATH
