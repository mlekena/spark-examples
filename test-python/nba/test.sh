#!/bin/bash
source ../../env.sh
PROJECTNAME="nba_analysis"
INPUTPATH="/${PROJECTNAME}/input/"
OUTPUTPATH="/${PROJECTNAME}/output/"
TESTDATA="data_fraction.csv"
SRC="./player_zones.py"
/usr/local/hadoop/bin/hdfs dfs -rm -r $INPUTPATH
/usr/local/hadoop/bin/hdfs dfs -rm -r $OUTPUTPATH
/usr/local/hadoop/bin/hdfs dfs -mkdir -p $INPUTPATH
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal $TESTDATA $INPUTPATH
/usr/local/hadoop/bin/hdfs dfs -ls $INPUTPATH
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 $SRC --data_file hdfs://$SPARK_MASTER:9000$INPUTPATH
