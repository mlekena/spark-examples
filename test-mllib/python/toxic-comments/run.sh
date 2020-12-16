#!/bin/bash
source ../../../env.sh
PROJECTNAME="toxic_comment_classifier"
FULLDATAPATH=/$PROJECTNAME/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r $FULLDATAPATH
/usr/local/hadoop/bin/hdfs dfs -mkdir -p $FULLDATAPATH
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ./sample_train.csv $FULLDATAPATH
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ./sample_test.csv $FULLDATAPATH
/usr/local/hadoop/bin/hdfs dfs -ls $FULLDATAPATH
echo "#########################################################################################"
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 $PROJECTNAME.py \
--test_data_file=hdfs://$SPARK_MASTER:9000/$PROJECTNAME/input/sample_test.csv \
--train_data_file=hdfs://$SPARK_MASTER:9000/$PROJECTNAME/input/sample_train.csv
