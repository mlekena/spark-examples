#!/bin/bash
source ../../../env.sh
PROJECTNAME="toxic_commment_classifier"
/usr/local/hadoop/bin/hdfs dfs -rm -r /$PROJECTNAME/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /$PROJECTNAME/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ./sample_train.csv /$PROJECTNAME/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ./sample_test.csv /$PROJECTNAME/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./$PROJECTNAME.py \
--test_data_file=.hdfs://$SPARK_MASTER:9000/input/sample_test.csv \
--train_data_file=hdfs://$SPARK_MASTER:9000/input/sample_train.csv
