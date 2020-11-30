from __future__ import print_function

import sys
from operator import add
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.functions import concat, col, lit



reload(sys) 
sys.setdefaultencoding('utf8')
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("lab2Q3").getOrCreate()

    data = spark.read.option("header",True).csv(sys.argv[1])
    T = data.select("Violation Time").rdd.map(lambda row : row[0]).collect()


    counts = spark.sparkContext.parallelize(T, 3).map(lambda x: (x, 1))\
    .reduceByKey(lambda x,y: x+y).map(lambda x: (x[1], x[0])).sortByKey(False)

    counts.saveAsTextFile("hdfs://10.128.0.8:9000/Lab2_Q3/output")
    output = counts.collect()

 
    for (count, word) in output:
        print("%i: %s" % (count, word))

    spark.stop()

