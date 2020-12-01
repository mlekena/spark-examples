#!/usr/bin/python3
"""
Extra Requirements:
    pandas
"""
from __future__ import print_function

import sys
import argparse
from operator import add
from pyspark.sql import Row
# $example on$
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
# $example off$
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, DoubleType, ArrayType, StructField
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import pandas_udf, udf, collect_list
import pandas as pd

reload(sys)
sys.setdefaultencoding('utf8')

parser = argparse.ArgumentParser()
parser.add_argument(
    "--data_file", help="Data file to read in must be specified using --data_file")
parser.add_argument(
    "--data_output", help="Data output path must be specified using --data_output", action="store_const", const=None)

#  SHOT_DIST, CLOSEST_DEF_DIST, SHOT_CLOCK
init_k_zones = [
    [8, 12, 20],
    [15, 9, 15],
    [22, 6, 10],
    [30, 3, 5]
]


def deb_print(outstr):
    print("{}{}{}".format("#"*20, outstr, "#"*20))


def main():
    print("*"*50)
    print("*"*50)
    print("*"*50)
    spark = SparkSession.builder.appName("MostComfortableZones").getOrCreate()
    columns_of_interest = ['player_name',
                           'SHOT_DIST', 'CLOSE_DEF_DIST', 'SHOT_CLOCK']

    # used in find_k_clusters to defined returned dataframe column schema
    columns_schema_ddl = ','.join(["{} {}".format(col, typ)
                                   for col, typ in zip(columns_of_interest,
                                                       [StringType(), DoubleType(), DoubleType(), DoubleType()])
                                   ])

    args = parser.parse_args()
    deb_print("using data file {}".format(args.data_file))

    zone_data = spark.read.csv(args.data_file, header=True).select(
        *columns_of_interest).dropna()  # .fillna(2) # https://hackingandslacking.com/cleaning-pyspark-dataframes-1a3f5fdcedd1
    zone_data.show()
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    converted_zone_data = zone_data.withColumn('SHOT_DIST', zone_data['SHOT_DIST'].cast(DoubleType())) \
        .withColumn('CLOSE_DEF_DIST', zone_data['CLOSE_DEF_DIST'].cast(DoubleType())) \
        .withColumn('SHOT_CLOCK', zone_data['SHOT_CLOCK'].cast(
            DoubleType()))  # https://www.datasciencemadesimple.com/typecast-integer-to-string-and-string-to-integer-in-pyspark/

    # Convert zone data into VectorRow data cells
    # Exclude player_name as strings are supported
    assembler = VectorAssembler(
        inputCols=columns_of_interest[1:], outputCol='features')

    trainingData = assembler.transform(converted_zone_data)

    trainingData.show()

    all_player_names = trainingData.select("player_name").distinct().collect()
    result = list()
    for pname in all_player_names:
        players_features = trainingData.where(
            col("player_name") == pname).select("features")
        kmeanifier = KMeans().setK(4).setSeed(10)
        model = kmeanifier.fit(players_features)

        # predictions = model.transform(players_features)

        result.append(model.clusterCenters())

    print(result)
    # silhouette = evaluator.evaluate(predictions)
    # zone_data.show(5)
    # exp = zone_data.rdd.map(assign_nearest_group)
    # kzones_df = spark.create

    # zone_data.show(10)
    # print("Silhouette with squared euclidean distance = " + str(silhouette))

    # deb_print("csv zone_data read")
    # output = exp.collect()
    # deb_print("lines collected")
    # deb_print("output len: {}".format(len(output)))
    # for w in output[0:10]:
    #     print(w) pip install --upgrade pip

    print("*"*50)
    deb_print("END PROGRAM")
    spark.stop()


if __name__ == "__main__":
    main()
