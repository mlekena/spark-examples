from __future__ import print_function

import sys
import argparse
from operator import add

from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType
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


def assign_nearest_group(in_row):
    return in_row[0]


def main():
    print("*"*50)
    print("*"*50)
    print("*"*50)
    columns_of_interest = ['player_name',
                           'SHOT_DIST', 'CLOSE_DEF_DIST', 'SHOT_CLOCK']
    args = parser.parse_args()
    spark = SparkSession.builder.appName("MostComfortableZones").getOrCreate()
    deb_print("using data file {}".format(args.data_file))

    zone_data = spark.read.csv(args.data_file, header=True).select(
        *columns_of_interest).na.fill(15).withColumn("KGROUP", lit(-1))
    zone_data.show(5)
    exp = zone_data.rdd.map(assign_nearest_group)
    # kzones_df = spark.create

    # zone_data.show(10)
    deb_print("csv zone_data read")
    output = exp.collect()
    deb_print("lines collected")
    deb_print("output len: {}".format(len(output)))
    for w in output[0:10]:
        print(w)

    print("*"*50)
    deb_print("END PROGRAM")


if __name__ == "__main__":
    main()
