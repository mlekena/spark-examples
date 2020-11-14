from __future__ import print_function

import sys
import argparse
from operator import add

from pyspark.sql import SparkSession

reload(sys)
sys.setdefaultencoding('utf8')

parser = argparse.ArgumentParser
parser.add_argument(
    "--data_file", help="Data file to read in must be specified using --data_file")
parser.add_argument(
    "--data_output", help="Data output path must be specified using --data_output", action="")

#  SHOT_DIST, CLOSEST_DEF_DIST, SHOT_CLOCK
init_k_zones = [
    [8, 12, 20],
    [15, 9, 15],
    [22, 6, 10],
    [30, 3, 5]
]


def main():
    columns_of_interest = ['player_name',
                           'SHOT_DIST', 'CLOSEST_DEF_DIST', 'SHOT_CLOCK']
    args = parser.parse_args()
    spark = SparkSession.builder.appName("MostComfortableZones").getOrCreate()

    # remove comma between quotes for string type csv fields making it hard to parse.
    lines = spark.read.option("header", True).csv(args.file)\
        .select(*columns_of_interest).rdd.map(lambda r: r[0])
    output = lines.collect()
    for w in output[0, 10]:
        print(w)


if __name__ == "__main__":
    main()
