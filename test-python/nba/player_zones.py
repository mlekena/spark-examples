from __future__ import print_function

import sys
import argparse
from operator import add

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
schema = StructType()\
    .add('GAME_ID', IntegerType(), True) \
    .add('MATCHUP', StringType(), True) \
    .add('LOCATION', StringType(), True) \
    .add('W', StringType(), True) \
    .add('FINAL_MARGIN', IntegerType(), True) \
    .add('SHOT_NUMBER', StringType(), True) \
    .add('PERIOD', StringType(), True) \
    .add('GAME_CLOCK', StringType(), True) \
    .add('SHOT_CLOCK', IntegerType(), True) \
    .add('DRIBBLES', StringType(), True) \
    .add('TOUCH_TIME', StringType(), True) \
    .add('SHOT_DIST', IntegerType(), True) \
    .add('PTS_TYPE', StringType(), True) \
    .add('SHOT_RESULT', StringType(), True) \
    .add('CLOSEST_DEFENDER', StringType(), True) \
    .add('CLOSEST_DEFENDER_PLAYER_ID', StringType(), True) \
    .add('CLOSE_DEF_DIST', IntegerType(), True) \
    .add('FGM', StringType(), True) \
    .add('PTS', StringType(), True) \
    .add('player_name', StringType(), True) \
    .add('player_id', StringType(), True)


def deb_print(outstr):
    print("{}{}{}".format("#"*20, outstr, "#"*20))


def main():
    print("*"*50)
    print("*"*50)
    print("*"*50)
    columns_of_interest = ['player_name',
                           'SHOT_DIST', 'CLOSE_DEF_DIST', 'SHOT_CLOCK']
    args = parser.parse_args()
    spark = SparkSession.builder.appName("MostComfortableZones").getOrCreate()
    deb_print("using data file {}".format(args.data_file))
    # remove comma between quotes for string type csv fields making it hard to parse.
    # lines = spark.read.format("csv") \
    #     .option("header", True).schema(schema) \
    #     .load(args.data_file)\
    #     .select(*columns_of_interest).rdd.map(lambda r: r[0])
    #zone_data = spark.read.csv(args.data_file, header=True,
    #                           schema=schema)
    zone_data = spark.read.csv(args.data_file, header=True)
    zone_data.show(10)
    zone_data.select(*columns_of_interest).show(10)

    #zone_data.show(10)
    deb_print("csv zone_data read")
    output = zone_data.collect()
    deb_print("lines collected")
    deb_print("output len: {}".format(len(output)))
    # for w in output[]:
    #     print(w)

    print("*"*50)
    deb_print("END PROGRAM")


if __name__ == "__main__":
    main()
