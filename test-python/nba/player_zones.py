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


def main():
    columns_of_interest = ['player_name',
                           'SHOT_DIST', 'CLOSE_DEF_DIST', 'SHOT_CLOCK']
    args = parser.parse_args()
    spark = SparkSession.builder.appName("MostComfortableZones").getOrCreate()

    # remove comma between quotes for string type csv fields making it hard to parse.
    # lines = spark.read.format("csv") \
    #     .option("header", True).schema(schema) \
    #     .load(args.data_file)\
    #     .select(*columns_of_interest).rdd.map(lambda r: r[0])
    lines = spark.csv(args.data_file, header=True,
                      schema=schema).rdd.map(lambda row: row[0])
    output = lines.collect()
    for w in output[0: 10]:
        print(w)


if __name__ == "__main__":
    main()
