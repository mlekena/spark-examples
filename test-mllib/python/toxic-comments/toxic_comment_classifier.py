import argparse
import pandas as pd

from pyspark.sql import SparkSession
import pyspark.sql.functions as sparkFunc
import pyspark.sql.types as T
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression

# Globally define to make available in functions. Instantiated in main
context = None

parser = argparse.ArgumentParser()
parser.add_argument(
    "--test_data_file", help="Data file to read in must be specified using --test_data_file")
parser.add_argument(
    "--train_data_file", help="Data output path must be specified using --train_data_file")


def to_spark_df(file_name):
    assert(context), "calling to_spark_df without an instantiated sparksession context"
    df = pd.read_csv(file_name)
    df.fillna("", inplace=True)
    df = context.createDataFrame(df)
    return (df)


def get_csv_data(hdfs_path):
    """You need to use the context to get data using hdfs paths"""
    assert(context), "calling to_spark_df without an instantiated sparksession context"
    data = context.read \
        .option("delimiter", ',') \
        .option("header", "true")\
        .csv(hdfs_path)
    return data


if __name__ == "__main__":

    context = (SparkSession.builder
               .appName("Toxic Comment Classification")
               .enableHiveSupport()
               .config("spark.sql.crossJoin.enabled", "true")
               .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
               .config("spark.default.parallelism", "2")
               .getOrCreate())

    context.sparkContext.setLogLevel("INFO")
    args = parser.parse_args()
    context.catalog.clearCache()
    train_data = get_csv_data(args.train_data_file)
    # test_data = to_spark_df(args.test_data_file)
    train_data.show()
