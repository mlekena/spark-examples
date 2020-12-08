import pandas as pd

from pyspark.sql import SparkSession
import pyspark.sql.functions as sparkFunc
import pyspark.sql.types as T
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression

if __name__ == "__main__":

    context = (SparkSession.builder
               .appName("Toxic Comment Classification")
               .enableHiveSupport()
               .config("spark.executor.memory", "4G")
               .config("spark.driver.memory", "18G")
               .config("spark.executor.cores", "7")
               .config("spark.python.worker.memory", "4G")
               .config("spark.driver.maxResultSize", "0")
               .config("spark.sql.crossJoin.enabled", "true")
               .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
               .config("spark.default.parallelism", "2")
               .getOrCreate())

    context.sparkContext.setLogLevel("INFO")
    