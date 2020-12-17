import argparse
import pandas as pd

from pyspark.sql import SparkSession
import pyspark.sql.functions as sparkFunc
import pyspark.sql.types as T
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import FloatType

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
        .option("multiLine", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
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
    test_data = get_csv_data(args.test_data_file)
    # train_data.select("id").show()

    # Create a list of all other columns besides 'id' and 'comment_text'
    out_cols = [i for i in train_data.columns if i not in [
        "id", "comment_text"]]
    print("\nCONVERT STR TO FLOATS\n")
    for cols in out_cols:
        train_data = train_data.withColumn(
            cols, train_data[cols].cast(FloatType()))
    train_data.show(5)
    # Basic sentence tokenizer
    print("\nTOKENIZE...\n")
    tokenizer = Tokenizer(inputCol="comment_text", outputCol="words")
    wordsData = tokenizer.transform(train_data)
    wordsData.show(5)

    # Count the words in a document
    # Here we count up each of the tokenized words for each row
    print("\nHASHTRANSFORM...\n")
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
    tf = hashingTF.transform(wordsData)
    tf.show(5)

    # Build the idf model and transform the original token frequencies into their tf-idf counterparts

    print("\nTRANSFORM TO TF_IDF...\n")
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(tf)
    tfidf = idfModel.transform(tf)
    tfidf.show(5)

    print("\nLEARN...\n")
    REG = 0.1
    lr = LogisticRegression(featuresCol="features",
                            labelCol='toxic', regParam=REG)
    lrModel = lr.fit(tfidf.limit(5000))
    res_train = lrModel.transform(tfidf)
    res_train.show(5)
    # Create a user-defined function (udf) to select the second element in each row of the column vector
    extract_prob = sparkFunc.udf(lambda x: float(x[1]), T.FloatType())
    (res_train.withColumn("proba", extract_prob("probability"))
     .select("proba", "prediction")
     .show())

    # test
    print("\nTEST LEARNED REGRESSION\n")
    test_tokens = tokenizer.transform(test_data)
    test_tf = hashingTF.transform(test_tokens)
    test_tfidf = idfModel.transform(test_tf)
    test_res = test_data.select('id')
    test_res.head()
    test_probs = []
    for col in out_cols:
        print(col)
        lr = LogisticRegression(featuresCol="features",
                                labelCol=col, regParam=REG)
        print("...fitting")
        lrModel = lr.fit(tfidf)
        print("...predicting")
        res = lrModel.transform(test_tfidf)
        print("...appending result")
        test_res = test_res.join(res.select('id', 'probability'), on="id")
        print("...extracting probability")
        test_res = test_res.withColumn(
            col, extract_prob('probability')).drop("probability")
        test_res.show(5)
    # test_res.coalesce(1).write.csv('./results/spark_lr.csv',
    #                               mode='overwrite', header=True)
    with open("test_results", "w") as ofile:
        for row in test_res.collect():
           ofile.write(row)

    # test_res.write.csv("est_test_data.csv", mode="overwrite", header=True)
