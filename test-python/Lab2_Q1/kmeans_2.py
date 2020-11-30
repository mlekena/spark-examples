from __future__ import print_function

# $example on$
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
# $example off$
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
#from org.apache.spark.sql.functions import countDistinct
import sys
import pandas as pd

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("KMeans_Lab2Q2")\
        .getOrCreate()

    # $example on$
    # Loads data.
    #df = spark.read.option("header",True).csv(sys.argv[1])
    df  = pd.read_csv(sys.argv[1]) ##FiltData.csv
    ## filter out instances without location codes
    cols=list(df.columns)
    n=3; c=cols.index("Street Code1"); filt=[]
    temp=list(np.min(df.iloc[:,c:c+n],axis=1))
    for i in range(len(temp)): 
        if temp[i]!=0:filt.append(1)
        else:filt.append(0)
    filt_pd=df[pd.Series(filt)==1]
    ## convert to spark dataframe
    filt_df=spark.createDataFrame(filt_pd)
    vecAssembler = VectorAssembler(inputCols=["Street Code1", "Street Code2", "Street Code3"], outputCol="features")
    new_df = vecAssembler.transform(filt_df)

    # Trains a k-means model.
    kmeans = KMeans().setK(4).setSeed(1)
    model = kmeans.fit(new_df.select('features'))

    # Make predictions
    predictions = model.transform(new_df)
    pcounts=predictions.groupby("predictions").count()
    print("K-Means Results: ")
    pcounts.show()

    # Evaluate clustering by computing Silhouette score
    evaluator = ClusteringEvaluator()

    silhouette = evaluator.evaluate(predictions)
    print("Silhouette with squared euclidean distance = " + str(silhouette))

    # Shows the result.
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)
    # $example off$

    spark.stop()


#spark-submit ./wc.py ../../test-data/test.txt