from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as f
from pyspark.ml.feature import Tokenizer, RegexTokenizer,StopWordsRemover

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def process(rdd):
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: Row(word=w))
        wordsDataFrame = spark.createDataFrame(rowRdd,["sentence"])
        wordsDataFrame = wordsDataFrame.select(f.lower(f.col("sentence")).alias("sentence"))
        wordsDataFrame = wordsDataFrame.select(
            f.regexp_replace(f.col("sentence"), "(\d+)", "").alias(
                "sentence"))
        

        # return wordsDataFrame
        tokenizer = Tokenizer(inputCol="replaced", outputCol="words")
        tokenized = tokenizer.transform(wordsDataFrame)

        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        removed = remover.transform(tokenized)
        removed.select("filtered").show(20,False)
        # return removed



    except:
        pass
