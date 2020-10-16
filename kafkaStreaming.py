import re
from kafka import KafkaClient, KafkaConsumer, KafkaProducer, KafkaClient
import tweepy
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os
from pyspark.ml.feature import Tokenizer, StopWordsRemover
import nltk
from pyspark.sql import SparkSession
from streaming_to_dataframe import process
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.7 pyspark-shell'

consumerKey = "*****************"
consumerSecretKey = "*****************"
accessToken = "*****************"
accessTokenSecret = "*****************"
topic = 'trump'

client = KafkaClient(bootstrap_servers='localhost:9092')
# producer = KafkaProducer(kafka)
future = client.cluster.request_update()
client.poll(future=future)

metadata = client.cluster
if not topic in metadata.topics():
    client.set_topics(topic)

producer = KafkaProducer()

auth = tweepy.OAuthHandler(consumerKey, consumerSecretKey)
auth.set_access_token(accessToken, accessTokenSecret)
api = tweepy.API(auth)


class MyStreamListener(tweepy.StreamListener):
    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False

    def on_status(self, status):
        # print(status.text)
        producer.send(topic=topic, value=status.text.encode('utf-8'))


myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
myStream.filter(track=[topic], is_async=True)

sc = SparkContext(appName='kafkaStreaming')
ssc = StreamingContext(sc, 1)

tweets = KafkaUtils.createStream(ssc=ssc, groupId='second', zkQuorum='localhost:2181', topics={topic: 1})
tweets = tweets.map(lambda x: x[1])
# tweets = tweets.flatMap(lambda line: line.split(" "))
tweets = tweets.foreachRDD(process)



ssc.start()  # Start the computation
ssc.awaitTermination()
