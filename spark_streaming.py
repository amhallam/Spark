from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json


sc = SparkContext(appName="PythonSparkStream")
sc.setLogLevel("INFO")

#interval frame is 5 mins
ssc = StreamingContext(sc,300)
KafkaStream = KafkaUtils.createStream(ssc,"ZOOKEEPER","CONSUMER_GROUP",{"TOPIC",1},{"security.protocol":"PLAINTEXTSASL","startingOffsets":"earliest"})

Stream = kafkStream.map(lambda v : v[1])
Stream.pprint()
Stream.count().pprint()

FilteredStream = Stream.filter(lambda L : "KEYWORD" in L)
FilteredStream.saveAsTextFiles("HDFS_LOCATION)
