from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from kafka.admin import KafkaAdminClient
from file_list import KafkaTopicCreator
from utility import space_remover
import os
from dotenv import load_dotenv
load_dotenv()


class KafkaStreamer:

    def __init__(self):
        self.spark = SparkSession.builder.appName("kafkaStream").master("local[*]").getOrCreate()
            # .config("spark.jars",)\
        self.spark.sparkContext.setLogLevel("ERROR")

    def stream(self,topics):
        topics = ",".join(topics) if len(topics) > 0 else topics[0]

        df = self.spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("BOOTSTRAP_SERVERS")) \
        .option("subscribe",topics).load()

        ds = df \
        .selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    kafka_streamer = KafkaStreamer()
    kafka_topic_creator = KafkaTopicCreator(KafkaAdminClient(
                bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"), 
                client_id='test')  
            )
    sheets = kafka_topic_creator.get_list_of_sheets(os.getenv("DIR_ID"))
    sheets = space_remover(sheets)
    kafka_streamer.stream(sheets)
