from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class KafkaStreamer:

    def __init__(self):
        self.spark = SparkSession.builder.appName("kafkaStream").master("local[*]").getOrCreate()
            # .config("spark.jars",)\
        self.spark.sparkContext.setLogLevel("ERROR")

    def stream(self):
        df = self.spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "DEMOSheets").load()
        # df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers")
        
        ds = df \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    kafka_streamer = KafkaStreamer()
    kafka_streamer.stream()
