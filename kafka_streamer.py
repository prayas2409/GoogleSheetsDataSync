from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka.admin import KafkaAdminClient
from kafka_topic_creator import KafkaTopicCreator
from utility import space_remover
import os
from dotenv import load_dotenv
load_dotenv()


class KafkaStreamer:

    def __init__(self):
        """[Initialize spark for streaming]
        """        
        self.spark = SparkSession.builder.appName("kafkaStream").master("local[*]").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR") # collect logs for spark level > level error

    def stream(self,topics:list,schema:StructType):
        """[Streams data from kafka as structured streaming]

        Args:
            topics (list): [List of topics to stream from]
            schema (StructType): [Schema for the data to be streamed]
        """        
        topics = ",".join(topics) if len(topics) > 0 else topics[0]

        df = self.spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("BOOTSTRAP_SERVERS")) \
        .option("subscribe",topics).load()

        val_df = df.selectExpr("CAST(value as STRING)")
        json_df = val_df.select(from_json(col("value"),schema).alias("data")).select("data.*")
        ds = json_df \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", "checkpoints") \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    kafka_streamer = KafkaStreamer()
    kafka_topic_creator = KafkaTopicCreator(KafkaAdminClient(
                bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"), 
                client_id=os.getenv("CLIENT_ID"))  
            )
    sheets = kafka_topic_creator.get_list_of_sheets(os.getenv("DIR_ID"))
    sheets = space_remover(sheets)
    # Defining the structure for the data present in Google sheet
    schema_demo_sheet= StructType([ StructField("A", StringType(), True),
                StructField("B", StringType(), True),
                StructField("C", StringType(), True)])
    kafka_streamer.stream(sheets,schema_demo_sheet)
