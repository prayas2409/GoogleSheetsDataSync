from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
import os
from sheets_reader import SheetsReader
from file_list import KafkaTopicCreator

class KafkaCustomProducer:
    """[Class to send data to kafka]
    """    

    def __init__(self):
        """[Intialize the Producer for our bootstrap servers]
        """
        self.producer = KafkaProducer(bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"),client_id="test",acks='all')

    def send_to_kafka(self,topic:str,data:list):
        """[Sends data to kafka for given topic]

        Args:
            topic (str): [Name of topic in which we need to send data]
            data (list): [List consisting of data to be sent to kafka]
        """  
        try:      
            for each in data:
                future = self.producer.send(topic,bytes(str(each),'utf-8'))
                result = future.get(timeout=60)
        except Exception as e:
            print("Could not send data because ",e)

if __name__ == "__main__":
    try:
        kafka_producer = KafkaCustomProducer()
        kafka_topic_creator = KafkaTopicCreator(KafkaAdminClient(
                bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"), 
                client_id='test')  
            )
        sheet_reader = SheetsReader()
        sheets = kafka_topic_creator.get_list_of_sheets(os.getenv("DIR_ID"))
        for sheet in sheets:
            kafka_producer.send_to_kafka(sheet.replace(" ",""),sheet_reader.get_data(sheet))
    except Exception as e:
        print("could not complete because ",e) 