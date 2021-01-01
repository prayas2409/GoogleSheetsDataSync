from google.oauth2 import service_account
from getfilelistpy import getfilelist
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import errors
import os
from dotenv import load_dotenv

# initial loading of env variables
load_dotenv() #load all env variables from .env file

class KafkaTopicCreator:

    def __init__(self, kafka_admin_client : KafkaAdminClient):
        """[summary]

        Args:
            kafka_admin_client ([KafkaAdminClient]): [Needs object of Kafka Admin consist of bootstrap servers]
        """        
        self.kafka_admin_client = kafka_admin_client

    def get_list_of_sheets(self,dir_id:str):
        """[summary]

        Args:
            dir_id ([string]): [The id for the directory from which we need to take sheets]

        Returns:
            [list]: [list of sheets in directory]
        """        
        try:
            SCOPES = ['https://www.googleapis.com/auth/drive']
            SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_FILE_NAME_JSON')
            credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
            resource = {
            "service_account": credentials,
            "id": dir_id,
            "fields": "files(name)",
            }
            res = getfilelist.GetFileList(resource)  # or r = getfilelist.GetFolderTree(resource)
            files=res["fileList"][0]["files"]
            # The files consist a list of dictionary for all files in directory 
            sheets = [ each["name"].replace(" ","") for each in files if "sheet" in each["name"].lower() ]
            return sheets
        except Exception as e:
            print("Process stopped as ",e)

    def create_topics(self,sheet_names):
        try:
            topic_list = [ NewTopic(name=sheet, num_partitions=1, replication_factor=1) for sheet in sheet_names]
            kafka_admin_client.create_topics(new_topics=topic_list, validate_only=False)
        except errors.TopicAlreadyExistsError as e:
            print(e)

if __name__ == "__main__":
    dir_id = os.getenv("DIR_ID") # google drive directory id
    kafka_admin_client = KafkaAdminClient(
            bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"), 
            client_id='test'
    )
    kafka_topic_creator = KafkaTopicCreator(kafka_admin_client)
    sheet_names = kafka_topic_creator.get_list_of_sheets(dir_id)
    existing_topics = kafka_admin_client.list_topics()
    new_sheets = set(sheet_names) - set(existing_topics) # need to create only new topics
    if new_sheets.__len__() !=0 :
        kafka_topic_creator.create_topics(new_sheets)
        print("print created topics for ", new_sheets)
    else:
        print("no new sheets")
