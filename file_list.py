from google.oauth2 import service_account
from getfilelistpy import getfilelist
from kafka.admin import KafkaAdminClient, NewTopic
import os
from dotenv import load_dotenv

load_dotenv() #load all env variables from .env file

def get_list_of_sheets(service_acc_file,dir_id):
    try:
        SCOPES = ['https://www.googleapis.com/auth/drive']
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
        print("Code stopped as ",e)

SERVICE_ACCOUNT_FILE = 'token.json'
dir_id = os.getenv("DIR_ID") # google drive directory id
sheets_names = get_list_of_sheets(SERVICE_ACCOUNT_FILE,dir_id)


