import gspread
# import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import os
from dotenv import load_dotenv
load_dotenv()

class SheetsReader:
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

    def __init__(self):
        """[intialize the credentials]
        """        
        # add credentials to the account
        creds = ServiceAccountCredentials.from_json_keyfile_name(os.getenv("SERVICE_FILE_NAME_JSON"), self.scope) 
        # authorize the clientsheet 
        self.client = gspread.authorize(creds)

    def get_data(self,sheet_name:str):
        """[Reads the data from given sheet]

        Args:
            sheet_name ([str]): [name of the sheet whose data needs to be fetched]

        Returns:
            [list]: [Data read from the given sheet]
        """        
        # get the instance of the Spreadsheet
        sheet = self.client.open(sheet_name)
        sheet_instance = sheet.get_worksheet(0)
        records_data = sheet_instance.get_all_records()
        return records_data


if __name__== "__main__":
    sheet_reader = SheetsReader()
    sheet = "DEMO Sheets"
    data = sheet_reader.get_data(sheet)
    print(data)