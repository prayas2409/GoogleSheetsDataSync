from google.oauth2 import service_account
from getfilelistpy import getfilelist

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'token.json'
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

resource = {
    "service_account": credentials,
    "id": "1-5x3SaFP7T0gWVkgxnYq_sxPAOwNwTpL",
    "fields": "files(name)",
}
res = getfilelist.GetFileList(resource)  # or r = getfilelist.GetFolderTree(resource)
files=res["fileList"][0]["files"]
print(type(files))
sheets = [ each["name"] for each in files if "Sheets" in each["name"] ]

