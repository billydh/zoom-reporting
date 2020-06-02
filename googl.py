from typing import List

from google.oauth2 import service_account
from googleapiclient import discovery
from pandas import DataFrame


class Googl:
    def __init__(self, service_account_file: str, scopes: List[str]):
        self.service_account_file = service_account_file
        self.scopes = scopes
        self.google_sheet_type = "application/vnd.google-apps.spreadsheet"
        self.creds = service_account.Credentials.from_service_account_file(self.service_account_file,
                                                                           scopes=self.scopes)
        self.drive = discovery.build("drive", "v3", credentials=self.creds)
        self.sheets = discovery.build("sheets", "v4", credentials=self.creds)

    def get_folder_id(self, folder_name: str) -> str:
        folders: dict = self.drive.files().list(q="mimeType='application/vnd.google-apps.folder'").execute()
        print(folders)
        folder_id = [x.get("id") for x in folders.get("files") if x.get("name") == folder_name]

        return folder_id[0]

    def create_new_sheet(self, file_name: str, parent_folder_id: str) -> str:
        new_sheet_metadata = {
            "name": file_name,
            "parents": [parent_folder_id],
            "mimeType": self.google_sheet_type
        }

        new_sheet = self.drive.files().create(body=new_sheet_metadata).execute()
        print(new_sheet)

        return new_sheet.get("id")

    def insert_df_to_sheet(self, google_sheet_id: str, df: DataFrame) -> dict:
        response = self.sheets.spreadsheets().values().append(
            spreadsheetId=google_sheet_id,
            valueInputOption="RAW",
            range="A1",
            body={"majorDimension": "ROWS",
                  "values": df.T.reset_index().T.values.tolist()}
        ).execute()

        return response

    def get_sheet_link(self, google_sheet_id: str,
                       return_all_fields: bool = False, fields_to_return: str = "webViewLink"):
        fields = "*" if return_all_fields else fields_to_return
        response = self.drive.files().get(fileId=google_sheet_id, fields=fields).execute()

        return response
