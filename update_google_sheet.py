import os
import pandas as pd
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build

class GoogleSheetUploader:
    def __init__(self):
        # Configure logging to display the date, time, and message details
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        # Dynamically define the base directory of the script
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.transform_dir = os.path.join(self.base_dir, 'transform')

        # Specify the path to the service account credentials file
        self.service_account_file = os.path.join(self.base_dir, 'credentials.json')

        # Define the Google API scopes needed for Google Sheets
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets']

        # Authenticate and create the Google Sheets API service
        self.service = self.authenticate_and_build_service()

        # Map file names to their respective Google Sheets IDs and specify column orders
        self.file_sheet_map = self.map_files_to_sheet_ids_and_columns()

    def authenticate_and_build_service(self):
        # Log the process of authenticating and building the Google Sheets API service
        self.logger.info("Authenticating and building the Google Sheets service...")
        try:
            # Load credentials from the service account file with specified scopes
            creds = service_account.Credentials.from_service_account_file(
                self.service_account_file, scopes=self.scopes)
            service = build('sheets', 'v4', credentials=creds)
            self.logger.info("Google Sheets service built successfully.")
            return service
        except Exception as e:
            # Log any errors during authentication and raise the exception
            self.logger.error(f"Error authenticating or building the Google Sheets service: {e}")
            raise

    def map_files_to_sheet_ids_and_columns(self):
        # Log the process of mapping files to their respective Google Sheets IDs and column orders
        self.logger.info("Mapping files to Google Sheets IDs and column orders...")
        file_sheet_map = {
            'permits_company.xlsx': {
                'sheet_id': '1G5l39R0l_baVnM6N8ttEvVI-OBUcxsL9iqMEH8GsRiE',
                'column_order': ['Year', 'Month', 'Permits', 'company_name']
            },
            'permits-by-nationality.xlsx': {
                'sheet_id': '14xvzRR4thC-xyeZNOvk6aIPEMLmphQxu96_uF8TUbkA',
            },
            'permits by sector.xlsx': {
                'sheet_id': '1pu9DnHS-6poIs63CgHYatNRkmzdR-tXKFRx_w6724rY',
            }
        }
        return file_sheet_map

    def upload_data(self):
        # Iterate over each file and its settings in the map to upload data to Google Sheets
        for file_name, settings in self.file_sheet_map.items():
            sheet_id = settings.get('sheet_id')
            if not sheet_id:
                # Log and skip the upload if no sheet ID is provided
                self.logger.warning(f"No Sheet ID provided for {file_name}. Skipping upload.")
                continue

            column_order = settings.get('column_order', None)
            file_path = os.path.join(self.transform_dir, file_name)
            self.logger.info(f"Processing file: {file_name}")
            df = pd.read_excel(file_path)

            # Replace NaN values with empty strings
            df.fillna('', inplace=True)

            # Reorder columns if specified, or sort them alphabetically if not
            if column_order:
                if set(column_order).issubset(df.columns):
                    df = df[column_order]
                    self.logger.info(f"Columns reordered for {file_name}.")
                else:
                    self.logger.warning(f"Expected columns missing in {file_name}. Using alphabetical order.")
                    df = df.sort_index(axis=1)
            else:
                self.logger.info("No column order specified; sorting columns alphabetically.")
                df = df.sort_index(axis=1)

            values = [df.columns.values.tolist()] + df.values.tolist()
            self.logger.info("Converting the DataFrame to a list of lists...")

            # Attempt to update the Google Sheets with the data
            range_name = 'Sheet1!A1'
            body = {'values': values}
            try:
                self.logger.info(f"Uploading data to Google Sheets ID: {file_name} - {sheet_id}")
                result = self.service.spreadsheets().values().update(
                    spreadsheetId=sheet_id, range=range_name,
                    valueInputOption='RAW', body=body).execute()
                self.logger.info(f'{result.get("updatedCells")} cells updated.')
            except Exception as e:
                self.logger.error(f"Error updating the Google Sheets for {file_name}: {e}")

if __name__ == "__main__":
    uploader = GoogleSheetUploader()
    uploader.upload_data()
    uploader.logger.info("Script completed successfully.")
