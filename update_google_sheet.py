import os
import pandas as pd
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("Starting script...")

# Define the base directory dynamically
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define the path to the Excel file
EXCEL_FILE_PATH = os.path.join(BASE_DIR, 'transform', 'normalized.xlsx')
logger.info(f"Excel file path set: {EXCEL_FILE_PATH}")

# Define the path to the JSON credentials file
SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, 'credentials.json')
logger.info(f"JSON credentials file path set: {SERVICE_ACCOUNT_FILE}")

# Read the Excel sheet
logger.info("Reading the Excel sheet...")
try:
    df = pd.read_excel(EXCEL_FILE_PATH)
    logger.info("Excel sheet read successfully.")
except Exception as e:
    logger.error(f"Error reading the Excel sheet: {e}")
    raise

# Replace NaN values with empty strings
logger.info("Replacing NaN values with empty strings...")
df = df.fillna('')
logger.info("NaN values replaced.")

# Define the scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
logger.info("Scopes defined.")

# Authenticate and build the Google Sheets service
logger.info("Authenticating and building the Google Sheets service...")
try:
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    logger.info("Google Sheets service built successfully.")
except Exception as e:
    logger.error(f"Error authenticating or building the Google Sheets service: {e}")
    raise

# Google Sheets spreadsheet ID and data range
SPREADSHEET_ID = '1G5l39R0l_baVnM6N8ttEvVI-OBUcxsL9iqMEH8GsRiE'
RANGE_NAME = 'Sheet1!A1'
logger.info(f"Google Sheets ID: {SPREADSHEET_ID}")
logger.info(f"Data range: {RANGE_NAME}")

# Ensure columns are in the same order as the Google Sheets
# Replace with your column order if needed
expected_columns = ['Year', 'Month', 'Permits', 'company_name']
df = df[expected_columns]

# Convert the DataFrame to a list of lists
logger.info("Converting the DataFrame to a list of lists...")
values = [df.columns.values.tolist()] + df.values.tolist()
logger.info("DataFrame converted.")

# Write data to the Google Sheets
logger.info("Writing data to the Google Sheets...")
body = {
    'values': values
}
try:
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
        valueInputOption='RAW', body=body).execute()
    logger.info(f'{result.get("updatedCells")} cells updated.')
except Exception as e:
    logger.error(f"Error updating the Google Sheets: {e}")
    raise

logger.info("Script completed successfully.")
