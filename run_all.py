import os
import re
import requests
import pandas as pd
import logging
import warnings
import subprocess
import sys
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ContentChangeChecker:
    def __init__(self, url, date_file_path):
        self.url = url
        self.date_file_path = date_file_path
        self.logger = logging.getLogger(__name__)

    def check_for_change(self):
        self.logger.info("Starting content change check.")
        try:
            self.logger.info(f"Sending request to URL: {self.url}")
            response = requests.get(self.url)
            response.raise_for_status()
            self.logger.info("Request successful.")

            self.logger.info("Parsing the response content with BeautifulSoup.")
            soup = BeautifulSoup(response.text, 'html.parser')
            date_tag = soup.find('p', class_='article-date article-date--larger')
            if not date_tag:
                self.logger.error("Date tag not found in the response content.")
                return False

            current_date = date_tag.get_text(strip=True).split('|')[0].strip()
            self.logger.info(f"Processed current date: {current_date}")

            self.logger.info(f"Checking if date file exists at path: {self.date_file_path}")
            previous_date = None
            if os.path.exists(self.date_file_path):
                with open(self.date_file_path, 'r') as file:
                    previous_date = file.read().strip()
                self.logger.info(f"Read previous date from file: {previous_date}")
            else:
                self.logger.warning(f"Date file not found at {self.date_file_path}. Assuming first run.")

            if current_date != previous_date:
                self.logger.info("Content has changed. Updating the date file.")
                os.makedirs(os.path.dirname(self.date_file_path), exist_ok=True)
                with open(self.date_file_path, 'w') as file:
                    file.write(current_date)
                self.logger.info(f"Updated date file with current date: {current_date}")
                return True
            else:
                self.logger.info("Content has not changed.")
                return False
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request to {self.url} failed: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error during content check: {e}")
            return False

class EmploymentPermitDataProcessor:
    def __init__(self, base_url, start_url, download_folder, output_folder, start_year):
        self.base_url = base_url
        self.start_url = start_url
        self.download_folder = download_folder
        self.output_folder = output_folder
        self.start_year = start_year
        self.current_year = datetime.now().year

        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder)
            logging.info(f"Created download folder: {self.download_folder}")

        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
            logging.info(f"Created output folder: {self.output_folder}")

    def download_files(self):
        for year in range(self.start_year, self.current_year + 1):
            url = self.base_url.format(year)
            try:
                logging.info(f"Processing URL: {url}")
                response = requests.get(url)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')

                    file_links = soup.find_all('a', href=lambda href: href and (
                                href.endswith('.xls') or href.endswith('.xlsx')))

                    for link in file_links:
                        full_link = link['href']
                        if not full_link.startswith('http'):
                            full_link = f"{self.start_url}{full_link}"

                        file_name = full_link.split('/')[-1]
                        file_path = os.path.join(self.download_folder, file_name)

                        if not os.path.exists(file_path) or year == self.current_year:
                            logging.info(f"Downloading file: {file_name}")
                            file_response = requests.get(full_link)
                            if file_response.status_code == 200:
                                with open(file_path, 'wb') as file:
                                    file.write(file_response.content)
                                logging.info(f"File {file_name} downloaded and saved to '{file_path}'")
                            else:
                                logging.error(
                                    f"Error downloading file {file_name}. Status code: {file_response.status_code}")
                        else:
                            logging.info(f"File {file_name} already exists, skipping download.")
                else:
                    logging.error(
                        f"Page for year {year} not found or there was an error. Status code: {response.status_code}")
            except Exception as e:
                logging.error(f"Error processing page for year {year}: {e}")

    def process_files_companies(self):
        patterns = ['companies-issued-with-permits-', 'permits-issued-to-companies-']
        matching_files = self.get_matching_files(self.download_folder, patterns)

        dfs = []
        for file in matching_files:
            file_path = os.path.join(self.download_folder, file)
            try:
                logging.info(f"Processing file: {file}")
                if 'companies-issued-with-permits-' in file:
                    df_long = self.load_and_standardize_excel_with_year(file_path)
                elif 'permits-issued-to-companies-' in file:
                    df_long = self.load_and_standardize_excel_with_month(file_path)
                df_long.reset_index(drop=True, inplace=True)
                df_long = df_long[df_long['company_name'] != 'Jan - Dec']
                dfs.append(df_long)
            except Exception as e:
                logging.error(f"Error processing file {file}: {e}")

        if dfs:
            all_columns = set()
            for df in dfs:
                all_columns.update(df.columns)

            aligned_dfs = []
            for df in dfs:
                for col in all_columns:
                    if col not in df.columns:
                        df[col] = pd.NA
                aligned_dfs.append(df[list(all_columns)])

            df_concatenated = pd.concat(aligned_dfs, ignore_index=True, sort=False)
            df_concatenated = df_concatenated.drop_duplicates()

            normalized_file_name = f'{self.output_folder}/permits_company.xlsx'
            df_concatenated.to_excel(normalized_file_name, index=False)

            logging.info(f"Combined data saved in '{normalized_file_name}'")

            years_present = df_concatenated['Year'].unique()
            years_present.sort()
            logging.info(f"Years present in the data: {years_present}")

            records_per_year = df_concatenated['Year'].value_counts().sort_index()
            logging.info(f"Number of records per year:\n{records_per_year}")

    def get_matching_files(self, directory, patterns):
        matching_files = []
        for file in os.listdir(directory):
            if any(file.startswith(pattern) and file.endswith('.xlsx') for pattern in patterns):
                matching_files.append(file)
        matching_files.sort()
        return matching_files

    def determine_skiprows(self, file_path):
        df = pd.read_excel(file_path, nrows=2)
        for col in df.columns:
            if 'Employer' in str(col) or 'Name' in str(col):
                return 0
        return 1

    def load_and_standardize_excel_with_month(self, file_path):
        skiprows = self.determine_skiprows(file_path)
        df = pd.read_excel(file_path, skiprows=skiprows, engine='openpyxl')
        employer_col = [col for col in df.columns if 'Employer' in str(col) or 'Name' in str(col)]
        if employer_col:
            df.rename(columns={employer_col[0]: 'company_name'}, inplace=True)
        if df.iloc[0].astype(str).str.contains('Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec').any():
            df.columns = ['company_name'] + df.iloc[0, 1:].tolist()
            df = df[1:]
        df.columns = df.columns.astype(str)
        df = df.loc[:, ~df.columns.str.contains('Unnamed')]
        df = df.dropna(axis=1, how='all')
        df = df.dropna(subset=['company_name'])
        df = df.dropna(how='all', axis=0)
        df = df[df['company_name'] != 'Grand Total']
        df['company_name'] = df['company_name'].apply(lambda x: x.encode('utf-8', 'ignore').decode('utf-8'))
        id_vars = ['company_name']
        value_vars = [col for col in df.columns if col not in id_vars]
        df_long = df.melt(id_vars=id_vars, var_name='Month', value_name='Permits')
        df_long = df_long.dropna(subset=['Month'])
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        df_long = df_long[df_long['Month'].isin(months)]
        df_long['Year'] = re.search(r'(\d{4})', file_path).group(1)
        df_long = df_long.dropna(subset=['Permits'])
        return df_long

    def load_and_standardize_excel_with_year(self, file_path):
        skiprows = self.determine_skiprows(file_path)
        df = pd.read_excel(file_path, skiprows=skiprows)

        employer_col = [col for col in df.columns if 'Employer' in str(col) or 'Name' in str(col)]
        if not employer_col:
            raise ValueError("Column 'Employer Name' not found")
        df.rename(columns={employer_col[0]: 'company_name'}, inplace=True)

        df = df.dropna(subset=['company_name'])
        df = df[df['company_name'] != 'Grand Total']
        df['company_name'] = df['company_name'].apply(lambda x: x.encode('utf-8', 'ignore').decode('utf-8'))

        permit_col = [col for col in df.columns if 'Total' in str(col) or 'Permit' in str(col)]
        if permit_col:
            df.rename(columns={permit_col[0]: 'Permits'}, inplace=True)
        else:
            df['Permits'] = pd.NA

        df = df[['company_name', 'Permits']]
        df['Month'] = ''
        df['Year'] = re.search(r'(\d{4})', file_path).group(1)
        df = df.dropna(subset=['Permits'])
        return df
    def process_files_nationalities(self):
        files = self.get_matching_files(self.download_folder, ["permits-by-nationality-"])
        logging.info(f"Found {len(files)} files to process.")

        combined_df = pd.DataFrame()

        for file_path in files:
            try:
                file_path = os.path.join(self.download_folder, file_path)
                year = os.path.basename(file_path).split('-')[-1].split('.')[0]
                logging.info(f"Processing file for year: {year} - {file_path}")

                df = pd.read_excel(file_path)

                if 'Nationality' in df.columns:
                    if all(col in df.columns for col in ['Nationality', 'New', 'Renewal', 'Refused', 'Withdrawn']):
                        df_filtered = df[['Nationality', 'New', 'Renewal', 'Refused', 'Withdrawn']].copy()
                        df_filtered = df_filtered.rename(columns={'New': 'Issued', 'Renewal': 'Renewal'})
                    else:
                        logging.warning(f"Skipping file {file_path} due to missing columns.")
                        continue
                elif 'Unnamed: 1' in df.columns and 'Nationality' in df.iloc[0].tolist():
                    df = pd.read_excel(file_path, skiprows=1)
                    if df.shape[1] >= 8:
                        df_filtered = df.iloc[:, [2, 3, 4, 6, 7]].copy()
                        df_filtered.columns = ['Nationality', 'Issued', 'Renewal', 'Refused', 'Withdrawn']
                    else:
                        logging.warning(f"Skipping file {file_path} due to missing columns.")
                        continue
                else:
                    if df.shape[1] >= 4:
                        df_filtered = df.rename(columns={df.columns[0]: 'Nationality',
                                                         df.columns[1]: 'Issued',
                                                         df.columns[2]: 'Refused',
                                                         df.columns[3]: 'Withdrawn'})
                    else:
                        logging.warning(f"Skipping file {file_path} due to missing columns.")
                        continue

                df_filtered = df_filtered[~df_filtered['Nationality'].isin(['Grand Total', 'Jan - Dec', ''])]
                df_filtered = df_filtered.dropna(subset=['Nationality'])

                df_filtered['Year'] = year

                combined_df = pd.concat([combined_df, df_filtered], ignore_index=True)
                logging.info(f"Successfully processed file for year: {year}")
            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")

        combined_long_df = pd.melt(combined_df, id_vars=['Year', 'Nationality'], var_name='Category',
                                   value_name='Count')

        combined_long_df = combined_long_df[pd.to_numeric(combined_long_df['Count'], errors='coerce').notnull()]

        combined_long_df = combined_long_df.dropna(subset=['Count'])

        output_file = os.path.join(self.output_folder, "permits-by-nationality.xlsx")
        combined_long_df.to_excel(output_file, index=False)
        logging.info(f"Combined data saved to {output_file}")

class ProcessSector:

    def __init__(self, base_dir, output_dir):
        self.base_dir = base_dir
        self.output_dir = output_dir
        os.makedirs(self.base_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

    def get_matching_files(self, directory, patterns):
        matching_files = []
        for file in os.listdir(directory):
            if any(file.startswith(pattern) and file.endswith('.xlsx') for pattern in patterns):
                matching_files.append(file)
        matching_files.sort()
        return matching_files
    def extract_year_from_filename(self, filename):
        match = re.search(r'\d{4}', filename)
        if match:
            return int(match.group())
        return None

    def process_lt_2020(self, data):
        if data.columns[0].startswith('Unnamed'):
            data.columns = data.iloc[0]
            data = data.drop(index=0)
            data.columns = ['Delete', 'Year', 'Month', 'Sector', 'New', 'Renewal', 'Total', 'Refused', 'Withdrawn']
            data.drop(columns=['Delete', 'Total'], inplace=True)
        else:
            data.rename(columns={'Unnamed: 2': 'Sector'}, inplace=True)
            data.drop(columns=['Total'], inplace=True)

        data['Year'].fillna(method='ffill', inplace=True)
        data['Month'].fillna(method='ffill', inplace=True)

        data_cleaned = data.dropna(subset=['Sector'])
        data_cleaned = data_cleaned[~data_cleaned['Sector'].str.contains("Jan - Dec", na=False)]

        return data_cleaned.melt(id_vars=['Year', 'Month', 'Sector'], var_name='Type', value_name='Count')

    def process_permits_gt_2020(self, data, year):
        data.rename(columns={'Unnamed: 1': 'Grand Total', 'Unnamed: 2': 'Jan', 'Unnamed: 3': 'Feb',
                             'Unnamed: 4': 'Mar', 'Unnamed: 5': 'Apr', 'Unnamed: 6': 'May', 'Unnamed: 7': 'Jun',
                             'Unnamed: 8': 'Jul', 'Unnamed: 9': 'Aug', 'Unnamed: 10': 'Sep', 'Unnamed: 11': 'Oct',
                             'Unnamed: 12': 'Nov', 'Unnamed: 13': 'Dec'}, inplace=True)

        data.drop(columns=['Grand Total'], inplace=True)

        data['Year'] = year
        data['Type'] = 'New'

        data['Sector'] = data['Economic Sector'].str.replace(r'^\w+\s-\s', '', regex=True)
        data.drop(columns=['Economic Sector'], inplace=True)

        data = data[~data['Sector'].str.contains("Grand Total", na=False)]

        data_long = data.melt(id_vars=['Year', 'Sector', 'Type'], var_name='Month', value_name='Count')
        return data_long

    def process_permits_gt_2022(self, data, year, file_path):
        available_months = pd.read_excel(file_path, nrows=1, skiprows=1).columns[2:]
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        used_months = [month for month in months if month in available_months]

        data.columns = ['Economic Sector', 'Grand Total'] + list(used_months)
        data.drop(columns=['Grand Total'], inplace=True)
        data['Year'] = year
        data['Type'] = 'New'

        data['Sector'] = data['Economic Sector'].str.replace(r'^\w+\s-\s', '', regex=True)
        data.drop(columns=['Economic Sector'], inplace=True)
        data = data[~data['Sector'].str.contains("Grand Total", na=False)]
        data_long = data.melt(id_vars=['Year', 'Sector', 'Type'], var_name='Month', value_name='Count')
        return data_long

    def process_permits_gt_2024(self, data, year, file_path):
        header = pd.read_excel(file_path, nrows=1, skiprows=1).columns
        columns = ['Economic Sector'] + header[1:].tolist()
        data.columns = columns

        data.drop(columns=['Grand Total'], inplace=True)
        data['Year'] = year
        data['Type'] = 'New'

        data['Sector'] = data['Economic Sector'].str.replace(r'^\w+\s-\s', '', regex=True)
        data.drop(columns=['Economic Sector'], inplace=True)
        data = data[~data['Sector'].str.contains("Grand Total", na=False)]

        data_long = data.melt(id_vars=['Year', 'Sector', 'Type'], var_name='Month', value_name='Count')
        data_long['Count'] = data_long['Count'].astype(str)
        data_long = data_long[~data_long['Count'].str.contains("Issued", na=False)]
        data_long['Count'] = pd.to_numeric(data_long['Count'], errors='coerce').fillna(0).astype(int)
        return data_long
    def process_permits_data_combined(self, file_path):
        file_path = os.path.join(self.base_dir, file_path)
        year = self.extract_year_from_filename(file_path)
        if year < 2020:
            data = pd.read_excel(file_path)
            return self.process_lt_2020(data=data)
        elif year >= 2024:
            data = pd.read_excel(file_path, skiprows=2)
            return self.process_permits_gt_2024(data=data, year=year, file_path=file_path)
        elif year >= 2022:
            data = pd.read_excel(file_path, skiprows=2)
            return self.process_permits_gt_2022(data=data, year=year, file_path=file_path)
        else:
            data = pd.read_excel(file_path, skiprows=1)
            return self.process_permits_gt_2020(data=data, year=year)

    def process(self):
        file_paths = self.get_matching_files(directory=self.base_dir, patterns=["permits-by-sector-"])

        all_data = pd.concat([self.process_permits_data_combined(path) for path in file_paths])

        all_data['Count'] = all_data['Count'].fillna(0)

        output_path = os.path.join(self.output_dir, 'permits by sector.xlsx')
        all_data.to_excel(output_path, index=False)

class GoogleSheetUploader:
    def __init__(self, transform_dir):
        self.logger = logging.getLogger(__name__)
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.transform_dir = transform_dir
        self.service_account_file = os.path.join(self.base_dir, 'credentials.json')
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets']
        self.service = self.authenticate_and_build_service()
        self.file_sheet_map = self.map_files_to_sheet_ids_and_columns()

    def authenticate_and_build_service(self):
        self.logger.info("Authenticating and building the Google Sheets service...")
        try:
            creds = service_account.Credentials.from_service_account_file(
                self.service_account_file, scopes=self.scopes)
            service = build('sheets', 'v4', credentials=creds)
            self.logger.info("Google Sheets service built successfully.")
            return service
        except Exception as e:
            self.logger.error(f"Error authenticating or building the Google Sheets service: {e}")
            raise

    def map_files_to_sheet_ids_and_columns(self):
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
        for file_name, settings in self.file_sheet_map.items():
            sheet_id = settings.get('sheet_id')
            if not sheet_id:
                self.logger.warning(f"No Sheet ID provided for {file_name}. Skipping upload.")
                continue

            column_order = settings.get('column_order', None)
            file_path = os.path.join(self.transform_dir, file_name)
            self.logger.info(f"Processing file: {file_name}")
            df = pd.read_excel(file_path)

            df.fillna('', inplace=True)

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

def main(download_folder="extract", output_folder="transform", date_file_path="/tmp/date_file.txt"):
    current_year = datetime.now().year
    base_url = "https://enterprise.gov.ie/en/publications/employment-permit-statistics-{}.html"
    start_url = "https://enterprise.gov.ie"
    start_year = 2009

    content_checker = ContentChangeChecker(base_url.format(current_year), date_file_path)
    if content_checker.check_for_change():
        logging.info("Content changed. Starting data processing and upload.")
        processor = EmploymentPermitDataProcessor(base_url, start_url, download_folder, output_folder, start_year)
        processor_sector = ProcessSector(download_folder, output_folder)
        uploader = GoogleSheetUploader(output_folder)

        processor.download_files()
        processor.process_files_companies()
        processor.process_files_nationalities()
        processor_sector.process()
        uploader.upload_data()
        logging.info("Data processing and upload completed.")
    else:
        logging.info("Content has not changed. Skipping data processing and upload.")

    # Start the Flask app after processing
    logging.info("Starting Flask application...")
    try:
        # Get the path to the virtual environment's bin directory
        venv_bin_path = os.path.join(sys.prefix, 'bin')
        gunicorn_path = os.path.join(venv_bin_path, 'gunicorn')

        # Use subprocess.Popen to run gunicorn in the background
        subprocess.Popen([gunicorn_path, "--bind", "0.0.0.0:4000", "app:app"])
        logging.info("Flask application started successfully.")
    except Exception as e:
        logging.error(f"Error starting Flask application: {e}")

if __name__ == "__main__":
    main()