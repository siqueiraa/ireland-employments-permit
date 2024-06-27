import os
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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

                        # Skip downloading if the file already exists, except for the current year
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

    def process_files(self):
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
                df_long.reset_index(drop=True, inplace=True)  # Ensure unique indices
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

            normalized_file_name = f'{self.output_folder}/normalized.xlsx'
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
            df.rename(columns={employer_col[0]: 'Employer Name'}, inplace=True)
        if df.iloc[0].astype(str).str.contains('Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec').any():
            df.columns = ['Employer Name'] + df.iloc[0, 1:].tolist()
            df = df[1:]
        df.columns = df.columns.astype(str)
        df = df.loc[:, ~df.columns.str.contains('Unnamed')]
        df = df.dropna(axis=1, how='all')
        df = df.dropna(subset=['Employer Name'])
        df = df.dropna(how='all', axis=0)
        df = df[df['Employer Name'] != 'Grand Total']
        id_vars = ['Employer Name']
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
        df.rename(columns={employer_col[0]: 'Employer Name'}, inplace=True)

        df = df.dropna(subset=['Employer Name'])
        df = df[df['Employer Name'] != 'Grand Total']

        permit_col = [col for col in df.columns if 'Total' in str(col) or 'Permit' in str(col)]
        if permit_col:
            df.rename(columns={permit_col[0]: 'Permits'}, inplace=True)
        else:
            df['Permits'] = pd.NA

        df = df[['Employer Name', 'Permits']]
        df['Month'] = ''
        df['Year'] = re.search(r'(\d{4})', file_path).group(1)
        df = df.dropna(subset=['Permits'])
        return df


if __name__ == "__main__":
    base_url = "https://enterprise.gov.ie/en/publications/employment-permit-statistics-{}.html"
    start_url = "https://enterprise.gov.ie"
    download_folder = "extract"
    output_folder = "transform"
    start_year = 2009

    processor = EmploymentPermitDataProcessor(base_url, start_url, download_folder, output_folder, start_year)
    processor.download_files()
    processor.process_files()
