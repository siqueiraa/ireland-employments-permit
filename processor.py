import os
import re
import requests
import pandas as pd
import logging
import warnings

from bs4 import BeautifulSoup
from datetime import datetime

from process_sector import ProcessSector
warnings.filterwarnings("ignore")
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
                df_long.reset_index(drop=True, inplace=True)  # Ensure unique indices
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

        # Initialize an empty DataFrame to store combined data
        combined_df = pd.DataFrame()

        # Process each file with consideration of their structure
        for file_path in files:
            try:
                file_path = os.path.join(self.download_folder, file_path)
                # Extract year from file name
                year = os.path.basename(file_path).split('-')[-1].split('.')[0]
                logging.info(f"Processing file for year: {year} - {file_path}")

                # Read the Excel file
                df = pd.read_excel(file_path)

                if 'Nationality' in df.columns:
                    # Handle 2018 structure
                    if all(col in df.columns for col in ['Nationality', 'New', 'Renewal', 'Refused', 'Withdrawn']):
                        df_filtered = df[['Nationality', 'New', 'Renewal', 'Refused', 'Withdrawn']].copy()
                        df_filtered = df_filtered.rename(columns={'New': 'Issued', 'Renewal': 'Renewal'})
                    else:
                        logging.warning(f"Skipping file {file_path} due to missing columns.")
                        continue  # Skip the file if required columns are not present
                elif 'Unnamed: 1' in df.columns and 'Nationality' in df.iloc[0].tolist():
                    # Handle 2019 structure
                    df = pd.read_excel(file_path, skiprows=1)
                    if df.shape[1] >= 8:
                        df_filtered = df.iloc[:, [2, 3, 4, 6, 7]].copy()
                        df_filtered.columns = ['Nationality', 'Issued', 'Renewal', 'Refused', 'Withdrawn']
                    else:
                        logging.warning(f"Skipping file {file_path} due to missing columns.")
                        continue  # Skip the file if required columns are not present
                else:
                    # Handle 2020-2024 structure
                    if df.shape[1] >= 4:
                        df_filtered = df.rename(columns={df.columns[0]: 'Nationality',
                                                         df.columns[1]: 'Issued',
                                                         df.columns[2]: 'Refused',
                                                         df.columns[3]: 'Withdrawn'})
                    else:
                        logging.warning(f"Skipping file {file_path} due to missing columns.")
                        continue  # Skip the file if required columns are not present

                # Drop rows where 'Nationality' is 'Grand Total' or 'Jan - Dec' or is null
                df_filtered = df_filtered[~df_filtered['Nationality'].isin(['Grand Total', 'Jan - Dec', ''])]
                df_filtered = df_filtered.dropna(subset=['Nationality'])

                # Add the year column
                df_filtered['Year'] = year

                # Combine the data into the main DataFrame
                combined_df = pd.concat([combined_df, df_filtered], ignore_index=True)
                logging.info(f"Successfully processed file for year: {year}")
            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")

        # Reshape the DataFrame to long format
        combined_long_df = pd.melt(combined_df, id_vars=['Year', 'Nationality'], var_name='Category',
                                   value_name='Count')

        # Ensure only numeric values are in 'Count' and remove non-numeric rows
        combined_long_df = combined_long_df[pd.to_numeric(combined_long_df['Count'], errors='coerce').notnull()]

        # Remove rows with empty 'Count'
        combined_long_df = combined_long_df.dropna(subset=['Count'])

        # Save the combined DataFrame to an Excel file
        output_file = os.path.join(self.output_folder, "permits-by-nationality.xlsx")
        combined_long_df.to_excel(output_file, index=False)
        logging.info(f"Combined data saved to {output_file}")


if __name__ == "__main__":
    base_url = "https://enterprise.gov.ie/en/publications/employment-permit-statistics-{}.html"
    start_url = "https://enterprise.gov.ie"
    download_folder = "extract"
    output_folder = "transform"
    start_year = 2009

    processor = EmploymentPermitDataProcessor(base_url, start_url, download_folder, output_folder, start_year)
    processor_sector = ProcessSector(download_folder, output_folder)
    processor.download_files()
    processor.process_files_companies()
    processor.process_files_nationalities()
    processor_sector.process()
