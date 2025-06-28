import pandas as pd
import os
import re


class ProcessSector:

    def __init__(self, base_dir, output_dir):
        """Initialize directories and ensure they exist."""
        self.base_dir = base_dir
        self.output_dir = output_dir
        os.makedirs(self.base_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

    def get_matching_files(self, directory, patterns):
        """Get list of files matching specific patterns."""
        matching_files = []
        for file in os.listdir(directory):
            if any(file.startswith(pattern) and file.endswith('.xlsx') for pattern in patterns):
                matching_files.append(file)
        matching_files.sort()
        return matching_files
    def extract_year_from_filename(self, filename):
        """Extract year from filename using regex."""
        match = re.search(r'\d{4}', filename)
        if match:
            return int(match.group())
        return None

    def process_lt_2020(self, data):
        """Process data for years less than 2020, adjusting headers and formatting."""
        if data.columns[0].startswith('Unnamed'):
            data.columns = data.iloc[0]  # Configura a primeira linha como cabeçalho
            data = data.drop(index=0)
            # Renomear colunas para padrão
            data.columns = ['Delete', 'Year', 'Month', 'Sector', 'New', 'Renewal', 'Total', 'Refused', 'Withdrawn']
            data.drop(columns=['Delete', 'Total'], inplace=True)
        else:
            # Renomear diretamente se as colunas já estiverem corretas
            data.rename(columns={'Unnamed: 2': 'Sector'}, inplace=True)
            data.drop(columns=['Total'], inplace=True)

        # Preencher valores de 'Year' e 'Month'
        data['Year'].fillna(method='ffill', inplace=True)
        data['Month'].fillna(method='ffill', inplace=True)

        # Remover linhas onde 'Sector' está em branco ou é um totalizador anual
        data_cleaned = data.dropna(subset=['Sector'])
        data_cleaned = data_cleaned[~data_cleaned['Sector'].str.contains("Jan - Dec", na=False)]

        # Transformar os dados para o formato longo
        return data_cleaned.melt(id_vars=['Year', 'Month', 'Sector'], var_name='Type', value_name='Count')


    def process_permits_gt_2020(self, data, year):
        """Process data for years greater than 2020, focusing on new permits."""
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
        """Process data for years greater than 2022, using specific month columns."""
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
        """Combine processing steps by extracting the year and choosing the appropriate method."""
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
        """Main processing function to loop through all matching files and process data."""
        file_paths = self.get_matching_files(directory=self.base_dir, patterns=["permits-by-sector-"])

        all_data = pd.concat([self.process_permits_data_combined(path) for path in file_paths])

        all_data['Count'] = all_data['Count'].fillna(0)

        output_path = os.path.join(self.output_dir, 'permits by sector.xlsx')
        all_data.to_excel(output_path, index=False)


