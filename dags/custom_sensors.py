import requests
from bs4 import BeautifulSoup
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import os

class ContentChangeSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, url, date_file_path, *args, **kwargs):
        super(ContentChangeSensor, self).__init__(*args, **kwargs)
        self.url = url
        self.date_file_path = date_file_path

    def read_previous_date(self):
        if os.path.exists(self.date_file_path):
            with open(self.date_file_path, 'r') as file:
                return file.read().strip()
        return None

    def write_current_date(self, date):
        with open(self.date_file_path, 'w') as file:
            file.write(date)

    def poke(self, context):
        response = requests.get(self.url)
        soup = BeautifulSoup(response.text, 'html.parser')
        date_tag = soup.find('p', class_='article-date article-date--larger')
        if not date_tag:
            self.log.error("Date tag not found")
            return False

        current_date = date_tag.get_text(strip=True)
        previous_date = self.read_previous_date()

        self.log.info(f"Current date: {current_date}")
        self.log.info(f"Previous date: {previous_date}")

        if current_date != previous_date:
            self.log.info("Content has changed.")
            self.write_current_date(current_date)
            return True
        else:
            self.log.info("Content has not changed.")
            return False
