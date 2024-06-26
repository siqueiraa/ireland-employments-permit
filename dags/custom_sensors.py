import requests
from bs4 import BeautifulSoup
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import re
import os

class ContentChangeSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, url, date_file_path, *args, **kwargs):
        super(ContentChangeSensor, self).__init__(*args, **kwargs)
        self.url = url
        self.date_file_path = date_file_path

    def poke(self, context):
        response = requests.get(self.url)
        soup = BeautifulSoup(response.text, 'html.parser')
        date_tag = soup.find('p', class_='article-date article-date--larger')
        if not date_tag:
            self.log.error("Date tag not found")
            return False

        current_date_text = date_tag.get_text(strip=True)
        # Extract the date part using regex
        match = re.search(r'\d{1,2}(?:st|nd|rd|th)\s\w+\s\d{4}', current_date_text)
        if not match:
            self.log.error("Date format not recognized")
            return False

        current_date = match.group(0)

        # Read the previous date from the file
        if os.path.exists(self.date_file_path):
            with open(self.date_file_path, 'r') as file:
                previous_date = file.read().strip()
        else:
            previous_date = None

        self.log.info(f"Current date: {current_date}")
        self.log.info(f"Previous date: {previous_date}")

        if current_date != previous_date:
            self.log.info("Content has changed.")
            # Update the file with the new date
            with open(self.date_file_path, 'w') as file:
                file.write(current_date)
            return True
        else:
            self.log.info("Content has not changed.")
            return False
