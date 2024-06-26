import requests
from bs4 import BeautifulSoup
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

class ContentChangeSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, url, previous_date, *args, **kwargs):
        super(ContentChangeSensor, self).__init__(*args, **kwargs)
        self.url = url
        self.previous_date = previous_date

    def poke(self, context):
        response = requests.get(self.url)
        soup = BeautifulSoup(response.text, 'html.parser')
        date_tag = soup.find('p', class_='article-date article-date--larger')
        if not date_tag:
            self.log.error("Date tag not found")
            return False

        current_date = date_tag.get_text(strip=True)
        self.log.info(f"Current date: {current_date}")
        self.log.info(f"Previous date: {self.previous_date}")

        if current_date != self.previous_date:
            self.log.info("Content has changed.")
            return True
        else:
            self.log.info("Content has not changed.")
            return False
