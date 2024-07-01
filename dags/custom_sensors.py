import requests
from bs4 import BeautifulSoup
from airflow.sensors.base import BaseSensorOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

class ContentChangeSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, url, date_file_path, *args, **kwargs):
        super(ContentChangeSensor, self).__init__(*args, **kwargs)
        self.url = url
        self.date_file_path = date_file_path

    def poke(self, context):
        self.log.info("Starting the poke method")

        try:
            self.log.info(f"Sending request to URL: {self.url}")
            try:
                response = requests.get(self.url)
                response.raise_for_status()  # This will raise an HTTPError for bad responses
                self.log.info("Request successful")
            except requests.exceptions.RequestException as e:
                self.log.error(f"Request to {self.url} failed: {e}")
                return False

            self.log.info("Parsing the response content with BeautifulSoup")
            soup = BeautifulSoup(response.text, 'html.parser')
            date_tag = soup.find('p', class_='article-date article-date--larger')
            if not date_tag:
                self.log.error("Date tag not found in the response content")
                return False

            self.log.info(f"Raw date tag content: {date_tag.get_text()}")
            current_date = date_tag.get_text(strip=True).split('|')[0].strip()
            self.log.info(f"Processed current date: {current_date}")

            self.log.info(f"Checking if date file exists at path: {self.date_file_path}")
            try:
                with open(self.date_file_path, 'r') as file:
                    previous_date = file.read().strip()
                    self.log.info(f"Read previous date from file: {previous_date}")
            except FileNotFoundError:
                self.log.warning(f"Date file not found at {self.date_file_path}. Assuming first run.")
                previous_date = None

            self.log.info(f"Previous date: {previous_date}")

            if current_date != previous_date:
                self.log.info("Content has changed. Updating the date file.")
                try:
                    with open(self.date_file_path, 'w') as file:
                        file.write(current_date)
                        self.log.info(f"Updated date file with current date: {current_date}")
                except IOError as e:
                    self.log.error(f"Failed to write to date file: {e}")
                    return False
                context['ti'].xcom_push(key='content_changed', value=True)
                return True
            else:
                self.log.info("Content has not changed.")
                context['ti'].xcom_push(key='content_changed', value=False)
                return True
        except AirflowException as e:
            self.log.error(f"{repr(e)} - ChangeSensor failed: possibly because the sensor condition not met. "
                           "Will retry...")
            return False
        except Exception as e:
            self.log.error(f"Unexpected error: {e}")
            return False
