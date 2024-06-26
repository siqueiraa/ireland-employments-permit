# Ireland Employment Permit Data Processor

This project consists of two main components: data processing and a web API. The data processor downloads and processes employment permit statistics from an oficial source, while the web API serves the processed data.

## Table of Contents
- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Setup](#setup)
- [Usage](#usage)
- [Endpoints](#endpoints)

## Project Structure
- **extract/**: Folder where raw data files will be downloaded.
- **transform/**: Folder where the processed data file `normalized.xlsx` will be saved.
- **processor.py**: Script to download and process the data.
- **app.py**: Flask application to serve the processed data.
- **requirements.txt**: File listing the Python dependencies.

## Requirements
The project requires the following Python packages:
- `pandas`
- `beautifulsoup4`
- `requests`
- `flask`

You can install these dependencies using the following command:
```sh
pip install -r requirements.txt
