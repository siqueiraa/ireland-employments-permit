from flask import Flask, jsonify, send_file, abort
import pandas as pd
import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Data Loading at Startup ---
DATA_CACHE = []
FILE_PATH = os.path.join('transform', 'permits_company.xlsx')

def load_data():
    """Loads and processes data from the Excel file into a cache."""
    if not os.path.exists(FILE_PATH):
        logging.error(f"Fatal: Data file not found at {FILE_PATH}. Application cannot start.")
        sys.exit(1)  # Exit if the file is essential for the app to run
    try:
        df = pd.read_excel(FILE_PATH)
        df['Month'] = df['Month'].fillna('')
        logging.info(f"Data successfully loaded from {FILE_PATH} into cache.")
        return df.to_dict(orient='records')
    except Exception as e:
        logging.error(f"Fatal: Error loading data file {FILE_PATH}: {e}. Application cannot start.")
        sys.exit(1)

# Load data into the cache when the application starts
DATA_CACHE = load_data()
# --- End Data Loading ---

app = Flask(__name__)

# Endpoint to get the data from the in-memory cache
@app.route('/data', methods=['GET'])
def get_data():
    """Serves the cached data."""
    logging.info("Serving data from in-memory cache.")
    return jsonify(DATA_CACHE)


# Endpoint to download the normalized.xlsx file
@app.route('/download', methods=['GET'])
def download_file():
    """Provides the source Excel file for download."""
    if not os.path.exists(FILE_PATH):
        logging.error(f"File not found: {FILE_PATH}")
        abort(404, description="Resource not found")

    try:
        logging.info(f"Sending file for download: {FILE_PATH}")
        return send_file(FILE_PATH, as_attachment=True)
    except Exception as e:
        logging.error(f"Error sending the file: {e}")
        abort(500, description=f"Error sending the file: {str(e)}")

if __name__ == '__main__':
    # This block is for direct execution (e.g., local development)
    # Gunicorn will not use this part.
    app.run(host='0.0.0.0', port=4000, debug=True)

