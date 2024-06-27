from flask import Flask, jsonify, send_file, abort
import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Endpoint to get the data from normalized.xlsx
@app.route('/data', methods=['GET'])
def get_data():
    file_path = os.path.join('transform', 'normalized.xlsx')
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        abort(404, description="Resource not found")

    try:
        df = pd.read_excel(file_path)
        data = df.to_dict(orient='records')
        data['Month'] = data['Month'].fillna('')

        logging.info(f"Data read successfully from {file_path}")
        return jsonify(data)
    except Exception as e:
        logging.error(f"Error reading the file: {e}")
        abort(500, description=f"Error reading the file: {str(e)}")


# Endpoint to download the normalized.xlsx file
@app.route('/download', methods=['GET'])
def download_file():
    file_path = os.path.join('transform', 'normalized.xlsx')
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        abort(404, description="Resource not found")

    try:
        logging.info(f"Sending file: {file_path}")
        return send_file(file_path, as_attachment=True)
    except Exception as e:
        logging.error(f"Error sending the file: {e}")
        abort(500, description=f"Error sending the file: {str(e)}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000, debug=True)

